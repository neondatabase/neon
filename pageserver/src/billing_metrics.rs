//!
//! Periodically collect consumption metrics for all active tenants
//! and push them to a HTTP endpoint.
//! Cache metrics to send only the updated ones.
//!

use anyhow;
use tracing::*;
use utils::id::TimelineId;

use crate::task_mgr;
use crate::tenant_mgr;
use pageserver_api::models::TenantState;
use utils::id::TenantId;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::Url;

/// BillingMetric struct that defines the format for one metric entry
/// i.e.
///
/// ```json
/// {
/// "metric": "remote_storage_size",
/// "type": "absolute",
/// "tenant_id": "5d07d9ce9237c4cd845ea7918c0afa7d",
/// "timeline_id": "00000000000000000000000000000000",
/// "time": ...,
/// "value": 12345454,
/// }
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct BillingMetric {
    pub metric: BillingMetricKind,
    pub metric_type: &'static str,
    pub tenant_id: TenantId,
    pub timeline_id: Option<TimelineId>,
    pub time: DateTime<Utc>,
    pub value: u64,
}

impl BillingMetric {
    pub fn new_absolute(
        metric: BillingMetricKind,
        tenant_id: TenantId,
        timeline_id: Option<TimelineId>,
        value: u64,
    ) -> Self {
        Self {
            metric,
            metric_type: "absolute",
            tenant_id,
            timeline_id,
            time: Utc::now(),
            value,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BillingMetricKind {
    /// Amount of WAL produced , by a timeline, i.e. last_record_lsn
    /// This is an absolute, per-timeline metric.
    WrittenSize,
    /// Size of all tenant branches including WAL
    /// This is an absolute, per-tenant metric.
    /// This is the same metric that tenant/tenant_id/size endpoint returns.
    SyntheticStorageSize,
    /// Size of all the layer files in the tenant's directory on disk on the pageserver.
    /// This is an absolute, per-tenant metric.
    /// See also prometheus metric RESIDENT_PHYSICAL_SIZE.
    ResidentSize,
    /// Size of the remote storage (S3) directory.
    /// This is an absolute, per-tenant metric.
    RemoteStorageSize,
}

impl FromStr for BillingMetricKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "written_size" => Ok(Self::WrittenSize),
            "synthetic_storage_size" => Ok(Self::SyntheticStorageSize),
            "resident_size" => Ok(Self::ResidentSize),
            "remote_storage_size" => Ok(Self::RemoteStorageSize),
            _ => anyhow::bail!("invalid value \"{s}\" for metric type"),
        }
    }
}

impl fmt::Display for BillingMetricKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            BillingMetricKind::WrittenSize => "written_size",
            BillingMetricKind::SyntheticStorageSize => "synthetic_storage_size",
            BillingMetricKind::ResidentSize => "resident_size",
            BillingMetricKind::RemoteStorageSize => "remote_storage_size",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BillingMetricsKey {
    tenant_id: TenantId,
    timeline_id: Option<TimelineId>,
    metric: BillingMetricKind,
}

#[derive(serde::Serialize)]
struct EventChunk<'a> {
    events: &'a [BillingMetric],
}

/// Main thread that serves metrics collection
pub async fn collect_metrics(
    metric_collection_endpoint: &Url,
    metric_collection_interval: Duration,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(metric_collection_interval);

    info!("starting collect_metrics");

    // define client here to reuse it for all requests
    let client = reqwest::Client::new();
    let mut cached_metrics: HashMap<BillingMetricsKey, u64> = HashMap::new();

    loop {
        tokio::select! {
            _ = task_mgr::shutdown_watcher() => {
                info!("collect_metrics received cancellation request");
                return Ok(());
            },
            _ = ticker.tick() => {
                collect_metrics_task(&client, &mut cached_metrics, metric_collection_endpoint).await?;
            }
        }
    }
}

/// One iteration of metrics collection
///
/// Gather per-tenant and per-timeline metrics and send them to the `metric_collection_endpoint`.
/// Cache metrics to avoid sending the same metrics multiple times.
pub async fn collect_metrics_task(
    client: &reqwest::Client,
    cached_metrics: &mut HashMap<BillingMetricsKey, u64>,
    metric_collection_endpoint: &reqwest::Url,
) -> anyhow::Result<()> {
    let mut current_metrics: Vec<(BillingMetricsKey, u64)> = Vec::new();
    trace!(
        "starting collect_metrics_task. metric_collection_endpoint: {}",
        metric_collection_endpoint
    );

    // get list of tenants
    let tenants = tenant_mgr::list_tenants().await;

    // iterate through list of Active tenants and collect metrics
    for (tenant_id, tenant_state) in tenants {
        if tenant_state != TenantState::Active {
            continue;
        }

        let tenant = tenant_mgr::get_tenant(tenant_id, true).await?;

        let mut tenant_resident_size = 0;

        // iterate through list of timelines in tenant
        for timeline in tenant.list_timelines().iter() {
            let timeline_written_size = u64::from(timeline.get_last_record_lsn());

            current_metrics.push((
                BillingMetricsKey {
                    tenant_id,
                    timeline_id: Some(timeline.timeline_id),
                    metric: BillingMetricKind::WrittenSize,
                },
                timeline_written_size,
            ));

            let timeline_resident_size = timeline.get_resident_physical_size();
            tenant_resident_size += timeline_resident_size;

            debug!(
                "per-timeline current metrics for tenant: {}: timeline {} resident_size={} last_record_lsn {} (as bytes)",
                tenant_id, timeline.timeline_id, timeline_resident_size, timeline_written_size)
        }

        let tenant_remote_size = tenant.get_remote_size().await?;
        debug!(
            "collected current metrics for tenant: {}: state={:?} resident_size={} remote_size={}",
            tenant_id, tenant_state, tenant_resident_size, tenant_remote_size
        );

        current_metrics.push((
            BillingMetricsKey {
                tenant_id,
                timeline_id: None,
                metric: BillingMetricKind::ResidentSize,
            },
            tenant_resident_size,
        ));

        current_metrics.push((
            BillingMetricsKey {
                tenant_id,
                timeline_id: None,
                metric: BillingMetricKind::RemoteStorageSize,
            },
            tenant_remote_size,
        ));

        // TODO add SyntheticStorageSize metric
    }

    // Filter metrics
    current_metrics.retain(|(curr_key, curr_val)| match cached_metrics.get(curr_key) {
        Some(val) => val != curr_val,
        None => true,
    });

    if current_metrics.is_empty() {
        trace!("no new metrics to send");
        return Ok(());
    }

    // Send metrics.
    // Split into chunks of 1000 metrics to avoid exceeding the max request size
    const CHUNK_SIZE: usize = 1000;
    let chunks = current_metrics.chunks(CHUNK_SIZE);

    let mut chunk_to_send: Vec<BillingMetric> = Vec::with_capacity(1000);

    for chunk in chunks {
        chunk_to_send.clear();
        // enrich metrics with timestamp and metric_kind before sending
        chunk_to_send.extend(chunk.iter().map(|(curr_key, curr_val)| {
            BillingMetric::new_absolute(
                curr_key.metric,
                curr_key.tenant_id,
                curr_key.timeline_id,
                *curr_val,
            )
        }));

        let chunk_json = serde_json::value::to_raw_value(&EventChunk {
            events: &chunk_to_send,
        })
        .expect("BillingMetric should not fail serialization");

        let res = client
            .post(metric_collection_endpoint.clone())
            .json(&chunk_json)
            .send()
            .await;

        match res {
            Ok(res) => {
                if res.status().is_success() {
                    // update cached metrics after they were sent successfully
                    for (curr_key, curr_val) in chunk.iter() {
                        cached_metrics.insert(curr_key.clone(), *curr_val);
                    }
                } else {
                    error!("metrics endpoint refused the sent metrics: {:?}", res);
                }
            }
            Err(err) => {
                error!("failed to send metrics: {:?}", err);
            }
        }
    }

    Ok(())
}
