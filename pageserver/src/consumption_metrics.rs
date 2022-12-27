//!
//! Periodically collect consumption metrics for all active tenants
//! and push them to a HTTP endpoint.
//! Cache metrics to send only the updated ones.
//!

use anyhow;
use tracing::*;
use utils::id::NodeId;
use utils::id::TimelineId;

use crate::task_mgr;
use crate::tenant::mgr;
use pageserver_api::models::TenantState;
use utils::id::TenantId;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rand::Rng;
use reqwest::Url;

/// ConsumptionMetric struct that defines the format for one metric entry
/// i.e.
///
/// ```json
/// {
/// "metric": "remote_storage_size",
/// "type": "absolute",
/// "tenant_id": "5d07d9ce9237c4cd845ea7918c0afa7d",
/// "timeline_id": "a03ebb4f5922a1c56ff7485cc8854143",
/// "time": "2022-12-28T11:07:19.317310284Z",
/// "idempotency_key": "2022-12-28 11:07:19.317310324 UTC-1-4019",
/// "value": 12345454,
/// }
/// ```
#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ConsumptionMetric {
    pub metric: ConsumptionMetricKind,
    #[serde(rename = "type")]
    pub metric_type: &'static str,
    #[serde_as(as = "DisplayFromStr")]
    pub tenant_id: TenantId,
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeline_id: Option<TimelineId>,
    pub time: DateTime<Utc>,
    pub idempotency_key: String,
    pub value: u64,
}

impl ConsumptionMetric {
    pub fn new_absolute<R: Rng + ?Sized>(
        metric: ConsumptionMetricKind,
        tenant_id: TenantId,
        timeline_id: Option<TimelineId>,
        value: u64,
        node_id: NodeId,
        rng: &mut R,
    ) -> Self {
        Self {
            metric,
            metric_type: "absolute",
            tenant_id,
            timeline_id,
            time: Utc::now(),
            // key that allows metric collector to distinguish unique events
            idempotency_key: format!("{}-{}-{:04}", Utc::now(), node_id, rng.gen_range(0..=9999)),
            value,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumptionMetricKind {
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
    /// Logical size of the data in the timeline
    /// This is an absolute, per-timeline metric
    TimelineLogicalSize,
}

impl FromStr for ConsumptionMetricKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "written_size" => Ok(Self::WrittenSize),
            "synthetic_storage_size" => Ok(Self::SyntheticStorageSize),
            "resident_size" => Ok(Self::ResidentSize),
            "remote_storage_size" => Ok(Self::RemoteStorageSize),
            "timeline_logical_size" => Ok(Self::TimelineLogicalSize),
            _ => anyhow::bail!("invalid value \"{s}\" for metric type"),
        }
    }
}

impl fmt::Display for ConsumptionMetricKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ConsumptionMetricKind::WrittenSize => "written_size",
            ConsumptionMetricKind::SyntheticStorageSize => "synthetic_storage_size",
            ConsumptionMetricKind::ResidentSize => "resident_size",
            ConsumptionMetricKind::RemoteStorageSize => "remote_storage_size",
            ConsumptionMetricKind::TimelineLogicalSize => "timeline_logical_size",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConsumptionMetricsKey {
    tenant_id: TenantId,
    timeline_id: Option<TimelineId>,
    metric: ConsumptionMetricKind,
}

#[derive(serde::Serialize)]
struct EventChunk<'a> {
    events: &'a [ConsumptionMetric],
}

/// Main thread that serves metrics collection
pub async fn collect_metrics(
    metric_collection_endpoint: &Url,
    metric_collection_interval: Duration,
    node_id: NodeId,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(metric_collection_interval);

    info!("starting collect_metrics");

    // define client here to reuse it for all requests
    let client = reqwest::Client::new();
    let mut cached_metrics: HashMap<ConsumptionMetricsKey, u64> = HashMap::new();

    loop {
        tokio::select! {
            _ = task_mgr::shutdown_watcher() => {
                info!("collect_metrics received cancellation request");
                return Ok(());
            },
            _ = ticker.tick() => {
                collect_metrics_task(&client, &mut cached_metrics, metric_collection_endpoint, node_id).await?;
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
    cached_metrics: &mut HashMap<ConsumptionMetricsKey, u64>,
    metric_collection_endpoint: &reqwest::Url,
    node_id: NodeId,
) -> anyhow::Result<()> {
    let mut current_metrics: Vec<(ConsumptionMetricsKey, u64)> = Vec::new();
    trace!(
        "starting collect_metrics_task. metric_collection_endpoint: {}",
        metric_collection_endpoint
    );

    // get list of tenants
    let tenants = mgr::list_tenants().await;

    // iterate through list of Active tenants and collect metrics
    for (tenant_id, tenant_state) in tenants {
        if tenant_state != TenantState::Active {
            continue;
        }

        let tenant = mgr::get_tenant(tenant_id, true).await?;

        let mut tenant_resident_size = 0;

        // iterate through list of timelines in tenant
        for timeline in tenant.list_timelines().iter() {
            // collect per-timeline metrics only for active timelines
            if timeline.is_active() {
                let timeline_written_size = u64::from(timeline.get_last_record_lsn());

                current_metrics.push((
                    ConsumptionMetricsKey {
                        tenant_id,
                        timeline_id: Some(timeline.timeline_id),
                        metric: ConsumptionMetricKind::WrittenSize,
                    },
                    timeline_written_size,
                ));

                let (timeline_logical_size, is_exact) = timeline.get_current_logical_size()?;
                // Only send timeline logical size when it is fully calculated.
                if is_exact {
                    current_metrics.push((
                        ConsumptionMetricsKey {
                            tenant_id,
                            timeline_id: Some(timeline.timeline_id),
                            metric: ConsumptionMetricKind::TimelineLogicalSize,
                        },
                        timeline_logical_size,
                    ));
                }
            }

            let timeline_resident_size = timeline.get_resident_physical_size();
            tenant_resident_size += timeline_resident_size;
        }

        let tenant_remote_size = tenant.get_remote_size().await?;
        debug!(
            "collected current metrics for tenant: {}: state={:?} resident_size={} remote_size={}",
            tenant_id, tenant_state, tenant_resident_size, tenant_remote_size
        );

        current_metrics.push((
            ConsumptionMetricsKey {
                tenant_id,
                timeline_id: None,
                metric: ConsumptionMetricKind::ResidentSize,
            },
            tenant_resident_size,
        ));

        current_metrics.push((
            ConsumptionMetricsKey {
                tenant_id,
                timeline_id: None,
                metric: ConsumptionMetricKind::RemoteStorageSize,
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

    let mut chunk_to_send: Vec<ConsumptionMetric> = Vec::with_capacity(1000);

    for chunk in chunks {
        chunk_to_send.clear();

        // this code block is needed to convince compiler
        // that rng is not reused aroung await point
        {
            // enrich metrics with timestamp and metric_kind before sending
            let mut rng = rand::thread_rng();
            chunk_to_send.extend(chunk.iter().map(|(curr_key, curr_val)| {
                ConsumptionMetric::new_absolute(
                    curr_key.metric,
                    curr_key.tenant_id,
                    curr_key.timeline_id,
                    *curr_val,
                    node_id,
                    &mut rng,
                )
            }));
        }

        let chunk_json = serde_json::value::to_raw_value(&EventChunk {
            events: &chunk_to_send,
        })
        .expect("ConsumptionMetric should not fail serialization");

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
