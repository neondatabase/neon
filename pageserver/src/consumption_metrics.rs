//!
//! Periodically collect consumption metrics for all active tenants
//! and push them to a HTTP endpoint.
//! Cache metrics to send only the updated ones.
//!
use crate::context::{DownloadBehavior, RequestContext};
use crate::task_mgr::{self, TaskKind, BACKGROUND_RUNTIME};
use crate::tenant::mgr;
use anyhow;
use chrono::Utc;
use consumption_metrics::{idempotency_key, Event, EventChunk, EventType, CHUNK_SIZE};
use pageserver_api::models::TenantState;
use reqwest::Url;
use serde::Serialize;
use serde_with::{serde_as, DisplayFromStr};
use std::collections::HashMap;
use std::time::Duration;
use tracing::*;
use utils::id::{NodeId, TenantId, TimelineId};

const WRITTEN_SIZE: &str = "written_size";
const SYNTHETIC_STORAGE_SIZE: &str = "synthetic_storage_size";
const RESIDENT_SIZE: &str = "resident_size";
const REMOTE_STORAGE_SIZE: &str = "remote_storage_size";
const TIMELINE_LOGICAL_SIZE: &str = "timeline_logical_size";

#[serde_as]
#[derive(Serialize)]
struct Ids {
    #[serde_as(as = "DisplayFromStr")]
    tenant_id: TenantId,
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    timeline_id: Option<TimelineId>,
}

/// Key that uniquely identifies the object, this metric describes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PageserverConsumptionMetricsKey {
    pub tenant_id: TenantId,
    pub timeline_id: Option<TimelineId>,
    pub metric: &'static str,
}

/// Main thread that serves metrics collection
pub async fn collect_metrics(
    metric_collection_endpoint: &Url,
    metric_collection_interval: Duration,
    synthetic_size_calculation_interval: Duration,
    node_id: NodeId,
    ctx: RequestContext,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(metric_collection_interval);

    info!("starting collect_metrics");

    // spin up background worker that caclulates tenant sizes
    let worker_ctx =
        ctx.detached_child(TaskKind::CalculateSyntheticSize, DownloadBehavior::Download);
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::CalculateSyntheticSize,
        None,
        None,
        "synthetic size calculation",
        false,
        async move {
            calculate_synthetic_size_worker(synthetic_size_calculation_interval, &worker_ctx)
                .instrument(info_span!("synthetic_size_worker"))
                .await?;
            Ok(())
        },
    );

    // define client here to reuse it for all requests
    let client = reqwest::Client::new();
    let mut cached_metrics: HashMap<PageserverConsumptionMetricsKey, u64> = HashMap::new();

    loop {
        tokio::select! {
            _ = task_mgr::shutdown_watcher() => {
                info!("collect_metrics received cancellation request");
                return Ok(());
            },
            _ = ticker.tick() => {
                if let Err(err) = collect_metrics_iteration(&client, &mut cached_metrics, metric_collection_endpoint, node_id, &ctx).await
                {
                    error!("metrics collection failed: {err:?}");
                }
            }
        }
    }
}

/// One iteration of metrics collection
///
/// Gather per-tenant and per-timeline metrics and send them to the `metric_collection_endpoint`.
/// Cache metrics to avoid sending the same metrics multiple times.
///
/// TODO
/// - refactor this function (chunking+sending part) to reuse it in proxy module;
/// - improve error handling. Now if one tenant fails to collect metrics,
/// the whole iteration fails and metrics for other tenants are not collected.
pub async fn collect_metrics_iteration(
    client: &reqwest::Client,
    cached_metrics: &mut HashMap<PageserverConsumptionMetricsKey, u64>,
    metric_collection_endpoint: &reqwest::Url,
    node_id: NodeId,
    ctx: &RequestContext,
) -> anyhow::Result<()> {
    let mut current_metrics: Vec<(PageserverConsumptionMetricsKey, u64)> = Vec::new();
    trace!(
        "starting collect_metrics_iteration. metric_collection_endpoint: {}",
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
                    PageserverConsumptionMetricsKey {
                        tenant_id,
                        timeline_id: Some(timeline.timeline_id),
                        metric: WRITTEN_SIZE,
                    },
                    timeline_written_size,
                ));

                let (timeline_logical_size, is_exact) = timeline.get_current_logical_size(ctx)?;
                // Only send timeline logical size when it is fully calculated.
                if is_exact {
                    current_metrics.push((
                        PageserverConsumptionMetricsKey {
                            tenant_id,
                            timeline_id: Some(timeline.timeline_id),
                            metric: TIMELINE_LOGICAL_SIZE,
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
            PageserverConsumptionMetricsKey {
                tenant_id,
                timeline_id: None,
                metric: RESIDENT_SIZE,
            },
            tenant_resident_size,
        ));

        current_metrics.push((
            PageserverConsumptionMetricsKey {
                tenant_id,
                timeline_id: None,
                metric: REMOTE_STORAGE_SIZE,
            },
            tenant_remote_size,
        ));

        // Note that this metric is calculated in a separate bgworker
        // Here we only use cached value, which may lag behind the real latest one
        let tenant_synthetic_size = tenant.get_cached_synthetic_size();
        current_metrics.push((
            PageserverConsumptionMetricsKey {
                tenant_id,
                timeline_id: None,
                metric: SYNTHETIC_STORAGE_SIZE,
            },
            tenant_synthetic_size,
        ));
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
    let chunks = current_metrics.chunks(CHUNK_SIZE);

    let mut chunk_to_send: Vec<Event<Ids>> = Vec::with_capacity(CHUNK_SIZE);

    for chunk in chunks {
        chunk_to_send.clear();

        // enrich metrics with type,timestamp and idempotency key before sending
        chunk_to_send.extend(chunk.iter().map(|(curr_key, curr_val)| Event {
            kind: EventType::Absolute { time: Utc::now() },
            metric: curr_key.metric,
            idempotency_key: idempotency_key(node_id.to_string()),
            value: *curr_val,
            extra: Ids {
                tenant_id: curr_key.tenant_id,
                timeline_id: curr_key.timeline_id,
            },
        }));

        let chunk_json = serde_json::value::to_raw_value(&EventChunk {
            events: &chunk_to_send,
        })
        .expect("PageserverConsumptionMetric should not fail serialization");

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

/// Caclculate synthetic size for each active tenant
pub async fn calculate_synthetic_size_worker(
    synthetic_size_calculation_interval: Duration,
    ctx: &RequestContext,
) -> anyhow::Result<()> {
    info!("starting calculate_synthetic_size_worker");

    let mut ticker = tokio::time::interval(synthetic_size_calculation_interval);

    loop {
        tokio::select! {
            _ = task_mgr::shutdown_watcher() => {
                return Ok(());
            },
        _ = ticker.tick() => {

                let tenants = mgr::list_tenants().await;
                // iterate through list of Active tenants and collect metrics
                for (tenant_id, tenant_state) in tenants {

                    if tenant_state != TenantState::Active {
                        continue;
                    }

                    if let Ok(tenant) = mgr::get_tenant(tenant_id, true).await
                    {
                        if let Err(e) = tenant.calculate_synthetic_size(ctx).await {
                            error!("failed to calculate synthetic size for tenant {}: {}", tenant_id, e);
                        }
                    }

                }
            }
        }
    }
}
