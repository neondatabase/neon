//!
//! Periodically collect consumption metrics for all active tenants
//! and push them to a HTTP endpoint.
//! Cache metrics to send only the updated ones.
//!
use crate::context::{DownloadBehavior, RequestContext};
use crate::task_mgr::{self, TaskKind, BACKGROUND_RUNTIME};
use crate::tenant::{mgr, LogicalSizeCalculationCause};
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

const DEFAULT_HTTP_REPORTING_TIMEOUT: Duration = Duration::from_secs(60);

#[serde_as]
#[derive(Serialize, Debug)]
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
    // pub kind: EventType,
    pub metric: &'static str,
}

impl PageserverConsumptionMetricsKey {
    const fn written_size(tenant_id: TenantId, timeline_id: TimelineId) -> Self {
        PageserverConsumptionMetricsKey {
            tenant_id,
            timeline_id: Some(timeline_id),
            metric: "written_size",
        }
    }

    /// Values will be the difference of the latest written_size (last_record_lsn) to what we
    /// previously sent.
    const fn written_size_delta(tenant_id: TenantId, timeline_id: TimelineId) -> Self {
        PageserverConsumptionMetricsKey {
            tenant_id,
            timeline_id: Some(timeline_id),
            metric: "written_size_delta_bytes",
        }
    }

    const fn timeline_logical_size(tenant_id: TenantId, timeline_id: TimelineId) -> Self {
        PageserverConsumptionMetricsKey {
            tenant_id,
            timeline_id: Some(timeline_id),
            metric: "timeline_logical_size",
        }
    }

    const fn remote_storage_size(tenant_id: TenantId) -> Self {
        PageserverConsumptionMetricsKey {
            tenant_id,
            timeline_id: None,
            metric: "remote_storage_size",
        }
    }

    const fn resident_size(tenant_id: TenantId) -> Self {
        PageserverConsumptionMetricsKey {
            tenant_id,
            timeline_id: None,
            metric: "resident_size",
        }
    }

    const fn synthetic_size(tenant_id: TenantId) -> Self {
        PageserverConsumptionMetricsKey {
            tenant_id,
            timeline_id: None,
            metric: "synthetic_storage_size",
        }
    }
}

/// Main thread that serves metrics collection
pub async fn collect_metrics(
    metric_collection_endpoint: &Url,
    metric_collection_interval: Duration,
    cached_metric_collection_interval: Duration,
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
    let client = reqwest::ClientBuilder::new()
        .timeout(DEFAULT_HTTP_REPORTING_TIMEOUT)
        .build()
        .expect("Failed to create http client with timeout");
    let mut cached_metrics: HashMap<PageserverConsumptionMetricsKey, u64> = HashMap::new();
    let mut prev_iteration_time: std::time::Instant = std::time::Instant::now();

    loop {
        tokio::select! {
            _ = task_mgr::shutdown_watcher() => {
                info!("collect_metrics received cancellation request");
                return Ok(());
            },
            tick_at = ticker.tick() => {

                // send cached metrics every cached_metric_collection_interval
                let send_cached = prev_iteration_time.elapsed() >= cached_metric_collection_interval;

                if send_cached {
                    prev_iteration_time = std::time::Instant::now();
                }

                collect_metrics_iteration(&client, &mut cached_metrics, metric_collection_endpoint, node_id, &ctx, send_cached).await;

                crate::tenant::tasks::warn_when_period_overrun(
                    tick_at.elapsed(),
                    metric_collection_interval,
                    "consumption_metrics_collect_metrics",
                );
            }
        }
    }
}

/// One iteration of metrics collection
///
/// Gather per-tenant and per-timeline metrics and send them to the `metric_collection_endpoint`.
/// Cache metrics to avoid sending the same metrics multiple times.
///
/// This function handles all errors internally
/// and doesn't break iteration if just one tenant fails.
///
/// TODO
/// - refactor this function (chunking+sending part) to reuse it in proxy module;
pub async fn collect_metrics_iteration(
    client: &reqwest::Client,
    cached_metrics: &mut HashMap<PageserverConsumptionMetricsKey, u64>,
    metric_collection_endpoint: &reqwest::Url,
    node_id: NodeId,
    ctx: &RequestContext,
    send_cached: bool,
) {
    let mut current_metrics: Vec<(PageserverConsumptionMetricsKey, u64)> = Vec::new();
    trace!(
        "starting collect_metrics_iteration. metric_collection_endpoint: {}",
        metric_collection_endpoint
    );

    // get list of tenants
    let tenants = match mgr::list_tenants().await {
        Ok(tenants) => tenants,
        Err(err) => {
            error!("failed to list tenants: {:?}", err);
            return;
        }
    };

    // iterate through list of Active tenants and collect metrics
    for (tenant_id, tenant_state) in tenants {
        if tenant_state != TenantState::Active {
            continue;
        }

        let tenant = match mgr::get_tenant(tenant_id, true).await {
            Ok(tenant) => tenant,
            Err(err) => {
                // It is possible that tenant was deleted between
                // `list_tenants` and `get_tenant`, so just warn about it.
                warn!("failed to get tenant {tenant_id:?}: {err:?}");
                continue;
            }
        };

        let mut tenant_resident_size = 0;

        // iterate through list of timelines in tenant
        for timeline in tenant.list_timelines().iter() {
            // collect per-timeline metrics only for active timelines
            if timeline.is_active() {
                let timeline_written_size = u64::from(timeline.get_last_record_lsn());

                let key =
                    PageserverConsumptionMetricsKey::written_size(tenant_id, timeline.timeline_id);

                // last_record_lsn can only go up, right now at least, TODO: #2592 or related
                // features might change this.
                //
                // this will be None for the first item, do not send it then to avoid the need to
                // filter out huge values from the stream of deltas.
                let timeline_written_size_delta = cached_metrics
                    .get(&key)
                    .copied()
                    .map(|prev| timeline_written_size - prev);

                current_metrics.push((key, timeline_written_size));

                if let Some(timeline_written_size_delta) = timeline_written_size_delta {
                    current_metrics.push((
                        PageserverConsumptionMetricsKey::written_size_delta(
                            tenant_id,
                            timeline.timeline_id,
                        ),
                        timeline_written_size_delta,
                    ));
                }

                let span = info_span!("collect_metrics_iteration", tenant_id = %timeline.tenant_id, timeline_id = %timeline.timeline_id);
                match span.in_scope(|| timeline.get_current_logical_size(ctx)) {
                    // Only send timeline logical size when it is fully calculated.
                    Ok((size, is_exact)) if is_exact => {
                        current_metrics.push((
                            PageserverConsumptionMetricsKey::timeline_logical_size(
                                tenant_id,
                                timeline.timeline_id,
                            ),
                            size,
                        ));
                    }
                    Ok((_, _)) => {}
                    Err(err) => {
                        error!(
                            "failed to get current logical size for timeline {}: {err:?}",
                            timeline.timeline_id
                        );
                        continue;
                    }
                };
            }

            let timeline_resident_size = timeline.get_resident_physical_size();
            tenant_resident_size += timeline_resident_size;
        }

        match tenant.get_remote_size().await {
            Ok(tenant_remote_size) => {
                current_metrics.push((
                    PageserverConsumptionMetricsKey::remote_storage_size(tenant_id),
                    tenant_remote_size,
                ));
            }
            Err(err) => {
                error!(
                    "failed to get remote size for tenant {}: {err:?}",
                    tenant_id
                );
            }
        }

        current_metrics.push((
            PageserverConsumptionMetricsKey::resident_size(tenant_id),
            tenant_resident_size,
        ));

        // Note that this metric is calculated in a separate bgworker
        // Here we only use cached value, which may lag behind the real latest one
        let tenant_synthetic_size = tenant.get_cached_synthetic_size();

        if tenant_synthetic_size != 0 {
            // only send non-zeroes because otherwise these show up as errors in logs
            current_metrics.push((
                PageserverConsumptionMetricsKey::synthetic_size(tenant_id),
                tenant_synthetic_size,
            ));
        }
    }

    // Filter metrics, unless we want to send all metrics, including cached ones.
    // See: https://github.com/neondatabase/neon/issues/3485
    if !send_cached {
        current_metrics.retain(|(curr_key, curr_val)| match cached_metrics.get(curr_key) {
            Some(val) => val != curr_val,
            None => true,
        });
    }

    if current_metrics.is_empty() {
        trace!("no new metrics to send");
        return;
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

        const MAX_RETRIES: u32 = 3;

        for attempt in 0..MAX_RETRIES {
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
                        for metric in chunk_to_send
                            .iter()
                            .filter(|metric| metric.value > (1u64 << 40))
                        {
                            // Report if the metric value is suspiciously large
                            error!("potentially abnormal metric value: {:?}", metric);
                        }
                    }
                    break;
                }
                Err(err) if err.is_timeout() => {
                    error!(attempt, "timeout sending metrics, retrying immediately");
                    continue;
                }
                Err(err) => {
                    error!(attempt, ?err, "failed to send metrics");
                    break;
                }
            }
        }
    }
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
        tick_at = ticker.tick() => {

                let tenants = match mgr::list_tenants().await {
                    Ok(tenants) => tenants,
                    Err(e) => {
                        warn!("cannot get tenant list: {e:#}");
                        continue;
                    }
                };
                // iterate through list of Active tenants and collect metrics
                for (tenant_id, tenant_state) in tenants {

                    if tenant_state != TenantState::Active {
                        continue;
                    }

                    if let Ok(tenant) = mgr::get_tenant(tenant_id, true).await
                    {
                        if let Err(e) = tenant.calculate_synthetic_size(
                            LogicalSizeCalculationCause::ConsumptionMetricsSyntheticSize,
                            ctx).await {
                            error!("failed to calculate synthetic size for tenant {}: {}", tenant_id, e);
                        }
                    }

                }

                crate::tenant::tasks::warn_when_period_overrun(
                    tick_at.elapsed(),
                    synthetic_size_calculation_interval,
                    "consumption_metrics_synthetic_size_worker",
                );
            }
        }
    }
}
