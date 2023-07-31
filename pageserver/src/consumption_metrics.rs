//!
//! Periodically collect consumption metrics for all active tenants
//! and push them to a HTTP endpoint.
//! Cache metrics to send only the updated ones.
//!
use crate::context::{DownloadBehavior, RequestContext};
use crate::task_mgr::{self, TaskKind, BACKGROUND_RUNTIME};
use crate::tenant::mgr::GetTenantError;
use crate::tenant::{mgr, LogicalSizeCalculationCause, Tenant};
use anyhow;
use chrono::{DateTime, Utc};
use consumption_metrics::{idempotency_key, Event, EventChunk, EventType, CHUNK_SIZE};
use pageserver_api::models::TenantState;
use reqwest::Url;
use serde_with::{serde_as, DisplayFromStr};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::*;
use utils::id::{NodeId, TenantId, TimelineId};
use utils::lsn::Lsn;

const DEFAULT_HTTP_REPORTING_TIMEOUT: Duration = Duration::from_secs(60);

#[serde_as]
#[derive(serde::Serialize, Debug)]
#[cfg_attr(test, derive(serde::Deserialize))]
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

impl PageserverConsumptionMetricsKey {
    const fn absolute_values(self) -> AbsoluteValueFactory {
        AbsoluteValueFactory(self)
    }
    const fn incremental_values(self) -> IncrementalValueFactory {
        IncrementalValueFactory(self)
    }
}

/// Helper type which each individual metric kind can return to produce only absolute values.
struct AbsoluteValueFactory(PageserverConsumptionMetricsKey);

impl AbsoluteValueFactory {
    fn at(
        self,
        time: DateTime<Utc>,
        val: u64,
    ) -> (PageserverConsumptionMetricsKey, (EventType, u64)) {
        let key = self.0;
        (key, (EventType::Absolute { time }, val))
    }
}

/// Helper type which each individual metric kind can return to produce only incremental values.
struct IncrementalValueFactory(PageserverConsumptionMetricsKey);

impl IncrementalValueFactory {
    #[allow(clippy::wrong_self_convention)]
    fn from_previous_up_to(
        self,
        prev_end: DateTime<Utc>,
        up_to: DateTime<Utc>,
        val: u64,
    ) -> (PageserverConsumptionMetricsKey, (EventType, u64)) {
        let key = self.0;
        // cannot assert prev_end < up_to because these are realtime clock based
        (
            key,
            (
                EventType::Incremental {
                    start_time: prev_end,
                    stop_time: up_to,
                },
                val,
            ),
        )
    }

    fn key(&self) -> &PageserverConsumptionMetricsKey {
        &self.0
    }
}

// the static part of a PageserverConsumptionMetricsKey
impl PageserverConsumptionMetricsKey {
    const fn written_size(tenant_id: TenantId, timeline_id: TimelineId) -> AbsoluteValueFactory {
        PageserverConsumptionMetricsKey {
            tenant_id,
            timeline_id: Some(timeline_id),
            metric: "written_size",
        }
        .absolute_values()
    }

    /// Values will be the difference of the latest written_size (last_record_lsn) to what we
    /// previously sent.
    const fn written_size_delta(
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> IncrementalValueFactory {
        PageserverConsumptionMetricsKey {
            tenant_id,
            timeline_id: Some(timeline_id),
            metric: "written_size_bytes_delta",
        }
        .incremental_values()
    }

    const fn timeline_logical_size(
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> AbsoluteValueFactory {
        PageserverConsumptionMetricsKey {
            tenant_id,
            timeline_id: Some(timeline_id),
            metric: "timeline_logical_size",
        }
        .absolute_values()
    }

    const fn remote_storage_size(tenant_id: TenantId) -> AbsoluteValueFactory {
        PageserverConsumptionMetricsKey {
            tenant_id,
            timeline_id: None,
            metric: "remote_storage_size",
        }
        .absolute_values()
    }

    const fn resident_size(tenant_id: TenantId) -> AbsoluteValueFactory {
        PageserverConsumptionMetricsKey {
            tenant_id,
            timeline_id: None,
            metric: "resident_size",
        }
        .absolute_values()
    }

    const fn synthetic_size(tenant_id: TenantId) -> AbsoluteValueFactory {
        PageserverConsumptionMetricsKey {
            tenant_id,
            timeline_id: None,
            metric: "synthetic_storage_size",
        }
        .absolute_values()
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
    let mut cached_metrics = HashMap::new();
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

                let tenants = match crate::tenant::mgr::list_tenants().await {
                    Ok(tenants) => tenants,
                    Err(e) => { warn!("failed to get tenants: {e:#}"); continue; }
                };

                let active_only = |tenant_id| crate::tenant::mgr::get_tenant(tenant_id, true);

                collect_metrics_iteration(&tenants, &active_only, &client, &mut cached_metrics, metric_collection_endpoint, node_id, &ctx, send_cached).await;

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
pub async fn collect_metrics_iteration<F, Fut>(
    tenants: &[(TenantId, TenantState)],
    get_tenant: F,
    client: &reqwest::Client,
    cached_metrics: &mut HashMap<PageserverConsumptionMetricsKey, (EventType, u64)>,
    metric_collection_endpoint: &reqwest::Url,
    node_id: NodeId,
    ctx: &RequestContext,
    send_cached: bool,
) where
    F: Fn(TenantId) -> Fut,
    Fut: std::future::Future<Output = Result<Arc<Tenant>, GetTenantError>>,
{
    let mut current_metrics: Vec<(PageserverConsumptionMetricsKey, (EventType, u64))> = Vec::new();
    trace!(
        "starting collect_metrics_iteration. metric_collection_endpoint: {}",
        metric_collection_endpoint
    );

    // iterate through list of Active tenants and collect metrics
    for (tenant_id, tenant_state) in tenants {
        if tenant_state != &TenantState::Active {
            continue;
        }

        let tenant_id = *tenant_id;

        let tenant = match get_tenant(tenant_id).await {
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
        for timeline in tenant.list_timelines() {
            // collect per-timeline metrics only for active timelines

            let timeline_id = timeline.timeline_id;

            match TimelineSnapshot::collect(&timeline, ctx) {
                Ok(Some(snap)) => {
                    snap.to_metrics(
                        tenant_id,
                        timeline_id,
                        Utc::now(),
                        &mut current_metrics,
                        cached_metrics,
                    );
                }
                Ok(None) => {}
                Err(e) => {
                    error!(
                        "failed to get metrics values for tenant {tenant_id} timeline {}: {e:#?}",
                        timeline.timeline_id
                    );
                    continue;
                }
            }

            let timeline_resident_size = timeline.get_resident_physical_size();
            tenant_resident_size += timeline_resident_size;
        }

        match tenant.get_remote_size().await {
            Ok(tenant_remote_size) => {
                current_metrics.push(
                    PageserverConsumptionMetricsKey::remote_storage_size(tenant_id)
                        .at(Utc::now(), tenant_remote_size),
                );
            }
            Err(err) => {
                error!(
                    "failed to get remote size for tenant {}: {err:?}",
                    tenant_id
                );
            }
        }

        current_metrics.push(
            PageserverConsumptionMetricsKey::resident_size(tenant_id)
                .at(Utc::now(), tenant_resident_size),
        );

        // Note that this metric is calculated in a separate bgworker
        // Here we only use cached value, which may lag behind the real latest one
        let tenant_synthetic_size = tenant.get_cached_synthetic_size();

        if tenant_synthetic_size != 0 {
            // only send non-zeroes because otherwise these show up as errors in logs
            current_metrics.push(
                PageserverConsumptionMetricsKey::synthetic_size(tenant_id)
                    .at(Utc::now(), tenant_synthetic_size),
            );
        }
    }

    // Filter metrics, unless we want to send all metrics, including cached ones.
    // See: https://github.com/neondatabase/neon/issues/3485
    if !send_cached {
        current_metrics.retain(|(curr_key, (kind, curr_val))| {
            if kind.is_incremental() {
                // incremental values (currently only written_size_delta) should not get any cache
                // deduplication because they will be used by upstream for "is still alive."
                true
            } else {
                match cached_metrics.get(curr_key) {
                    Some((_, val)) => val != curr_val,
                    None => true,
                }
            }
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
        chunk_to_send.extend(chunk.iter().map(|(curr_key, (when, curr_val))| Event {
            kind: *when,
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

struct TimelineSnapshot {
    loaded_at: (Lsn, SystemTime),
    last_record_lsn: Lsn,
    current_exact_logical_size: Option<u64>,
}

impl TimelineSnapshot {
    fn collect(
        t: &Arc<crate::tenant::Timeline>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Option<Self>> {
        use anyhow::Context;

        if t.is_active() {
            let loaded_at = t.loaded_at;
            let last_record_lsn = t.get_last_record_lsn();

            let current_exact_logical_size = {
                let span = info_span!("collect_metrics_iteration", tenant_id = %t.tenant_id, timeline_id = %t.timeline_id);
                let res = span
                    .in_scope(|| t.get_current_logical_size(ctx))
                    .context("get_current_logical_size");
                match res? {
                    // Only send timeline logical size when it is fully calculated.
                    (size, is_exact) if is_exact => Some(size),
                    (_, _) => None,
                }
            };

            Ok(Some(TimelineSnapshot {
                loaded_at,
                last_record_lsn,
                current_exact_logical_size,
            }))
        } else {
            Ok(None)
        }
    }

    fn to_metrics(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        now: DateTime<Utc>,
        metrics: &mut Vec<(PageserverConsumptionMetricsKey, (EventType, u64))>,
        cache: &HashMap<PageserverConsumptionMetricsKey, (EventType, u64)>,
    ) {
        let timeline_written_size = u64::from(self.last_record_lsn);

        let (key, written_size_now) =
            PageserverConsumptionMetricsKey::written_size(tenant_id, timeline_id)
                .at(now, timeline_written_size);

        // last_record_lsn can only go up, right now at least, TODO: #2592 or related
        // features might change this.

        let written_size_delta_key =
            PageserverConsumptionMetricsKey::written_size_delta(tenant_id, timeline_id);

        // use this when available, because in a stream of incremental values, it will be
        // accurate where as when last_record_lsn stops moving, we will only cache the last
        // one of those.
        let last_stop_time = cache
            .get(written_size_delta_key.key())
            .map(|(until, _val)| {
                until
                    .incremental_timerange()
                    .expect("never create EventType::Absolute for written_size_delta")
                    .end
            });

        // by default, use the last sent written_size as the basis for
        // calculating the delta. if we don't yet have one, use the load time value.
        let prev = cache
            .get(&key)
            .map(|(prev_at, prev)| {
                // use the prev time from our last incremental update, or default to latest
                // absolute update on the first round.
                let prev_at = prev_at
                    .absolute_time()
                    .expect("never create EventType::Incremental for written_size");
                let prev_at = last_stop_time.unwrap_or(prev_at);
                (*prev_at, *prev)
            })
            .unwrap_or_else(|| {
                // if we don't have a previous point of comparison, compare to the load time
                // lsn.
                let (disk_consistent_lsn, loaded_at) = &self.loaded_at;
                (DateTime::from(*loaded_at), disk_consistent_lsn.0)
            });

        // written_size_bytes_delta
        metrics.extend(
            if let Some(delta) = written_size_now.1.checked_sub(prev.1) {
                let up_to = written_size_now
                    .0
                    .absolute_time()
                    .expect("never create EventType::Incremental for written_size");
                let key_value = written_size_delta_key.from_previous_up_to(prev.0, *up_to, delta);
                Some(key_value)
            } else {
                None
            },
        );

        // written_size
        metrics.push((key, written_size_now));

        if let Some(size) = self.current_exact_logical_size {
            metrics.push(
                PageserverConsumptionMetricsKey::timeline_logical_size(tenant_id, timeline_id)
                    .at(now, size),
            );
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, convert::Infallible};

    use pageserver_api::models::TenantState;
    use utils::{id::TimelineId, lsn::Lsn};

    #[tokio::test]
    async fn written_size_bytes_delta_while_last_record_lsn_advances() {
        use consumption_metrics::EventType;
        use hyper::body::to_bytes;
        use hyper::service::{make_service_fn, service_fn};
        use hyper::{Body, Request, Response};
        use tokio::net::TcpListener;
        use tokio::sync::mpsc;

        // Just a wrapper around a slice of events
        // to deserialize it as `{"events" : [ ] }
        #[derive(serde::Deserialize)]
        struct EventChunkOwned {
            pub events: Vec<EventOwned>,
        }

        #[allow(dead_code)]
        #[derive(serde::Deserialize, Debug)]
        struct EventOwned {
            #[serde(flatten)]
            #[serde(rename = "type")]
            kind: EventType,
            metric: String,
            idempotency_key: String,
            value: u64,
            #[serde(flatten)]
            extra: super::Ids,
        }

        let (tx, mut rx) = mpsc::channel(1);

        let make_svc = make_service_fn(move |_| {
            let tx = tx.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                    let tx = tx.clone();
                    async move {
                        let body = to_bytes(req.into_body()).await.unwrap();
                        let events: EventChunkOwned = serde_json::from_slice(&body).unwrap();
                        tx.send(events).await.map_err(|_| ()).unwrap();
                        Ok::<_, Infallible>(Response::new(Body::empty()))
                    }
                }))
            }
        });

        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();

        tokio::spawn(async move {
            hyper::Server::from_tcp(tcp.into_std().unwrap())
                .unwrap()
                .serve(make_svc)
                .await
        });

        let harness = crate::tenant::harness::TenantHarness::create(
            "written_size_bytes_delta_while_last_record_lsn_advances",
        )
        .unwrap();

        let (tenant, ctx) = harness.load().await;
        let timeline_id = TimelineId::generate();

        let timeline = tenant
            // initdb lsn is not updated to any place where one would expect to find it
            // initdb lsn cannot be zero or unaligned however
            .create_test_timeline(timeline_id, Lsn(8), 14, &ctx)
            .await
            .unwrap();

        assert_eq!(tenant.list_timelines().len(), 1);

        assert_eq!(
            Lsn(8),
            timeline.get_disk_consistent_lsn(),
            "set by create_test_timeline"
        );
        assert_eq!(
            Lsn(0),
            timeline.loaded_at.0,
            "sadly not set by create_test_timeline, or put in metadata"
        );

        timeline.advance_to_lsn_in_test(Lsn(8));
        assert_eq!(Lsn(8), timeline.get_last_record_lsn());

        let client = reqwest::ClientBuilder::new().build().unwrap();
        let mut cache = HashMap::new();

        let mut url = reqwest::Url::parse("http://127.0.0.1/does_not_matter").unwrap();
        url.set_port(Some(addr.port())).unwrap();

        let tenants = [(tenant.tenant_id(), TenantState::Active)];
        let get_tenant = |tenant_id| {
            assert_eq!(tenant_id, tenant.tenant_id());
            futures::future::ready(Ok(tenant.clone()))
        };

        // Helper to avoid typing all this everytime.
        macro_rules! just_one_iteration {
            () => {{
                super::collect_metrics_iteration(
                    &tenants,
                    &get_tenant,
                    &client,
                    &mut cache,
                    &url,
                    utils::id::NodeId(123),
                    &ctx,
                    false,
                )
                .await;
            }};
        }

        just_one_iteration!();

        let last_timerange = {
            let events = rx.recv().await.unwrap();

            let event = events
                .events
                .iter()
                .find(|e| e.metric == "written_size_bytes_delta")
                .unwrap();

            assert_eq!(
                8, event.value,
                "create_test_timeline creates one starting at zero, we've since advanced"
            );
            let timerange_now = event.kind.incremental_timerange().unwrap();
            assert_eq!(
                timerange_now.start,
                &chrono::DateTime::<chrono::Utc>::from(timeline.loaded_at.1)
            );
            assert!(timerange_now.start <= timerange_now.end);
            timerange_now.start.to_owned()..timerange_now.end.to_owned()
        };

        just_one_iteration!();

        let last_timerange = {
            let events = rx.recv().await.unwrap();

            let event = events
                .events
                .iter()
                .find(|e| e.metric == "written_size_bytes_delta")
                .unwrap();
            assert_eq!(0, event.value);
            assert_eq!(timeline.tenant_id, event.extra.tenant_id);
            assert_eq!(Some(timeline.timeline_id), event.extra.timeline_id);
            let timerange_now = event.kind.incremental_timerange().unwrap();
            assert_eq!(&last_timerange.end, timerange_now.start);
            assert!(timerange_now.start <= timerange_now.end);
            timerange_now.start.to_owned()..timerange_now.end.to_owned()
        };

        timeline.advance_to_lsn_in_test(Lsn(16));

        just_one_iteration!();

        let last_timerange = {
            let events = rx.recv().await.unwrap();

            let event = events
                .events
                .iter()
                .find(|e| e.metric == "written_size_bytes_delta")
                .unwrap();
            assert_eq!(8, event.value);
            assert_eq!(timeline.tenant_id, event.extra.tenant_id);
            assert_eq!(Some(timeline.timeline_id), event.extra.timeline_id);
            let timerange_now = event.kind.incremental_timerange().unwrap();
            assert_eq!(&last_timerange.end, timerange_now.start);
            assert!(timerange_now.start <= timerange_now.end);
            timerange_now.start.to_owned()..timerange_now.end.to_owned()
        };

        let mut last_timerange = last_timerange;

        for _ in 0..10 {
            just_one_iteration!();

            last_timerange = {
                let events = rx.recv().await.unwrap();

                let event = events
                    .events
                    .iter()
                    .find(|e| e.metric == "written_size_bytes_delta")
                    .expect("the event should keep appearing even if delta is zero");
                assert_eq!(0, event.value);
                assert_eq!(timeline.tenant_id, event.extra.tenant_id);
                assert_eq!(Some(timeline.timeline_id), event.extra.timeline_id);
                let timerange_now = event.kind.incremental_timerange().unwrap();
                assert_eq!(&last_timerange.end, timerange_now.start);
                assert!(timerange_now.start <= timerange_now.end);
                timerange_now.start.to_owned()..timerange_now.end.to_owned()
            };
        }
    }
}
