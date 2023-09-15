//!
//! Periodically collect consumption metrics for all active tenants
//! and push them to a HTTP endpoint.
//! Cache metrics to send only the updated ones.
//!
use crate::context::{DownloadBehavior, RequestContext};
use crate::task_mgr::{self, TaskKind, BACKGROUND_RUNTIME};
use crate::tenant::{mgr, LogicalSizeCalculationCause};
use chrono::{DateTime, Utc};
use consumption_metrics::{idempotency_key, Event, EventChunk, EventType, CHUNK_SIZE};
use futures::stream::StreamExt;
use pageserver_api::models::TenantState;
use reqwest::Url;
use serde::Serialize;
use serde_with::{serde_as, DisplayFromStr};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::id::{NodeId, TenantId, TimelineId};
use utils::lsn::Lsn;

const DEFAULT_HTTP_REPORTING_TIMEOUT: Duration = Duration::from_secs(60);

#[serde_as]
#[derive(Serialize, Debug, Clone, Copy)]
struct Ids {
    #[serde_as(as = "DisplayFromStr")]
    tenant_id: TenantId,
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    timeline_id: Option<TimelineId>,
}

/// Name of the metric, used by `MetricsKey` factory methods and `deserialize_cached_events`
/// instead of static str.
// Do not rename any of these without first consulting with data team and partner
// management.
// FIXME: write those tests before refactoring to this!
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
enum Name {
    /// Timeline last_record_lsn, absolute
    #[serde(rename = "written_size")]
    WrittenSize,
    /// Timeline last_record_lsn, incremental
    #[serde(rename = "written_data_bytes_delta")]
    WrittenSizeDelta,
    /// Timeline logical size
    #[serde(rename = "timeline_logical_size")]
    LogicalSize,
    /// Tenant remote size
    #[serde(rename = "remote_storage_size")]
    RemoteSize,
    /// Tenant resident size
    #[serde(rename = "resident_size")]
    ResidentSize,
    /// Tenant synthetic size
    #[serde(rename = "synthetic_storage_size")]
    SyntheticSize,
}

/// Key that uniquely identifies the object this metric describes.
///
/// This is a denormalization done at the MetricsKey const methods; these should not be constructed
/// elsewhere.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MetricsKey {
    tenant_id: TenantId,
    timeline_id: Option<TimelineId>,
    metric: &'static str,
}

impl MetricsKey {
    const fn absolute_values(self) -> AbsoluteValueFactory {
        AbsoluteValueFactory(self)
    }
    const fn incremental_values(self) -> IncrementalValueFactory {
        IncrementalValueFactory(self)
    }
}

/// Helper type which each individual metric kind can return to produce only absolute values.
struct AbsoluteValueFactory(MetricsKey);

impl AbsoluteValueFactory {
    fn at(self, time: DateTime<Utc>, val: u64) -> RawMetric {
        let key = self.0;
        (key, (EventType::Absolute { time }, val))
    }
}

/// Helper type which each individual metric kind can return to produce only incremental values.
struct IncrementalValueFactory(MetricsKey);

impl IncrementalValueFactory {
    #[allow(clippy::wrong_self_convention)]
    fn from_previous_up_to(
        self,
        prev_end: DateTime<Utc>,
        up_to: DateTime<Utc>,
        val: u64,
    ) -> RawMetric {
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

    fn key(&self) -> &MetricsKey {
        &self.0
    }
}

// the static part of a MetricsKey
impl MetricsKey {
    /// Absolute value of [`Timeline::get_last_record_lsn`].
    ///
    /// [`Timeline::get_last_record_lsn`]: crate::tenant::Timeline::get_last_record_lsn
    const fn written_size(tenant_id: TenantId, timeline_id: TimelineId) -> AbsoluteValueFactory {
        MetricsKey {
            tenant_id,
            timeline_id: Some(timeline_id),
            metric: "written_size",
        }
        .absolute_values()
    }

    /// Values will be the difference of the latest [`MetricsKey::written_size`] to what we
    /// previously sent, starting from the previously sent incremental time range ending at the
    /// latest absolute measurement.
    const fn written_size_delta(
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> IncrementalValueFactory {
        MetricsKey {
            tenant_id,
            timeline_id: Some(timeline_id),
            // the name here is correctly about data not size, because that is what is wanted by
            // downstream pipeline
            metric: "written_data_bytes_delta",
        }
        .incremental_values()
    }

    /// Exact [`Timeline::get_current_logical_size`].
    ///
    /// [`Timeline::get_current_logical_size`]: crate::tenant::Timeline::get_current_logical_size
    const fn timeline_logical_size(
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> AbsoluteValueFactory {
        MetricsKey {
            tenant_id,
            timeline_id: Some(timeline_id),
            metric: "timeline_logical_size",
        }
        .absolute_values()
    }

    /// [`Tenant::remote_size`]
    ///
    /// [`Tenant::remote_size`]: crate::tenant::Tenant::remote_size
    const fn remote_storage_size(tenant_id: TenantId) -> AbsoluteValueFactory {
        MetricsKey {
            tenant_id,
            timeline_id: None,
            metric: "remote_storage_size",
        }
        .absolute_values()
    }

    /// Sum of [`Timeline::resident_physical_size`] for each `Tenant`.
    ///
    /// [`Timeline::resident_physical_size`]: crate::tenant::Timeline::resident_physical_size
    const fn resident_size(tenant_id: TenantId) -> AbsoluteValueFactory {
        MetricsKey {
            tenant_id,
            timeline_id: None,
            metric: "resident_size",
        }
        .absolute_values()
    }

    /// [`Tenant::cached_synthetic_size`] as refreshed by [`calculate_synthetic_size_worker`].
    ///
    /// [`Tenant::cached_synthetic_size`]: crate::tenant::Tenant::cached_synthetic_size
    const fn synthetic_size(tenant_id: TenantId) -> AbsoluteValueFactory {
        MetricsKey {
            tenant_id,
            timeline_id: None,
            metric: "synthetic_storage_size",
        }
        .absolute_values()
    }
}

/// Basically a key-value pair, but usually in a Vec except for [`Cache`].
///
/// This is as opposed to `consumption_metrics::Event` which is the externally communicated form.
/// Difference is basically the missing idempotency key, which lives only for the duration of
/// upload attempts.
type RawMetric = (MetricsKey, (EventType, u64));

/// Caches the [`RawMetric`]s
///
/// In practice, during startup, last sent values are stored here to be used in calculating new
/// ones. After successful uploading, the cached values are updated to cache. This used to be used
/// for deduplication, but that is no longer needed.
type Cache = HashMap<MetricsKey, (EventType, u64)>;

/// Main thread that serves metrics collection
pub async fn collect_metrics(
    metric_collection_endpoint: &Url,
    metric_collection_interval: Duration,
    _cached_metric_collection_interval: Duration,
    synthetic_size_calculation_interval: Duration,
    node_id: NodeId,
    ctx: RequestContext,
) -> anyhow::Result<()> {
    if _cached_metric_collection_interval != Duration::ZERO {
        tracing::warn!(
            "cached_metric_collection_interval is no longer used, please set it to zero."
        )
    }
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

    loop {
        tokio::select! {
            _ = task_mgr::shutdown_watcher() => {
                info!("collect_metrics received cancellation request");
                return Ok(());
            },
            tick_at = ticker.tick() => {

                collect_metrics_iteration(&client, &mut cached_metrics, metric_collection_endpoint, node_id, &ctx).await;

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
async fn collect_metrics_iteration(
    client: &reqwest::Client,
    cached_metrics: &mut Cache,
    metric_collection_endpoint: &reqwest::Url,
    node_id: NodeId,
    ctx: &RequestContext,
) {
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

    let tenants = futures::stream::iter(tenants).filter_map(|(id, state)| async move {
        if state != TenantState::Active {
            None
        } else {
            mgr::get_tenant(id, true)
                .await
                .ok()
                .map(|tenant| (id, tenant))
        }
    });

    let current_metrics = collect(tenants, cached_metrics, ctx).await;

    if current_metrics.is_empty() {
        trace!("no new metrics to send");
        return;
    }

    let cancel = task_mgr::shutdown_token();

    // Send metrics.
    // Split into chunks of 1000 metrics to avoid exceeding the max request size
    let chunks = current_metrics.chunks(CHUNK_SIZE);

    let mut chunk_to_send: Vec<Event<Ids, &'static str>> = Vec::with_capacity(CHUNK_SIZE);

    let node_id = node_id.to_string();

    let mut buffer = bytes::BytesMut::new();

    for chunk in chunks {
        chunk_to_send.clear();

        // enrich metrics with type,timestamp and idempotency key before sending
        chunk_to_send.extend(chunk.iter().map(|(curr_key, (when, curr_val))| Event {
            kind: *when,
            metric: curr_key.metric,
            idempotency_key: idempotency_key(&node_id),
            value: *curr_val,
            extra: Ids {
                tenant_id: curr_key.tenant_id,
                timeline_id: curr_key.timeline_id,
            },
        }));

        use bytes::BufMut;

        // FIXME: this is a new panic we did not have before. not really essential, but panics from
        // this task currently restart pageserver.
        serde_json::to_writer(
            (&mut buffer).writer(),
            &EventChunk {
                events: (&chunk_to_send).into(),
            },
        )
        .expect("serialization must not fail and bytesmut grows");

        let body = buffer.split().freeze();

        let res = upload(client, metric_collection_endpoint, body, &cancel).await;

        if res.is_ok() {
            for (curr_key, curr_val) in chunk.iter() {
                cached_metrics.insert(curr_key.clone(), *curr_val);
            }
        } else {
            // no need to log, backoff::retry and upload have done it, just give up uploading.
        }
    }
}

async fn collect<S>(
    tenants: S,
    cache: &HashMap<MetricsKey, (EventType, u64)>,
    ctx: &RequestContext,
) -> Vec<(MetricsKey, (EventType, u64))>
where
    S: futures::stream::Stream<Item = (TenantId, Arc<crate::tenant::Tenant>)>,
{
    let mut current_metrics: Vec<(MetricsKey, (EventType, u64))> = Vec::new();

    let mut tenants = std::pin::pin!(tenants);

    while let Some((tenant_id, tenant)) = tenants.next().await {
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
                        cache,
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

            tenant_resident_size += timeline.resident_physical_size();
        }

        TenantSnapshot::collect(&tenant, tenant_resident_size).to_metrics(
            tenant_id,
            Utc::now(),
            &mut current_metrics,
        );
    }

    current_metrics
}

enum UploadError {
    Rejected(reqwest::StatusCode),
    Reqwest(reqwest::Error),
    Cancelled,
}

impl std::fmt::Debug for UploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // use same impl because backoff::retry will log this using both
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for UploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use UploadError::*;

        match self {
            Rejected(code) => write!(f, "server rejected the metrics with {code}"),
            Reqwest(e) => write!(f, "request failed: {e}"),
            Cancelled => write!(f, "cancelled"),
        }
    }
}

impl UploadError {
    fn is_reject(&self) -> bool {
        matches!(self, UploadError::Rejected(_))
    }
}

async fn upload(
    client: &reqwest::Client,
    metric_collection_endpoint: &reqwest::Url,
    body: bytes::Bytes,
    cancel: &CancellationToken,
) -> Result<(), UploadError> {
    let warn_after = 3;
    let max_attempts = 10;
    let res = utils::backoff::retry(
        move || {
            let body = body.clone();
            async move {
                let res = client
                    .post(metric_collection_endpoint.clone())
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(body)
                    .send()
                    .await;

                let res = res.and_then(|res| res.error_for_status());

                match res {
                    Ok(_response) => Ok(()),
                    Err(e) => {
                        let status = e.status().filter(|s| s.is_client_error());
                        if let Some(status) = status {
                            Err(UploadError::Rejected(status))
                        } else {
                            Err(UploadError::Reqwest(e))
                        }
                    }
                }
            }
        },
        UploadError::is_reject,
        warn_after,
        max_attempts,
        "upload consumption_metrics",
        utils::backoff::Cancel::new(cancel.clone(), || UploadError::Cancelled),
    )
    .await;

    match &res {
        Ok(_) => {}
        Err(e) if e.is_reject() => {
            // permanent errors currently do not get logged by backoff::retry
            // display alternate has no effect, but keeping it here for easier pattern matching.
            tracing::error!("failed to upload metrics: {e:#}");
        }
        Err(_) => {
            // these have been logged already
        }
    }

    res
}

/// Testing helping in-between abstraction allowing testing metrics without actual Tenants.
struct TenantSnapshot {
    resident_size: u64,
    remote_size: u64,
    synthetic_size: u64,
}

impl TenantSnapshot {
    /// Collect tenant status to have metrics created out of it.
    ///
    /// `resident_size` is calculated of the timelines we had access to for other metrics, so we
    /// cannot just list timelines here.
    fn collect(t: &Arc<crate::tenant::Tenant>, resident_size: u64) -> Self {
        TenantSnapshot {
            resident_size,
            remote_size: t.remote_size(),
            // Note that this metric is calculated in a separate bgworker
            // Here we only use cached value, which may lag behind the real latest one
            synthetic_size: t.cached_synthetic_size(),
        }
    }

    fn to_metrics(&self, tenant_id: TenantId, now: DateTime<Utc>, metrics: &mut Vec<RawMetric>) {
        let remote_size = MetricsKey::remote_storage_size(tenant_id).at(now, self.remote_size);

        let resident_size = MetricsKey::resident_size(tenant_id).at(now, self.resident_size);

        let synthetic_size = if self.synthetic_size != 0 {
            // only send non-zeroes because otherwise these show up as errors in logs
            Some(MetricsKey::synthetic_size(tenant_id).at(now, self.synthetic_size))
        } else {
            None
        };

        metrics.extend(
            [Some(remote_size), Some(resident_size), synthetic_size]
                .into_iter()
                .flatten(),
        );
    }
}

/// Internal type to make timeline metric production testable.
///
/// As this value type contains all of the information needed from a timeline to produce the
/// metrics, it can easily be created with different values in test.
struct TimelineSnapshot {
    loaded_at: (Lsn, SystemTime),
    last_record_lsn: Lsn,
    current_exact_logical_size: Option<u64>,
}

impl TimelineSnapshot {
    /// Collect the metrics from an actual timeline.
    ///
    /// Fails currently only when [`Timeline::get_current_logical_size`] fails.
    ///
    /// [`Timeline::get_current_logical_size`]: crate::tenant::Timeline::get_current_logical_size
    fn collect(
        t: &Arc<crate::tenant::Timeline>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Option<Self>> {
        use anyhow::Context;

        if !t.is_active() {
            // no collection for broken or stopping needed, we will still keep the cached values
            // though at the caller.
            Ok(None)
        } else {
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
        }
    }

    /// Produce the timeline consumption metrics into the `metrics` argument.
    fn to_metrics(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        now: DateTime<Utc>,
        metrics: &mut Vec<RawMetric>,
        cache: &Cache,
    ) {
        let timeline_written_size = u64::from(self.last_record_lsn);

        let written_size_delta_key = MetricsKey::written_size_delta(tenant_id, timeline_id);

        let last_stop_time = cache
            .get(written_size_delta_key.key())
            .map(|(until, _val)| {
                until
                    .incremental_timerange()
                    .expect("never create EventType::Absolute for written_size_delta")
                    .end
            });

        let (key, written_size_now) =
            MetricsKey::written_size(tenant_id, timeline_id).at(now, timeline_written_size);

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
            metrics.push(MetricsKey::timeline_logical_size(tenant_id, timeline_id).at(now, size));
        }
    }
}

/// Caclculate synthetic size for each active tenant
async fn calculate_synthetic_size_worker(
    synthetic_size_calculation_interval: Duration,
    ctx: &RequestContext,
) -> anyhow::Result<()> {
    info!("starting calculate_synthetic_size_worker");

    // reminder: this ticker is ready right away
    let mut ticker = tokio::time::interval(synthetic_size_calculation_interval);
    let cause = LogicalSizeCalculationCause::ConsumptionMetricsSyntheticSize;

    loop {
        let tick_at = tokio::select! {
            _ = task_mgr::shutdown_watcher() => return Ok(()),
            tick_at = ticker.tick() => tick_at,
        };

        let tenants = match mgr::list_tenants().await {
            Ok(tenants) => tenants,
            Err(e) => {
                warn!("cannot get tenant list: {e:#}");
                continue;
            }
        };

        for (tenant_id, tenant_state) in tenants {
            if tenant_state != TenantState::Active {
                continue;
            }

            if let Ok(tenant) = mgr::get_tenant(tenant_id, true).await {
                if let Err(e) = tenant.calculate_synthetic_size(cause, ctx).await {
                    error!("failed to calculate synthetic size for tenant {tenant_id}: {e:#}");
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use std::time::SystemTime;
    use utils::{
        id::{TenantId, TimelineId},
        lsn::Lsn,
    };

    use crate::consumption_metrics::MetricsKey;

    use super::TimelineSnapshot;
    use chrono::{DateTime, Utc};

    #[test]
    fn startup_collected_timeline_metrics_before_advancing() {
        let tenant_id = TenantId::generate();
        let timeline_id = TimelineId::generate();

        let mut metrics = Vec::new();
        let cache = HashMap::new();

        let initdb_lsn = Lsn(0x10000);
        let disk_consistent_lsn = Lsn(initdb_lsn.0 * 2);

        let snap = TimelineSnapshot {
            loaded_at: (disk_consistent_lsn, SystemTime::now()),
            last_record_lsn: disk_consistent_lsn,
            current_exact_logical_size: Some(0x42000),
        };

        let now = DateTime::<Utc>::from(SystemTime::now());

        snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

        assert_eq!(
            metrics,
            &[
                MetricsKey::written_size_delta(tenant_id, timeline_id).from_previous_up_to(
                    snap.loaded_at.1.into(),
                    now,
                    0
                ),
                MetricsKey::written_size(tenant_id, timeline_id).at(now, disk_consistent_lsn.0),
                MetricsKey::timeline_logical_size(tenant_id, timeline_id).at(now, 0x42000)
            ]
        );
    }

    #[test]
    fn startup_collected_timeline_metrics_second_round() {
        let tenant_id = TenantId::generate();
        let timeline_id = TimelineId::generate();

        let [now, before, init] = time_backwards();

        let now = DateTime::<Utc>::from(now);
        let before = DateTime::<Utc>::from(before);

        let initdb_lsn = Lsn(0x10000);
        let disk_consistent_lsn = Lsn(initdb_lsn.0 * 2);

        let mut metrics = Vec::new();
        let cache = HashMap::from([
            MetricsKey::written_size(tenant_id, timeline_id).at(before, disk_consistent_lsn.0)
        ]);

        let snap = TimelineSnapshot {
            loaded_at: (disk_consistent_lsn, init),
            last_record_lsn: disk_consistent_lsn,
            current_exact_logical_size: Some(0x42000),
        };

        snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

        assert_eq!(
            metrics,
            &[
                MetricsKey::written_size_delta(tenant_id, timeline_id)
                    .from_previous_up_to(before, now, 0),
                MetricsKey::written_size(tenant_id, timeline_id).at(now, disk_consistent_lsn.0),
                MetricsKey::timeline_logical_size(tenant_id, timeline_id).at(now, 0x42000)
            ]
        );
    }

    #[test]
    fn startup_collected_timeline_metrics_nth_round_at_same_lsn() {
        let tenant_id = TenantId::generate();
        let timeline_id = TimelineId::generate();

        let [now, just_before, before, init] = time_backwards();

        let now = DateTime::<Utc>::from(now);
        let just_before = DateTime::<Utc>::from(just_before);
        let before = DateTime::<Utc>::from(before);

        let initdb_lsn = Lsn(0x10000);
        let disk_consistent_lsn = Lsn(initdb_lsn.0 * 2);

        let mut metrics = Vec::new();
        let cache = HashMap::from([
            // at t=before was the last time the last_record_lsn changed
            MetricsKey::written_size(tenant_id, timeline_id).at(before, disk_consistent_lsn.0),
            // end time of this event is used for the next ones
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_previous_up_to(
                before,
                just_before,
                0,
            ),
        ]);

        let snap = TimelineSnapshot {
            loaded_at: (disk_consistent_lsn, init),
            last_record_lsn: disk_consistent_lsn,
            current_exact_logical_size: Some(0x42000),
        };

        snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

        assert_eq!(
            metrics,
            &[
                MetricsKey::written_size_delta(tenant_id, timeline_id).from_previous_up_to(
                    just_before,
                    now,
                    0
                ),
                MetricsKey::written_size(tenant_id, timeline_id).at(now, disk_consistent_lsn.0),
                MetricsKey::timeline_logical_size(tenant_id, timeline_id).at(now, 0x42000)
            ]
        );
    }

    #[test]
    fn metric_image_stability() {
        // it is important that these strings stay as they are

        let tenant_id = TenantId::from_array([0; 16]);
        let timeline_id = TimelineId::from_array([0xff; 16]);

        let now = DateTime::parse_from_rfc3339("2023-09-15T00:00:00.123456789Z").unwrap();
        let before = DateTime::parse_from_rfc3339("2023-09-14T00:00:00.123456789Z").unwrap();

        let [now, before] = [DateTime::<Utc>::from(now), DateTime::from(before)];

        let examples = [
            (
                line!(),
                MetricsKey::written_size(tenant_id, timeline_id).at(now, 0),
                r#"{"type":"absolute","time":"2023-09-15T00:00:00.123456789Z","metric":"written_size","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":0,"tenant_id":"00000000000000000000000000000000","timeline_id":"ffffffffffffffffffffffffffffffff"}"#,
            ),
            (
                line!(),
                MetricsKey::written_size_delta(tenant_id, timeline_id)
                    .from_previous_up_to(before, now, 0),
                r#"{"type":"incremental","start_time":"2023-09-14T00:00:00.123456789Z","stop_time":"2023-09-15T00:00:00.123456789Z","metric":"written_data_bytes_delta","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":0,"tenant_id":"00000000000000000000000000000000","timeline_id":"ffffffffffffffffffffffffffffffff"}"#,
            ),
            (
                line!(),
                MetricsKey::timeline_logical_size(tenant_id, timeline_id).at(now, 0),
                r#"{"type":"absolute","time":"2023-09-15T00:00:00.123456789Z","metric":"timeline_logical_size","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":0,"tenant_id":"00000000000000000000000000000000","timeline_id":"ffffffffffffffffffffffffffffffff"}"#,
            ),
            (
                line!(),
                MetricsKey::remote_storage_size(tenant_id).at(now, 0),
                r#"{"type":"absolute","time":"2023-09-15T00:00:00.123456789Z","metric":"remote_storage_size","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":0,"tenant_id":"00000000000000000000000000000000"}"#,
            ),
            (
                line!(),
                MetricsKey::resident_size(tenant_id).at(now, 0),
                r#"{"type":"absolute","time":"2023-09-15T00:00:00.123456789Z","metric":"resident_size","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":0,"tenant_id":"00000000000000000000000000000000"}"#,
            ),
            (
                line!(),
                MetricsKey::synthetic_size(tenant_id).at(now, 1),
                r#"{"type":"absolute","time":"2023-09-15T00:00:00.123456789Z","metric":"synthetic_storage_size","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":1,"tenant_id":"00000000000000000000000000000000"}"#,
            ),
        ];

        let idempotency_key = consumption_metrics::IdempotencyKey::for_tests(now, "1", 0);

        for (line, (key, (kind, value)), expected) in examples {
            let e = consumption_metrics::Event {
                kind,
                metric: key.metric,
                idempotency_key: idempotency_key.to_string(),
                value,
                extra: super::Ids {
                    tenant_id: key.tenant_id,
                    timeline_id: key.timeline_id,
                },
            };
            let actual = serde_json::to_string(&e).unwrap();
            assert_eq!(expected, actual, "example from line {line}");
        }
    }

    fn time_backwards<const N: usize>() -> [std::time::SystemTime; N] {
        let mut times = [std::time::SystemTime::UNIX_EPOCH; N];
        times[0] = std::time::SystemTime::now();
        for behind in 1..N {
            times[behind] = times[0] - std::time::Duration::from_secs(behind as u64);
        }

        times
    }
}
