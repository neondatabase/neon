//! Periodically collect consumption metrics for all active tenants
//! and push them to a HTTP endpoint.
use crate::context::{DownloadBehavior, RequestContext};
use crate::task_mgr::{self, TaskKind, BACKGROUND_RUNTIME};
use crate::tenant::{mgr, LogicalSizeCalculationCause};
use chrono::{DateTime, Utc};
use consumption_metrics::EventType;
use futures::stream::StreamExt;
use pageserver_api::models::TenantState;
use reqwest::Url;
use serde::Serialize;
use serde_with::{serde_as, DisplayFromStr};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::*;
use utils::id::{NodeId, TenantId, TimelineId};
use utils::lsn::Lsn;

use anyhow::Context;

mod disk_cache;
mod metrics;
mod upload;

const DEFAULT_HTTP_REPORTING_TIMEOUT: Duration = Duration::from_secs(60);

#[serde_as]
#[derive(Serialize, serde::Deserialize, Debug, Clone, Copy)]
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
#[serde_with::serde_as]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
struct MetricsKey {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    tenant_id: TenantId,

    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    timeline_id: Option<TimelineId>,

    metric: Name,
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

    fn key(&self) -> &MetricsKey {
        &self.0
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
            metric: Name::WrittenSize,
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
            metric: Name::WrittenSizeDelta,
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
            metric: Name::LogicalSize,
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
            metric: Name::RemoteSize,
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
            metric: Name::ResidentSize,
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
            metric: Name::SyntheticSize,
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
    local_disk_storage: PathBuf,
    ctx: RequestContext,
) -> anyhow::Result<()> {
    if _cached_metric_collection_interval != Duration::ZERO {
        tracing::warn!(
            "cached_metric_collection_interval is no longer used, please set it to zero."
        )
    }

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

    let final_path: Arc<PathBuf> = Arc::new(local_disk_storage);

    let cancel = task_mgr::shutdown_token();
    let restore_and_reschedule = restore_and_reschedule(&final_path, metric_collection_interval);

    let mut cached_metrics = tokio::select! {
        _ = cancel.cancelled() => return Ok(()),
        ret = restore_and_reschedule => ret,
    };

    // define client here to reuse it for all requests
    let client = reqwest::ClientBuilder::new()
        .timeout(DEFAULT_HTTP_REPORTING_TIMEOUT)
        .build()
        .expect("Failed to create http client with timeout");

    let node_id = node_id.to_string();

    // reminder: ticker is ready immediatedly
    let mut ticker = tokio::time::interval(metric_collection_interval);

    loop {
        let tick_at = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            tick_at = ticker.tick() => tick_at,
        };

        // these are point in time, with variable "now"
        let metrics = collect_all_metrics(&cached_metrics, &ctx).await;

        if metrics.is_empty() {
            continue;
        }

        let metrics = Arc::new(metrics);

        // why not race cancellation here? because we are one of the last tasks, and if we are
        // already here, better to try to flush the new values.

        let flush = async {
            match disk_cache::flush_metrics_to_disk(&metrics, &final_path).await {
                Ok(()) => {
                    tracing::debug!("flushed metrics to disk");
                }
                Err(e) => {
                    // idea here is that if someone creates a directory as our final_path, then they
                    // might notice it from the logs before shutdown and remove it
                    tracing::error!("failed to persist metrics to {final_path:?}: {e:#}");
                }
            }
        };

        let upload = async {
            let res = upload::upload_metrics(
                &client,
                metric_collection_endpoint,
                &cancel,
                &node_id,
                &metrics,
                &mut cached_metrics,
            )
            .await;
            if let Err(e) = res {
                // serialization error which should never happen
                tracing::error!("failed to upload due to {e:#}");
            }
        };

        // let these run concurrently
        let (_, _) = tokio::join!(flush, upload);

        crate::tenant::tasks::warn_when_period_overrun(
            tick_at.elapsed(),
            metric_collection_interval,
            "consumption_metrics_collect_metrics",
        );
    }
}

/// Called on the first iteration in an attempt to join the metric uploading schedule from previous
/// pageserver session. Pageserver is supposed to upload at intervals regardless of restarts.
///
/// Cancellation safe.
async fn restore_and_reschedule(
    final_path: &Arc<PathBuf>,
    metric_collection_interval: Duration,
) -> Cache {
    let (cached, earlier_metric_at) = match disk_cache::read_metrics_from_disk(final_path.clone())
        .await
    {
        Ok(found_some) => {
            // there is no min needed because we write these sequentially in
            // collect_all_metrics
            let earlier_metric_at = found_some
                .iter()
                .map(|(_, (et, _))| et.recorded_at())
                .copied()
                .next();

            let cached = found_some.into_iter().collect::<Cache>();

            (cached, earlier_metric_at)
        }
        Err(e) => {
            use std::io::{Error, ErrorKind};

            let root = e.root_cause();

            let maybe_ioerr = root.downcast_ref::<Error>();
            let is_not_found = maybe_ioerr.is_some_and(|e| e.kind() == ErrorKind::NotFound);

            if !is_not_found {
                tracing::info!("failed to read any previous metrics from {final_path:?}: {e:#}");
            }

            (HashMap::new(), None)
        }
    };

    if let Some(earlier_metric_at) = earlier_metric_at {
        let earlier_metric_at: SystemTime = earlier_metric_at.into();

        let error = reschedule(earlier_metric_at, metric_collection_interval).await;

        if let Some(error) = error {
            if error.as_secs() >= 60 {
                tracing::info!(
                    error_ms = error.as_millis(),
                    "startup scheduling error due to restart"
                )
            }
        }
    }

    cached
}

async fn reschedule(
    earlier_metric_at: SystemTime,
    metric_collection_interval: Duration,
) -> Option<Duration> {
    let now = SystemTime::now();
    match now.duration_since(earlier_metric_at) {
        Ok(from_last_send) if from_last_send < metric_collection_interval => {
            let sleep_for = metric_collection_interval - from_last_send;

            let deadline = std::time::Instant::now() + sleep_for;

            tokio::time::sleep_until(deadline.into()).await;

            let now = std::time::Instant::now();

            // executor threads might be busy, add extra measurements
            Some(if now < deadline {
                deadline - now
            } else {
                now - deadline
            })
        }
        Ok(from_last_send) => Some(from_last_send.saturating_sub(metric_collection_interval)),
        Err(_) => {
            tracing::warn!(
                ?now,
                ?earlier_metric_at,
                "oldest recorded metric is in future; first values will come out with inconsistent timestamps"
            );
            earlier_metric_at.duration_since(now).ok()
        }
    }
}

async fn collect_all_metrics(cached_metrics: &Cache, ctx: &RequestContext) -> Vec<RawMetric> {
    let started_at = std::time::Instant::now();

    let tenants = match mgr::list_tenants().await {
        Ok(tenants) => tenants,
        Err(err) => {
            error!("failed to list tenants: {:?}", err);
            return vec![];
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

    let res = collect(tenants, cached_metrics, ctx).await;

    tracing::info!(
        elapsed_ms = started_at.elapsed().as_millis(),
        total = res.len(),
        "collected metrics"
    );

    res
}

async fn collect<S>(tenants: S, cache: &Cache, ctx: &RequestContext) -> Vec<RawMetric>
where
    S: futures::stream::Stream<Item = (TenantId, Arc<crate::tenant::Tenant>)>,
{
    let mut current_metrics: Vec<RawMetric> = Vec::new();

    let mut tenants = std::pin::pin!(tenants);

    while let Some((tenant_id, tenant)) = tenants.next().await {
        let mut tenant_resident_size = 0;

        for timeline in tenant.list_timelines() {
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

        let snap = TenantSnapshot::collect(&tenant, tenant_resident_size);
        snap.to_metrics(tenant_id, Utc::now(), cache, &mut current_metrics);
    }

    current_metrics
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

    fn to_metrics(
        &self,
        tenant_id: TenantId,
        now: DateTime<Utc>,
        cached: &Cache,
        metrics: &mut Vec<RawMetric>,
    ) {
        let remote_size = MetricsKey::remote_storage_size(tenant_id).at(now, self.remote_size);

        let resident_size = MetricsKey::resident_size(tenant_id).at(now, self.resident_size);

        let synthetic_size = {
            let factory = MetricsKey::synthetic_size(tenant_id);
            let mut synthetic_size = self.synthetic_size;

            if synthetic_size == 0 {
                if let Some((_, value)) = cached.get(factory.key()) {
                    // use the latest value from previous session
                    synthetic_size = *value;
                }
            }

            if synthetic_size != 0 {
                // only send non-zeroes because otherwise these show up as errors in logs
                Some(factory.at(now, synthetic_size))
            } else {
                None
            }
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

        let up_to = now;

        if let Some(delta) = written_size_now.1.checked_sub(prev.1) {
            let key_value = written_size_delta_key.from_previous_up_to(prev.0, up_to, delta);
            // written_size_delta
            metrics.push(key_value);
            // written_size
            metrics.push((key, written_size_now));
        } else {
            // the cached value was ahead of us, report zero until we've caught up
            metrics.push(written_size_delta_key.from_previous_up_to(prev.0, up_to, 0));
            // the cached value was ahead of us, report the same until we've caught up
            metrics.push((key, (written_size_now.0, prev.1)));
        }

        {
            let factory = MetricsKey::timeline_logical_size(tenant_id, timeline_id);
            let current_or_previous = self
                .current_exact_logical_size
                .or_else(|| cache.get(factory.key()).map(|(_, val)| *val));

            if let Some(size) = current_or_previous {
                metrics.push(factory.at(now, size));
            }
        }
    }
}

/// Caclculate synthetic size for each active tenant
async fn calculate_synthetic_size_worker(
    synthetic_size_calculation_interval: Duration,
    ctx: &RequestContext,
) -> anyhow::Result<()> {
    info!("starting calculate_synthetic_size_worker");

    // reminder: ticker is ready immediatedly
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
