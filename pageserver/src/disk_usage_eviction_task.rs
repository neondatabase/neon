//! This module implements the pageserver-global disk-usage-based layer eviction task.
//!
//! Function `launch_disk_usage_global_eviction_task` starts a pageserver-global background
//! loop that evicts layers in response to a shortage of available bytes
//! in the $repo/tenants directory's filesystem.
//!
//! The loop runs periodically at a configurable `period`.
//!
//! Each loop iteration uses `statvfs` to determine filesystem-level space usage.
//! It compares the returned usage data against two different types of thresholds.
//! The iteration tries to evict layers until app-internal accounting says we should be below the thresholds.
//! We cross-check this internal accounting with the real world by making another `statvfs` at the end of the iteration.
//! We're good if that second statvfs shows that we're _actually_ below the configured thresholds.
//! If we're still above one or more thresholds, we emit a warning log message, leaving it to the operator to investigate further.
//!
//! There are two thresholds:
//! `max_usage_pct` is the relative available space, expressed in percent of the total filesystem space.
//! If the actual usage is higher, the threshold is exceeded.
//! `min_avail_bytes` is the absolute available space in bytes.
//! If the actual usage is lower, the threshold is exceeded.
//!
//! The iteration evicts layers in LRU fashion, but, with a weak reservation per tenant.
//! The reservation is to keep the most recently accessed X bytes per tenant resident.
//! All layers that don't make the cut are put on a list and become eviction candidates.
//! We evict until we're below the two thresholds.
//!
//! If the above strategy wouldn't free enough space, we fall back to global LRU right away,
//! not respecting any per-tenant reservations.
//!
//! This value for the per-tenant reservation is referred to as `tenant_min_resident_size`
//! throughout the code, but, no actual variable carries that name.
//! The per-tenant default value is the `max(tenant's layer file sizes, regardless of local or remote)`.
//! The idea is to allow at least one layer to be resident per tenant, to ensure it can make forward progress
//! during page reconstruction.
//! An alternative default for all tenants can be specified in the `tenant_config` section of the config.
//! Lastly, each tenant can have an override in their respectice tenant config (`min_resident_size_override`).

// Implementation notes:
// - The `#[allow(dead_code)]` above various structs are to suppress warnings about only the Debug impl
//   reading these fields. We use the Debug impl for semi-structured logging, though.

use std::{collections::HashMap, ops::ControlFlow, sync::Arc, time::Duration};

use anyhow::Context;
use nix::dir::Dir;
use remote_storage::GenericRemoteStorage;
use serde::{Deserialize, Serialize};
use sync_wrapper::SyncWrapper;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn, Instrument};
use utils::{id::TenantId, serde_percent::Percent};

use crate::{
    config::PageServerConf,
    task_mgr::{self, TaskKind, BACKGROUND_RUNTIME},
    tenant::{self, LocalLayerInfoForDiskUsageEviction, Timeline},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskUsageEvictionTaskConfig {
    pub max_usage_pct: Percent,
    pub min_avail_bytes: u64,
    #[serde(with = "humantime_serde")]
    pub period: Duration,
}

#[derive(Default)]
pub struct State {
    /// Exclude http requests and background task from running at the same time.
    mutex: tokio::sync::Mutex<()>,
}

pub fn launch_disk_usage_global_eviction_task(
    conf: &'static PageServerConf,
    storage: GenericRemoteStorage,
    state: Arc<State>,
) -> anyhow::Result<()> {
    let Some(task_config) = &conf.disk_usage_based_eviction else {
        info!("disk usage based eviction task not configured");
        return Ok(());
    };

    let tenants_dir_fd = {
        let tenants_path = conf.tenants_path();
        nix::dir::Dir::open(
            &tenants_path,
            nix::fcntl::OFlag::O_DIRECTORY,
            nix::sys::stat::Mode::empty(),
        )
        .with_context(|| format!("open tenants_path {tenants_path:?}"))?
    };

    info!("launching disk usage based eviction task");

    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::DiskUsageEviction,
        None,
        None,
        "disk usage based eviction",
        false,
        async move {
            disk_usage_eviction_task(
                &state,
                task_config,
                storage,
                tenants_dir_fd,
                task_mgr::shutdown_token(),
            )
            .await;
            info!("disk usage based eviction task finishing");
            Ok(())
        },
    );

    Ok(())
}

#[instrument(skip_all)]
async fn disk_usage_eviction_task(
    state: &State,
    task_config: &DiskUsageEvictionTaskConfig,
    storage: GenericRemoteStorage,
    tenants_dir_fd: Dir,
    cancel: CancellationToken,
) {
    // nix::dir::Dir is Send but not Sync.
    // One would think that that is sufficient, but rustc complains that the &tenants_dir_fd
    // that we pass to disk_usage_eviction_iteration below will outlive the .await;
    // The reason is that the &tenants_dir_fd is not sync because of stdlib-enforced axiom
    //  T: Sync <=> &T: Send
    // The solution is to use SyncWrapper, which, by owning the tenants_dir_fd, can impl Sync.
    let mut tenants_dir_fd = SyncWrapper::new(tenants_dir_fd);

    use crate::tenant::tasks::random_init_delay;
    {
        if random_init_delay(task_config.period, &cancel)
            .await
            .is_err()
        {
            info!("shutting down");
            return;
        }
    }

    let mut iteration_no = 0;
    loop {
        iteration_no += 1;
        let start = Instant::now();

        async {
            let res = disk_usage_eviction_task_iteration(
                state,
                task_config,
                &storage,
                &mut tenants_dir_fd,
                &cancel,
            )
            .await;

            match res {
                Ok(()) => {}
                Err(e) => {
                    // these stat failures are expected to be very rare
                    warn!("iteration failed, unexpected error: {e:#}");
                }
            }
        }
        .instrument(tracing::info_span!("iteration", iteration_no))
        .await;

        let sleep_until = start + task_config.period;
        tokio::select! {
            _ = tokio::time::sleep_until(sleep_until) => {},
            _ = cancel.cancelled() => {
                info!("shutting down");
                break
            }
        }
    }
}

pub trait Usage: Clone + Copy + std::fmt::Debug {
    fn has_pressure(&self) -> bool;
    fn add_available_bytes(&mut self, bytes: u64);
}

async fn disk_usage_eviction_task_iteration(
    state: &State,
    task_config: &DiskUsageEvictionTaskConfig,
    storage: &GenericRemoteStorage,
    tenants_dir_fd: &mut SyncWrapper<Dir>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let usage_pre = filesystem_level_usage::get(tenants_dir_fd, task_config)
        .context("get filesystem-level disk usage before evictions")?;
    let res = disk_usage_eviction_task_iteration_impl(state, storage, usage_pre, cancel).await;
    match res {
        Ok(outcome) => {
            debug!(?outcome, "disk_usage_eviction_iteration finished");
            match outcome {
                IterationOutcome::NoPressure | IterationOutcome::Cancelled => {
                    // nothing to do, select statement below will handle things
                }
                IterationOutcome::Finished(outcome) => {
                    // Verify with statvfs whether we made any real progress
                    let after = filesystem_level_usage::get(tenants_dir_fd, task_config)
                        // It's quite unlikely to hit the error here. Keep the code simple and bail out.
                        .context("get filesystem-level disk usage after evictions")?;

                    debug!(?after, "disk usage");

                    if after.has_pressure() {
                        // Don't bother doing an out-of-order iteration here now.
                        // In practice, the task period is set to a value in the tens-of-seconds range,
                        // which will cause another iteration to happen soon enough.
                        // TODO: deltas between the three different usages would be helpful,
                        // consider MiB, GiB, TiB
                        warn!(?outcome, ?after, "disk usage still high");
                    } else {
                        info!(?outcome, ?after, "disk usage pressure relieved");
                    }
                }
            }
        }
        Err(e) => {
            error!("disk_usage_eviction_iteration failed: {:#}", e);
        }
    }

    Ok(())
}

#[derive(Debug, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum IterationOutcome<U> {
    NoPressure,
    Cancelled,
    Finished(IterationOutcomeFinished<U>),
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
pub struct IterationOutcomeFinished<U> {
    /// The actual usage observed before we started the iteration.
    before: U,
    /// The expected value for `after`, according to internal accounting, after phase 1.
    planned: PlannedUsage<U>,
    /// The outcome of phase 2, where we actually do the evictions.
    ///
    /// If all layers that phase 1 planned to evict _can_ actually get evicted, this will
    /// be the same as `planned`.
    assumed: AssumedUsage<U>,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
struct AssumedUsage<U> {
    /// The expected value for `after`, after phase 2.
    projected_after: U,
    /// The layers we failed to evict during phase 2.
    failed: LayerCount,
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
struct PlannedUsage<U> {
    respecting_tenant_min_resident_size: U,
    fallback_to_global_lru: Option<U>,
}

#[allow(dead_code)]
#[derive(Debug, Default, Serialize)]
struct LayerCount {
    file_sizes: u64,
    count: usize,
}

#[allow(clippy::needless_late_init)]
pub async fn disk_usage_eviction_task_iteration_impl<U: Usage>(
    state: &State,
    storage: &GenericRemoteStorage,
    usage_pre: U,
    cancel: &CancellationToken,
) -> anyhow::Result<IterationOutcome<U>> {
    let _g = state
        .mutex
        .try_lock()
        .map_err(|_| anyhow::anyhow!("iteration is already executing"))?;

    // planned post-eviction usage
    let mut usage_planned_min_resident_size_respecting = usage_pre;
    let mut usage_planned_global_lru = None;
    // achieved post-eviction usage according to internal accounting
    let mut usage_assumed = usage_pre;
    // actual usage read after batched evictions

    debug!(?usage_pre, "disk usage");

    if !usage_pre.has_pressure() {
        return Ok(IterationOutcome::NoPressure);
    }

    warn!(
        ?usage_pre,
        "running disk usage based eviction due to pressure"
    );

    let mut lru_candidates: Vec<(_, LocalLayerInfoForDiskUsageEviction)> = Vec::new();

    // get a snapshot of the list of tenants
    let tenants = tenant::mgr::list_tenants()
        .await
        .context("get list of tenants")?;

    {
        let mut tmp = Vec::new();
        for (tenant_id, _state) in &tenants {
            let flow = extend_lru_candidates(
                Mode::RespectTenantMinResidentSize,
                *tenant_id,
                &mut lru_candidates,
                &mut tmp,
                cancel,
            )
            .await;

            if let ControlFlow::Break(()) = flow {
                return Ok(IterationOutcome::Cancelled);
            }

            assert!(tmp.is_empty(), "tmp has to be fully drained each iteration");
        }
    }

    if cancel.is_cancelled() {
        return Ok(IterationOutcome::Cancelled);
    }

    // phase1: select victims to relieve pressure
    lru_candidates.sort_unstable_by_key(|(_, layer)| layer.last_activity_ts);
    let mut batched: HashMap<_, Vec<LocalLayerInfoForDiskUsageEviction>> = HashMap::new();
    for (i, (timeline, layer)) in lru_candidates.into_iter().enumerate() {
        if !usage_planned_min_resident_size_respecting.has_pressure() {
            debug!(
                no_candidates_evicted = i,
                "took enough candidates for pressure to be relieved"
            );
            break;
        }

        usage_planned_min_resident_size_respecting.add_available_bytes(layer.file_size());

        batched
            .entry(TimelineKey(timeline.clone()))
            .or_default()
            .push(layer);
    }
    // If we can't relieve pressure while respecting tenant_min_resident_size, fall back to global LRU.
    if usage_planned_min_resident_size_respecting.has_pressure() {
        // NB: tests depend on parts of this log message
        warn!(?usage_pre, ?usage_planned_min_resident_size_respecting, "tenant_min_resident_size-respecting LRU would not relieve pressure, falling back to global LRU");
        batched.clear();
        let mut usage_planned = usage_pre;
        let mut global_lru_candidates = Vec::new();
        let mut tmp = Vec::new();
        for (tenant_id, _state) in &tenants {
            let flow = extend_lru_candidates(
                Mode::GlobalLru,
                *tenant_id,
                &mut global_lru_candidates,
                &mut tmp,
                cancel,
            )
            .await;

            if let ControlFlow::Break(()) = flow {
                return Ok(IterationOutcome::Cancelled);
            }

            assert!(tmp.is_empty(), "tmp has to be fully drained each iteration");
        }
        global_lru_candidates.sort_unstable_by_key(|(_, layer)| layer.last_activity_ts);
        for (timeline, layer) in global_lru_candidates {
            usage_planned.add_available_bytes(layer.file_size());
            batched
                .entry(TimelineKey(timeline.clone()))
                .or_default()
                .push(layer);
            if cancel.is_cancelled() {
                return Ok(IterationOutcome::Cancelled);
            }
        }
        usage_planned_global_lru = Some(usage_planned);
    }
    let usage_planned = PlannedUsage {
        respecting_tenant_min_resident_size: usage_planned_min_resident_size_respecting,
        fallback_to_global_lru: usage_planned_global_lru,
    };

    debug!(?usage_planned, "usage planned");

    // phase2: evict victims batched by timeline
    let mut batch = Vec::new();
    let mut evictions_failed = LayerCount::default();
    for (timeline, layers) in batched {
        let tenant_id = timeline.tenant_id;
        let timeline_id = timeline.timeline_id;

        batch.clear();
        batch.extend(layers.iter().map(|x| &x.layer).cloned());
        let batch_size = batch.len();

        debug!(%timeline_id, "evicting batch for timeline");

        async {
            let results = timeline.evict_layers(storage, &batch, cancel.clone()).await;

            match results {
                Err(e) => {
                    warn!("failed to evict batch: {:#}", e);
                }
                Ok(results) => {
                    assert_eq!(results.len(), layers.len());
                    for (result, layer) in results.into_iter().zip(layers.iter()) {
                        match result {
                            Some(Ok(true)) => {
                                usage_assumed.add_available_bytes(layer.file_size());
                            }
                            Some(Ok(false)) => {
                                // this is:
                                // - Replacement::{NotFound, Unexpected}
                                // - it cannot be is_remote_layer, filtered already
                                evictions_failed.file_sizes += layer.file_size();
                                evictions_failed.count += 1;
                            }
                            None => {
                                assert!(cancel.is_cancelled());
                                return;
                            }
                            Some(Err(e)) => {
                                // we really shouldn't be getting this, precondition failure
                                error!("failed to evict layer: {:#}", e);
                            }
                        }
                    }
                }
            }
        }
        .instrument(tracing::info_span!("evict_batch", %tenant_id, %timeline_id, batch_size))
        .await;

        if cancel.is_cancelled() {
            return Ok(IterationOutcome::Cancelled);
        }
    }

    Ok(IterationOutcome::Finished(IterationOutcomeFinished {
        before: usage_pre,
        planned: usage_planned,
        assumed: AssumedUsage {
            projected_after: usage_assumed,
            failed: evictions_failed,
        },
    }))
}

/// Different modes of gathering tenant's least recently used layers.
#[derive(Debug)]
enum Mode {
    /// Add all but the most recently used `min_resident_size` worth of layers to the candidates
    /// list.
    ///
    /// `min_resident_size` defaults to maximum layer file size of the tenant. This ensures that
    /// the tenant will always have one layer resident. If we cannot compute `min_resident_size`
    /// accurately because metadata is missing we use hardcoded constant. `min_resident_size` can
    /// be overridden per tenant for important tenants.
    RespectTenantMinResidentSize,
    /// Consider all layer files from all tenants in LRU order.
    ///
    /// This is done if the `min_resident_size` respecting does not relieve pressure.
    GlobalLru,
}

#[instrument(skip_all, fields(?mode, %tenant_id))]
async fn extend_lru_candidates(
    mode: Mode,
    tenant_id: TenantId,
    lru_candidates: &mut Vec<(Arc<Timeline>, LocalLayerInfoForDiskUsageEviction)>,
    scratch: &mut Vec<(Arc<Timeline>, LocalLayerInfoForDiskUsageEviction)>,
    cancel: &CancellationToken,
) -> ControlFlow<()> {
    debug!("begin");

    let tenant = match tenant::mgr::get_tenant(tenant_id, true).await {
        Ok(tenant) => tenant,
        Err(e) => {
            // this can happen if tenant has lifecycle transition after we fetched it
            debug!("failed to get tenant: {e:#}");
            return ControlFlow::Continue(());
        }
    };

    if cancel.is_cancelled() {
        return ControlFlow::Break(());
    }

    // If one of the timelines becomes `!is_active()` during the iteration,
    // for example because we're shutting down, then `max_layer_size` can be too small.
    // That's OK. This code only runs under a disk pressure situation, and being
    // a little unfair to tenants during shutdown in such a situation is tolerable.
    let mut max_layer_size = 0;
    for tl in tenant.list_timelines() {
        if !tl.is_active() {
            continue;
        }
        let info = tl.get_local_layers_for_disk_usage_eviction();
        debug!(timeline_id=%tl.timeline_id, "timeline resident layers count: {}", info.resident_layers.len());
        scratch.extend(
            info.resident_layers
                .into_iter()
                .map(|layer_infos| (tl.clone(), layer_infos)),
        );
        max_layer_size = max_layer_size.max(info.max_layer_size.unwrap_or(0));

        if cancel.is_cancelled() {
            return ControlFlow::Break(());
        }
    }

    let min_resident_size = match mode {
        Mode::GlobalLru => {
            lru_candidates.append(scratch);
            return ControlFlow::Continue(());
        }
        Mode::RespectTenantMinResidentSize => tenant
            .get_min_resident_size_override()
            .unwrap_or(max_layer_size),
    };

    scratch.sort_unstable_by_key(|(_, layer_info)| layer_info.last_activity_ts);

    let mut current: u64 = scratch.iter().map(|(_, layer)| layer.file_size()).sum();
    for (tl, layer) in scratch.drain(..) {
        if cancel.is_cancelled() {
            return ControlFlow::Break(());
        }
        if current <= min_resident_size {
            break;
        }
        current -= layer.file_size();
        debug!(?layer, "adding layer to lru_candidates");
        lru_candidates.push((tl, layer));
    }

    ControlFlow::Continue(())
}

struct TimelineKey(Arc<Timeline>);

impl PartialEq for TimelineKey {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for TimelineKey {}

impl std::hash::Hash for TimelineKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

impl std::ops::Deref for TimelineKey {
    type Target = Timeline;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

mod filesystem_level_usage {
    use anyhow::Context;
    use nix::{
        dir::Dir,
        sys::statvfs::{self, Statvfs},
    };
    use sync_wrapper::SyncWrapper;

    use super::DiskUsageEvictionTaskConfig;

    #[derive(Debug, Clone, Copy)]
    #[allow(dead_code)]
    pub struct Usage<'a> {
        config: &'a DiskUsageEvictionTaskConfig,

        /// Filesystem capacity
        total_bytes: u64,
        /// Free filesystem space
        avail_bytes: u64,
    }

    impl super::Usage for Usage<'_> {
        fn has_pressure(&self) -> bool {
            let usage_pct =
                (100.0 * (1.0 - ((self.avail_bytes as f64) / (self.total_bytes as f64)))) as u64;

            let pressures = [
                (
                    "min_avail_bytes",
                    self.avail_bytes < self.config.min_avail_bytes,
                ),
                (
                    "max_usage_pct",
                    usage_pct > self.config.max_usage_pct.get() as u64,
                ),
            ];

            pressures.into_iter().any(|(_, has_pressure)| has_pressure)
        }

        fn add_available_bytes(&mut self, bytes: u64) {
            self.avail_bytes += bytes;
        }
    }

    pub fn get<'a>(
        tenants_dir_fd: &mut SyncWrapper<Dir>,
        config: &'a DiskUsageEvictionTaskConfig,
    ) -> anyhow::Result<Usage<'a>> {
        let stat: Statvfs = statvfs::fstatvfs(tenants_dir_fd.get_mut())
            .context("statvfs failed, presumably directory got unlinked")?;

        // https://unix.stackexchange.com/a/703650
        let blocksize = if stat.fragment_size() > 0 {
            stat.fragment_size()
        } else {
            stat.block_size()
        };

        // use blocks_available (b_avail) since, pageserver runs as unprivileged user
        let avail_bytes = stat.blocks_available() * blocksize;
        let total_bytes = stat.blocks() * blocksize;

        Ok(Usage {
            config,
            total_bytes,
            avail_bytes,
        })
    }
}
