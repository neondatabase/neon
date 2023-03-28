//! This module implements the pageserver-global disk-usage-based layer eviction task.
//!
//! # Mechanics
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
//! # Eviction Policy
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

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::Context;
use nix::dir::Dir;
use remote_storage::GenericRemoteStorage;
use serde::{Deserialize, Serialize};
use sync_wrapper::SyncWrapper;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn, Instrument};
use utils::serde_percent::Percent;

use crate::{
    config::PageServerConf,
    task_mgr::{self, TaskKind, BACKGROUND_RUNTIME},
    tenant::{self, storage_layer::PersistentLayer, Timeline},
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

    debug!(?usage_pre, "disk usage");

    if !usage_pre.has_pressure() {
        return Ok(IterationOutcome::NoPressure);
    }

    warn!(
        ?usage_pre,
        "running disk usage based eviction due to pressure"
    );

    // Collect list of all layers in the system, sorted in the order that they should
    // be evicted.
    let all_candidates = collect_eviction_candidates(cancel).await?;
    if cancel.is_cancelled() {
        return Ok(IterationOutcome::Cancelled);
    }

    // XXX: Print the whole list, for debbugging
    let now: SystemTime = SystemTime::now();
    for (i, candidate) in all_candidates.iter().enumerate() {
        debug!(
            "cand {}/{}: {}, size {}, at {}, overage {}",
            i,
            all_candidates.len(),
            candidate.layer.local_path().unwrap().display(),
            candidate.layer.file_size(),
            now.duration_since(candidate.last_activity_ts)
                .unwrap()
                .as_micros(),
            candidate.tenant_resident_size_overage
        );
    }

    // phase1: select victims to relieve pressure
    //
    // Walk through the list of candidates, until we have accumulated enough layers to get
    // us back under the pressure threshold. 'usage_planned' is updated so that it tracks
    // how much disk space would be used after evicting all the layers up to the current
    // point in the list. The layers are collected in 'batched', grouped per timeline.
    //
    // If we get far enough in the list that we start to evict layers that are below
    // the tenant's min-resident-size threshold, print a warning, and memorize the disk
    // usage at that point, in 'usage_planned_min_resident_size_respecting'.
    let mut batched: HashMap<_, Vec<Arc<dyn PersistentLayer>>> = HashMap::new();
    let mut min_resident_size_violated = false;
    let mut usage_planned = usage_pre;
    let mut usage_planned_min_resident_size_respecting = None;
    for (i, candidate) in all_candidates.into_iter().enumerate() {
        if !usage_planned.has_pressure() {
            debug!(
                no_candidates_evicted = i,
                "took enough candidates for pressure to be relieved"
            );
            break;
        }

        if !min_resident_size_violated {
            if candidate.tenant_resident_size_overage < 0 {
                warn!(?usage_pre, ?usage_planned, "tenant_min_resident_size-respecting LRU would not relieve pressure, falling back to global LRU at {}", i);
                min_resident_size_violated = true;
                usage_planned_min_resident_size_respecting = Some(usage_planned.clone());
            }
        } else {
            // all layers with overage >= 0 should come first
            assert!(candidate.tenant_resident_size_overage < 0);
        }
        usage_planned.add_available_bytes(candidate.layer.file_size());

        batched
            .entry(TimelineKey(candidate.timeline.clone()))
            .or_default()
            .push(candidate.layer);
    }

    let usage_planned = if min_resident_size_violated {
        PlannedUsage {
            respecting_tenant_min_resident_size: usage_planned_min_resident_size_respecting
                .unwrap(),
            fallback_to_global_lru: Some(usage_planned),
        }
    } else {
        PlannedUsage {
            respecting_tenant_min_resident_size: usage_planned,
            fallback_to_global_lru: None,
        }
    };
    debug!(?usage_planned, "usage planned");

    // phase2: evict victims batched by timeline

    // achieved post-eviction usage according to internal accounting
    let mut usage_assumed = usage_pre;

    let mut evictions_failed = LayerCount::default();
    for (timeline, batch) in batched {
        let tenant_id = timeline.tenant_id;
        let timeline_id = timeline.timeline_id;
        let batch_size = batch.len();

        debug!(%timeline_id, "evicting batch for timeline");

        async {
            let results = timeline.evict_layers(storage, &batch, cancel.clone()).await;

            match results {
                Err(e) => {
                    warn!("failed to evict batch: {:#}", e);
                }
                Ok(results) => {
                    assert_eq!(results.len(), batch.len());
                    for (result, layer) in results.into_iter().zip(batch.iter()) {
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

// Result type of `collect_eviction_candidates`
//
// `collect_eviction_candidates' returns a vector of these, in the preference order
// that they should be evicted.
struct EvictionCandidate {
    timeline: Arc<Timeline>,
    layer: Arc<dyn PersistentLayer>,
    last_activity_ts: SystemTime,
    tenant_resident_size_overage: i64,
}

/// Collect a list of all non-remote layers in the system, from all timelines in all tenants.
///
/// Returns all layers in the order that they should be evicted. The current policy is to
/// first evict layers in global LRU order, but retain at least min_resident_size bytes of
/// data for each tenant. After that, if necessary, we evict the remaining layers, also in
/// global LRU order. A different policy could be implemented by changing the returned order
/// here.
///
/// For each layer, we return its last-activity timestamp, and its "overage" over the
/// tenant's min resident size limit. In other words, 'tenant_resident_size_overage'
/// means: If we evicted this layer, and all the layers of this tenant in the result list
/// before this one, how much would the total size of all the tenant's remaining layers
/// exceed the the tenant's min resident size? Layers that belong to the "reservation",
/// 'tenant_resident_size_overage' is negative.
///
/// For example, imagine that there are two tenants, A and B, with five layers each, a-e.
/// Each layer has size 100, and both tenant's min_resident_size is 150.
/// `collect_eviction_candidates` would return them in this order:
///
/// last_activity_ts    tenant/layer    overage
/// 18:30               A/c        250
/// 19:00               A/b        150
/// 18:29               B/c        250
/// 19:05               B/b        150
/// 20:00               B/a         50
/// 20:03               A/a         50
/// --- min resident size respecting cutoff point  ---
/// 20:30               A/d        -50
/// 20:40               B/d        -50
/// 20:45               B/e       -150
/// 20:58               A/e       -150
///
/// If the task is cancelled by the `cancel` token, returns an empty Vec. The caller
/// should check for `cancel.is_cancelled`.
///
async fn collect_eviction_candidates(
    cancel: &CancellationToken,
) -> anyhow::Result<Vec<EvictionCandidate>> {
    // get a snapshot of the list of tenants
    let tenants = tenant::mgr::list_tenants()
        .await
        .context("get list of tenants")?;

    let mut candidates: Vec<EvictionCandidate> = Vec::new();
    for (tenant_id, _state) in &tenants {
        if cancel.is_cancelled() {
            return Ok(candidates);
        }
        let tenant = match tenant::mgr::get_tenant(*tenant_id, true).await {
            Ok(tenant) => tenant,
            Err(e) => {
                // this can happen if tenant has lifecycle transition after we fetched it
                debug!("failed to get tenant: {e:#}");
                continue;
            }
        };

        // collect layers from all timelines in this tenant
        let mut tenant_candidates = Vec::new();
        for tl in tenant.list_timelines() {
            if !tl.is_active() {
                continue;
            }
            let info = tl.get_local_layers_for_disk_usage_eviction();
            debug!(timeline_id=%tl.timeline_id, "timeline resident layers count: {}", info.resident_layers.len());
            tenant_candidates.extend(
                info.resident_layers
                    .into_iter()
                    .map(|layer_infos| (tl.clone(), layer_infos)),
            );

            if cancel.is_cancelled() {
                return Ok(candidates);
            }
        }

        // sort this tenant's layers by last_activity_ts, calculate the "overage" for each
        // layer, and add them to the result.
        tenant_candidates.sort_unstable_by_key(|(_, layer_info)| layer_info.last_activity_ts);

        let min_resident_size = if let Some(s) = tenant.get_min_resident_size_override() {
            info!(
                "using overridden min resident size {} for tenant {}",
                s,
                tenant.tenant_id()
            );
            s
        } else {
            // By default, use the size of the largest resident layer
            let s = tenant_candidates
                .iter()
                .map(|(_, layer_info)| layer_info.file_size())
                .max()
                .unwrap_or(0);
            info!(
                "using max layer size {} for tenant {}",
                s,
                tenant.tenant_id()
            );
            s
        };

        let mut cumulative_resident_size_overage: i128 = -i128::from(min_resident_size);
        for (timeline, layer_info) in tenant_candidates.into_iter() {
            let file_size = layer_info.file_size();
            candidates.push(EvictionCandidate {
                timeline,
                last_activity_ts: layer_info.last_activity_ts,
                layer: layer_info.layer,
                tenant_resident_size_overage: cumulative_resident_size_overage
                    .clamp(i64::MIN as i128, i64::MAX as i128)
                    as i64,
            });
            cumulative_resident_size_overage += i128::from(file_size);
        }
    }

    // Final sort. Layers above their tenant's min-resident size threshold first, in
    // LRU order, and then all the rest also in LRU order
    candidates.sort_unstable_by_key(|candidate| {
        (
            candidate.tenant_resident_size_overage < 0,
            candidate.last_activity_ts,
        )
    });

    Ok(candidates)
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
