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
//! The iteration evicts layers in LRU fashion with a reservation of up to `tenant_min_resident_size` bytes of the most recent layers per tenant.
//! The layers not part of the per-tenant reservation are evicted least-recently-used first until we're below all thresholds.
use std::{
    collections::{hash_map::Entry, HashMap},
    ops::ControlFlow,
    time::Duration,
};

use anyhow::Context;
use nix::dir::Dir;
use remote_storage::GenericRemoteStorage;
use serde::{Deserialize, Serialize};
use sync_wrapper::SyncWrapper;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, info_span, instrument, warn, Instrument};
use utils::id::{TenantId, TimelineId};

use crate::{
    config::PageServerConf,
    task_mgr::{self, TaskKind, BACKGROUND_RUNTIME},
    tenant::{self, LocalLayerInfoForDiskUsageEviction},
};

use self::filesystem_level_usage::{Usage, UsageWithPressure};

fn deserialize_pct_0_to_100<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let v: u64 = serde::de::Deserialize::deserialize(deserializer)?;
    if v > 100 {
        return Err(serde::de::Error::custom(
            "must be an integer between 0 and 100",
        ));
    }
    Ok(v)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskUsageEvictionTaskConfig {
    #[serde(deserialize_with = "deserialize_pct_0_to_100")]
    pub max_usage_pct: u64,
    pub min_avail_bytes: u64,
    pub tenant_min_resident_size: u64,
    #[serde(with = "humantime_serde")]
    pub period: Duration,
}

pub fn launch_disk_usage_global_eviction_task(
    conf: &'static PageServerConf,
    _: GenericRemoteStorage,
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
            disk_usage_eviction_task(task_config, tenants_dir_fd, task_mgr::shutdown_token()).await;
            info!("disk usage based eviction task finishing");
            Ok(())
        },
    );

    Ok(())
}

#[instrument(skip_all)]
async fn disk_usage_eviction_task(
    task_config: &DiskUsageEvictionTaskConfig,
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
        match disk_usage_eviction_task_iteration(
            task_config,
            &mut tenants_dir_fd,
            &cancel,
            iteration_no,
        )
        .await
        {
            ControlFlow::Continue(()) => continue,
            ControlFlow::Break(()) => break,
        }
    }
}

#[instrument(skip_all, fields(iteration_no))]
async fn disk_usage_eviction_task_iteration(
    task_config: &DiskUsageEvictionTaskConfig,
    tenants_dir_fd: &mut SyncWrapper<Dir>,
    cancel: &CancellationToken,
    iteration_no: u64,
) -> ControlFlow<()> {
    let start = Instant::now();
    let res = disk_usage_eviction_task_iteration_impl(task_config, tenants_dir_fd, cancel).await;
    match res {
        Ok(outcome) => {
            debug!(?outcome, "disk_usage_eviction_iteration finished");
            match outcome {
                IterationOutcome::NoPressure | IterationOutcome::Cancelled => {
                    // nothing to do, select statement below will handle things
                }
                IterationOutcome::Finished(outcome) => {
                    if outcome.after_actual.has_pressure() {
                        // Don't bother doing an out-of-order iteration here now.
                        // In practice, the task period is set to a value in the tens-of-seconds range,
                        // which will cause another iteration to happen soon enough.
                        warn!(?outcome, "disk usage still high");
                    } else {
                        info!(?outcome, "disk usage pressure relieved");
                    }
                }
            }
        }
        Err(e) => {
            error!("disk_usage_eviction_iteration failed: {:#}", e);
        }
    }

    let sleep_until = start + task_config.period;
    tokio::select! {
        _ = tokio::time::sleep_until(sleep_until) => ControlFlow::Continue(()),
        _ = cancel.cancelled() => {
            info!("shutting down");
            ControlFlow::Break(())
        }
    }
}

#[derive(Debug)]
enum IterationOutcome {
    NoPressure,
    Cancelled,
    Finished(IterationOutcomeFinished),
}

// The `#[allow(dead_code)]` is to suppress warnings about only the Debug impl reading these fields.
// We use the Debug impl for logging, so, it's allright.
#[allow(dead_code)]
#[derive(Debug)]
struct IterationOutcomeFinished {
    before: UsageWithPressure,
    after_actual: UsageWithPressure,
    after_expected: Usage,
}

async fn disk_usage_eviction_task_iteration_impl(
    task_config: &DiskUsageEvictionTaskConfig,
    tenants_dir_fd: &mut SyncWrapper<Dir>,
    cancel: &CancellationToken,
) -> anyhow::Result<IterationOutcome> {
    let usage_pre = filesystem_level_usage::get(tenants_dir_fd)?.with_pressure(task_config);

    debug!(?usage_pre, "disk usage");

    if !usage_pre.has_pressure() {
        return Ok(IterationOutcome::NoPressure);
    }

    warn!(
        ?usage_pre,
        "running disk usage based eviction due to pressure"
    );

    // get a snapshot of the list of tenants
    let mut data: Vec<(
        TenantId,
        Vec<(TimelineId, LocalLayerInfoForDiskUsageEviction)>,
    )> = tenant::mgr::list_tenants()
        .await
        .context("get list of tenants")?
        .into_iter()
        .map(|(tenant_id, _)| (tenant_id, Vec::new()))
        .collect();

    // get layer infos for all timelines for all tenants
    for (tenant_id, layer_infos) in &mut data {
        let tenant = match tenant::mgr::get_tenant(*tenant_id, false).await {
            Ok(tenant) => tenant,
            // this can happen if tenant has lifecycle transition after we fetched it above
            Err(e) => {
                warn!("failed to get tenant {tenant_id}: {e:#}");
                continue;
            }
        };
        if cancel.is_cancelled() {
            return Ok(IterationOutcome::Cancelled);
        }

        let timelines = tenant.list_timelines();
        for tl in timelines {
            layer_infos.extend(
                tl.get_local_layers_for_disk_usage_eviction()
                    .into_iter()
                    .map(|layer_infos| (tl.timeline_id, layer_infos)),
            );
            if cancel.is_cancelled() {
                return Ok(IterationOutcome::Cancelled);
            }
        }
    }

    // sort layers by last activity
    for (_, layers) in &mut data {
        layers.sort_unstable_by_key(|(_, layer_info)| layer_info.last_activity_ts);
        if cancel.is_cancelled() {
            return Ok(IterationOutcome::Cancelled);
        }
    }

    // carve out the per-tenant reservation and throw everything else into a global list of LRU candidates
    let mut lru_candidates = Vec::new();
    for (tenant_id, layers) in &data {
        let min_resident_size = task_config.tenant_min_resident_size; // XXX allow per-tenant override, including "unlimited"
        let mut post_evict_resident_size: u64 =
            layers.iter().map(|(_, layer)| layer.file_size).sum();
        for (timeline_id, layer) in layers {
            if cancel.is_cancelled() {
                return Ok(IterationOutcome::Cancelled);
            }
            if post_evict_resident_size <= min_resident_size {
                continue;
            }
            lru_candidates.push((tenant_id, timeline_id, layer));
            post_evict_resident_size -= layer.file_size;
        }
    }

    // do LRU eviction
    lru_candidates.sort_unstable_by_key(|(_, _, layer)| layer.last_activity_ts);
    let mut usage_expected = usage_pre.usage.clone();
    let mut held_tenants = HashMap::new();
    let mut held_timelines = HashMap::new(); // cache held timelines; XXX limit size?
    for (tenant_id, timeline_id, layer) in lru_candidates {
        let tenant = match held_tenants.entry(tenant_id) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let tenant = match tenant::mgr::get_tenant(*tenant_id, false).await {
                    Ok(tenant) => tenant,
                    Err(e) => {
                        warn! {%tenant_id, "failed to get tenant: {:#}", e};
                        continue;
                    }
                };
                e.insert(tenant)
            }
        };
        let timeline = match held_timelines.entry(timeline_id) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let timeline = match tenant.get_timeline(*timeline_id, false) {
                    Ok(timeline) => timeline,
                    Err(e) => {
                        warn! {%tenant_id, %timeline_id, "failed to get timeline: {:#}", e};
                        continue;
                    }
                };
                e.insert(timeline)
            }
        };
        // XXX batching? but, if the batch fails due to one layer, we should retry the remaining ones
        match timeline.evict_layer(&layer.file_name.file_name()).await {
            Err(e) => {
                warn! { %tenant_id, %timeline_id, "failed to evict layer: {:#}", e};
            }
            Ok(None) | Ok(Some(false)) => {
                info! { %tenant_id, %timeline_id, "layer does not exist, likely it was deleted" };
                // XXX should we account for this in usage_expected.avail_bytes ?
            }
            Ok(Some(true)) => {
                usage_expected.set_avail_bytes(usage_expected.avail_bytes() + layer.file_size);
            }
        }
        if !usage_expected.with_pressure(task_config).has_pressure() {
            break;
        }
    }

    // Verify with statvfs whether we made any real progress
    let usage_post = filesystem_level_usage::get(tenants_dir_fd)
        // It's quite unlikely to hit the error here. Keep the code simple and bail out.
        .context("get filesystem-level disk usage after evictions")?
        .with_pressure(task_config);

    debug!(?usage_post, "disk usage");

    Ok(IterationOutcome::Finished(IterationOutcomeFinished {
        before: usage_pre,
        after_actual: usage_post,
        after_expected: usage_expected,
    }))
}

mod filesystem_level_usage {
    use anyhow::Context;
    use nix::{
        dir::Dir,
        sys::statvfs::{self, Statvfs},
    };
    use sync_wrapper::SyncWrapper;

    use super::DiskUsageEvictionTaskConfig;

    #[derive(Debug, Clone)]
    // The `#[allow(dead_code)]` is to suppress warnings about only the Debug impl reading these fields.
    // We use the Debug impl for logging, so, it's allright.
    #[allow(dead_code)]

    pub struct Usage {
        total_bytes: u64,
        avail_bytes: u64,
        usage_pct_cache: Option<u64>,
    }

    pub type Pressure = [(&'static str, bool); 2];

    #[derive(Debug)]
    // The `#[allow(dead_code)]` is to suppress warnings about only the Debug impl reading these fields.
    // We use the Debug impl for logging, so, it's allright.
    #[allow(dead_code)]

    pub struct UsageWithPressure {
        pub usage: Usage,
        pub pressure: Pressure,
    }

    impl UsageWithPressure {
        #[inline]
        pub fn has_pressure(&self) -> bool {
            self.pressure.iter().any(|(_, has_pressure)| *has_pressure)
        }
    }

    impl Usage {
        pub fn avail_bytes(&self) -> u64 {
            self.avail_bytes
        }
        pub fn set_avail_bytes(&mut self, avail_bytes: u64) {
            self.avail_bytes = avail_bytes;
            self.usage_pct_cache = None;
        }

        pub fn usage_pct(&mut self) -> u64 {
            self.usage_pct_cache.unwrap_or_else(|| {
                let usage_pct = (100.0
                    * (1.0 - ((self.avail_bytes as f64) / (self.total_bytes as f64))))
                    as u64;
                self.usage_pct_cache = Some(usage_pct);
                usage_pct
            })
        }
        #[inline]
        pub fn with_pressure(
            &mut self,
            task_config: &DiskUsageEvictionTaskConfig,
        ) -> UsageWithPressure {
            UsageWithPressure {
                usage: self.clone(),
                pressure: [
                    (
                        "min_avail_bytes",
                        self.avail_bytes < task_config.min_avail_bytes,
                    ),
                    (
                        "max_usage_pct",
                        self.usage_pct() > task_config.max_usage_pct,
                    ),
                ],
            }
        }
    }

    pub fn get(tenants_dir_fd: &mut SyncWrapper<Dir>) -> anyhow::Result<Usage> {
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
            total_bytes,
            avail_bytes,
            usage_pct_cache: None,
        })
    }
}
