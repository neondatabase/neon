//! An utilization metric which is used to decide on which pageserver to put next tenant.
//!
//! The metric is exposed via `GET /v1/utilization`. Refer and maintain it's openapi spec as the
//! truth.

use anyhow::Context;
use std::path::Path;
use utils::serde_percent::Percent;

use pageserver_api::models::PageserverUtilization;

use crate::{config::PageServerConf, tenant::mgr::TenantManager};

pub(crate) fn regenerate(
    conf: &PageServerConf,
    tenants_path: &Path,
    tenant_manager: &TenantManager,
) -> anyhow::Result<PageserverUtilization> {
    let statvfs = nix::sys::statvfs::statvfs(tenants_path)
        .map_err(std::io::Error::from)
        .context("statvfs tenants directory")?;

    // https://unix.stackexchange.com/a/703650
    let blocksz = if statvfs.fragment_size() > 0 {
        statvfs.fragment_size()
    } else {
        statvfs.block_size()
    };

    #[cfg_attr(not(target_os = "macos"), allow(clippy::unnecessary_cast))]
    let free = statvfs.blocks_available() as u64 * blocksz;

    #[cfg_attr(not(target_os = "macos"), allow(clippy::unnecessary_cast))]
    let used = statvfs
        .blocks()
        // use blocks_free instead of available here to match df in case someone compares
        .saturating_sub(statvfs.blocks_free()) as u64
        * blocksz;

    let captured_at = std::time::SystemTime::now();

    let (disk_wanted_bytes, shard_count) = tenant_manager.calculate_utilization()?;

    // Calculate how much disk space can actually be used (we will not use disk space
    // beyond the eviction threshold)
    let max_usage_pct = match conf.disk_usage_based_eviction.clone() {
        Some(e) => e.max_usage_pct,
        None => Percent::new(100).unwrap(),
    };
    let disk_usable_capacity = ((used + free) * max_usage_pct.get() as u64) / 100;

    // Utilization score will be a number scaled to 100.  The scale is totally arbitrary: these
    // scores are only meaningful when compared to one another.  The value can go over 100
    // if we are uncomfortably overloaded.
    let disk_utilization_score = disk_wanted_bytes * 100 / disk_usable_capacity;

    // In case we have many tiny tenants, also apply an absolute limit on the number of
    // shards we will host on one pageserver
    const MAX_SHARDS: usize = 20000;
    let shard_utilization_score = (shard_count * 100 / MAX_SHARDS) as u64;

    let doc = PageserverUtilization {
        disk_usage_bytes: used,
        free_space_bytes: free,
        disk_wanted_bytes,
        utilization_score: std::cmp::max(disk_utilization_score, shard_utilization_score),
        captured_at: utils::serde_system_time::SystemTime(captured_at),
    };

    // TODO: make utilization_score into a metric

    Ok(doc)
}
