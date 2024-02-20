//! An utilization metric which is used to decide on which pageserver to put next tenant.
//!
//! The metric is exposed via `GET /v1/utilization`. Refer and maintain it's openapi spec as the
//! truth.

use anyhow::Context;
use std::path::Path;

use pageserver_api::models::PageserverUtilization;

/// u64 in json is problematic for many parsers. This is 2**53 - 1, which will be equal when parsed
/// as u64 or f64.
const MAX_JSON_INTEGER: u64 = 9007199254740991;

pub(crate) fn regenerate(tenants_path: &Path) -> anyhow::Result<PageserverUtilization> {
    // TODO: currently the http api ratelimits this to 1Hz at most, which is probably good enough

    let statvfs = nix::sys::statvfs::statvfs(tenants_path)
        .map_err(std::io::Error::from)
        .context("statvfs tenants directory")?;

    let blocksz = statvfs.block_size();

    let free = statvfs.blocks_available() as u64 * blocksz;
    let used = crate::metrics::RESIDENT_PHYSICAL_SIZE_GLOBAL.get();
    let captured_at = std::time::SystemTime::now();

    let doc = PageserverUtilization {
        disk_usage_bytes: used,
        free_space_bytes: free,
        // lower is better; start with a constant
        utilization_score: MAX_JSON_INTEGER,
        captured_at,
    };

    // TODO: make utilization_score into a metric

    Ok(doc)
}
