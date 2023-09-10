//! Custom `sysinfo`-like calculations, reading from Linux's `/proc/...`
//!
//! Ordinarily, this sort of thing would be best left to the `sysinfo` library. But, because the
//! vm-monitor only runs on Linux anyways, *and* because we want some additional information
//! that's not exported in `sysinfo` (/proc/meminfo's Buffers and Cached), it's simple enough to
//! just implement this ourselves.
//!
//! Also, for the particular use case of the vm-monitor, it's helpful to be more explicit about
//! which kernel values our scaling refers to.

use anyhow::{anyhow, Context};

/// Values extracted from `/proc/loadavg`
///
/// For more, see `proc(5)`.
pub struct LoadAvg {
    /// 1-minute load average
    pub one: f32,
    /// 5-minute load average
    pub five: f32,
    /// 15-minute load average
    pub fifteen: f32,
}

impl LoadAvg {
    /// Reads the load average values from `/proc/loadavg`
    pub async fn read() -> anyhow::Result<Self> {
        let content = tokio::fs::read_to_string("/proc/loadavg")
            .await
            .context("failed to read /proc/loadavg")?;

        // The contents of /proc/loadavg looks like:
        //
        //   3.23 3.35 3.16 4/2945 371707
        //
        // Where the first three numbers indicate the 1-minute, 5-minute, and 15-minute load
        // average, respectively.
        // For more, see proc(5).

        let mut loads = content
            .trim()
            .split(' ')
            .take(3)
            .map(|v| v.parse::<f32>().ok());

        (|| {
            Some(LoadAvg {
                one: loads.next()??,
                five: loads.next()??,
                fifteen: loads.next()??,
            })
        })()
        .ok_or_else(|| anyhow!("failed to parse content of /proc/loadavg {content:?}"))
    }
}

/// Values extracted from `/proc/meminfo`
///
/// For more, see `proc(5)`.
pub struct MemInfo {
    /// `MemTotal`: total memory
    ///
    /// `proc(5)` defines this as:
    ///
    /// > Total usable RAM (i.e., physical RAM minus a few reserved bits and the kernel binary
    /// > code).
    pub total_bytes: u64,
    /// `MemFree`: free memory, in bytes
    pub free_bytes: u64,
    /// `MemAvailable`: memory that is not in use or could be reclaimed
    ///
    /// `proc(5)` defines this as:
    ///
    /// > An estimate of how much memory is available for starting new applications, without
    /// > swapping.
    pub available_bytes: u64,
    /// `Buffers`: memory consumed by raw disk cache
    ///
    /// `proc(5)` defines this as:
    ///
    /// > Relatively temporary storage for raw disk blocks that shouldn’t get tremendously large
    /// > (20 MB or so).
    ///
    /// The "20 MB or so" is... not always accurate.
    pub buffers_bytes: u64,
    /// `Cached`: memory used by the kernel's page cache (i.e. caching of files read from disk)
    ///
    /// `proc(5)` defines this as:
    ///
    /// > In‐memory cache for files read from the disk (the page cache). Doesn’t include
    /// > `SwapCached`.
    pub cached_bytes: u64,
}

impl MemInfo {
    /// Reads the memory values from `/proc/meminfo`
    pub async fn read() -> anyhow::Result<Self> {
        let content = tokio::fs::read_to_string("/proc/meminfo")
            .await
            .context("failed to read /proc/meminfo")?;

        let mut total_bytes = None;
        let mut free_bytes = None;
        let mut available_bytes = None;
        let mut buffers_bytes = None;
        let mut cached_bytes = None;

        for line in content.lines() {
            // Each line in /proc/meminfo looks like, e.g.:
            //
            //     Buffers:         1018904 kB
            //
            // So we just need to extract the field name (in this case, 'Buffers') and the value,
            // without the trailing ' kB' (which, by the way, is actually kibibytes...).
            let (name, trailing) = line
                .split_once(':')
                .ok_or_else(|| anyhow!("line missing ':' {line:?}"))?;

            let field = match name {
                "MemTotal" => &mut total_bytes,
                "MemFree" => &mut free_bytes,
                "MemAvailable" => &mut available_bytes,
                "Buffers" => &mut buffers_bytes,
                "Cached" => &mut cached_bytes,
                _ => continue,
            };

            let value = trailing
                .trim_start()
                .split(' ')
                .next()
                .expect("str::split() should always have at least one item")
                .parse::<u64>()
                .with_context(|| format!("failed to parse value as u64 from line {line:?}"))?;

            *field = Some(value * 1024); // convert from KiB -> bytes
        }

        let check_missing = |field: &str, value: Option<u64>| {
            value.ok_or_else(|| anyhow!("field {field} not found"))
        };

        Ok(MemInfo {
            total_bytes: check_missing("MemTotal", total_bytes)?,
            free_bytes: check_missing("MemFree", free_bytes)?,
            available_bytes: check_missing("MemAvailable", available_bytes)?,
            buffers_bytes: check_missing("Buffers", buffers_bytes)?,
            cached_bytes: check_missing("Cached", cached_bytes)?,
        })
    }
}
