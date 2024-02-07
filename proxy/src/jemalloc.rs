use std::time::Duration;

use metrics::IntGauge;
use prometheus::{register_int_gauge_with_registry, Registry};
use tikv_jemalloc_ctl::{config, epoch, epoch_mib, stats, version};

pub struct MetricRecorder {
    epoch: epoch_mib,
    active: stats::active_mib,
    active_gauge: IntGauge,
    allocated: stats::allocated_mib,
    allocated_gauge: IntGauge,
    mapped: stats::mapped_mib,
    mapped_gauge: IntGauge,
    metadata: stats::metadata_mib,
    metadata_gauge: IntGauge,
    resident: stats::resident_mib,
    resident_gauge: IntGauge,
    retained: stats::retained_mib,
    retained_gauge: IntGauge,
}

impl MetricRecorder {
    pub fn new(registry: &Registry) -> Result<Self, anyhow::Error> {
        tracing::info!(
            config = config::malloc_conf::read()?,
            version = version::read()?,
            "starting jemalloc recorder"
        );

        Ok(Self {
            epoch: epoch::mib()?,
            active: stats::active::mib()?,
            active_gauge: register_int_gauge_with_registry!(
                "jemalloc_active_bytes",
                "Total number of bytes in active pages allocated by the process",
                registry
            )?,
            allocated: stats::allocated::mib()?,
            allocated_gauge: register_int_gauge_with_registry!(
                "jemalloc_allocated_bytes",
                "Total number of bytes allocated by the process",
                registry
            )?,
            mapped: stats::mapped::mib()?,
            mapped_gauge: register_int_gauge_with_registry!(
                "jemalloc_mapped_bytes",
                "Total number of bytes in active extents mapped by the allocator",
                registry
            )?,
            metadata: stats::metadata::mib()?,
            metadata_gauge: register_int_gauge_with_registry!(
                "jemalloc_metadata_bytes",
                "Total number of bytes dedicated to jemalloc metadata",
                registry
            )?,
            resident: stats::resident::mib()?,
            resident_gauge: register_int_gauge_with_registry!(
                "jemalloc_resident_bytes",
                "Total number of bytes in physically resident data pages mapped by the allocator",
                registry
            )?,
            retained: stats::retained::mib()?,
            retained_gauge: register_int_gauge_with_registry!(
                "jemalloc_retained_bytes",
                "Total number of bytes in virtual memory mappings that were retained rather than being returned to the operating system",
                registry
            )?,
        })
    }

    fn _poll(&self) -> Result<(), anyhow::Error> {
        self.epoch.advance()?;
        self.active_gauge.set(self.active.read()? as i64);
        self.allocated_gauge.set(self.allocated.read()? as i64);
        self.mapped_gauge.set(self.mapped.read()? as i64);
        self.metadata_gauge.set(self.metadata.read()? as i64);
        self.resident_gauge.set(self.resident.read()? as i64);
        self.retained_gauge.set(self.retained.read()? as i64);
        Ok(())
    }

    #[inline]
    pub fn poll(&self) {
        if let Err(error) = self._poll() {
            tracing::warn!(%error, "Failed to poll jemalloc stats");
        }
    }

    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                self.poll();
                interval.tick().await;
            }
        })
    }
}
