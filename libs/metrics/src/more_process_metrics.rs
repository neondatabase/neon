//! process metrics that the [`::prometheus`] crate doesn't provide.

// This module has heavy inspiration from the prometheus crate's `process_collector.rs`.

use crate::UIntGauge;

pub struct Collector {
    descs: Vec<prometheus::core::Desc>,
    vmlck: crate::UIntGauge,
}

const NMETRICS: usize = 1;

impl prometheus::core::Collector for Collector {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        let Ok(myself) = procfs::process::Process::myself() else {
            return vec![];
        };
        let mut mfs = Vec::with_capacity(NMETRICS);
        if let Ok(status) = myself.status() {
            if let Some(vmlck) = status.vmlck {
                self.vmlck.set(vmlck);
                mfs.extend(self.vmlck.collect())
            }
        }
        mfs
    }
}

impl Collector {
    pub fn new() -> Self {
        let mut descs = Vec::new();

        let vmlck =
            UIntGauge::new("libmetrics_process_status_vmlck", "/proc/self/status vmlck").unwrap();
        descs.extend(
            prometheus::core::Collector::desc(&vmlck)
                .into_iter()
                .cloned(),
        );

        Self { descs, vmlck }
    }
}

impl Default for Collector {
    fn default() -> Self {
        Self::new()
    }
}
