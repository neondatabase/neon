//! process metrics that the [`::prometheus`] crate doesn't provide.

// This module has heavy inspiration from the prometheus crate's `process_collector.rs`.

use once_cell::sync::Lazy;
use prometheus::Gauge;

use crate::UIntGauge;

pub struct Collector {
    descs: Vec<prometheus::core::Desc>,
    vmlck: crate::UIntGauge,
    cpu_seconds_highres: Gauge,
}

const NMETRICS: usize = 2;

static CLK_TCK_F64: Lazy<f64> = Lazy::new(|| {
    let long = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if long == -1 {
        panic!("sysconf(_SC_CLK_TCK) failed");
    }
    let convertible_to_f64: i32 =
        i32::try_from(long).expect("sysconf(_SC_CLK_TCK) is larger than i32");
    convertible_to_f64 as f64
});

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
        if let Ok(stat) = myself.stat() {
            let cpu_seconds = stat.utime + stat.stime;
            self.cpu_seconds_highres
                .set(cpu_seconds as f64 / *CLK_TCK_F64);
            mfs.extend(self.cpu_seconds_highres.collect());
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

        let cpu_seconds_highres = Gauge::new(
            "libmetrics_process_cpu_seconds_highres",
            "Total user and system CPU time spent in seconds.\
             Sub-second resolution, hence better than `process_cpu_seconds_total`.",
        )
        .unwrap();
        descs.extend(
            prometheus::core::Collector::desc(&cpu_seconds_highres)
                .into_iter()
                .cloned(),
        );

        Self {
            descs,
            vmlck,
            cpu_seconds_highres,
        }
    }
}

impl Default for Collector {
    fn default() -> Self {
        Self::new()
    }
}
