use metrics::{IntGauge, IntGaugeVec};

use super::callbacks::callback_get_lfc_metrics;

pub(crate) struct LfcMetricsCollector {
    lfc_cache_size_limit: IntGauge,
    lfc_hits: IntGauge,
    lfc_misses: IntGauge,
    lfc_used: IntGauge,
    lfc_writes: IntGauge,
    lfc_approximate_working_set_size_windows_vec: IntGaugeVec,
    lfc_approximate_working_set_size_windows: [IntGauge; 60],
}

impl LfcMetricsCollector {
    pub fn new() -> LfcMetricsCollector {
        let lfc_approximate_working_set_size_windows_vec = IntGaugeVec::new(
            metrics::opts!(
                "lfc_approximate_working_set_size_windows",
                "Approximate working set size in pages of 8192 bytes",
            ),
            &[&"duration_seconds"],
        )
        .unwrap();

        let lfc_approximate_working_set_size_windows: [IntGauge; 60] = (1..=60)
            .map(|minutes| {
                lfc_approximate_working_set_size_windows_vec
                    .with_label_values(&[&(minutes * 60).to_string()])
            })
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();

        LfcMetricsCollector {
            lfc_cache_size_limit: IntGauge::new(
                "lfc_cache_size_limit",
                "LFC cache size limit in bytes",
            )
            .unwrap(),
            lfc_hits: IntGauge::new("lfc_hits", "LFC cache hits").unwrap(),
            lfc_misses: IntGauge::new("lfc_misses", "LFC cache misses").unwrap(),
            lfc_used: IntGauge::new("lfc_used", "LFC chunks used (chunk = 1MB)").unwrap(),
            lfc_writes: IntGauge::new("lfc_writes", "LFC cache writes").unwrap(),

            lfc_approximate_working_set_size_windows_vec,
            lfc_approximate_working_set_size_windows,
        }
    }
}

impl metrics::core::Collector for LfcMetricsCollector {
    fn desc(&self) -> Vec<&metrics::core::Desc> {
        let mut descs = Vec::new();

        descs.append(&mut self.lfc_cache_size_limit.desc());
        descs.append(&mut self.lfc_hits.desc());
        descs.append(&mut self.lfc_misses.desc());
        descs.append(&mut self.lfc_used.desc());
        descs.append(&mut self.lfc_writes.desc());
        descs.append(&mut self.lfc_approximate_working_set_size_windows_vec.desc());

        descs
    }

    fn collect(&self) -> Vec<metrics::proto::MetricFamily> {
        let mut values = Vec::new();

        // update the gauges
        let lfc_metrics = callback_get_lfc_metrics();
        self.lfc_cache_size_limit
            .set(lfc_metrics.lfc_cache_size_limit);
        self.lfc_hits.set(lfc_metrics.lfc_hits);
        self.lfc_misses.set(lfc_metrics.lfc_misses);
        self.lfc_used.set(lfc_metrics.lfc_used);
        self.lfc_writes.set(lfc_metrics.lfc_writes);
        for i in 0..60 {
            let val = lfc_metrics.lfc_approximate_working_set_size_windows[i];
            self.lfc_approximate_working_set_size_windows[i].set(val);
        }

        values.append(&mut self.lfc_cache_size_limit.collect());
        values.append(&mut self.lfc_hits.collect());
        values.append(&mut self.lfc_misses.collect());
        values.append(&mut self.lfc_used.collect());
        values.append(&mut self.lfc_writes.collect());
        values.append(&mut self.lfc_approximate_working_set_size_windows_vec.collect());

        values
    }
}
