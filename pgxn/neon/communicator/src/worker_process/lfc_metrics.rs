use measured::{
    FixedCardinalityLabel, Gauge, GaugeVec, LabelGroup, MetricGroup,
    label::{LabelName, LabelValue, StaticLabelSet},
    metric::{MetricEncoding, gauge::GaugeState, group::Encoding},
};

use super::callbacks::callback_get_lfc_metrics;

pub(crate) struct LfcMetricsCollector;

#[derive(MetricGroup)]
#[metric(new())]
struct LfcMetricsGroup {
    /// LFC cache size limit in bytes
    lfc_cache_size_limit: Gauge,
    /// LFC cache hits
    lfc_hits: Gauge,
    /// LFC cache misses
    lfc_misses: Gauge,
    /// LFC chunks used (chunk = 1MB)
    lfc_used: Gauge,
    /// LFC cache writes
    lfc_writes: Gauge,
    /// Approximate working set size in pages of 8192 bytes
    #[metric(init = GaugeVec::dense())]
    lfc_approximate_working_set_size_windows: GaugeVec<StaticLabelSet<MinuteAsSeconds>>,
}

impl<T: Encoding> MetricGroup<T> for LfcMetricsCollector
where
    GaugeState: MetricEncoding<T>,
{
    fn collect_group_into(&self, enc: &mut T) -> Result<(), <T as Encoding>::Err> {
        let g = LfcMetricsGroup::new();

        let lfc_metrics = callback_get_lfc_metrics();

        g.lfc_cache_size_limit.set(lfc_metrics.lfc_cache_size_limit);
        g.lfc_hits.set(lfc_metrics.lfc_hits);
        g.lfc_misses.set(lfc_metrics.lfc_misses);
        g.lfc_used.set(lfc_metrics.lfc_used);
        g.lfc_writes.set(lfc_metrics.lfc_writes);

        for i in 0..60 {
            let val = lfc_metrics.lfc_approximate_working_set_size_windows[i];
            g.lfc_approximate_working_set_size_windows
                .set(MinuteAsSeconds(i), val);
        }

        g.collect_group_into(enc)
    }
}

/// This stores the values in range 0..60,
/// encodes them as seconds (60, 120, 180, ..., 3600)
#[derive(Clone, Copy)]
struct MinuteAsSeconds(usize);

impl FixedCardinalityLabel for MinuteAsSeconds {
    fn cardinality() -> usize {
        60
    }

    fn encode(&self) -> usize {
        self.0
    }

    fn decode(value: usize) -> Self {
        Self(value)
    }
}

impl LabelValue for MinuteAsSeconds {
    fn visit<V: measured::label::LabelVisitor>(&self, v: V) -> V::Output {
        v.write_int((self.0 + 1) as i64 * 60)
    }
}

impl LabelGroup for MinuteAsSeconds {
    fn visit_values(&self, v: &mut impl measured::label::LabelGroupVisitor) {
        v.write_value(LabelName::from_str("duration_seconds"), self);
    }
}
