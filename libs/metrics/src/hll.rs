//! HyperLogLog is an algorithm for the count-distinct problem,
//! approximating the number of distinct elements in a multiset.
//! Calculating the exact cardinality of the distinct elements
//! of a multiset requires an amount of memory proportional to
//! the cardinality, which is impractical for very large data sets.
//! Probabilistic cardinality estimators, such as the HyperLogLog algorithm,
//! use significantly less memory than this, but can only approximate the cardinality.

use std::{
    hash::{BuildHasher, BuildHasherDefault, Hash},
    sync::atomic::AtomicU8,
};

use measured::{
    label::{LabelGroupVisitor, LabelName, LabelValue, LabelVisitor},
    metric::{counter::CounterState, name::MetricNameEncoder, Metric, MetricType, MetricVec},
    text::TextEncoder,
    LabelGroup,
};
use twox_hash::xxh3;

/// Create an [`HyperLogLogVec`] and registers to default registry.
#[macro_export(local_inner_macros)]
macro_rules! register_hll_vec {
    ($N:literal, $OPTS:expr, $LABELS_NAMES:expr $(,)?) => {{
        let hll_vec = $crate::HyperLogLogVec::<$N>::new($OPTS, $LABELS_NAMES).unwrap();
        $crate::register(Box::new(hll_vec.clone())).map(|_| hll_vec)
    }};

    ($N:literal, $NAME:expr, $HELP:expr, $LABELS_NAMES:expr $(,)?) => {{
        $crate::register_hll_vec!($N, $crate::opts!($NAME, $HELP), $LABELS_NAMES)
    }};
}

/// Create an [`HyperLogLog`] and registers to default registry.
#[macro_export(local_inner_macros)]
macro_rules! register_hll {
    ($N:literal, $OPTS:expr $(,)?) => {{
        let hll = $crate::HyperLogLog::<$N>::with_opts($OPTS).unwrap();
        $crate::register(Box::new(hll.clone())).map(|_| hll)
    }};

    ($N:literal, $NAME:expr, $HELP:expr $(,)?) => {{
        $crate::register_hll!($N, $crate::opts!($NAME, $HELP))
    }};
}

/// HLL is a probabilistic cardinality measure.
///
/// How to use this time-series for a metric name `my_metrics_total_hll`:
///
/// ```promql
/// # harmonic mean
/// 1 / (
///     sum (
///         2 ^ -(
///             # HLL merge operation
///             max (my_metrics_total_hll{}) by (hll_shard, other_labels...)
///         )
///     ) without (hll_shard)
/// )
/// * alpha
/// * shards_count
/// * shards_count
/// ```
///
/// If you want an estimate over time, you can use the following query:
///
/// ```promql
/// # harmonic mean
/// 1 / (
///     sum (
///         2 ^ -(
///             # HLL merge operation
///             max (
///                 max_over_time(my_metrics_total_hll{}[$__rate_interval])
///             ) by (hll_shard, other_labels...)
///         )
///     ) without (hll_shard)
/// )
/// * alpha
/// * shards_count
/// * shards_count
/// ```
///
/// In the case of low cardinality, you might want to use the linear counting approximation:
///
/// ```promql
/// # LinearCounting(m, V) = m log (m / V)
/// shards_count * ln(shards_count /
///     # calculate V = how many shards contain a 0
///     count(max (proxy_connecting_endpoints{}) by (hll_shard, protocol) == 0) without (hll_shard)
/// )
/// ```
///
/// See <https://en.wikipedia.org/wiki/HyperLogLog#Practical_considerations> for estimates on alpha
pub type HyperLogLogVec<L, const N: usize> = MetricVec<HyperLogLogState<N>, L>;
pub type HyperLogLog<const N: usize> = Metric<HyperLogLogState<N>>;

pub struct HyperLogLogState<const N: usize> {
    shards: [AtomicU8; N],
}
impl<const N: usize> Default for HyperLogLogState<N> {
    fn default() -> Self {
        #[allow(clippy::declare_interior_mutable_const)]
        const ZERO: AtomicU8 = AtomicU8::new(0);
        Self { shards: [ZERO; N] }
    }
}

impl<const N: usize> MetricType for HyperLogLogState<N> {
    type Metadata = ();
}

impl<const N: usize> HyperLogLogState<N> {
    pub fn measure(&self, item: &impl Hash) {
        // changing the hasher will break compatibility with previous measurements.
        self.record(BuildHasherDefault::<xxh3::Hash64>::default().hash_one(item));
    }

    fn record(&self, hash: u64) {
        let p = N.ilog2() as u8;
        let j = hash & (N as u64 - 1);
        let rho = (hash >> p).leading_zeros() as u8 + 1 - p;
        self.shards[j as usize].fetch_max(rho, std::sync::atomic::Ordering::Relaxed);
    }

    fn take_sample(&self) -> [u8; N] {
        self.shards.each_ref().map(|x| {
            // We reset the counter to 0 so we can perform a cardinality measure over any time slice in prometheus.

            // This seems like it would be a race condition,
            // but HLL is not impacted by a write in one shard happening in between.
            // This is because in PromQL we will be implementing a harmonic mean of all buckets.
            // we will also merge samples in a time series using `max by (hll_shard)`.

            // TODO: maybe we shouldn't reset this on every collect, instead, only after a time window.
            // this would mean that a dev port-forwarding the metrics url won't break the sampling.
            x.swap(0, std::sync::atomic::Ordering::Relaxed)
        })
    }
}

impl<W: std::io::Write, const N: usize> measured::metric::MetricEncoding<TextEncoder<W>>
    for HyperLogLogState<N>
{
    fn write_type(
        name: impl MetricNameEncoder,
        enc: &mut TextEncoder<W>,
    ) -> Result<(), std::io::Error> {
        enc.write_type(&name, measured::text::MetricType::Gauge)
    }
    fn collect_into(
        &self,
        _: &(),
        labels: impl LabelGroup,
        name: impl MetricNameEncoder,
        enc: &mut TextEncoder<W>,
    ) -> Result<(), std::io::Error> {
        struct I64(i64);
        impl LabelValue for I64 {
            fn visit<V: LabelVisitor>(&self, v: V) -> V::Output {
                v.write_int(self.0)
            }
        }

        struct HllShardLabel {
            hll_shard: i64,
        }

        impl LabelGroup for HllShardLabel {
            fn visit_values(&self, v: &mut impl LabelGroupVisitor) {
                const LE: &LabelName = LabelName::from_str("hll_shard");
                v.write_value(LE, &I64(self.hll_shard));
            }
        }

        self.take_sample()
            .into_iter()
            .enumerate()
            .try_for_each(|(hll_shard, val)| {
                CounterState::new(val as u64).collect_into(
                    &(),
                    labels.by_ref().compose_with(HllShardLabel {
                        hll_shard: hll_shard as i64,
                    }),
                    name.by_ref(),
                    enc,
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use measured::{label::StaticLabelSet, FixedCardinalityLabel};
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use rand_distr::{Distribution, Zipf};

    use crate::HyperLogLogVec;

    #[derive(FixedCardinalityLabel, Clone, Copy)]
    #[label(singleton = "x")]
    enum Label {
        A,
        B,
    }

    fn collect(hll: &HyperLogLogVec<StaticLabelSet<Label>, 32>) -> ([u8; 32], [u8; 32]) {
        // cannot go through the `hll.collect_family_into` interface yet...
        // need to see if I can fix the conflicting impls problem in measured.
        (
            hll.get_metric(hll.with_labels(Label::A)).take_sample(),
            hll.get_metric(hll.with_labels(Label::B)).take_sample(),
        )
    }

    fn get_cardinality(samples: &[[u8; 32]]) -> f64 {
        let mut buckets = [0.0; 32];
        for &sample in samples {
            for (i, m) in sample.into_iter().enumerate() {
                buckets[i] = f64::max(buckets[i], m as f64);
            }
        }

        buckets
            .into_iter()
            .map(|f| 2.0f64.powf(-f))
            .sum::<f64>()
            .recip()
            * 0.697
            * 32.0
            * 32.0
    }

    fn test_cardinality(n: usize, dist: impl Distribution<f64>) -> ([usize; 3], [f64; 3]) {
        let hll = HyperLogLogVec::<StaticLabelSet<Label>, 32>::new();

        let mut iter = StdRng::seed_from_u64(0x2024_0112).sample_iter(dist);
        let mut set_a = HashSet::new();
        let mut set_b = HashSet::new();

        for x in iter.by_ref().take(n) {
            set_a.insert(x.to_bits());
            hll.get_metric(hll.with_labels(Label::A))
                .measure(&x.to_bits());
        }
        for x in iter.by_ref().take(n) {
            set_b.insert(x.to_bits());
            hll.get_metric(hll.with_labels(Label::B))
                .measure(&x.to_bits());
        }
        let merge = &set_a | &set_b;

        let (a, b) = collect(&hll);
        let len = get_cardinality(&[a, b]);
        let len_a = get_cardinality(&[a]);
        let len_b = get_cardinality(&[b]);

        ([merge.len(), set_a.len(), set_b.len()], [len, len_a, len_b])
    }

    #[test]
    fn test_cardinality_small() {
        let (actual, estimate) = test_cardinality(100, Zipf::new(100, 1.2f64).unwrap());

        assert_eq!(actual, [46, 30, 32]);
        assert!(51.3 < estimate[0] && estimate[0] < 51.4);
        assert!(44.0 < estimate[1] && estimate[1] < 44.1);
        assert!(39.0 < estimate[2] && estimate[2] < 39.1);
    }

    #[test]
    fn test_cardinality_medium() {
        let (actual, estimate) = test_cardinality(10000, Zipf::new(10000, 1.2f64).unwrap());

        assert_eq!(actual, [2529, 1618, 1629]);
        assert!(2309.1 < estimate[0] && estimate[0] < 2309.2);
        assert!(1566.6 < estimate[1] && estimate[1] < 1566.7);
        assert!(1629.5 < estimate[2] && estimate[2] < 1629.6);
    }

    #[test]
    fn test_cardinality_large() {
        let (actual, estimate) = test_cardinality(1_000_000, Zipf::new(1_000_000, 1.2f64).unwrap());

        assert_eq!(actual, [129077, 79579, 79630]);
        assert!(126067.2 < estimate[0] && estimate[0] < 126067.3);
        assert!(83076.8 < estimate[1] && estimate[1] < 83076.9);
        assert!(64251.2 < estimate[2] && estimate[2] < 64251.3);
    }

    #[test]
    fn test_cardinality_small2() {
        let (actual, estimate) = test_cardinality(100, Zipf::new(200, 0.8f64).unwrap());

        assert_eq!(actual, [92, 58, 60]);
        assert!(116.1 < estimate[0] && estimate[0] < 116.2);
        assert!(81.7 < estimate[1] && estimate[1] < 81.8);
        assert!(69.3 < estimate[2] && estimate[2] < 69.4);
    }

    #[test]
    fn test_cardinality_medium2() {
        let (actual, estimate) = test_cardinality(10000, Zipf::new(20000, 0.8f64).unwrap());

        assert_eq!(actual, [8201, 5131, 5051]);
        assert!(6846.4 < estimate[0] && estimate[0] < 6846.5);
        assert!(5239.1 < estimate[1] && estimate[1] < 5239.2);
        assert!(4292.8 < estimate[2] && estimate[2] < 4292.9);
    }

    #[test]
    fn test_cardinality_large2() {
        let (actual, estimate) = test_cardinality(1_000_000, Zipf::new(2_000_000, 0.8f64).unwrap());

        assert_eq!(actual, [777847, 482069, 482246]);
        assert!(699437.4 < estimate[0] && estimate[0] < 699437.5);
        assert!(374948.9 < estimate[1] && estimate[1] < 374949.0);
        assert!(434609.7 < estimate[2] && estimate[2] < 434609.8);
    }
}
