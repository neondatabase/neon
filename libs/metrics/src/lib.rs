//! We re-export those from prometheus crate to
//! make sure that we use the same dep version everywhere.
//! Otherwise, we might not see all metrics registered via
//! a default registry.
#![deny(clippy::undocumented_unsafe_blocks)]

use ::measured::label::LabelGroupVisitor;
use ::measured::label::LabelName;
use measured::label::NoLabels;
use measured::metric::counter::CounterState;
use measured::metric::gauge::GaugeState;
use measured::metric::group::Encoding;
use measured::metric::name::MetricName;
use measured::metric::MetricEncoding;
use measured::metric::MetricFamilyEncoding;
use measured::FixedCardinalityLabel;
use measured::LabelGroup;
use measured::MetricGroup;
use once_cell::sync::Lazy;
use prometheus::core::{
    Atomic, AtomicU64, Collector, GenericCounter, GenericCounterVec, GenericGauge, GenericGaugeVec,
};
pub use prometheus::opts;
pub use prometheus::register;
pub use prometheus::Error;
pub use prometheus::{core, default_registry, proto};
pub use prometheus::{exponential_buckets, linear_buckets};
pub use prometheus::{register_counter_vec, Counter, CounterVec};
pub use prometheus::{register_gauge, Gauge};
pub use prometheus::{register_gauge_vec, GaugeVec};
pub use prometheus::{register_histogram, Histogram};
pub use prometheus::{register_histogram_vec, HistogramVec};
pub use prometheus::{register_int_counter, IntCounter};
pub use prometheus::{register_int_counter_vec, IntCounterVec};
pub use prometheus::{register_int_gauge, IntGauge};
pub use prometheus::{register_int_gauge_vec, IntGaugeVec};
pub use prometheus::{Encoder, TextEncoder};
use prometheus::{Registry, Result};

pub mod launch_timestamp;
mod wrappers;
pub use wrappers::{CountedReader, CountedWriter};
mod hll;
pub mod metric_vec_duration;
pub use hll::{HyperLogLog, HyperLogLogVec};
#[cfg(target_os = "linux")]
pub mod more_process_metrics;

pub type UIntGauge = GenericGauge<AtomicU64>;
pub type UIntGaugeVec = GenericGaugeVec<AtomicU64>;

#[macro_export]
macro_rules! register_uint_gauge_vec {
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr $(,)?) => {{
        let gauge_vec = UIntGaugeVec::new($crate::opts!($NAME, $HELP), $LABELS_NAMES).unwrap();
        $crate::register(Box::new(gauge_vec.clone())).map(|_| gauge_vec)
    }};
}

#[macro_export]
macro_rules! register_uint_gauge {
    ($NAME:expr, $HELP:expr $(,)?) => {{
        let gauge = $crate::UIntGauge::new($NAME, $HELP).unwrap();
        $crate::register(Box::new(gauge.clone())).map(|_| gauge)
    }};
}

/// Special internal registry, to collect metrics independently from the default registry.
/// Was introduced to fix deadlock with lazy registration of metrics in the default registry.
static INTERNAL_REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

/// Register a collector in the internal registry. MUST be called before the first call to `gather()`.
/// Otherwise, we can have a deadlock in the `gather()` call, trying to register a new collector
/// while holding the lock.
pub fn register_internal(c: Box<dyn Collector>) -> Result<()> {
    INTERNAL_REGISTRY.register(c)
}

/// Gathers all Prometheus metrics and records the I/O stats just before that.
///
/// Metrics gathering is a relatively simple and standalone operation, so
/// it might be fine to do it this way to keep things simple.
pub fn gather() -> Vec<prometheus::proto::MetricFamily> {
    update_rusage_metrics();
    let mut mfs = prometheus::gather();
    let mut internal_mfs = INTERNAL_REGISTRY.gather();
    mfs.append(&mut internal_mfs);
    mfs
}

static DISK_IO_BYTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "libmetrics_disk_io_bytes_total",
        "Bytes written and read from disk, grouped by the operation (read|write)",
        &["io_operation"]
    )
    .expect("Failed to register disk i/o bytes int gauge vec")
});

static MAXRSS_KB: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "libmetrics_maxrss_kb",
        "Memory usage (Maximum Resident Set Size)"
    )
    .expect("Failed to register maxrss_kb int gauge")
});

pub const DISK_WRITE_SECONDS_BUCKETS: &[f64] = &[
    0.000_050, 0.000_100, 0.000_500, 0.001, 0.003, 0.005, 0.01, 0.05, 0.1, 0.3, 0.5,
];

pub struct BuildInfo {
    pub revision: &'static str,
    pub build_tag: &'static str,
}

// todo: allow label group without the set
impl LabelGroup for BuildInfo {
    fn visit_values(&self, v: &mut impl LabelGroupVisitor) {
        const REVISION: &LabelName = LabelName::from_static("revision");
        v.write_value(REVISION, &self.revision);
        const BUILD_TAG: &LabelName = LabelName::from_static("build_tag");
        v.write_value(BUILD_TAG, &self.build_tag);
    }
}

impl<T: Encoding> MetricFamilyEncoding<T> for BuildInfo
where
    GaugeState: MetricEncoding<T>,
{
    fn collect_into(&self, name: impl measured::metric::name::MetricNameEncoder, enc: &mut T) {
        enc.write_help(&name, "Build/version information");
        GaugeState::write_type(&name, enc);
        GaugeState {
            count: std::sync::atomic::AtomicU64::new(1),
        }
        .collect_into(&(), self, name, enc);
    }
}

#[derive(MetricGroup)]
#[metric(new(build_info: BuildInfo))]
pub struct LibMetrics {
    #[metric(init = build_info)]
    build_info: BuildInfo,

    #[metric(flatten)]
    rusage: Rusage,

    serve_count: CollectionCounter,
}

#[derive(Default)]
struct Rusage;

#[derive(Default)]
struct RusageDiskIo(i64, i64);

impl<T: Encoding> MetricFamilyEncoding<T> for RusageDiskIo
where
    GaugeState: MetricEncoding<T>,
{
    fn collect_into(&self, name: impl measured::metric::name::MetricNameEncoder, enc: &mut T) {
        enc.write_help(
            &name,
            "Bytes written and read from disk, grouped by the operation (read|write)",
        );
        GaugeState::write_type(&name, enc);
        GaugeState {
            count: std::sync::atomic::AtomicU64::new((self.0 * BYTES_IN_BLOCK) as u64),
        }
        .collect_into(&(), IoOperation::Read, &name, enc);
        GaugeState {
            count: std::sync::atomic::AtomicU64::new((self.1 * BYTES_IN_BLOCK) as u64),
        }
        .collect_into(&(), IoOperation::Write, &name, enc);
    }
}
#[derive(Default)]
struct RusageMaxRss(i64);

impl<T: Encoding> MetricFamilyEncoding<T> for RusageMaxRss
where
    GaugeState: MetricEncoding<T>,
{
    fn collect_into(&self, name: impl measured::metric::name::MetricNameEncoder, enc: &mut T) {
        enc.write_help(&name, "Memory usage (Maximum Resident Set Size)");
        GaugeState::write_type(&name, enc);
        GaugeState {
            count: std::sync::atomic::AtomicU64::new(self.0 as u64),
        }
        .collect_into(&(), IoOperation::Read, &name, enc);
    }
}

#[derive(FixedCardinalityLabel)]
#[label(singleton = "io_operation")]
enum IoOperation {
    Read,
    Write,
}

impl<T: Encoding> MetricGroup<T> for Rusage
where
    RusageDiskIo: MetricFamilyEncoding<T>,
    RusageMaxRss: MetricFamilyEncoding<T>,
{
    fn collect_into(&self, enc: &mut T) {
        const DISK_IO: &MetricName = MetricName::from_static("disk_io_bytes_total");
        const MAXRSS: &MetricName = MetricName::from_static("maxrss_kb");

        let rusage_stats = get_rusage_stats();

        MetricFamilyEncoding::collect_into(
            &RusageDiskIo(rusage_stats.ru_inblock, rusage_stats.ru_oublock),
            DISK_IO,
            enc,
        );
        MetricFamilyEncoding::collect_into(&RusageMaxRss(rusage_stats.ru_maxrss), MAXRSS, enc);
    }
}

#[derive(Default)]
struct CollectionCounter(CounterState);

impl<T: Encoding> MetricFamilyEncoding<T> for CollectionCounter
where
    CounterState: MetricEncoding<T>,
{
    fn collect_into(&self, name: impl measured::metric::name::MetricNameEncoder, enc: &mut T) {
        self.0
            .count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        enc.write_help(&name, "Number of metric requests made");
        self.0.collect_into(&(), NoLabels, name, enc);
    }
}

pub fn set_build_info_metric(revision: &str, build_tag: &str) {
    let metric = register_int_gauge_vec!(
        "libmetrics_build_info",
        "Build/version information",
        &["revision", "build_tag"]
    )
    .expect("Failed to register build info metric");
    metric.with_label_values(&[revision, build_tag]).set(1);
}
const BYTES_IN_BLOCK: i64 = 512;

// Records I/O stats in a "cross-platform" way.
// Compiles both on macOS and Linux, but current macOS implementation always returns 0 as values for I/O stats.
// An alternative is to read procfs (`/proc/[pid]/io`) which does not work under macOS at all, hence abandoned.
//
// Uses https://www.freebsd.org/cgi/man.cgi?query=getrusage to retrieve the number of block operations
// performed by the process.
// We know the size of the block, so we can determine the I/O bytes out of it.
// The value might be not 100% exact, but should be fine for Prometheus metrics in this case.
#[allow(clippy::unnecessary_cast)]
fn update_rusage_metrics() {
    let rusage_stats = get_rusage_stats();

    DISK_IO_BYTES
        .with_label_values(&["read"])
        .set(rusage_stats.ru_inblock * BYTES_IN_BLOCK);
    DISK_IO_BYTES
        .with_label_values(&["write"])
        .set(rusage_stats.ru_oublock * BYTES_IN_BLOCK);
    MAXRSS_KB.set(rusage_stats.ru_maxrss);
}

fn get_rusage_stats() -> libc::rusage {
    let mut rusage = std::mem::MaybeUninit::uninit();

    // SAFETY: kernel will initialize the struct for us
    unsafe {
        let ret = libc::getrusage(libc::RUSAGE_SELF, rusage.as_mut_ptr());
        assert!(ret == 0, "getrusage failed: bad args");
        rusage.assume_init()
    }
}

/// Create an [`IntCounterPairVec`] and registers to default registry.
#[macro_export(local_inner_macros)]
macro_rules! register_int_counter_pair_vec {
    ($NAME1:expr, $HELP1:expr, $NAME2:expr, $HELP2:expr, $LABELS_NAMES:expr $(,)?) => {{
        match (
            $crate::register_int_counter_vec!($NAME1, $HELP1, $LABELS_NAMES),
            $crate::register_int_counter_vec!($NAME2, $HELP2, $LABELS_NAMES),
        ) {
            (Ok(inc), Ok(dec)) => Ok($crate::IntCounterPairVec::new(inc, dec)),
            (Err(e), _) | (_, Err(e)) => Err(e),
        }
    }};
}
/// Create an [`IntCounterPair`] and registers to default registry.
#[macro_export(local_inner_macros)]
macro_rules! register_int_counter_pair {
    ($NAME1:expr, $HELP1:expr, $NAME2:expr, $HELP2:expr $(,)?) => {{
        match (
            $crate::register_int_counter!($NAME1, $HELP1),
            $crate::register_int_counter!($NAME2, $HELP2),
        ) {
            (Ok(inc), Ok(dec)) => Ok($crate::IntCounterPair::new(inc, dec)),
            (Err(e), _) | (_, Err(e)) => Err(e),
        }
    }};
}

/// A Pair of [`GenericCounterVec`]s. Like an [`GenericGaugeVec`] but will always observe changes
pub struct GenericCounterPairVec<P: Atomic> {
    inc: GenericCounterVec<P>,
    dec: GenericCounterVec<P>,
}

/// A Pair of [`GenericCounter`]s. Like an [`GenericGauge`] but will always observe changes
pub struct GenericCounterPair<P: Atomic> {
    inc: GenericCounter<P>,
    dec: GenericCounter<P>,
}

impl<P: Atomic> GenericCounterPairVec<P> {
    pub fn new(inc: GenericCounterVec<P>, dec: GenericCounterVec<P>) -> Self {
        Self { inc, dec }
    }

    /// `get_metric_with_label_values` returns the [`GenericCounterPair<P>`] for the given slice
    /// of label values (same order as the VariableLabels in Desc). If that combination of
    /// label values is accessed for the first time, a new [`GenericCounterPair<P>`] is created.
    ///
    /// An error is returned if the number of label values is not the same as the
    /// number of VariableLabels in Desc.
    pub fn get_metric_with_label_values(&self, vals: &[&str]) -> Result<GenericCounterPair<P>> {
        Ok(GenericCounterPair {
            inc: self.inc.get_metric_with_label_values(vals)?,
            dec: self.dec.get_metric_with_label_values(vals)?,
        })
    }

    /// `with_label_values` works as `get_metric_with_label_values`, but panics if an error
    /// occurs.
    pub fn with_label_values(&self, vals: &[&str]) -> GenericCounterPair<P> {
        self.get_metric_with_label_values(vals).unwrap()
    }
}

impl<P: Atomic> GenericCounterPair<P> {
    pub fn new(inc: GenericCounter<P>, dec: GenericCounter<P>) -> Self {
        Self { inc, dec }
    }

    /// Increment the gauge by 1, returning a guard that decrements by 1 on drop.
    pub fn guard(&self) -> GenericCounterPairGuard<P> {
        self.inc.inc();
        GenericCounterPairGuard(self.dec.clone())
    }

    /// Increment the gauge by n, returning a guard that decrements by n on drop.
    pub fn guard_by(&self, n: P::T) -> GenericCounterPairGuardBy<P> {
        self.inc.inc_by(n);
        GenericCounterPairGuardBy(self.dec.clone(), n)
    }

    /// Increase the gauge by 1.
    #[inline]
    pub fn inc(&self) {
        self.inc.inc();
    }

    /// Decrease the gauge by 1.
    #[inline]
    pub fn dec(&self) {
        self.dec.inc();
    }

    /// Add the given value to the gauge. (The value can be
    /// negative, resulting in a decrement of the gauge.)
    #[inline]
    pub fn inc_by(&self, v: P::T) {
        self.inc.inc_by(v);
    }

    /// Subtract the given value from the gauge. (The value can be
    /// negative, resulting in an increment of the gauge.)
    #[inline]
    pub fn dec_by(&self, v: P::T) {
        self.dec.inc_by(v);
    }
}

/// Guard returned by [`GenericCounterPair::guard`]
pub struct GenericCounterPairGuard<P: Atomic>(GenericCounter<P>);

impl<P: Atomic> Drop for GenericCounterPairGuard<P> {
    fn drop(&mut self) {
        self.0.inc();
    }
}
/// Guard returned by [`GenericCounterPair::guard_by`]
pub struct GenericCounterPairGuardBy<P: Atomic>(GenericCounter<P>, P::T);

impl<P: Atomic> Drop for GenericCounterPairGuardBy<P> {
    fn drop(&mut self) {
        self.0.inc_by(self.1);
    }
}

/// A Pair of [`IntCounterVec`]s. Like an [`IntGaugeVec`] but will always observe changes
pub type IntCounterPairVec = GenericCounterPairVec<AtomicU64>;

/// A Pair of [`IntCounter`]s. Like an [`IntGauge`] but will always observe changes
pub type IntCounterPair = GenericCounterPair<AtomicU64>;

/// A guard for [`IntCounterPair`] that will decrement the gauge on drop
pub type IntCounterPairGuard = GenericCounterPairGuard<AtomicU64>;
