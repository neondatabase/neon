//! We re-export those from prometheus crate to
//! make sure that we use the same dep version everywhere.
//! Otherwise, we might not see all metrics registered via
//! a default registry.
use lazy_static::lazy_static;
use once_cell::race::OnceBox;
pub use prometheus::{exponential_buckets, linear_buckets};
pub use prometheus::{register_histogram, Histogram};
pub use prometheus::{register_histogram_vec, HistogramVec};
pub use prometheus::{register_int_counter, IntCounter};
pub use prometheus::{register_int_counter_vec, IntCounterVec};
pub use prometheus::{register_int_gauge, IntGauge};
pub use prometheus::{register_int_gauge_vec, IntGaugeVec};
pub use prometheus::{Encoder, TextEncoder};

mod wrappers;
pub use wrappers::{CountedReader, CountedWriter};

/// Gathers all Prometheus metrics and records the I/O stats just before that.
///
/// Metrics gathering is a relatively simple and standalone operation, so
/// it might be fine to do it this way to keep things simple.
pub fn gather() -> Vec<prometheus::proto::MetricFamily> {
    update_io_metrics();
    prometheus::gather()
}

static COMMON_METRICS_PREFIX: OnceBox<&str> = OnceBox::new();

/// Sets a prefix which will be used for all common metrics, typically a service
/// name like 'pageserver'. Should be executed exactly once in the beginning of
/// any executable which uses common metrics.
pub fn set_common_metrics_prefix(prefix: &'static str) {
    COMMON_METRICS_PREFIX.set(prefix.into()).unwrap();
}

/// Prepends a prefix to a common metric name so they are distinguished between
/// different services, see https://github.com/zenithdb/zenith/pull/681
/// A call to set_common_metrics_prefix() is necessary prior to calling this.
pub fn new_common_metric_name(unprefixed_metric_name: &str) -> String {
    format!(
        "{}_{}",
        COMMON_METRICS_PREFIX.get().unwrap(),
        unprefixed_metric_name
    )
}

lazy_static! {
    static ref DISK_IO_BYTES: IntGaugeVec = register_int_gauge_vec!(
        new_common_metric_name("disk_io_bytes"),
        "Bytes written and read from disk, grouped by the operation (read|write)",
        &["io_operation"]
    )
    .expect("Failed to register disk i/o bytes int gauge vec");
}

// Records I/O stats in a "cross-platform" way.
// Compiles both on macOS and Linux, but current macOS implementation always returns 0 as values for I/O stats.
// An alternative is to read procfs (`/proc/[pid]/io`) which does not work under macOS at all, hence abandoned.
//
// Uses https://www.freebsd.org/cgi/man.cgi?query=getrusage to retrieve the number of block operations
// performed by the process.
// We know the size of the block, so we can determine the I/O bytes out of it.
// The value might be not 100% exact, but should be fine for Prometheus metrics in this case.
#[allow(clippy::unnecessary_cast)]
fn update_io_metrics() {
    let rusage_stats = get_rusage_stats();

    const BYTES_IN_BLOCK: i64 = 512;
    DISK_IO_BYTES
        .with_label_values(&["read"])
        .set(rusage_stats.ru_inblock * BYTES_IN_BLOCK);
    DISK_IO_BYTES
        .with_label_values(&["write"])
        .set(rusage_stats.ru_oublock * BYTES_IN_BLOCK);
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
