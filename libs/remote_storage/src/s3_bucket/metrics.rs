use metrics::{register_histogram_vec, register_int_counter_vec, Histogram, IntCounter};
use once_cell::sync::Lazy;

pub(super) static BUCKET_METRICS: Lazy<BucketMetrics> = Lazy::new(Default::default);

#[derive(Clone, Copy, Debug)]
pub(super) enum RequestKind {
    Get = 0,
    Put = 1,
    Delete = 2,
    List = 3,
}

use RequestKind::*;

impl RequestKind {
    const fn as_str(&self) -> &'static str {
        match self {
            Get => "get_object",
            Put => "put_object",
            Delete => "delete_object",
            List => "list_objects",
        }
    }
    const fn as_index(&self) -> usize {
        *self as usize
    }
}

pub(super) struct RequestTyped<C>([C; 4]);

impl<C> RequestTyped<C> {
    pub(super) fn get(&self, kind: RequestKind) -> &C {
        &self.0[kind.as_index()]
    }

    fn build_with(mut f: impl FnMut(RequestKind) -> C) -> Self {
        use RequestKind::*;
        let mut it = [Get, Put, Delete, List].into_iter();
        let arr = std::array::from_fn::<C, 4, _>(|index| {
            let next = it.next().unwrap();
            assert_eq!(index, next.as_index());
            f(next)
        });

        if let Some(next) = it.next() {
            panic!("unexpected {next:?}");
        }

        RequestTyped(arr)
    }
}

impl RequestTyped<Histogram> {
    pub(super) fn observe_elapsed(&self, kind: RequestKind, started_at: std::time::Instant) {
        self.get(kind).observe(started_at.elapsed().as_secs_f64())
    }
}

pub(super) struct PassFailCancelledRequestTyped<C> {
    success: RequestTyped<C>,
    fail: RequestTyped<C>,
    cancelled: RequestTyped<C>,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum AttemptOutcome {
    Ok,
    Err,
    Cancelled,
}

impl<T, E> From<&Result<T, E>> for AttemptOutcome {
    fn from(value: &Result<T, E>) -> Self {
        match value {
            Ok(_) => AttemptOutcome::Ok,
            Err(_) => AttemptOutcome::Err,
        }
    }
}

impl AttemptOutcome {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            AttemptOutcome::Ok => "ok",
            AttemptOutcome::Err => "err",
            AttemptOutcome::Cancelled => "cancelled",
        }
    }
}

impl<C> PassFailCancelledRequestTyped<C> {
    pub(super) fn get(&self, kind: RequestKind, outcome: AttemptOutcome) -> &C {
        let target = match outcome {
            AttemptOutcome::Ok => &self.success,
            AttemptOutcome::Err => &self.fail,
            AttemptOutcome::Cancelled => &self.cancelled,
        };
        target.get(kind)
    }

    fn build_with(mut f: impl FnMut(RequestKind, AttemptOutcome) -> C) -> Self {
        let success = RequestTyped::build_with(|kind| f(kind, AttemptOutcome::Ok));
        let fail = RequestTyped::build_with(|kind| f(kind, AttemptOutcome::Err));
        let cancelled = RequestTyped::build_with(|kind| f(kind, AttemptOutcome::Cancelled));

        PassFailCancelledRequestTyped {
            success,
            fail,
            cancelled,
        }
    }
}

impl PassFailCancelledRequestTyped<Histogram> {
    pub(super) fn observe_elapsed(
        &self,
        kind: RequestKind,
        outcome: impl Into<AttemptOutcome>,
        started_at: std::time::Instant,
    ) {
        self.get(kind, outcome.into())
            .observe(started_at.elapsed().as_secs_f64())
    }
}

pub(super) struct BucketMetrics {
    /// Total requests attempted
    // TODO: remove after next release and migrate dashboards to `sum by (result) (remote_storage_s3_requests_count)`
    requests: RequestTyped<IntCounter>,
    /// Subset of attempted requests failed
    // TODO: remove after next release and migrate dashboards to `remote_storage_s3_requests_count{result="err"}`
    failed: RequestTyped<IntCounter>,

    pub(super) req_seconds: PassFailCancelledRequestTyped<Histogram>,
    pub(super) wait_seconds: RequestTyped<Histogram>,

    /// Track how many semaphore awaits were cancelled per request type.
    ///
    /// This is in case cancellations are happening more than expected.
    pub(super) cancelled_waits: RequestTyped<IntCounter>,
}

impl Default for BucketMetrics {
    fn default() -> Self {
        let requests = register_int_counter_vec!(
            "remote_storage_s3_requests_count",
            "Number of s3 requests of particular type",
            &["request_type"],
        )
        .expect("failed to define a metric");
        let requests =
            RequestTyped::build_with(|kind| requests.with_label_values(&[kind.as_str()]));

        let failed = register_int_counter_vec!(
            "remote_storage_s3_failures_count",
            "Number of failed s3 requests of particular type",
            &["request_type"],
        )
        .expect("failed to define a metric");
        let failed = RequestTyped::build_with(|kind| failed.with_label_values(&[kind.as_str()]));

        let buckets = [0.01, 0.10, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0];

        let req_seconds = register_histogram_vec!(
            "remote_storage_s3_request_seconds",
            "Seconds to complete a request",
            &["request_type", "result"],
            buckets.to_vec(),
        )
        .unwrap();
        let req_seconds = PassFailCancelledRequestTyped::build_with(|kind, outcome| {
            req_seconds.with_label_values(&[kind.as_str(), outcome.as_str()])
        });

        let wait_seconds = register_histogram_vec!(
            "remote_storage_s3_wait_seconds",
            "Seconds rate limited",
            &["request_type"],
            buckets.to_vec(),
        )
        .unwrap();
        let wait_seconds =
            RequestTyped::build_with(|kind| wait_seconds.with_label_values(&[kind.as_str()]));

        let cancelled_waits = register_int_counter_vec!(
            "remote_storage_s3_cancelled_waits_total",
            "Times a semaphore wait has been cancelled per request type",
            &["request_type"],
        )
        .unwrap();
        let cancelled_waits =
            RequestTyped::build_with(|kind| cancelled_waits.with_label_values(&[kind.as_str()]));

        Self {
            requests,
            failed,
            req_seconds,
            wait_seconds,
            cancelled_waits,
        }
    }
}

pub fn inc_get_object() {
    BUCKET_METRICS.requests.get(Get).inc()
}

pub fn inc_get_object_fail() {
    BUCKET_METRICS.failed.get(Get).inc()
}

pub fn inc_put_object() {
    BUCKET_METRICS.requests.get(Put).inc()
}

pub fn inc_put_object_fail() {
    BUCKET_METRICS.failed.get(Put).inc()
}

pub fn inc_delete_object() {
    BUCKET_METRICS.requests.get(Delete).inc()
}

pub fn inc_delete_objects(count: u64) {
    BUCKET_METRICS.requests.get(Delete).inc_by(count)
}

pub fn inc_delete_object_fail() {
    BUCKET_METRICS.failed.get(Delete).inc()
}

pub fn inc_delete_objects_fail(count: u64) {
    BUCKET_METRICS.failed.get(Delete).inc_by(count)
}

pub fn inc_list_objects() {
    BUCKET_METRICS.requests.get(List).inc()
}

pub fn inc_list_objects_fail() {
    BUCKET_METRICS.failed.get(List).inc()
}
