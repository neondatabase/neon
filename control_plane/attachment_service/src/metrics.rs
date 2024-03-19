use bytes::Bytes;
use measured::{
    label::{LabelValue, StaticLabelSet},
    FixedCardinalityLabel, MetricGroup,
};
use once_cell::sync::Lazy;
use std::sync::Mutex;

#[derive(FixedCardinalityLabel)]
pub(crate) enum ReconcileOutcome {
    #[label(rename = "ok")]
    Success,
    Error,
    Cancel,
}

#[derive(FixedCardinalityLabel, Clone)]
pub(crate) enum Method {
    Get,
    Put,
    Post,
    Delete,
    Other,
}

impl From<hyper::Method> for Method {
    fn from(value: hyper::Method) -> Self {
        if value == hyper::Method::GET {
            Method::Get
        } else if value == hyper::Method::PUT {
            Method::Put
        } else if value == hyper::Method::POST {
            Method::Post
        } else if value == hyper::Method::DELETE {
            Method::Delete
        } else {
            Method::Other
        }
    }
}

pub(crate) struct StatusCode(pub(crate) hyper::http::StatusCode);

impl LabelValue for StatusCode {
    fn visit<V: measured::label::LabelVisitor>(&self, v: V) -> V::Output {
        v.write_int(self.0.as_u16() as u64)
    }
}

impl FixedCardinalityLabel for StatusCode {
    fn cardinality() -> usize {
        (100..1000).len()
    }

    fn encode(&self) -> usize {
        self.0.as_u16() as usize
    }

    fn decode(value: usize) -> Self {
        Self(hyper::http::StatusCode::from_u16(u16::try_from(value).unwrap()).unwrap())
    }
}

#[derive(measured::LabelGroup)]
#[label(set = ReconcileCompleteLabelGroupSet)]
pub(crate) struct ReconcileCompleteLabelGroup {
    pub(crate) status: ReconcileOutcome,
}

#[derive(measured::LabelGroup)]
#[label(set = HttpRequestStatusLabelGroupSet)]
pub(crate) struct HttpRequestStatusLabelGroup<'a> {
    #[label(dynamic_with = lasso::ThreadedRodeo)]
    pub(crate) path: &'a str,
    pub(crate) method: Method,
    pub(crate) status: StatusCode,
}

#[derive(measured::LabelGroup)]
#[label(set = HttpRequestLatencyLabelGroupSet)]
pub(crate) struct HttpRequestLatencyLabelGroup<'a> {
    #[label(dynamic_with = lasso::ThreadedRodeo)]
    pub(crate) path: &'a str,
    pub(crate) method: Method,
}

impl Default for HttpRequestLatencyLabelGroupSet {
    fn default() -> Self {
        Self {
            path: lasso::ThreadedRodeo::new(),
            method: StaticLabelSet::new(),
        }
    }
}

#[derive(measured::LabelGroup, Clone)]
#[label(set = PageserverRequestLabelGroupSet)]
pub(crate) struct PageserverRequestLabelGroup<'a> {
    #[label(dynamic_with = lasso::ThreadedRodeo)]
    pub(crate) pageserver_id: &'a str,
    #[label(dynamic_with = lasso::ThreadedRodeo)]
    pub(crate) path: &'a str,
    pub(crate) method: Method,
}

impl Default for PageserverRequestLabelGroupSet {
    fn default() -> Self {
        Self {
            pageserver_id: lasso::ThreadedRodeo::new(),
            path: lasso::ThreadedRodeo::new(),
            method: StaticLabelSet::new(),
        }
    }
}

#[derive(measured::MetricGroup)]
pub(crate) struct StorageControllerMetricGroup {
    /// Count of how many times we spawn a reconcile task
    pub(crate) storage_controller_reconcile_spawn: measured::Counter,
    /// Reconciler tasks completed, broken down by success/failure/cancelled
    pub(crate) storage_controller_reconcile_complete:
        measured::CounterVec<ReconcileCompleteLabelGroupSet>,

    /// HTTP request status counters for handled requests
    pub(crate) storage_controller_http_request_status:
        measured::CounterVec<HttpRequestStatusLabelGroupSet>,
    /// HTTP request handler latency across all status codes
    pub(crate) storage_controller_http_request_latency:
        measured::HistogramVec<HttpRequestLatencyLabelGroupSet, 5>,

    /// Count of HTTP requests to the pageserver that resulted in an error,
    /// broken down by the pageserver node id, request name and method
    pub(crate) storage_controller_pageserver_request_error:
        measured::CounterVec<PageserverRequestLabelGroupSet>,

    /// Latency of HTTP requests to the pageserver, broken down by pageserver
    /// node id, request name and method. This include both successful and unsuccessful
    /// requests.
    pub(crate) storage_controller_pageserver_request_latency:
        measured::HistogramVec<PageserverRequestLabelGroupSet, 5>,

    /// Count of pass-through HTTP requests to the pageserver that resulted in an error,
    /// broken down by the pageserver node id, request name and method
    pub(crate) storage_controller_passthrough_request_error:
        measured::CounterVec<PageserverRequestLabelGroupSet>,

    /// Latency of pass-through HTTP requests to the pageserver, broken down by pageserver
    /// node id, request name and method. This include both successful and unsuccessful
    /// requests.
    pub(crate) storage_controller_passthrough_request_latency:
        measured::HistogramVec<PageserverRequestLabelGroupSet, 5>,
}

impl StorageControllerMetricGroup {
    pub(crate) fn new() -> Self {
        Self {
            storage_controller_reconcile_spawn: measured::Counter::new(),

            storage_controller_reconcile_complete: measured::CounterVec::new(
                ReconcileCompleteLabelGroupSet {
                    status: StaticLabelSet::new(),
                },
            ),
            storage_controller_http_request_status: measured::CounterVec::new(
                HttpRequestStatusLabelGroupSet {
                    path: lasso::ThreadedRodeo::new(),
                    method: StaticLabelSet::new(),
                    status: StaticLabelSet::new(),
                },
            ),

            storage_controller_http_request_latency: measured::HistogramVec::new(
                measured::metric::histogram::Thresholds::exponential_buckets(0.1, 2.0),
            ),

            storage_controller_pageserver_request_error: measured::CounterVec::new(
                PageserverRequestLabelGroupSet {
                    pageserver_id: lasso::ThreadedRodeo::new(),
                    path: lasso::ThreadedRodeo::new(),
                    method: StaticLabelSet::new(),
                },
            ),

            storage_controller_pageserver_request_latency: measured::HistogramVec::new(
                measured::metric::histogram::Thresholds::exponential_buckets(0.1, 2.0),
            ),

            storage_controller_passthrough_request_error: measured::CounterVec::new(
                PageserverRequestLabelGroupSet {
                    pageserver_id: lasso::ThreadedRodeo::new(),
                    path: lasso::ThreadedRodeo::new(),
                    method: StaticLabelSet::new(),
                },
            ),

            storage_controller_passthrough_request_latency: measured::HistogramVec::new(
                measured::metric::histogram::Thresholds::exponential_buckets(0.1, 2.0),
            ),
        }
    }
}

pub(crate) struct StorageControllerMetrics {
    pub(crate) metrics_group: StorageControllerMetricGroup,
    encoder: Mutex<measured::text::TextEncoder>,
}

impl StorageControllerMetrics {
    pub(crate) fn encode(&self) -> Bytes {
        let mut encoder = self.encoder.lock().unwrap();
        self.metrics_group.collect_into(&mut *encoder);
        encoder.finish()
    }
}

impl Default for StorageControllerMetrics {
    fn default() -> Self {
        Self {
            metrics_group: StorageControllerMetricGroup::new(),
            encoder: Mutex::new(measured::text::TextEncoder::new()),
        }
    }
}

pub(crate) static METRICS_REGISTRY: Lazy<StorageControllerMetrics> =
    Lazy::new(StorageControllerMetrics::default);

pub fn preinitialize_metrics() {
    Lazy::force(&METRICS_REGISTRY);
}
