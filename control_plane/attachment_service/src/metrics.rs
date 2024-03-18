use bytes::Bytes;
use measured::MetricGroup;
use once_cell::sync::Lazy;
use std::sync::Mutex;

#[derive(measured::FixedCardinalityLabel)]
pub(crate) enum ReconcileOutcome {
    #[label(rename = "ok")]
    Success,
    Error,
    Cancel,
}

#[derive(measured::LabelGroup)]
#[label(set = ReconcileCompleteLabelGroupSet)]
pub(crate) struct ReconcileCompleteLabelGroup {
    pub(crate) status: ReconcileOutcome,
}

#[derive(measured::MetricGroup)]
#[metric(new())]
pub(crate) struct StorageControllerMetricGroup {
    /// Count of how many times we spawn a reconcile task
    pub(crate) storage_controller_reconcile_spawn: measured::Counter,
    /// Reconciler tasks completed, broken down by success/failure/cancelled
    pub(crate) storage_controller_reconcile_complete:
        measured::CounterVec<ReconcileCompleteLabelGroupSet>,
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
