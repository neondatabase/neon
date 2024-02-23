use metrics::{register_int_counter, register_int_counter_vec, IntCounter, IntCounterVec};
use once_cell::sync::Lazy;

pub(crate) struct ReconcilerMetrics {
    pub(crate) spawned: IntCounter,
    pub(crate) complete: IntCounterVec,
}

impl ReconcilerMetrics {
    // Labels used on [`Self::complete`]
    pub(crate) const SUCCESS: &'static str = "ok";
    pub(crate) const ERROR: &'static str = "success";
    pub(crate) const CANCEL: &'static str = "cancel";
}

pub(crate) static RECONCILER: Lazy<ReconcilerMetrics> = Lazy::new(|| ReconcilerMetrics {
    spawned: register_int_counter!(
        "storage_controller_reconcile_spawn",
        "Count of how many times we spawn a reconcile task",
    )
    .expect("failed to define a metric"),
    complete: register_int_counter_vec!(
        "storage_controller_reconcile_complete",
        "Reconciler tasks completed, broken down by success/failure/cancelled",
        &["status"],
    )
    .expect("failed to define a metric"),
});

pub fn preinitialize_metrics() {
    Lazy::force(&RECONCILER);
}
