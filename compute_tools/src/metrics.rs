use metrics::core::{AtomicF64, Collector, GenericGauge};
use metrics::proto::MetricFamily;
use metrics::{
    IntCounter, IntCounterVec, IntGaugeVec, UIntGaugeVec, register_gauge, register_int_counter,
    register_int_counter_vec, register_int_gauge_vec, register_uint_gauge_vec,
};
use once_cell::sync::Lazy;

pub(crate) static INSTALLED_EXTENSIONS: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "compute_installed_extensions",
        "Number of databases where the version of extension is installed",
        &["extension_name", "version", "owned_by_superuser"]
    )
    .expect("failed to define a metric")
});

// Normally, any HTTP API request is described by METHOD (e.g. GET, POST, etc.) + PATH,
// but for all our APIs we defined a 'slug'/method/operationId in the OpenAPI spec.
// And it's fair to call it a 'RPC' (Remote Procedure Call).
pub enum CPlaneRequestRPC {
    GetConfig,
}

impl CPlaneRequestRPC {
    pub fn as_str(&self) -> &str {
        match self {
            CPlaneRequestRPC::GetConfig => "GetConfig",
        }
    }
}

pub const UNKNOWN_HTTP_STATUS: &str = "unknown";

pub(crate) static CPLANE_REQUESTS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "compute_ctl_cplane_requests_total",
        "Total number of control plane requests made by compute_ctl by status",
        &["rpc", "http_status"]
    )
    .expect("failed to define a metric")
});

/// Total number of failed database migrations. Per-compute, this is actually a boolean metric,
/// either empty or with a single value (1, migration_id) because we stop at the first failure.
/// Yet, the sum over the fleet will provide the total number of failures.
pub(crate) static DB_MIGRATION_FAILED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "compute_ctl_db_migration_failed_total",
        "Total number of failed database migrations",
        &["migration_id"]
    )
    .expect("failed to define a metric")
});

pub(crate) static REMOTE_EXT_REQUESTS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "compute_ctl_remote_ext_requests_total",
        "Total number of requests made by compute_ctl to download extensions from S3 proxy by status",
        &["http_status", "filename"]
    )
    .expect("failed to define a metric")
});

// Size of audit log directory in bytes
pub(crate) static AUDIT_LOG_DIR_SIZE: Lazy<GenericGauge<AtomicF64>> = Lazy::new(|| {
    register_gauge!(
        "compute_audit_log_dir_size",
        "Size of audit log directory in bytes",
    )
    .expect("failed to define a metric")
});

// Report that `compute_ctl` is up and what's the current compute status.
pub(crate) static COMPUTE_CTL_UP: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "compute_ctl_up",
        "Whether compute_ctl is running",
        &["build_tag", "status"]
    )
    .expect("failed to define a metric")
});

/// Needed as neon.file_cache_prewarm_batch == 0 doesn't mean we never tried to prewarm.
/// On the other hand, smth. like LFC_PREWARMED_PAGES is excessive as we can query this
/// directly from extension
pub(crate) static LFC_PREWARM_REQUESTS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "compute_ctl_lfc_prewarm_requests_total",
        "Total number of LFC prewarm requests made by compute_ctl",
    )
    .expect("failed to define a metric")
});

pub(crate) static LFC_PREWARM_OFFLOAD_REQUESTS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "compute_ctl_lfc_prewarm_offload_requests_total",
        "Total number of LFC prewarm offload requests made by compute_ctl",
    )
    .expect("failed to define a metric")
});

pub fn collect() -> Vec<MetricFamily> {
    let mut metrics = COMPUTE_CTL_UP.collect();
    metrics.extend(INSTALLED_EXTENSIONS.collect());
    metrics.extend(CPLANE_REQUESTS_TOTAL.collect());
    metrics.extend(REMOTE_EXT_REQUESTS_TOTAL.collect());
    metrics.extend(DB_MIGRATION_FAILED.collect());
    metrics.extend(AUDIT_LOG_DIR_SIZE.collect());
    metrics.extend(LFC_PREWARM_REQUESTS.collect());
    metrics.extend(LFC_PREWARM_OFFLOAD_REQUESTS.collect());
    metrics
}
