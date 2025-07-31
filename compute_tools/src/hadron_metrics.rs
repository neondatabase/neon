use metrics::{
    IntCounter, IntGaugeVec, core::Collector, proto::MetricFamily, register_int_counter,
    register_int_gauge_vec,
};
use once_cell::sync::Lazy;

// Counter keeping track of the number of PageStream request errors reported by Postgres.
// An error is registered every time Postgres calls compute_ctl's /refresh_configuration API.
// Postgres will invoke this API if it detected trouble with PageStream requests (get_page@lsn,
// get_base_backup, etc.) it sends to any pageserver. An increase in this counter value typically
// indicates Postgres downtime, as PageStream requests are critical for Postgres to function.
pub static POSTGRES_PAGESTREAM_REQUEST_ERRORS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pg_cctl_pagestream_request_errors_total",
        "Number of PageStream request errors reported by the postgres process"
    )
    .expect("failed to define a metric")
});

// Counter keeping track of the number of compute configuration errors due to Postgres statement
// timeouts. An error is registered every time `ComputeNode::reconfigure()` fails due to Postgres
// error code 57014 (query cancelled). This statement timeout typically occurs when postgres is
// stuck in a problematic retry loop when the PS is reject its connection requests (usually due
// to PG pointing at the wrong PS). We should investigate the root cause when this counter value
// increases by checking PG and PS logs.
pub static COMPUTE_CONFIGURE_STATEMENT_TIMEOUT_ERRORS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pg_cctl_configure_statement_timeout_errors_total",
        "Number of compute configuration errors due to Postgres statement timeouts."
    )
    .expect("failed to define a metric")
});

pub static COMPUTE_ATTACHED: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "pg_cctl_attached",
        "Compute node attached status (1 if attached)",
        &[
            "pg_compute_id",
            "pg_instance_id",
            "tenant_id",
            "timeline_id"
        ]
    )
    .expect("failed to define a metric")
});

pub fn collect() -> Vec<MetricFamily> {
    let mut metrics = Vec::new();
    metrics.extend(POSTGRES_PAGESTREAM_REQUEST_ERRORS.collect());
    metrics.extend(COMPUTE_CONFIGURE_STATEMENT_TIMEOUT_ERRORS.collect());
    metrics.extend(COMPUTE_ATTACHED.collect());
    metrics
}

pub fn initialize_metrics() {
    Lazy::force(&POSTGRES_PAGESTREAM_REQUEST_ERRORS);
    Lazy::force(&COMPUTE_CONFIGURE_STATEMENT_TIMEOUT_ERRORS);
    Lazy::force(&COMPUTE_ATTACHED);
}
