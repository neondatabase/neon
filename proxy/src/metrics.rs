use ::metrics::{
    exponential_buckets, register_histogram, register_histogram_vec, register_hll_vec,
    register_int_counter_pair_vec, register_int_counter_vec, register_int_gauge,
    register_int_gauge_vec, Histogram, HistogramVec, HyperLogLogVec, IntCounterPairVec,
    IntCounterVec, IntGauge, IntGaugeVec,
};
use metrics::{register_int_counter_pair, IntCounterPair};

use once_cell::sync::Lazy;
use tokio::time;

pub static NUM_DB_CONNECTIONS_GAUGE: Lazy<IntCounterPairVec> = Lazy::new(|| {
    register_int_counter_pair_vec!(
        "proxy_opened_db_connections_total",
        "Number of opened connections to a database.",
        "proxy_closed_db_connections_total",
        "Number of closed connections to a database.",
        &["protocol"],
    )
    .unwrap()
});

pub static NUM_CLIENT_CONNECTION_GAUGE: Lazy<IntCounterPairVec> = Lazy::new(|| {
    register_int_counter_pair_vec!(
        "proxy_opened_client_connections_total",
        "Number of opened connections from a client.",
        "proxy_closed_client_connections_total",
        "Number of closed connections from a client.",
        &["protocol"],
    )
    .unwrap()
});

pub static NUM_CONNECTION_REQUESTS_GAUGE: Lazy<IntCounterPairVec> = Lazy::new(|| {
    register_int_counter_pair_vec!(
        "proxy_accepted_connections_total",
        "Number of client connections accepted.",
        "proxy_closed_connections_total",
        "Number of client connections closed.",
        &["protocol"],
    )
    .unwrap()
});

pub static COMPUTE_CONNECTION_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "proxy_compute_connection_latency_seconds",
        "Time it took for proxy to establish a connection to the compute endpoint",
        // http/ws/tcp, true/false, true/false, success/failure
        // 3 * 2 * 2 * 2 = 24 counters
        &["protocol", "cache_miss", "pool_miss", "outcome"],
        // largest bucket = 2^16 * 0.5ms = 32s
        exponential_buckets(0.0005, 2.0, 16).unwrap(),
    )
    .unwrap()
});

pub static CONSOLE_REQUEST_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "proxy_console_request_latency",
        "Time it took for proxy to establish a connection to the compute endpoint",
        // proxy_wake_compute/proxy_get_role_info
        &["request"],
        // largest bucket = 2^16 * 0.2ms = 13s
        exponential_buckets(0.0002, 2.0, 16).unwrap(),
    )
    .unwrap()
});

pub static ALLOWED_IPS_BY_CACHE_OUTCOME: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_allowed_ips_cache_misses",
        "Number of cache hits/misses for allowed ips",
        // hit/miss
        &["outcome"],
    )
    .unwrap()
});

pub static RATE_LIMITER_ACQUIRE_LATENCY: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "proxy_control_plane_token_acquire_seconds",
        "Time it took for proxy to establish a connection to the compute endpoint",
        // largest bucket = 3^16 * 0.05ms = 2.15s
        exponential_buckets(0.00005, 3.0, 16).unwrap(),
    )
    .unwrap()
});

pub static RATE_LIMITER_LIMIT: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "semaphore_control_plane_limit",
        "Current limit of the semaphore control plane",
        &["limit"], // 2 counters
    )
    .unwrap()
});

pub static NUM_CONNECTION_ACCEPTED_BY_SNI: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_accepted_connections_by_sni",
        "Number of connections (per sni).",
        &["kind"],
    )
    .unwrap()
});

pub static ALLOWED_IPS_NUMBER: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "proxy_allowed_ips_number",
        "Number of allowed ips",
        vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0, 50.0, 100.0],
    )
    .unwrap()
});

pub static HTTP_CONTENT_LENGTH: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "proxy_http_conn_content_length_bytes",
        "Time it took for proxy to establish a connection to the compute endpoint",
        // largest bucket = 3^16 * 0.05ms = 2.15s
        exponential_buckets(8.0, 2.0, 20).unwrap()
    )
    .unwrap()
});

pub static GC_LATENCY: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "proxy_http_pool_reclaimation_lag_seconds",
        "Time it takes to reclaim unused connection pools",
        // 1us -> 65ms
        exponential_buckets(1e-6, 2.0, 16).unwrap(),
    )
    .unwrap()
});

pub static ENDPOINT_POOLS: Lazy<IntCounterPair> = Lazy::new(|| {
    register_int_counter_pair!(
        "proxy_http_pool_endpoints_registered_total",
        "Number of endpoints we have registered pools for",
        "proxy_http_pool_endpoints_unregistered_total",
        "Number of endpoints we have unregistered pools for",
    )
    .unwrap()
});

pub static NUM_OPEN_CLIENTS_IN_HTTP_POOL: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "proxy_http_pool_opened_connections",
        "Number of opened connections to a database.",
    )
    .unwrap()
});

pub static NUM_CANCELLATION_REQUESTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_cancellation_requests_total",
        "Number of cancellation requests (per found/not_found).",
        &["source", "kind"],
    )
    .unwrap()
});

#[derive(Clone)]
pub struct LatencyTimer {
    // time since the stopwatch was started
    start: Option<time::Instant>,
    // accumulated time on the stopwatch
    pub accumulated: std::time::Duration,
    // label data
    protocol: &'static str,
    cache_miss: bool,
    pool_miss: bool,
    outcome: &'static str,
}

pub struct LatencyTimerPause<'a> {
    timer: &'a mut LatencyTimer,
}

impl LatencyTimer {
    pub fn new(protocol: &'static str) -> Self {
        Self {
            start: Some(time::Instant::now()),
            accumulated: std::time::Duration::ZERO,
            protocol,
            cache_miss: false,
            // by default we don't do pooling
            pool_miss: true,
            // assume failed unless otherwise specified
            outcome: "failed",
        }
    }

    pub fn pause(&mut self) -> LatencyTimerPause<'_> {
        // stop the stopwatch and record the time that we have accumulated
        let start = self.start.take().expect("latency timer should be started");
        self.accumulated += start.elapsed();
        LatencyTimerPause { timer: self }
    }

    pub fn cache_miss(&mut self) {
        self.cache_miss = true;
    }

    pub fn pool_hit(&mut self) {
        self.pool_miss = false;
    }

    pub fn success(&mut self) {
        // stop the stopwatch and record the time that we have accumulated
        if let Some(start) = self.start.take() {
            self.accumulated += start.elapsed();
        }

        // success
        self.outcome = "success";
    }
}

impl Drop for LatencyTimerPause<'_> {
    fn drop(&mut self) {
        // start the stopwatch again
        self.timer.start = Some(time::Instant::now());
    }
}

impl Drop for LatencyTimer {
    fn drop(&mut self) {
        let duration =
            self.start.map(|start| start.elapsed()).unwrap_or_default() + self.accumulated;
        COMPUTE_CONNECTION_LATENCY
            .with_label_values(&[
                self.protocol,
                bool_to_str(self.cache_miss),
                bool_to_str(self.pool_miss),
                self.outcome,
            ])
            .observe(duration.as_secs_f64())
    }
}

pub static NUM_CONNECTION_FAILURES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_connection_failures_total",
        "Number of connection failures (per kind).",
        &["kind"],
    )
    .unwrap()
});

pub static NUM_WAKEUP_FAILURES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_connection_failures_breakdown",
        "Number of wake-up failures (per kind).",
        &["retry", "kind"],
    )
    .unwrap()
});

pub static NUM_BYTES_PROXIED_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_io_bytes",
        "Number of bytes sent/received between all clients and backends.",
        &["direction"],
    )
    .unwrap()
});

pub const fn bool_to_str(x: bool) -> &'static str {
    if x {
        "true"
    } else {
        "false"
    }
}

pub static CONNECTING_ENDPOINTS: Lazy<HyperLogLogVec<32>> = Lazy::new(|| {
    register_hll_vec!(
        32,
        "proxy_connecting_endpoints",
        "HLL approximate cardinality of endpoints that are connecting",
        &["protocol"],
    )
    .unwrap()
});

pub static ERROR_BY_KIND: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_errors_total",
        "Number of errors by a given classification",
        &["type"],
    )
    .unwrap()
});

pub static ENDPOINT_ERRORS_BY_KIND: Lazy<HyperLogLogVec<32>> = Lazy::new(|| {
    register_hll_vec!(
        32,
        "proxy_endpoints_affected_by_errors",
        "Number of endpoints affected by errors of a given classification",
        &["type"],
    )
    .unwrap()
});
