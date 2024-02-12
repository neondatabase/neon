use std::{hash::BuildHasherDefault, sync::OnceLock};

use ::metrics::{register_hll_vec, HyperLogLogVec};
use lasso::{Spur, ThreadedRodeo};
use measured::{
    label::StaticLabelSet, metric::histogram::Thresholds, Counter, CounterVec,
    FixedCardinalityLabel, Gauge, GaugeVec, Histogram, HistogramVec, LabelGroup, MetricGroup,
};

use once_cell::sync::Lazy;
use rustc_hash::FxHasher;
use tokio::time;

#[derive(MetricGroup)]
pub struct Metrics {
    #[metric(namespace = "proxy")]
    pub proxy: ProxyMetrics,

    // the one metric not called proxy_....
    pub semaphore_control_plane_limit: GaugeVec<StaticLabelSet<RateLimit>>,
}

impl Metrics {
    pub fn get() -> &'static Self {
        static SELF: OnceLock<Metrics> = OnceLock::new();
        SELF.get_or_init(|| Metrics {
            proxy: ProxyMetrics::default(),
            semaphore_control_plane_limit: GaugeVec::default(),
        })
    }
}

#[derive(MetricGroup)]
#[metric(new())]
pub struct ProxyMetrics {
    #[metric(flatten)]
    pub db_connections: NumDbConnectionsGauge,
    #[metric(flatten)]
    pub client_connections: NumClientConnectionsGauge,
    #[metric(flatten)]
    pub connection_requests: NumConnectionRequestsGauge,
    #[metric(flatten)]
    pub http_endpoint_pools: HttpEndpointPools,

    /// Time it took for proxy to establish a connection to the compute endpoint.
    // largest bucket = 2^16 * 0.5ms = 32s
    #[metric(init = HistogramVec::new(Thresholds::exponential_buckets(0.0005, 2.0)))]
    pub compute_connection_latency_seconds: HistogramVec<ComputeConnectionLatencySet, 16>,

    /// Time it took for proxy to receive a response from control plane.
    #[metric(init = HistogramVec::new_metric_vec(
        ConsoleRequestSet {
            request: ThreadedRodeo::with_hasher(BuildHasherDefault::default()),
        },
        // largest bucket = 2^16 * 0.2ms = 13s
        Thresholds::exponential_buckets(0.0002, 2.0))
    )]
    pub compute_console_request_latency: HistogramVec<ConsoleRequestSet, 16>,

    /// Time it takes to acquire a token to call console plane.
    // largest bucket = 3^16 * 0.05ms = 2.15s
    #[metric(init = Histogram::new_metric(Thresholds::exponential_buckets(0.00005, 3.0)))]
    pub control_plane_token_acquire_seconds: Histogram<16>,

    /// Size of the HTTP request body lengths.
    // largest bucket = 2^20 * 8B = 8MiB
    #[metric(init = Histogram::new_metric(Thresholds::exponential_buckets(8.0, 2.0)))]
    pub http_conn_content_length_bytes: Histogram<20>,

    /// Time it takes to reclaim unused connection pools.
    #[metric(init = Histogram::new_metric(Thresholds::exponential_buckets(0.0002, 2.0)))]
    pub http_pool_reclaimation_lag_seconds: Histogram<16>,

    /// Number of opened connections to a database.
    pub http_pool_opened_connections: Gauge,

    /// Number of cache hits/misses for allowed ips.
    pub allowed_ips_cache_misses: CounterVec<StaticLabelSet<CacheOutcome>>,

    /// Number of allowed ips
    // largest bucket = 2^16 * 0.2ms = 13s
    #[metric(init = Histogram::new_metric(Thresholds::exponential_buckets(0.0002, 2.0)))]
    pub allowed_ips_number: Histogram<10>,

    /// Number of connections (per sni).
    pub accepted_connections_by_sni: CounterVec<StaticLabelSet<SniKind>>,

    /// Number of connection failures (per kind).
    pub connection_failures_total: CounterVec<StaticLabelSet<ConnectionFailureKind>>,

    /// Number of wake-up failures (per kind).
    pub connection_failures_breakdown: CounterVec<ConnectionFailuresBreakdownSet>,

    /// Number of bytes sent/received between all clients and backends.
    pub io_bytes: CounterVec<StaticLabelSet<Direction>>,

    /// Number of errors by a given classification.
    pub errors_total: CounterVec<StaticLabelSet<crate::error::ErrorKind>>,
}

impl Default for ProxyMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(FixedCardinalityLabel)]
#[label(singleton = "direction")]
pub enum Direction {
    Tx,
    Rx,
}

#[derive(FixedCardinalityLabel, Clone, Copy, Debug)]
#[label(singleton = "protocol")]
pub enum Protocol {
    Http,
    Ws,
    Tcp,
    SniRouter,
}

impl Protocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            Protocol::Http => "http",
            Protocol::Ws => "ws",
            Protocol::Tcp => "tcp",
            Protocol::SniRouter => "sni_router",
        }
    }
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(FixedCardinalityLabel)]
pub enum Bool {
    True,
    False,
}

#[derive(FixedCardinalityLabel)]
#[label(singleton = "outcome")]
pub enum Outcome {
    Success,
    Failure,
}

#[derive(FixedCardinalityLabel)]
#[label(singleton = "outcome")]
pub enum CacheOutcome {
    Hit,
    Miss,
}

#[derive(LabelGroup)]
#[label(set = ConsoleRequestSet)]
pub struct ConsoleRequest<'a> {
    #[label(dynamic_with = ThreadedRodeo<Spur, BuildHasherDefault<FxHasher>>)]
    pub request: &'a str,
}

#[derive(MetricGroup, Default)]
pub struct HttpEndpointPools {
    /// Number of endpoints we have registered pools for
    pub http_pool_endpoints_registered_total: Counter,
    /// Number of endpoints we have unregistered pools for
    pub http_pool_endpoints_unregistered_total: Counter,
}

pub struct HttpEndpointPoolsGuard<'a> {
    dec: &'a Counter,
}

impl Drop for HttpEndpointPoolsGuard<'_> {
    fn drop(&mut self) {
        self.dec.inc();
    }
}

impl HttpEndpointPools {
    pub fn guard(&self) -> HttpEndpointPoolsGuard {
        self.http_pool_endpoints_registered_total.inc();
        HttpEndpointPoolsGuard {
            dec: &self.http_pool_endpoints_unregistered_total,
        }
    }
}

#[derive(MetricGroup, Default)]
pub struct NumDbConnectionsGauge {
    /// Number of opened connections to a database.
    pub opened_db_connections_total: CounterVec<StaticLabelSet<Protocol>>,
    /// Number of closed connections to a database.
    pub closed_db_connections_total: CounterVec<StaticLabelSet<Protocol>>,
}

pub struct NumDbConnectionsGuard<'a> {
    protocol: Protocol,
    dec: &'a CounterVec<StaticLabelSet<Protocol>>,
}

impl Drop for NumDbConnectionsGuard<'_> {
    fn drop(&mut self) {
        self.dec.inc(self.protocol);
    }
}

impl NumDbConnectionsGauge {
    pub fn guard(&self, protocol: Protocol) -> NumDbConnectionsGuard {
        self.opened_db_connections_total.inc(protocol);
        NumDbConnectionsGuard {
            protocol,
            dec: &self.closed_db_connections_total,
        }
    }
}

#[derive(MetricGroup, Default)]
pub struct NumClientConnectionsGauge {
    /// Number of opened connections from a client.
    pub opened_client_connections_total: CounterVec<StaticLabelSet<Protocol>>,
    /// Number of closed connections from a client.
    pub closed_client_connections_total: CounterVec<StaticLabelSet<Protocol>>,
}

pub struct NumClientConnectionsGuard<'a> {
    protocol: Protocol,
    dec: &'a CounterVec<StaticLabelSet<Protocol>>,
}

impl Drop for NumClientConnectionsGuard<'_> {
    fn drop(&mut self) {
        self.dec.inc(self.protocol);
    }
}

impl NumClientConnectionsGauge {
    pub fn guard(&self, protocol: Protocol) -> NumClientConnectionsGuard {
        self.opened_client_connections_total.inc(protocol);
        NumClientConnectionsGuard {
            protocol,
            dec: &self.closed_client_connections_total,
        }
    }
}

#[derive(MetricGroup, Default)]
pub struct NumConnectionRequestsGauge {
    /// Number of client connections accepted.
    pub accepted_connections_total: CounterVec<StaticLabelSet<Protocol>>,
    /// Number of client connections closed.
    pub closed_connections_total: CounterVec<StaticLabelSet<Protocol>>,
}

pub struct NumConnectionRequestsGuard<'a> {
    protocol: Protocol,
    dec: &'a CounterVec<StaticLabelSet<Protocol>>,
}

impl Drop for NumConnectionRequestsGuard<'_> {
    fn drop(&mut self) {
        self.dec.inc(self.protocol);
    }
}

impl NumConnectionRequestsGauge {
    pub fn guard(&self, protocol: Protocol) -> NumConnectionRequestsGuard {
        self.accepted_connections_total.inc(protocol);
        NumConnectionRequestsGuard {
            protocol,
            dec: &self.closed_connections_total,
        }
    }
}

#[derive(LabelGroup)]
#[label(set = ComputeConnectionLatencySet)]
pub struct ComputeConnectionLatencyGroup {
    protocol: Protocol,
    cache_miss: Bool,
    pool_miss: Bool,
    outcome: ConnectOutcome,
}

#[derive(FixedCardinalityLabel)]
#[label(singleton = "limit")]
pub enum RateLimit {
    Actual,
    Expected,
}

#[derive(FixedCardinalityLabel)]
#[label(singleton = "kind")]
pub enum SniKind {
    Sni,
    NoSni,
    PasswordHack,
}

#[derive(FixedCardinalityLabel)]
#[label(singleton = "kind")]
pub enum ConnectionFailureKind {
    ComputeCached,
    ComputeUncached,
}

#[derive(FixedCardinalityLabel)]
#[label(singleton = "kind")]
pub enum WakeupFailureKind {
    BadComputeAddress,
    ApiTransportError,
    QuotaExceeded,
    ApiConsoleLocked,
    ApiConsoleBadRequest,
    ApiConsoleOtherServerError,
    ApiConsoleOtherError,
    TimeoutError,
}

#[derive(LabelGroup)]
#[label(set = ConnectionFailuresBreakdownSet)]
pub struct ConnectionFailuresBreakdownGroup {
    pub kind: WakeupFailureKind,
    pub retry: Bool,
}

#[derive(Clone)]
pub struct LatencyTimer {
    // time since the stopwatch was started
    start: Option<time::Instant>,
    // accumulated time on the stopwatch
    pub accumulated: std::time::Duration,
    // label data
    protocol: Protocol,
    cache_miss: bool,
    pool_miss: bool,
    outcome: ConnectOutcome,
}

pub struct LatencyTimerPause<'a> {
    timer: &'a mut LatencyTimer,
}

impl LatencyTimer {
    pub fn new(protocol: Protocol) -> Self {
        Self {
            start: Some(time::Instant::now()),
            accumulated: std::time::Duration::ZERO,
            protocol,
            cache_miss: false,
            // by default we don't do pooling
            pool_miss: true,
            // assume failed unless otherwise specified
            outcome: ConnectOutcome::Failed,
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
        let start = self.start.take().expect("latency timer should be started");
        self.accumulated += start.elapsed();

        // success
        self.outcome = ConnectOutcome::Success;
    }
}

impl Drop for LatencyTimerPause<'_> {
    fn drop(&mut self) {
        // start the stopwatch again
        self.timer.start = Some(time::Instant::now());
    }
}

#[derive(FixedCardinalityLabel, Clone, Copy, Debug)]
enum ConnectOutcome {
    Success,
    Failed,
}

impl Drop for LatencyTimer {
    fn drop(&mut self) {
        let duration =
            self.start.map(|start| start.elapsed()).unwrap_or_default() + self.accumulated;
        Metrics::get()
            .proxy
            .compute_connection_latency_seconds
            .observe(
                ComputeConnectionLatencyGroup {
                    protocol: self.protocol,
                    cache_miss: self.cache_miss.into(),
                    pool_miss: self.pool_miss.into(),
                    outcome: self.outcome,
                },
                duration.as_secs_f64(),
            )
    }
}

impl From<bool> for Bool {
    fn from(value: bool) -> Self {
        if value {
            Bool::True
        } else {
            Bool::False
        }
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

pub static ENDPOINT_ERRORS_BY_KIND: Lazy<HyperLogLogVec<32>> = Lazy::new(|| {
    register_hll_vec!(
        32,
        "proxy_endpoints_affected_by_errors",
        "Number of endpoints affected by errors of a given classification",
        &["type"],
    )
    .unwrap()
});
