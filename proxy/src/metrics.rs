use std::sync::OnceLock;

use ::measured::metric::name::MetricName;
use ::metrics::{register_hll_vec, HyperLogLogVec};
use lasso::ThreadedRodeo;
use measured::{
    label::StaticLabelSet, metric::histogram::Thresholds, Counter, CounterVec,
    FixedCardinalityLabel, Gauge, GaugeVec, Histogram, HistogramVec, LabelGroup, MetricGroup,
};
use metrics::{register_hll, CounterPairAssoc, CounterPairVec, HyperLogLog};

use once_cell::sync::Lazy;
use tokio::time::{self, Instant};

use crate::console::messages::ColdStartInfo;

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
    pub db_connections: CounterPairVec<NumDbConnectionsGauge>,
    #[metric(flatten)]
    pub client_connections: CounterPairVec<NumClientConnectionsGauge>,
    #[metric(flatten)]
    pub connection_requests: CounterPairVec<NumConnectionRequestsGauge>,
    #[metric(flatten)]
    pub http_endpoint_pools: HttpEndpointPools,

    /// Time it took for proxy to establish a connection to the compute endpoint.
    // largest bucket = 2^16 * 0.5ms = 32s
    #[metric(metadata = Thresholds::exponential_buckets(0.0005, 2.0))]
    pub compute_connection_latency_seconds: HistogramVec<ComputeConnectionLatencySet, 16>,

    /// Time it took for proxy to receive a response from control plane.
    #[metric(
        // largest bucket = 2^16 * 0.2ms = 13s
        metadata = Thresholds::exponential_buckets(0.0002, 2.0),
    )]
    pub compute_console_request_latency: HistogramVec<ConsoleRequestSet, 16>,

    /// Time it takes to acquire a token to call console plane.
    // largest bucket = 3^16 * 0.05ms = 2.15s
    #[metric(metadata = Thresholds::exponential_buckets(0.00005, 3.0))]
    pub control_plane_token_acquire_seconds: Histogram<16>,

    /// Size of the HTTP request body lengths.
    // smallest bucket = 16 bytes
    // largest bucket = 4^12 * 16 bytes = 256MB
    #[metric(metadata = Thresholds::exponential_buckets(16.0, 4.0))]
    pub http_conn_content_length_bytes: HistogramVec<StaticLabelSet<HttpDirection>, 12>,

    /// Time it takes to reclaim unused connection pools.
    #[metric(metadata = Thresholds::exponential_buckets(1e-6, 2.0))]
    pub http_pool_reclaimation_lag_seconds: Histogram<16>,

    /// Number of opened connections to a database.
    pub http_pool_opened_connections: Gauge,

    /// Number of cache hits/misses for allowed ips.
    pub allowed_ips_cache_misses: CounterVec<StaticLabelSet<CacheOutcome>>,

    /// Number of allowed ips
    #[metric(metadata = Thresholds::with_buckets([0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0, 50.0, 100.0]))]
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

    /// Number of cancellation requests (per found/not_found).
    pub cancellation_requests_total: CounterVec<CancellationRequestSet>,

    /// Number of errors by a given classification
    pub redis_errors_total: CounterVec<RedisErrorsSet>,

    /// Number of TLS handshake failures
    pub tls_handshake_failures: Counter,

    /// Number of connection requests affected by authentication rate limits
    pub requests_auth_rate_limits_total: Counter,
}

impl Default for ProxyMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
#[label(singleton = "direction")]
pub enum HttpDirection {
    Request,
    Response,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
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

#[derive(FixedCardinalityLabel, Copy, Clone)]
pub enum Bool {
    True,
    False,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
#[label(singleton = "outcome")]
pub enum Outcome {
    Success,
    Failed,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
#[label(singleton = "outcome")]
pub enum CacheOutcome {
    Hit,
    Miss,
}

#[derive(LabelGroup)]
#[label(set = ConsoleRequestSet)]
pub struct ConsoleRequest<'a> {
    #[label(dynamic_with = ThreadedRodeo, default)]
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
pub struct NumDbConnectionsGauge;
impl CounterPairAssoc for NumDbConnectionsGauge {
    const INC_NAME: &'static MetricName = MetricName::from_str("opened_db_connections_total");
    const DEC_NAME: &'static MetricName = MetricName::from_str("closed_db_connections_total");
    const INC_HELP: &'static str = "Number of opened connections to a database.";
    const DEC_HELP: &'static str = "Number of closed connections to a database.";
    type LabelGroupSet = StaticLabelSet<Protocol>;
}
pub type NumDbConnectionsGuard<'a> = metrics::MeasuredCounterPairGuard<'a, NumDbConnectionsGauge>;

pub struct NumClientConnectionsGauge;
impl CounterPairAssoc for NumClientConnectionsGauge {
    const INC_NAME: &'static MetricName = MetricName::from_str("opened_client_connections_total");
    const DEC_NAME: &'static MetricName = MetricName::from_str("closed_client_connections_total");
    const INC_HELP: &'static str = "Number of opened connections from a client.";
    const DEC_HELP: &'static str = "Number of closed connections from a client.";
    type LabelGroupSet = StaticLabelSet<Protocol>;
}
pub type NumClientConnectionsGuard<'a> =
    metrics::MeasuredCounterPairGuard<'a, NumClientConnectionsGauge>;

pub struct NumConnectionRequestsGauge;
impl CounterPairAssoc for NumConnectionRequestsGauge {
    const INC_NAME: &'static MetricName = MetricName::from_str("accepted_connections_total");
    const DEC_NAME: &'static MetricName = MetricName::from_str("closed_connections_total");
    const INC_HELP: &'static str = "Number of client connections accepted.";
    const DEC_HELP: &'static str = "Number of client connections closed.";
    type LabelGroupSet = StaticLabelSet<Protocol>;
}
pub type NumConnectionRequestsGuard<'a> =
    metrics::MeasuredCounterPairGuard<'a, NumConnectionRequestsGauge>;

#[derive(LabelGroup)]
#[label(set = ComputeConnectionLatencySet)]
pub struct ComputeConnectionLatencyGroup {
    protocol: Protocol,
    cold_start_info: ColdStartInfo,
    outcome: ConnectOutcome,
    excluded: LatencyExclusions,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
pub enum LatencyExclusions {
    Client,
    ClientAndCplane,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
#[label(singleton = "limit")]
pub enum RateLimit {
    Actual,
    Expected,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
#[label(singleton = "kind")]
pub enum SniKind {
    Sni,
    NoSni,
    PasswordHack,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
#[label(singleton = "kind")]
pub enum ConnectionFailureKind {
    ComputeCached,
    ComputeUncached,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
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

#[derive(LabelGroup, Copy, Clone)]
#[label(set = RedisErrorsSet)]
pub struct RedisErrors<'a> {
    #[label(dynamic_with = ThreadedRodeo, default)]
    pub channel: &'a str,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
pub enum CancellationSource {
    FromClient,
    FromRedis,
    Local,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
pub enum CancellationOutcome {
    NotFound,
    Found,
}

#[derive(LabelGroup)]
#[label(set = CancellationRequestSet)]
pub struct CancellationRequest {
    pub source: CancellationSource,
    pub kind: CancellationOutcome,
}

pub enum Waiting {
    Cplane,
    Client,
    Compute,
}

#[derive(Default)]
struct Accumulated {
    cplane: time::Duration,
    client: time::Duration,
    compute: time::Duration,
}

pub struct LatencyTimer {
    // time since the stopwatch was started
    start: time::Instant,
    // time since the stopwatch was stopped
    stop: Option<time::Instant>,
    // accumulated time on the stopwatch
    accumulated: Accumulated,
    // label data
    protocol: Protocol,
    cold_start_info: ColdStartInfo,
    outcome: ConnectOutcome,
}

pub struct LatencyTimerPause<'a> {
    timer: &'a mut LatencyTimer,
    start: time::Instant,
    waiting_for: Waiting,
}

impl LatencyTimer {
    pub fn new(protocol: Protocol) -> Self {
        Self {
            start: time::Instant::now(),
            stop: None,
            accumulated: Accumulated::default(),
            protocol,
            cold_start_info: ColdStartInfo::Unknown,
            // assume failed unless otherwise specified
            outcome: ConnectOutcome::Failed,
        }
    }

    pub fn pause(&mut self, waiting_for: Waiting) -> LatencyTimerPause<'_> {
        LatencyTimerPause {
            timer: self,
            start: Instant::now(),
            waiting_for,
        }
    }

    pub fn cold_start_info(&mut self, cold_start_info: ColdStartInfo) {
        self.cold_start_info = cold_start_info;
    }

    pub fn success(&mut self) {
        // stop the stopwatch and record the time that we have accumulated
        self.stop = Some(time::Instant::now());

        // success
        self.outcome = ConnectOutcome::Success;
    }
}

impl Drop for LatencyTimerPause<'_> {
    fn drop(&mut self) {
        let dur = self.start.elapsed();
        match self.waiting_for {
            Waiting::Cplane => self.timer.accumulated.cplane += dur,
            Waiting::Client => self.timer.accumulated.client += dur,
            Waiting::Compute => self.timer.accumulated.compute += dur,
        }
    }
}

#[derive(FixedCardinalityLabel, Clone, Copy, Debug)]
enum ConnectOutcome {
    Success,
    Failed,
}

impl Drop for LatencyTimer {
    fn drop(&mut self) {
        let duration = self
            .stop
            .unwrap_or_else(time::Instant::now)
            .duration_since(self.start);

        let metric = &Metrics::get().proxy.compute_connection_latency_seconds;

        // Excluding client communication from the accumulated time.
        metric.observe(
            ComputeConnectionLatencyGroup {
                protocol: self.protocol,
                cold_start_info: self.cold_start_info,
                outcome: self.outcome,
                excluded: LatencyExclusions::Client,
            },
            duration
                .saturating_sub(self.accumulated.client)
                .as_secs_f64(),
        );

        // Exclude client and cplane communication from the accumulated time.
        let accumulated_total = self.accumulated.client + self.accumulated.cplane;
        metric.observe(
            ComputeConnectionLatencyGroup {
                protocol: self.protocol,
                cold_start_info: self.cold_start_info,
                outcome: self.outcome,
                excluded: LatencyExclusions::Client,
            },
            duration.saturating_sub(accumulated_total).as_secs_f64(),
        );
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

pub static ENDPOINTS_AUTH_RATE_LIMITED: Lazy<HyperLogLog<32>> = Lazy::new(|| {
    register_hll!(
        32,
        "proxy_endpoints_auth_rate_limits",
        "Number of endpoints affected by authentication rate limits",
    )
    .unwrap()
});
