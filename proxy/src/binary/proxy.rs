#[cfg(any(test, feature = "testing"))]
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

#[cfg(any(test, feature = "testing"))]
use anyhow::Context;
use anyhow::{bail, anyhow};
use arc_swap::ArcSwapOption;
use futures::future::Either;
use remote_storage::RemoteStorageConfig;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info};
use utils::sentry_init::init_sentry;
use utils::{project_build_tag, project_git_version};

use crate::auth::backend::jwt::JwkCache;
use crate::auth::backend::{ConsoleRedirectBackend, MaybeOwned};
use crate::cancellation::{CancellationHandler, handle_cancel_messages};
use crate::config::{
    self, AuthenticationConfig, CacheOptions, ComputeConfig, HttpConfig, ProjectInfoCacheOptions,
    ProxyConfig, ProxyProtocolV2, remote_storage_from_toml,
};
use crate::context::parquet::ParquetUploadArgs;
use crate::http::health_server::AppMetrics;
use crate::metrics::Metrics;
use crate::rate_limiter::{EndpointRateLimiter, RateBucketInfo, WakeComputeRateLimiter};
use crate::redis::connection_with_credentials_provider::ConnectionWithCredentialsProvider;
use crate::redis::kv_ops::RedisKVClient;
use crate::redis::{elasticache, notifications};
use crate::scram::threadpool::ThreadPool;
use crate::serverless::GlobalConnPoolOptions;
use crate::serverless::cancel_set::CancelSet;
use crate::tls::client_config::compute_client_config_with_root_certs;
#[cfg(any(test, feature = "testing"))]
use crate::url::ApiUrl;
use crate::{auth, control_plane, http, pglb, serverless, usage_metrics};

project_git_version!(GIT_VERSION);
project_build_tag!(BUILD_TAG);

use clap::{Parser, ValueEnum};

#[derive(Clone, Debug, ValueEnum)]
#[clap(rename_all = "kebab-case")]
enum AuthBackendType {
    #[clap(alias("cplane-v1"))]
    ControlPlane,

    #[clap(alias("link"))]
    ConsoleRedirect,

    #[cfg(any(test, feature = "testing"))]
    Postgres,
}

#[derive(Deserialize)]
struct Root {
    #[serde(flatten)]
    legacy: LegacyModes,
    introspection: Introspection,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum LegacyModes {
    Proxy {
        pglb: Pglb,
        neonkeeper: NeonKeeper,
        http: Option<Http>,
        pg_sni_router: Option<PgSniRouter>,
    },
    AuthBroker {
        neonkeeper: NeonKeeper,
        http: Http,
    },
    ConsoleRedirect {
        console_redirect: ConsoleRedirect,
    },
}

#[derive(Deserialize)]
struct Pglb {
    listener: Listener,
}

#[derive(Deserialize)]
struct Listener {
    /// address to bind to
    addr: SocketAddr,
    /// which header should we expect to see on this socket
    /// from the load balancer
    header: Option<ProxyHeader>,

    /// certificates used for TLS.
    /// first cert is the default.
    /// TLS not used if no certs provided.
    certs: Vec<KeyPair>,

    /// Timeout to use for TLS handshake
    timeout: Option<Duration>,
}

#[derive(Deserialize)]
enum ProxyHeader {
    /// Accept the PROXY! protocol V2.
    ProxyProtocolV2(ProxyProtocolV2Kind),
}

#[derive(Deserialize)]
enum ProxyProtocolV2Kind {
    /// Expect AWS TLVs in the header.
    Aws,
    /// Expect Azure TLVs in the header.
    Azure,
}

#[derive(Deserialize)]
struct KeyPair {
    key: PathBuf,
    cert: PathBuf,
}

#[derive(Deserialize)]
/// The service that authenticates all incoming connection attempts,
/// provides monitoring and also wakes computes.
struct NeonKeeper {
    cplane: ControlPlaneBackend,
    redis: Option<Redis>,
    auth: Vec<AuthMechanism>,

    /// map of endpoint->computeinfo
    compute: Cache,
    /// cache for GetEndpointAccessControls.
    project_info_cache: config::ProjectInfoCacheOptions,
    /// cache for all valid endpoints
    endpoint_cache_config: config::EndpointCacheConfig,

    request_log_export: Option<RequestLogExport>,
    data_transfer_export: Option<DataTransferExport>,
}

#[derive(Deserialize)]
struct Redis {
    /// Cancellation channel size (max queue size for redis kv client)
    cancellation_ch_size: usize,
    /// Cancellation ops batch size for redis
    cancellation_batch_size: usize,

    auth: RedisAuthentication,
}

#[derive(Deserialize)]
enum RedisAuthentication {
    /// i don't remember what this stands for.
    /// IAM roles for service accounts?
    Irsa {
        host: String,
        port: u16,
        cluster_name: Option<String>,
        user_id: Option<String>,
        aws_region: String,
    },
    Basic {
        url: url::Url,
    },
}

#[derive(Deserialize)]
struct PgSniRouter {
    /// The listener to use to proxy connections to compute,
    /// assuming the compute does not support TLS.
    listener: Listener,

    /// The listener to use to proxy connections to compute,
    /// assuming the compute requires TLS.
    listener_tls: Listener,

    /// append this domain zone to the SNI hostname to get the destination address
    dest: String,
}

#[derive(Deserialize)]
/// `psql -h pg.neon.tech`.
struct ConsoleRedirect {
    /// Connection requests from clients.
    listener: Listener,
    /// Messages from control plane to accept the connection.
    cplane: Listener,

    /// The base url to use for redirects.
    console: url::Url,

    timeout: Duration,
}

#[derive(Deserialize)]
enum ControlPlaneBackend {
    /// Use the HTTP API to access the control plane.
    Http(url::Url),
    /// Stub the control plane with a postgres instance.
    #[cfg(feature = "testing")]
    PostgresMock(url::Url),
}

#[derive(Deserialize)]
struct Http {
    listener: Listener,
    sql_over_http: SqlOverHttp,

    // todo: move into Pglb.
    websockets: Option<Websockets>,
}

#[derive(Deserialize)]
struct SqlOverHttp {
    pool_max_conns_per_endpoint: usize,
    pool_max_total_conns: usize,
    pool_idle_timeout: Duration,
    pool_gc_epoch: Duration,
    pool_shards: usize,

    client_conn_threshold: u64,
    cancel_set_shards: usize,

    timeout: Duration,
    max_request_size_bytes: usize,
    max_response_size_bytes: usize,

    auth: Vec<AuthMechanism>,
}

#[derive(Deserialize)]
enum AuthMechanism {
    Sasl {
        /// timeout for SASL handshake
        timeout: Duration,
    },
    CleartextPassword {
        /// number of threads for the thread pool
        threads: usize,
    },
    // add something about the jwks cache i guess.
    Jwt {},
}

#[derive(Deserialize)]
struct Websockets {
    auth: Vec<AuthMechanism>,
}

#[derive(Deserialize)]
/// The HTTP API used for internal monitoring.
struct Introspection {
    listener: Listener,
}

#[derive(Deserialize)]
enum RequestLogExport {
    Parquet {
        location: RemoteStorageConfig,
        disconnect: RemoteStorageConfig,

        /// The region identifier to tag the entries with.
        region: String,

        /// How many rows to include in a row group
        row_group_size: usize,

        /// How large each column page should be in bytes
        page_size: usize,

        /// How large the total parquet file should be in bytes
        size: i64,

        /// How long to wait before forcing a file upload
        maximum_duration: tokio::time::Duration,
        // /// What level of compression to use
        // compression: Compression,
    },
}

#[derive(Deserialize)]
enum Cache {
    /// Expire by LRU or by idle.
    /// Note: "live" in "time-to-live" actually means idle here.
    LruTtl {
        /// Max number of entries.
        size: usize,
        /// Entry's time-to-live.
        ttl: Duration,
    },
}

#[derive(Deserialize)]
struct DataTransferExport {
    /// http endpoint to receive periodic metric updates
    endpoint: Option<String>,
    /// how often metrics should be sent to a collection endpoint
    interval: Option<String>,

    /// interval for backup metric collection
    backup_interval: std::time::Duration,
    /// remote storage configuration for backup metric collection
    /// Encoded as toml (same format as pageservers), eg
    /// `{bucket_name='the-bucket',bucket_region='us-east-1',prefix_in_bucket='proxy',endpoint='http://minio:9000'}`
    backup_remote_storage: Option<RemoteStorageConfig>,
    /// chunk size for backup metric collection
    /// Size of each event is no more than 400 bytes, so 2**22 is about 200MB before the compression.
    backup_chunk_size: usize,
}

/// Neon proxy/router
#[derive(Parser)]
#[command(version = GIT_VERSION, about)]
struct ProxyCliArgs {
    /// Name of the region this proxy is deployed in
    #[clap(long, default_value_t = String::new())]
    region: String,
    /// listen for incoming client connections on ip:port
    #[clap(short, long, default_value = "127.0.0.1:4432")]
    proxy: SocketAddr,
    #[clap(value_enum, long, default_value_t = AuthBackendType::ConsoleRedirect)]
    auth_backend: AuthBackendType,
    /// listen for management callback connection on ip:port
    #[clap(short, long, default_value = "127.0.0.1:7000")]
    mgmt: SocketAddr,
    /// listen for incoming http connections (metrics, etc) on ip:port
    #[clap(long, default_value = "127.0.0.1:7001")]
    http: SocketAddr,
    /// listen for incoming wss connections on ip:port
    #[clap(long)]
    wss: Option<SocketAddr>,
    /// redirect unauthenticated users to the given uri in case of console redirect auth
    #[clap(short, long, default_value = "http://localhost:3000/psql_session/")]
    uri: String,
    /// cloud API endpoint for authenticating users
    #[clap(
        short,
        long,
        default_value = "http://localhost:3000/authenticate_proxy_request/"
    )]
    auth_endpoint: String,
    /// JWT used to connect to control plane.
    #[clap(
        long,
        value_name = "JWT",
        default_value = "",
        env = "NEON_PROXY_TO_CONTROLPLANE_TOKEN"
    )]
    control_plane_token: Arc<str>,
    /// if this is not local proxy, this toggles whether we accept jwt or passwords for http
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    is_auth_broker: bool,
    /// path to TLS key for client postgres connections
    ///
    /// tls-key and tls-cert are for backwards compatibility, we can put all certs in one dir
    #[clap(short = 'k', long, alias = "ssl-key")]
    tls_key: Option<PathBuf>,
    /// path to TLS cert for client postgres connections
    ///
    /// tls-key and tls-cert are for backwards compatibility, we can put all certs in one dir
    #[clap(short = 'c', long, alias = "ssl-cert")]
    tls_cert: Option<PathBuf>,
    /// Allow writing TLS session keys to the given file pointed to by the environment variable `SSLKEYLOGFILE`.
    #[clap(long, alias = "allow-ssl-keylogfile")]
    allow_tls_keylogfile: bool,
    /// path to directory with TLS certificates for client postgres connections
    #[clap(long)]
    certs_dir: Option<PathBuf>,
    /// timeout for the TLS handshake
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    handshake_timeout: tokio::time::Duration,
    /// cache for `wake_compute` api method (use `size=0` to disable)
    #[clap(long, default_value = config::CacheOptions::CACHE_DEFAULT_OPTIONS)]
    wake_compute_cache: String,
    /// lock for `wake_compute` api method. example: "shards=32,permits=4,epoch=10m,timeout=1s". (use `permits=0` to disable).
    #[clap(long, default_value = config::ConcurrencyLockOptions::DEFAULT_OPTIONS_WAKE_COMPUTE_LOCK)]
    wake_compute_lock: String,
    /// lock for `connect_compute` api method. example: "shards=32,permits=4,epoch=10m,timeout=1s". (use `permits=0` to disable).
    #[clap(long, default_value = config::ConcurrencyLockOptions::DEFAULT_OPTIONS_CONNECT_COMPUTE_LOCK)]
    connect_compute_lock: String,
    #[clap(flatten)]
    sql_over_http: SqlOverHttpArgs,
    /// timeout for scram authentication protocol
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    scram_protocol_timeout: tokio::time::Duration,
    /// size of the threadpool for password hashing
    #[clap(long, default_value_t = 4)]
    scram_thread_pool_size: u8,
    /// Endpoint rate limiter max number of requests per second.
    ///
    /// Provided in the form `<Requests Per Second>@<Bucket Duration Size>`.
    /// Can be given multiple times for different bucket sizes.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_ENDPOINT_SET)]
    endpoint_rps_limit: Vec<RateBucketInfo>,
    /// Wake compute rate limiter max number of requests per second.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_SET)]
    wake_compute_limit: Vec<RateBucketInfo>,
    /// Cancellation channel size (max queue size for redis kv client)
    #[clap(long, default_value_t = 1024)]
    cancellation_ch_size: usize,
    /// Cancellation ops batch size for redis
    #[clap(long, default_value_t = 8)]
    cancellation_batch_size: usize,
    /// redis url for plain authentication
    #[clap(long, alias("redis-notifications"))]
    redis_plain: Option<String>,
    /// what from the available authentications type to use for redis. Supported are "irsa" and "plain".
    #[clap(long)]
    redis_auth_type: Option<String>,
    /// redis host for irsa authentication
    #[clap(long)]
    redis_host: Option<String>,
    /// redis port for irsa authentication
    #[clap(long)]
    redis_port: Option<u16>,
    /// redis cluster name for irsa authentication
    #[clap(long)]
    redis_cluster_name: Option<String>,
    /// redis user_id for irsa authentication
    #[clap(long)]
    redis_user_id: Option<String>,
    /// aws region for irsa authentication
    #[clap(long, default_value_t = String::new())]
    aws_region: String,
    /// cache for `project_info` (use `size=0` to disable)
    #[clap(long, default_value = config::ProjectInfoCacheOptions::CACHE_DEFAULT_OPTIONS)]
    project_info_cache: String,
    /// cache for all valid endpoints
    #[clap(long, default_value = config::EndpointCacheConfig::CACHE_DEFAULT_OPTIONS)]
    endpoint_cache_config: String,
    #[clap(flatten)]
    parquet_upload: ParquetUploadArgs,

    /// http endpoint to receive periodic metric updates
    #[clap(long)]
    metric_collection_endpoint: Option<String>,
    /// how often metrics should be sent to a collection endpoint
    #[clap(long)]
    metric_collection_interval: Option<String>,
    /// interval for backup metric collection
    #[clap(long, default_value = "10m", value_parser = humantime::parse_duration)]
    metric_backup_collection_interval: std::time::Duration,
    /// remote storage configuration for backup metric collection
    /// Encoded as toml (same format as pageservers), eg
    /// `{bucket_name='the-bucket',bucket_region='us-east-1',prefix_in_bucket='proxy',endpoint='http://minio:9000'}`
    #[clap(long, value_parser = remote_storage_from_toml)]
    metric_backup_collection_remote_storage: Option<RemoteStorageConfig>,
    /// chunk size for backup metric collection
    /// Size of each event is no more than 400 bytes, so 2**22 is about 200MB before the compression.
    #[clap(long, default_value = "4194304")]
    metric_backup_collection_chunk_size: usize,

    /// Whether to retry the connection to the compute node
    #[clap(long, default_value = config::RetryConfig::CONNECT_TO_COMPUTE_DEFAULT_VALUES)]
    connect_to_compute_retry: String,
    /// Whether to retry the wake_compute request
    #[clap(long, default_value = config::RetryConfig::WAKE_COMPUTE_DEFAULT_VALUES)]
    wake_compute_retry: String,

    /// Configure if this is a private access proxy for the POC: In that case the proxy will ignore the IP allowlist
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    is_private_access_proxy: bool,

    /// Configure whether all incoming requests have a Proxy Protocol V2 packet.
    #[clap(value_enum, long, default_value_t = ProxyProtocolV2::Rejected)]
    proxy_protocol_v2: ProxyProtocolV2,

    /// Time the proxy waits for the webauth session to be confirmed by the control plane.
    // TODO: rename to `console_redirect_confirmation_timeout`.
    #[clap(long, default_value = "2m", value_parser = humantime::parse_duration)]
    webauth_confirmation_timeout: std::time::Duration,

    #[clap(flatten)]
    pg_sni_router: PgSniRouterArgs,
}

#[derive(clap::Args, Clone, Copy, Debug)]
struct SqlOverHttpArgs {
    /// timeout for http connection requests
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    sql_over_http_timeout: tokio::time::Duration,

    /// Whether the SQL over http pool is opt-in
    #[clap(long, default_value_t = true, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    sql_over_http_pool_opt_in: bool,

    /// How many connections to pool for each endpoint. Excess connections are discarded
    #[clap(long, default_value_t = 20)]
    sql_over_http_pool_max_conns_per_endpoint: usize,

    /// How many connections to pool for each endpoint. Excess connections are discarded
    #[clap(long, default_value_t = 20000)]
    sql_over_http_pool_max_total_conns: usize,

    /// How long pooled connections should remain idle for before closing
    #[clap(long, default_value = "5m", value_parser = humantime::parse_duration)]
    sql_over_http_idle_timeout: tokio::time::Duration,

    /// Duration each shard will wait on average before a GC sweep.
    /// A longer time will causes sweeps to take longer but will interfere less frequently.
    #[clap(long, default_value = "10m", value_parser = humantime::parse_duration)]
    sql_over_http_pool_gc_epoch: tokio::time::Duration,

    /// How many shards should the global pool have. Must be a power of two.
    /// More shards will introduce less contention for pool operations, but can
    /// increase memory used by the pool
    #[clap(long, default_value_t = 128)]
    sql_over_http_pool_shards: usize,

    #[clap(long, default_value_t = 10000)]
    sql_over_http_client_conn_threshold: u64,

    #[clap(long, default_value_t = 64)]
    sql_over_http_cancel_set_shards: usize,

    #[clap(long, default_value_t = 10 * 1024 * 1024)] // 10 MiB
    sql_over_http_max_request_size_bytes: usize,

    #[clap(long, default_value_t = 10 * 1024 * 1024)] // 10 MiB
    sql_over_http_max_response_size_bytes: usize,
}

#[derive(clap::Args, Clone, Debug)]
struct PgSniRouterArgs {
    /// listen for incoming client connections on ip:port
    #[clap(id = "sni-router-listen", long, default_value = "127.0.0.1:4432")]
    listen: SocketAddr,
    /// listen for incoming client connections on ip:port, requiring TLS to compute
    #[clap(id = "sni-router-listen-tls", long, default_value = "127.0.0.1:4433")]
    listen_tls: SocketAddr,
    /// path to TLS key for client postgres connections
    #[clap(id = "sni-router-tls-key", long)]
    tls_key: Option<PathBuf>,
    /// path to TLS cert for client postgres connections
    #[clap(id = "sni-router-tls-cert", long)]
    tls_cert: Option<PathBuf>,
    /// append this domain zone to the SNI hostname to get the destination address
    #[clap(id = "sni-router-destination", long)]
    dest: Option<String>,
}

pub async fn run() -> anyhow::Result<()> {
    let _logging_guard = crate::logging::init().await?;
    let _panic_hook_guard = utils::logging::replace_panic_hook_with_tracing_panic_hook();
    let _sentry_guard = init_sentry(Some(GIT_VERSION.into()), &[]);

    // TODO: refactor these to use labels
    info!("Version: {GIT_VERSION}");
    info!("Build_tag: {BUILD_TAG}");
    let neon_metrics = ::metrics::NeonMetrics::new(::metrics::BuildInfo {
        revision: GIT_VERSION,
        build_tag: BUILD_TAG,
    });

    let jemalloc = match crate::jemalloc::MetricRecorder::new() {
        Ok(t) => Some(t),
        Err(e) => {
            tracing::error!(error = ?e, "could not start jemalloc metrics loop");
            None
        }
    };

    let config: Root = toml::from_str(&tokio::fs::read_to_string("proxy.toml").await?)?;

    // client facing tasks. these will exit on error or on cancellation
    // cancellation returns Ok(())
    let mut client_tasks = JoinSet::new();

    // maintenance tasks. these never return unless there's an error
    let mut maintenance_tasks = JoinSet::new();

    let cancellation_token = CancellationToken::new();

    match config.legacy {
        LegacyModes::Proxy {
            pglb,
            neonkeeper,
            http,
            pg_sni_router,
        } => {
            let endpoint_rate_limiter = Arc::new(EndpointRateLimiter::new_with_shards(
                // todo: use neonkeeper config.
                EndpointRateLimiter::DEFAULT,
                64,
            ));

            info!("Starting proxy on {}", pglb.listener.addr);
            let proxy_listener = TcpListener::bind(pglb.listener.addr).await?;

            client_tasks.spawn(crate::proxy::task_main(
                config,
                auth_backend,
                proxy_listener,
                cancellation_token.clone(),
                cancellation_handler.clone(),
                endpoint_rate_limiter.clone(),
            ));

            if let Some(http) = http {
                info!("Starting wss on {}", http.listener.addr);
                let http_listener = TcpListener::bind(http.listener.addr).await?;

                client_tasks.spawn(serverless::task_main(
                    config,
                    auth_backend,
                    http_listener,
                    cancellation_token.clone(),
                    cancellation_handler.clone(),
                    endpoint_rate_limiter.clone(),
                ));
            };

            if let Some(redis) = neonkeeper.redis {
                let client = configure_redis(redis.auth);
            }

            if let Some(sni_router) = pg_sni_router {
                let listen = TcpListener::bind(sni_router.listener.addr).await?;
                let listen_tls = TcpListener::bind(sni_router.listener_tls.addr).await?;

                let [KeyPair { key, cert }] = sni_router
                    .listener
                    .certs
                    .try_into()
                    .map_err(|_| anyhow!("only 1 keypair is supported for pg-sni-router"))?;

                let tls_config = super::pg_sni_router::parse_tls(&key, &cert)?;

                let dest = Arc::new(sni_router.dest);

                client_tasks.spawn(super::pg_sni_router::task_main(
                    dest.clone(),
                    tls_config.clone(),
                    None,
                    listen,
                    cancellation_token.clone(),
                ));

                client_tasks.spawn(super::pg_sni_router::task_main(
                    dest,
                    tls_config,
                    Some(config.connect_to_compute.tls.clone()),
                    listen_tls,
                    cancellation_token.clone(),
                ));
            }

            match neonkeeper.request_log_export {
                Some(RequestLogExport::Parquet {
                    location,
                    disconnect,
                    region,
                    row_group_size,
                    page_size,
                    size,
                    maximum_duration,
                }) => {
                    client_tasks.spawn(crate::context::parquet::worker(
                        cancellation_token.clone(),
                        args.parquet_upload,
                        args.region,
                    ));
                }
                None => {}
            }

            if let (ControlPlaneBackend::Http(api), Some(redis)) =
                (neonkeeper.cplane, neonkeeper.redis)
            {
                // project info cache and invalidation of that cache.
                let cache = api.caches.project_info.clone();
                maintenance_tasks.spawn(notifications::task_main(client.clone(), cache.clone()));
                maintenance_tasks.spawn(async move { cache.clone().gc_worker().await });

                // cancellation key management
                let mut redis_kv_client = RedisKVClient::new(client.clone());
                maintenance_tasks.spawn(async move {
                    redis_kv_client.try_connect().await?;
                    handle_cancel_messages(
                        &mut redis_kv_client,
                        rx_cancel,
                        args.cancellation_batch_size,
                    )
                    .await?;

                    drop(redis_kv_client);

                    // `handle_cancel_messages` was terminated due to the tx_cancel
                    // being dropped. this is not worthy of an error, and this task can only return `Err`,
                    // so let's wait forever instead.
                    std::future::pending().await
                });

                // listen for notifications of new projects/endpoints/branches
                let cache = api.caches.endpoints_cache.clone();
                let span = tracing::info_span!("endpoints_cache");
                maintenance_tasks.spawn(
                    async move { cache.do_read(client, cancellation_token.clone()).await }
                        .instrument(span),
                );
            }
        }
        LegacyModes::AuthBroker { neonkeeper, http } => {
            let endpoint_rate_limiter = Arc::new(EndpointRateLimiter::new_with_shards(
                // todo: use neonkeeper config.
                EndpointRateLimiter::DEFAULT,
                64,
            ));

            info!("Starting wss on {}", http.listener.addr);
            let http_listener = TcpListener::bind(http.listener.addr).await?;

            if let Some(redis) = neonkeeper.redis {
                let client = configure_redis(redis.auth);
            }

            client_tasks.spawn(serverless::task_main(
                config,
                auth_backend,
                serverless_listener,
                cancellation_token.clone(),
                cancellation_handler.clone(),
                endpoint_rate_limiter.clone(),
            ));

            match neonkeeper.request_log_export {
                Some(RequestLogExport::Parquet {
                    location,
                    disconnect,
                    region,
                    row_group_size,
                    page_size,
                    size,
                    maximum_duration,
                }) => {
                    client_tasks.spawn(crate::context::parquet::worker(
                        cancellation_token.clone(),
                        args.parquet_upload,
                        args.region,
                    ));
                }
                None => {}
            }

            if let (ControlPlaneBackend::Http(api), Some(redis)) =
                (neonkeeper.cplane, neonkeeper.redis)
            {
                // project info cache and invalidation of that cache.
                let cache = api.caches.project_info.clone();
                maintenance_tasks.spawn(notifications::task_main(client.clone(), cache.clone()));
                maintenance_tasks.spawn(async move { cache.clone().gc_worker().await });

                // cancellation key management
                let mut redis_kv_client = RedisKVClient::new(client.clone());
                maintenance_tasks.spawn(async move {
                    redis_kv_client.try_connect().await?;
                    handle_cancel_messages(
                        &mut redis_kv_client,
                        rx_cancel,
                        args.cancellation_batch_size,
                    )
                    .await?;

                    drop(redis_kv_client);

                    // `handle_cancel_messages` was terminated due to the tx_cancel
                    // being dropped. this is not worthy of an error, and this task can only return `Err`,
                    // so let's wait forever instead.
                    std::future::pending().await
                });

                // listen for notifications of new projects/endpoints/branches
                let cache = api.caches.endpoints_cache.clone();
                let span = tracing::info_span!("endpoints_cache");
                maintenance_tasks.spawn(
                    async move { cache.do_read(client, cancellation_token.clone()).await }
                        .instrument(span),
                );
            }
        }
        LegacyModes::ConsoleRedirect { console_redirect } => {
            info!("Starting proxy on {}", console_redirect.listener.addr);
            let proxy_listener = TcpListener::bind(console_redirect.listener.addr).await?;

            info!("Starting mgmt on {}", console_redirect.listener.addr);
            let mgmt_listener = TcpListener::bind(console_redirect.listener.addr).await?;

            client_tasks.spawn(crate::console_redirect_proxy::task_main(
                config,
                auth_backend,
                proxy_listener,
                cancellation_token.clone(),
                cancellation_handler.clone(),
            ));
            maintenance_tasks.spawn(control_plane::mgmt::task_main(mgmt_listener));
        }
    }

    // Check that we can bind to address before further initialization
    info!("Starting http on {}", config.introspection.listener.addr);
    let http_listener = TcpListener::bind(config.introspection.listener.addr)
        .await?
        .into_std()?;

    // channel size should be higher than redis client limit to avoid blocking
    let cancel_ch_size = args.cancellation_ch_size;
    let (tx_cancel, rx_cancel) = tokio::sync::mpsc::channel(cancel_ch_size);
    let cancellation_handler = Arc::new(CancellationHandler::new(
        &config.connect_to_compute,
        Some(tx_cancel),
    ));

    maintenance_tasks.spawn(crate::signals::handle(cancellation_token.clone(), || {}));
    maintenance_tasks.spawn(http::health_server::task_main(
        http_listener,
        AppMetrics {
            jemalloc,
            neon_metrics,
            proxy: crate::metrics::Metrics::get(),
        },
    ));

    if let Some(metrics_config) = &config.metric_collection {
        // TODO: Add gc regardles of the metric collection being enabled.
        maintenance_tasks.spawn(usage_metrics::task_main(metrics_config));
    }

    let maintenance = loop {
        // get one complete task
        match futures::future::select(
            pin!(maintenance_tasks.join_next()),
            pin!(client_tasks.join_next()),
        )
        .await
        {
            // exit immediately on maintenance task completion
            Either::Left((Some(res), _)) => break crate::error::flatten_err(res)?,
            // exit with error immediately if all maintenance tasks have ceased (should be caught by branch above)
            Either::Left((None, _)) => bail!("no maintenance tasks running. invalid state"),
            // exit immediately on client task error
            Either::Right((Some(res), _)) => crate::error::flatten_err(res)?,
            // exit if all our client tasks have shutdown gracefully
            Either::Right((None, _)) => return Ok(()),
        }
    };

    // maintenance tasks return Infallible success values, this is an impossible value
    // so this match statically ensures that there are no possibilities for that value
    match maintenance {}
}

/// ProxyConfig is created at proxy startup, and lives forever.
fn build_config(args: &ProxyCliArgs) -> anyhow::Result<&'static ProxyConfig> {
    let thread_pool = ThreadPool::new(args.scram_thread_pool_size);
    Metrics::install(thread_pool.metrics.clone());

    let tls_config = match (&args.tls_key, &args.tls_cert) {
        (Some(key_path), Some(cert_path)) => Some(config::configure_tls(
            key_path,
            cert_path,
            args.certs_dir.as_deref(),
            args.allow_tls_keylogfile,
        )?),
        (None, None) => None,
        _ => bail!("either both or neither tls-key and tls-cert must be specified"),
    };
    let tls_config = ArcSwapOption::from(tls_config.map(Arc::new));

    let backup_metric_collection_config = config::MetricBackupCollectionConfig {
        remote_storage_config: args.metric_backup_collection_remote_storage.clone(),
        chunk_size: args.metric_backup_collection_chunk_size,
    };

    let metric_collection = match (
        &args.metric_collection_endpoint,
        &args.metric_collection_interval,
    ) {
        (Some(endpoint), Some(interval)) => Some(config::MetricCollectionConfig {
            endpoint: endpoint.parse()?,
            interval: humantime::parse_duration(interval)?,
            backup_metric_collection_config,
        }),
        (None, None) => None,
        _ => bail!(
            "either both or neither metric-collection-endpoint \
             and metric-collection-interval must be specified"
        ),
    };

    let config::ConcurrencyLockOptions {
        shards,
        limiter,
        epoch,
        timeout,
    } = args.connect_compute_lock.parse()?;
    info!(
        ?limiter,
        shards,
        ?epoch,
        "Using NodeLocks (connect_compute)"
    );
    let connect_compute_locks = control_plane::locks::ApiLocks::new(
        "connect_compute_lock",
        limiter,
        shards,
        timeout,
        epoch,
        &Metrics::get().proxy.connect_compute_lock,
    );

    let http_config = HttpConfig {
        accept_websockets: !args.is_auth_broker,
        pool_options: GlobalConnPoolOptions {
            max_conns_per_endpoint: args.sql_over_http.sql_over_http_pool_max_conns_per_endpoint,
            gc_epoch: args.sql_over_http.sql_over_http_pool_gc_epoch,
            pool_shards: args.sql_over_http.sql_over_http_pool_shards,
            idle_timeout: args.sql_over_http.sql_over_http_idle_timeout,
            opt_in: args.sql_over_http.sql_over_http_pool_opt_in,
            max_total_conns: args.sql_over_http.sql_over_http_pool_max_total_conns,
        },
        cancel_set: CancelSet::new(args.sql_over_http.sql_over_http_cancel_set_shards),
        client_conn_threshold: args.sql_over_http.sql_over_http_client_conn_threshold,
        max_request_size_bytes: args.sql_over_http.sql_over_http_max_request_size_bytes,
        max_response_size_bytes: args.sql_over_http.sql_over_http_max_response_size_bytes,
    };
    let authentication_config = AuthenticationConfig {
        jwks_cache: JwkCache::default(),
        thread_pool,
        scram_protocol_timeout: args.scram_protocol_timeout,
        ip_allowlist_check_enabled: !args.is_private_access_proxy,
        is_vpc_acccess_proxy: args.is_private_access_proxy,
        is_auth_broker: args.is_auth_broker,
        accept_jwts: args.is_auth_broker,
        console_redirect_confirmation_timeout: args.webauth_confirmation_timeout,
    };

    let compute_config = ComputeConfig {
        retry: config::RetryConfig::parse(&args.connect_to_compute_retry)?,
        tls: Arc::new(compute_client_config_with_root_certs()?),
        timeout: Duration::from_secs(2),
    };

    let config = ProxyConfig {
        tls_config,
        metric_collection,
        http_config,
        authentication_config,
        proxy_protocol_v2: args.proxy_protocol_v2,
        handshake_timeout: args.handshake_timeout,
        wake_compute_retry_config: config::RetryConfig::parse(&args.wake_compute_retry)?,
        connect_compute_locks,
        connect_to_compute: compute_config,
    };

    let config = Box::leak(Box::new(config));

    tokio::spawn(config.connect_compute_locks.garbage_collect_worker());

    Ok(config)
}

/// auth::Backend is created at proxy startup, and lives forever.
fn build_auth_backend(
    args: &ProxyCliArgs,
) -> anyhow::Result<Either<&'static auth::Backend<'static, ()>, &'static ConsoleRedirectBackend>> {
    match &args.auth_backend {
        AuthBackendType::ControlPlane => {
            let wake_compute_cache_config: CacheOptions = args.wake_compute_cache.parse()?;
            let project_info_cache_config: ProjectInfoCacheOptions =
                args.project_info_cache.parse()?;
            let endpoint_cache_config: config::EndpointCacheConfig =
                args.endpoint_cache_config.parse()?;

            info!("Using NodeInfoCache (wake_compute) with options={wake_compute_cache_config:?}");
            info!(
                "Using AllowedIpsCache (wake_compute) with options={project_info_cache_config:?}"
            );
            info!("Using EndpointCacheConfig with options={endpoint_cache_config:?}");
            let caches = Box::leak(Box::new(control_plane::caches::ApiCaches::new(
                wake_compute_cache_config,
                project_info_cache_config,
                endpoint_cache_config,
            )));

            let config::ConcurrencyLockOptions {
                shards,
                limiter,
                epoch,
                timeout,
            } = args.wake_compute_lock.parse()?;
            info!(?limiter, shards, ?epoch, "Using NodeLocks (wake_compute)");
            let locks = Box::leak(Box::new(control_plane::locks::ApiLocks::new(
                "wake_compute_lock",
                limiter,
                shards,
                timeout,
                epoch,
                &Metrics::get().wake_compute_lock,
            )));
            tokio::spawn(locks.garbage_collect_worker());

            let url: crate::url::ApiUrl = args.auth_endpoint.parse()?;

            let endpoint = http::Endpoint::new(url, http::new_client());

            let mut wake_compute_rps_limit = args.wake_compute_limit.clone();
            RateBucketInfo::validate(&mut wake_compute_rps_limit)?;
            let wake_compute_endpoint_rate_limiter =
                Arc::new(WakeComputeRateLimiter::new(wake_compute_rps_limit));

            let api = control_plane::client::cplane_proxy_v1::NeonControlPlaneClient::new(
                endpoint,
                args.control_plane_token.clone(),
                caches,
                locks,
                wake_compute_endpoint_rate_limiter,
            );

            let api = control_plane::client::ControlPlaneClient::ProxyV1(api);
            let auth_backend = auth::Backend::ControlPlane(MaybeOwned::Owned(api), ());
            let config = Box::leak(Box::new(auth_backend));

            Ok(Either::Left(config))
        }

        #[cfg(any(test, feature = "testing"))]
        AuthBackendType::Postgres => {
            let mut url: ApiUrl = args.auth_endpoint.parse()?;
            if url.password().is_none() {
                let password = env::var("PGPASSWORD")
                    .with_context(|| "auth-endpoint does not contain a password and environment variable `PGPASSWORD` is not set")?;
                url.set_password(Some(&password))
                    .expect("Failed to set password");
            }
            let api = control_plane::client::mock::MockControlPlane::new(
                url,
                !args.is_private_access_proxy,
            );
            let api = control_plane::client::ControlPlaneClient::PostgresMock(api);

            let auth_backend = auth::Backend::ControlPlane(MaybeOwned::Owned(api), ());

            let config = Box::leak(Box::new(auth_backend));

            Ok(Either::Left(config))
        }

        AuthBackendType::ConsoleRedirect => {
            let wake_compute_cache_config: CacheOptions = args.wake_compute_cache.parse()?;
            let project_info_cache_config: ProjectInfoCacheOptions =
                args.project_info_cache.parse()?;
            let endpoint_cache_config: config::EndpointCacheConfig =
                args.endpoint_cache_config.parse()?;

            info!("Using NodeInfoCache (wake_compute) with options={wake_compute_cache_config:?}");
            info!(
                "Using AllowedIpsCache (wake_compute) with options={project_info_cache_config:?}"
            );
            info!("Using EndpointCacheConfig with options={endpoint_cache_config:?}");
            let caches = Box::leak(Box::new(control_plane::caches::ApiCaches::new(
                wake_compute_cache_config,
                project_info_cache_config,
                endpoint_cache_config,
            )));

            let config::ConcurrencyLockOptions {
                shards,
                limiter,
                epoch,
                timeout,
            } = args.wake_compute_lock.parse()?;
            info!(?limiter, shards, ?epoch, "Using NodeLocks (wake_compute)");
            let locks = Box::leak(Box::new(control_plane::locks::ApiLocks::new(
                "wake_compute_lock",
                limiter,
                shards,
                timeout,
                epoch,
                &Metrics::get().wake_compute_lock,
            )));

            let url = args.uri.clone().parse()?;
            let ep_url: crate::url::ApiUrl = args.auth_endpoint.parse()?;
            let endpoint = http::Endpoint::new(ep_url, http::new_client());
            let mut wake_compute_rps_limit = args.wake_compute_limit.clone();
            RateBucketInfo::validate(&mut wake_compute_rps_limit)?;
            let wake_compute_endpoint_rate_limiter =
                Arc::new(WakeComputeRateLimiter::new(wake_compute_rps_limit));

            // Since we use only get_allowed_ips_and_secret() wake_compute_endpoint_rate_limiter
            // and locks are not used in ConsoleRedirectBackend,
            // but they are required by the NeonControlPlaneClient
            let api = control_plane::client::cplane_proxy_v1::NeonControlPlaneClient::new(
                endpoint,
                args.control_plane_token.clone(),
                caches,
                locks,
                wake_compute_endpoint_rate_limiter,
            );

            let backend = ConsoleRedirectBackend::new(url, api);
            let config = Box::leak(Box::new(backend));

            Ok(Either::Right(config))
        }
    }
}

async fn configure_redis(auth: RedisAuthentication) -> ConnectionWithCredentialsProvider {
    match auth {
        RedisAuthentication::Irsa {
            host,
            port,
            cluster_name,
            user_id,
            aws_region,
        } => ConnectionWithCredentialsProvider::new_with_credentials_provider(
            host,
            port,
            elasticache::CredentialsProvider::new(aws_region, cluster_name, user_id).await,
        ),
        RedisAuthentication::Basic { url } => {
            ConnectionWithCredentialsProvider::new_with_static_credentials(url.clone())
        }
        }
        }
        None => None,
    };

    // let redis_notifications_client = if let Some(url) = &args.redis_notifications {
    //     Some(ConnectionWithCredentialsProvider::new_with_static_credentials(&**url))
    // } else {
    //     regional_redis_client.clone()
    // };

    Ok(redis_client)
    }
        None => None,
    };

    // let redis_notifications_client = if let Some(url) = &args.redis_notifications {
    //     Some(ConnectionWithCredentialsProvider::new_with_static_credentials(&**url))
    // } else {
    //     regional_redis_client.clone()
    // };

    Ok(redis_client)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use clap::Parser;

    use crate::rate_limiter::RateBucketInfo;

    #[test]
    fn parse_endpoint_rps_limit() {
        let config = super::ProxyCliArgs::parse_from([
            "proxy",
            "--endpoint-rps-limit",
            "100@1s",
            "--endpoint-rps-limit",
            "20@30s",
        ]);

        assert_eq!(
            config.endpoint_rps_limit,
            vec![
                RateBucketInfo::new(100, Duration::from_secs(1)),
                RateBucketInfo::new(20, Duration::from_secs(30)),
            ]
        );
    }
}
