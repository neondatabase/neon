#[cfg(any(test, feature = "testing"))]
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

#[cfg(any(test, feature = "testing"))]
use anyhow::Context;
use anyhow::{bail, ensure};
use arc_swap::ArcSwapOption;
use futures::future::Either;
use itertools::{Itertools, Position};
use rand::{Rng, thread_rng};
use remote_storage::RemoteStorageConfig;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, warn};
use utils::sentry_init::init_sentry;
use utils::{project_build_tag, project_git_version};

use crate::auth::backend::jwt::JwkCache;
use crate::auth::backend::{ConsoleRedirectBackend, MaybeOwned};
use crate::batch::BatchQueue;
use crate::cancellation::{CancellationHandler, CancellationProcessor};
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
use crate::{auth, control_plane, http, serverless, usage_metrics};

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
    /// http endpoint to receive periodic metric updates
    #[clap(long)]
    metric_collection_endpoint: Option<String>,
    /// how often metrics should be sent to a collection endpoint
    #[clap(long)]
    metric_collection_interval: Option<String>,
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
    /// Redis rate limiter max number of requests per second.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_REDIS_SET)]
    redis_rps_limit: Vec<RateBucketInfo>,
    /// Cancellation channel size (max queue size for redis kv client)
    #[clap(long, default_value_t = 1024)]
    cancellation_ch_size: usize,
    /// Cancellation ops batch size for redis
    #[clap(long, default_value_t = 8)]
    cancellation_batch_size: usize,
    /// cache for `allowed_ips` (use `size=0` to disable)
    #[clap(long, default_value = config::CacheOptions::CACHE_DEFAULT_OPTIONS)]
    allowed_ips_cache: String,
    /// cache for `role_secret` (use `size=0` to disable)
    #[clap(long, default_value = config::CacheOptions::CACHE_DEFAULT_OPTIONS)]
    role_secret_cache: String,
    /// redis url for notifications (if empty, redis_host:port will be used for both notifications and streaming connections)
    #[clap(long)]
    redis_notifications: Option<String>,
    /// what from the available authentications type to use for the regional redis we have. Supported are "irsa" and "plain".
    #[clap(long, default_value = "irsa")]
    redis_auth_type: String,
    /// redis host for streaming connections (might be different from the notifications host)
    #[clap(long)]
    redis_host: Option<String>,
    /// redis port for streaming connections (might be different from the notifications host)
    #[clap(long)]
    redis_port: Option<u16>,
    /// redis cluster name, used in aws elasticache
    #[clap(long)]
    redis_cluster_name: Option<String>,
    /// redis user_id, used in aws elasticache
    #[clap(long)]
    redis_user_id: Option<String>,
    /// aws region to retrieve credentials
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
            error!(error = ?e, "could not start jemalloc metrics loop");
            None
        }
    };

    let args = ProxyCliArgs::parse();
    let config = build_config(&args)?;
    let auth_backend = build_auth_backend(&args)?;

    match auth_backend {
        Either::Left(auth_backend) => info!("Authentication backend: {auth_backend}"),
        Either::Right(auth_backend) => info!("Authentication backend: {auth_backend:?}"),
    }
    info!("Using region: {}", args.aws_region);
    let (regional_redis_client, redis_notifications_client) = configure_redis(&args).await?;

    // Check that we can bind to address before further initialization
    info!("Starting http on {}", args.http);
    let http_listener = TcpListener::bind(args.http).await?.into_std()?;

    info!("Starting mgmt on {}", args.mgmt);
    let mgmt_listener = TcpListener::bind(args.mgmt).await?;

    let proxy_listener = if args.is_auth_broker {
        None
    } else {
        info!("Starting proxy on {}", args.proxy);
        Some(TcpListener::bind(args.proxy).await?)
    };

    let sni_router_listeners = {
        let args = &args.pg_sni_router;
        if args.dest.is_some() {
            ensure!(
                args.tls_key.is_some(),
                "sni-router-tls-key must be provided"
            );
            ensure!(
                args.tls_cert.is_some(),
                "sni-router-tls-cert must be provided"
            );

            info!(
                "Starting pg-sni-router on {} and {}",
                args.listen, args.listen_tls
            );

            Some((
                TcpListener::bind(args.listen).await?,
                TcpListener::bind(args.listen_tls).await?,
            ))
        } else {
            None
        }
    };

    // TODO: rename the argument to something like serverless.
    // It now covers more than just websockets, it also covers SQL over HTTP.
    let serverless_listener = if let Some(serverless_address) = args.wss {
        info!("Starting wss on {serverless_address}");
        Some(TcpListener::bind(serverless_address).await?)
    } else if args.is_auth_broker {
        bail!("wss arg must be present for auth-broker")
    } else {
        None
    };

    let cancellation_token = CancellationToken::new();

    let redis_rps_limit = Vec::leak(args.redis_rps_limit.clone());
    RateBucketInfo::validate(redis_rps_limit)?;

    let redis_kv_client = regional_redis_client
        .as_ref()
        .map(|redis_publisher| RedisKVClient::new(redis_publisher.clone(), redis_rps_limit));

    let cancellation_handler = Arc::new(CancellationHandler::new(&config.connect_to_compute));

    let endpoint_rate_limiter = Arc::new(EndpointRateLimiter::new_with_shards(
        RateBucketInfo::to_leaky_bucket(&args.endpoint_rps_limit)
            .unwrap_or(EndpointRateLimiter::DEFAULT),
        64,
    ));

    // client facing tasks. these will exit on error or on cancellation
    // cancellation returns Ok(())
    let mut client_tasks = JoinSet::new();
    match auth_backend {
        Either::Left(auth_backend) => {
            if let Some(proxy_listener) = proxy_listener {
                client_tasks.spawn(crate::proxy::task_main(
                    config,
                    auth_backend,
                    proxy_listener,
                    cancellation_token.clone(),
                    cancellation_handler.clone(),
                    endpoint_rate_limiter.clone(),
                ));
            }

            if let Some(serverless_listener) = serverless_listener {
                client_tasks.spawn(serverless::task_main(
                    config,
                    auth_backend,
                    serverless_listener,
                    cancellation_token.clone(),
                    cancellation_handler.clone(),
                    endpoint_rate_limiter.clone(),
                ));
            }
        }
        Either::Right(auth_backend) => {
            if let Some(proxy_listener) = proxy_listener {
                client_tasks.spawn(crate::console_redirect_proxy::task_main(
                    config,
                    auth_backend,
                    proxy_listener,
                    cancellation_token.clone(),
                    cancellation_handler.clone(),
                ));
            }
        }
    }

    // spawn pg-sni-router mode.
    if let Some((listen, listen_tls)) = sni_router_listeners {
        let args = args.pg_sni_router;
        let dest = args.dest.expect("already asserted it is set");
        let key_path = args.tls_key.expect("already asserted it is set");
        let cert_path = args.tls_cert.expect("already asserted it is set");

        let tls_config = super::pg_sni_router::parse_tls(&key_path, &cert_path)?;

        let dest = Arc::new(dest);

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

    client_tasks.spawn(crate::context::parquet::worker(
        cancellation_token.clone(),
        args.parquet_upload,
    ));

    // maintenance tasks. these never return unless there's an error
    let mut maintenance_tasks = JoinSet::new();
    maintenance_tasks.spawn(crate::signals::handle(cancellation_token.clone(), || {}));
    maintenance_tasks.spawn(http::health_server::task_main(
        http_listener,
        AppMetrics {
            jemalloc,
            neon_metrics,
            proxy: crate::metrics::Metrics::get(),
        },
    ));
    maintenance_tasks.spawn(control_plane::mgmt::task_main(mgmt_listener));

    if let Some(metrics_config) = &config.metric_collection {
        // TODO: Add gc regardles of the metric collection being enabled.
        maintenance_tasks.spawn(usage_metrics::task_main(metrics_config));
    }

    #[cfg_attr(not(any(test, feature = "testing")), expect(irrefutable_let_patterns))]
    if let Either::Left(auth::Backend::ControlPlane(api, ())) = &auth_backend {
        if let crate::control_plane::client::ControlPlaneClient::ProxyV1(api) = &**api {
            match (redis_notifications_client, regional_redis_client.clone()) {
                (None, None) => {}
                (client1, client2) => {
                    let cache = api.caches.project_info.clone();
                    if let Some(client) = client1 {
                        maintenance_tasks.spawn(notifications::task_main(
                            client,
                            cache.clone(),
                            args.region.clone(),
                        ));
                    }
                    if let Some(client) = client2 {
                        maintenance_tasks.spawn(notifications::task_main(
                            client,
                            cache.clone(),
                            args.region.clone(),
                        ));
                    }
                    maintenance_tasks.spawn(async move { cache.clone().gc_worker().await });
                }
            }

            // Try to connect to Redis 3 times with 1 + (0..0.1) second interval.
            // This prevents immediate exit and pod restart,
            // which can cause hammering of the redis in case of connection issues.
            if let Some(mut redis_kv_client) = redis_kv_client {
                for attempt in (0..3).with_position() {
                    match redis_kv_client.try_connect().await {
                        Ok(()) => {
                            info!("Connected to Redis KV client");
                            cancellation_handler.init_tx(BatchQueue::new(CancellationProcessor {
                                client: redis_kv_client,
                                batch_size: args.cancellation_batch_size,
                            }));

                            break;
                        }
                        Err(e) => {
                            error!("Failed to connect to Redis KV client: {e}");
                            if matches!(attempt, Position::Last(_)) {
                                bail!(
                                    "Failed to connect to Redis KV client after {} attempts",
                                    attempt.into_inner()
                                );
                            }
                            let jitter = thread_rng().gen_range(0..100);
                            tokio::time::sleep(Duration::from_millis(1000 + jitter)).await;
                        }
                    }
                }
            }

            if let Some(regional_redis_client) = regional_redis_client {
                let cache = api.caches.endpoints_cache.clone();
                let con = regional_redis_client;
                let span = tracing::info_span!("endpoints_cache");
                maintenance_tasks.spawn(
                    async move { cache.do_read(con, cancellation_token.clone()).await }
                        .instrument(span),
                );
            }
        }
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
        region: args.region.clone(),
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

async fn configure_redis(
    args: &ProxyCliArgs,
) -> anyhow::Result<(
    Option<ConnectionWithCredentialsProvider>,
    Option<ConnectionWithCredentialsProvider>,
)> {
    // TODO: untangle the config args
    let regional_redis_client = match (args.redis_auth_type.as_str(), &args.redis_notifications) {
        ("plain", redis_url) => match redis_url {
            None => {
                bail!("plain auth requires redis_notifications to be set");
            }
            Some(url) => {
                Some(ConnectionWithCredentialsProvider::new_with_static_credentials(url.clone()))
            }
        },
        ("irsa", _) => match (&args.redis_host, args.redis_port) {
            (Some(host), Some(port)) => Some(
                ConnectionWithCredentialsProvider::new_with_credentials_provider(
                    host.clone(),
                    port,
                    elasticache::CredentialsProvider::new(
                        args.aws_region.clone(),
                        args.redis_cluster_name.clone(),
                        args.redis_user_id.clone(),
                    )
                    .await,
                ),
            ),
            (None, None) => {
                // todo: upgrade to error?
                warn!(
                    "irsa auth requires redis-host and redis-port to be set, continuing without regional_redis_client"
                );
                None
            }
            _ => {
                bail!("redis-host and redis-port must be specified together");
            }
        },
        _ => {
            bail!("unknown auth type given");
        }
    };

    let redis_notifications_client = if let Some(url) = &args.redis_notifications {
        Some(ConnectionWithCredentialsProvider::new_with_static_credentials(&**url))
    } else {
        regional_redis_client.clone()
    };

    Ok((regional_redis_client, redis_notifications_client))
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
