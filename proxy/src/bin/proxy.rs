use futures::future::Either;
use proxy::auth;
use proxy::config::AuthenticationConfig;
use proxy::config::CacheOptions;
use proxy::config::HttpConfig;
use proxy::console;
use proxy::console::provider::AllowedIpsCache;
use proxy::console::provider::NodeInfoCache;
use proxy::console::provider::RoleSecretCache;
use proxy::http;
use proxy::rate_limiter::EndpointRateLimiter;
use proxy::rate_limiter::RateBucketInfo;
use proxy::rate_limiter::RateLimiterConfig;
use proxy::serverless::GlobalConnPoolOptions;
use proxy::usage_metrics;

use anyhow::bail;
use proxy::config::{self, ProxyConfig};
use proxy::serverless;
use std::pin::pin;
use std::sync::Arc;
use std::{borrow::Cow, net::SocketAddr};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;
use utils::{project_build_tag, project_git_version, sentry_init::init_sentry};

project_git_version!(GIT_VERSION);
project_build_tag!(BUILD_TAG);

use clap::{Parser, ValueEnum};

#[derive(Clone, Debug, ValueEnum)]
enum AuthBackend {
    Console,
    #[cfg(feature = "testing")]
    Postgres,
    Link,
}

/// Neon proxy/router
#[derive(Parser)]
#[command(version = GIT_VERSION, about)]
struct ProxyCliArgs {
    /// listen for incoming client connections on ip:port
    #[clap(short, long, default_value = "127.0.0.1:4432")]
    proxy: String,
    #[clap(value_enum, long, default_value_t = AuthBackend::Link)]
    auth_backend: AuthBackend,
    /// listen for management callback connection on ip:port
    #[clap(short, long, default_value = "127.0.0.1:7000")]
    mgmt: String,
    /// listen for incoming http connections (metrics, etc) on ip:port
    #[clap(long, default_value = "127.0.0.1:7001")]
    http: String,
    /// listen for incoming wss connections on ip:port
    #[clap(long)]
    wss: Option<String>,
    /// redirect unauthenticated users to the given uri in case of link auth
    #[clap(short, long, default_value = "http://localhost:3000/psql_session/")]
    uri: String,
    /// cloud API endpoint for authenticating users
    #[clap(
        short,
        long,
        default_value = "http://localhost:3000/authenticate_proxy_request/"
    )]
    auth_endpoint: String,
    /// path to TLS key for client postgres connections
    ///
    /// tls-key and tls-cert are for backwards compatibility, we can put all certs in one dir
    #[clap(short = 'k', long, alias = "ssl-key")]
    tls_key: Option<String>,
    /// path to TLS cert for client postgres connections
    ///
    /// tls-key and tls-cert are for backwards compatibility, we can put all certs in one dir
    #[clap(short = 'c', long, alias = "ssl-cert")]
    tls_cert: Option<String>,
    /// path to directory with TLS certificates for client postgres connections
    #[clap(long)]
    certs_dir: Option<String>,
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
    #[clap(long, default_value = config::WakeComputeLockOptions::DEFAULT_OPTIONS_WAKE_COMPUTE_LOCK)]
    wake_compute_lock: String,
    /// Allow self-signed certificates for compute nodes (for testing)
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    allow_self_signed_compute: bool,
    #[clap(flatten)]
    sql_over_http: SqlOverHttpArgs,
    /// timeout for scram authentication protocol
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    scram_protocol_timeout: tokio::time::Duration,
    /// Require that all incoming requests have a Proxy Protocol V2 packet **and** have an IP address associated.
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    require_client_ip: bool,
    /// Disable dynamic rate limiter and store the metrics to ensure its production behaviour.
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    disable_dynamic_rate_limiter: bool,
    /// Rate limit algorithm. Makes sense only if `disable_rate_limiter` is `false`.
    #[clap(value_enum, long, default_value_t = proxy::rate_limiter::RateLimitAlgorithm::Aimd)]
    rate_limit_algorithm: proxy::rate_limiter::RateLimitAlgorithm,
    /// Timeout for rate limiter. If it didn't manage to aquire a permit in this time, it will return an error.
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    rate_limiter_timeout: tokio::time::Duration,
    /// Endpoint rate limiter max number of requests per second.
    ///
    /// Provided in the form '<Requests Per Second>@<Bucket Duration Size>'.
    /// Can be given multiple times for different bucket sizes.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_SET)]
    endpoint_rps_limit: Vec<RateBucketInfo>,
    /// Initial limit for dynamic rate limiter. Makes sense only if `rate_limit_algorithm` is *not* `None`.
    #[clap(long, default_value_t = 100)]
    initial_limit: usize,
    #[clap(flatten)]
    aimd_config: proxy::rate_limiter::AimdConfig,
    /// cache for `allowed_ips` (use `size=0` to disable)
    #[clap(long, default_value = config::CacheOptions::CACHE_DEFAULT_OPTIONS)]
    allowed_ips_cache: String,
    /// cache for `role_secret` (use `size=0` to disable)
    #[clap(long, default_value = config::CacheOptions::CACHE_DEFAULT_OPTIONS)]
    role_secret_cache: String,
    /// disable ip check for http requests. If it is too time consuming, it could be turned off.
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    disable_ip_check_for_http: bool,
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _logging_guard = proxy::logging::init().await?;
    let _panic_hook_guard = utils::logging::replace_panic_hook_with_tracing_panic_hook();
    let _sentry_guard = init_sentry(Some(GIT_VERSION.into()), &[]);

    info!("Version: {GIT_VERSION}");
    info!("Build_tag: {BUILD_TAG}");
    ::metrics::set_build_info_metric(GIT_VERSION, BUILD_TAG);

    let args = ProxyCliArgs::parse();
    let config = build_config(&args)?;

    info!("Authentication backend: {}", config.auth_backend);

    // Check that we can bind to address before further initialization
    let http_address: SocketAddr = args.http.parse()?;
    info!("Starting http on {http_address}");
    let http_listener = TcpListener::bind(http_address).await?.into_std()?;

    let mgmt_address: SocketAddr = args.mgmt.parse()?;
    info!("Starting mgmt on {mgmt_address}");
    let mgmt_listener = TcpListener::bind(mgmt_address).await?;

    let proxy_address: SocketAddr = args.proxy.parse()?;
    info!("Starting proxy on {proxy_address}");
    let proxy_listener = TcpListener::bind(proxy_address).await?;
    let cancellation_token = CancellationToken::new();

    let endpoint_rate_limiter = Arc::new(EndpointRateLimiter::new(&config.endpoint_rps_limit));

    // client facing tasks. these will exit on error or on cancellation
    // cancellation returns Ok(())
    let mut client_tasks = JoinSet::new();
    client_tasks.spawn(proxy::proxy::task_main(
        config,
        proxy_listener,
        cancellation_token.clone(),
        endpoint_rate_limiter.clone(),
    ));

    // TODO: rename the argument to something like serverless.
    // It now covers more than just websockets, it also covers SQL over HTTP.
    if let Some(serverless_address) = args.wss {
        let serverless_address: SocketAddr = serverless_address.parse()?;
        info!("Starting wss on {serverless_address}");
        let serverless_listener = TcpListener::bind(serverless_address).await?;

        client_tasks.spawn(serverless::task_main(
            config,
            serverless_listener,
            cancellation_token.clone(),
            endpoint_rate_limiter.clone(),
        ));
    }

    // maintenance tasks. these never return unless there's an error
    let mut maintenance_tasks = JoinSet::new();
    maintenance_tasks.spawn(proxy::handle_signals(cancellation_token));
    maintenance_tasks.spawn(http::health_server::task_main(http_listener));
    maintenance_tasks.spawn(console::mgmt::task_main(mgmt_listener));

    if let Some(metrics_config) = &config.metric_collection {
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
            Either::Left((Some(res), _)) => break proxy::flatten_err(res)?,
            // exit with error immediately if all maintenance tasks have ceased (should be caught by branch above)
            Either::Left((None, _)) => bail!("no maintenance tasks running. invalid state"),
            // exit immediately on client task error
            Either::Right((Some(res), _)) => proxy::flatten_err(res)?,
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
    let tls_config = match (&args.tls_key, &args.tls_cert) {
        (Some(key_path), Some(cert_path)) => Some(config::configure_tls(
            key_path,
            cert_path,
            args.certs_dir.as_ref(),
        )?),
        (None, None) => None,
        _ => bail!("either both or neither tls-key and tls-cert must be specified"),
    };

    if args.allow_self_signed_compute {
        warn!("allowing self-signed compute certificates");
    }

    let metric_collection = match (
        &args.metric_collection_endpoint,
        &args.metric_collection_interval,
    ) {
        (Some(endpoint), Some(interval)) => Some(config::MetricCollectionConfig {
            endpoint: endpoint.parse()?,
            interval: humantime::parse_duration(interval)?,
        }),
        (None, None) => None,
        _ => bail!(
            "either both or neither metric-collection-endpoint \
             and metric-collection-interval must be specified"
        ),
    };
    let rate_limiter_config = RateLimiterConfig {
        disable: args.disable_dynamic_rate_limiter,
        algorithm: args.rate_limit_algorithm,
        timeout: args.rate_limiter_timeout,
        initial_limit: args.initial_limit,
        aimd_config: Some(args.aimd_config),
    };

    let auth_backend = match &args.auth_backend {
        AuthBackend::Console => {
            let wake_compute_cache_config: CacheOptions = args.wake_compute_cache.parse()?;
            let allowed_ips_cache_config: CacheOptions = args.allowed_ips_cache.parse()?;
            let role_secret_cache_config: CacheOptions = args.role_secret_cache.parse()?;

            info!("Using NodeInfoCache (wake_compute) with options={wake_compute_cache_config:?}");
            info!("Using AllowedIpsCache (wake_compute) with options={allowed_ips_cache_config:?}");
            info!("Using RoleSecretCache (wake_compute) with options={role_secret_cache_config:?}");
            let caches = Box::leak(Box::new(console::caches::ApiCaches {
                node_info: NodeInfoCache::new(
                    "node_info_cache",
                    wake_compute_cache_config.size,
                    wake_compute_cache_config.ttl,
                    true,
                ),
                allowed_ips: AllowedIpsCache::new(
                    "allowed_ips_cache",
                    allowed_ips_cache_config.size,
                    allowed_ips_cache_config.ttl,
                    false,
                ),
                role_secret: RoleSecretCache::new(
                    "role_secret_cache",
                    role_secret_cache_config.size,
                    role_secret_cache_config.ttl,
                    false,
                ),
            }));

            let config::WakeComputeLockOptions {
                shards,
                permits,
                epoch,
                timeout,
            } = args.wake_compute_lock.parse()?;
            info!(permits, shards, ?epoch, "Using NodeLocks (wake_compute)");
            let locks = Box::leak(Box::new(
                console::locks::ApiLocks::new("wake_compute_lock", permits, shards, timeout)
                    .unwrap(),
            ));
            tokio::spawn(locks.garbage_collect_worker(epoch));

            let url = args.auth_endpoint.parse()?;
            let endpoint = http::Endpoint::new(url, http::new_client(rate_limiter_config));

            let api = console::provider::neon::Api::new(endpoint, caches, locks);
            auth::BackendType::Console(Cow::Owned(api), ())
        }
        #[cfg(feature = "testing")]
        AuthBackend::Postgres => {
            let url = args.auth_endpoint.parse()?;
            let api = console::provider::mock::Api::new(url);
            auth::BackendType::Postgres(Cow::Owned(api), ())
        }
        AuthBackend::Link => {
            let url = args.uri.parse()?;
            auth::BackendType::Link(Cow::Owned(url))
        }
    };
    let http_config = HttpConfig {
        request_timeout: args.sql_over_http.sql_over_http_timeout,
        pool_options: GlobalConnPoolOptions {
            max_conns_per_endpoint: args.sql_over_http.sql_over_http_pool_max_conns_per_endpoint,
            gc_epoch: args.sql_over_http.sql_over_http_pool_gc_epoch,
            pool_shards: args.sql_over_http.sql_over_http_pool_shards,
            idle_timeout: args.sql_over_http.sql_over_http_idle_timeout,
            opt_in: args.sql_over_http.sql_over_http_pool_opt_in,
        },
    };
    let authentication_config = AuthenticationConfig {
        scram_protocol_timeout: args.scram_protocol_timeout,
    };

    let mut endpoint_rps_limit = args.endpoint_rps_limit.clone();
    RateBucketInfo::validate(&mut endpoint_rps_limit)?;

    let config = Box::leak(Box::new(ProxyConfig {
        tls_config,
        auth_backend,
        metric_collection,
        allow_self_signed_compute: args.allow_self_signed_compute,
        http_config,
        authentication_config,
        require_client_ip: args.require_client_ip,
        disable_ip_check_for_http: args.disable_ip_check_for_http,
        endpoint_rps_limit,
        // TODO: add this argument
        cluster: String::new(),
    }));

    Ok(config)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use clap::Parser;
    use proxy::rate_limiter::RateBucketInfo;

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
