use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    pin::pin,
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, ensure};
use dashmap::DashMap;
use futures::{future::Either, FutureExt};
use proxy::{
    auth::backend::local::{JwksRoleSettings, LocalBackend, JWKS_ROLE_MAP},
    cancellation::CancellationHandlerMain,
    config::{self, AuthenticationConfig, HttpConfig, ProxyConfig, RetryConfig},
    console::{locks::ApiLocks, messages::JwksRoleMapping},
    http::health_server::AppMetrics,
    metrics::{Metrics, ThreadPoolMetrics},
    rate_limiter::{BucketRateLimiter, EndpointRateLimiter, LeakyBucketConfig, RateBucketInfo},
    scram::threadpool::ThreadPool,
    serverless::{self, cancel_set::CancelSet, GlobalConnPoolOptions},
};

project_git_version!(GIT_VERSION);
project_build_tag!(BUILD_TAG);

use clap::Parser;
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use utils::{project_build_tag, project_git_version, sentry_init::init_sentry};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Neon proxy/router
#[derive(Parser)]
#[command(version = GIT_VERSION, about)]
struct LocalProxyCliArgs {
    /// listen for incoming metrics connections on ip:port
    #[clap(long, default_value = "127.0.0.1:7001")]
    metrics: String,
    /// listen for incoming http connections on ip:port
    #[clap(long)]
    http: String,
    /// timeout for the TLS handshake
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    handshake_timeout: tokio::time::Duration,
    /// lock for `connect_compute` api method. example: "shards=32,permits=4,epoch=10m,timeout=1s". (use `permits=0` to disable).
    #[clap(long, default_value = config::ConcurrencyLockOptions::DEFAULT_OPTIONS_CONNECT_COMPUTE_LOCK)]
    connect_compute_lock: String,
    #[clap(flatten)]
    sql_over_http: SqlOverHttpArgs,
    /// User rate limiter max number of requests per second.
    ///
    /// Provided in the form `<Requests Per Second>@<Bucket Duration Size>`.
    /// Can be given multiple times for different bucket sizes.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_ENDPOINT_SET)]
    user_rps_limit: Vec<RateBucketInfo>,
    /// Whether the auth rate limiter actually takes effect (for testing)
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    auth_rate_limit_enabled: bool,
    /// Authentication rate limiter max number of hashes per second.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_AUTH_SET)]
    auth_rate_limit: Vec<RateBucketInfo>,
    /// The IP subnet to use when considering whether two IP addresses are considered the same.
    #[clap(long, default_value_t = 64)]
    auth_rate_limit_ip_subnet: u8,
    /// Whether to retry the connection to the compute node
    #[clap(long, default_value = config::RetryConfig::CONNECT_TO_COMPUTE_DEFAULT_VALUES)]
    connect_to_compute_retry: String,
    /// Address of the postgres server
    #[clap(long, default_value = "127.0.0.1:5432")]
    compute: SocketAddr,
    /// File address of the local proxy config file
    #[clap(long, default_value = "./localproxy.json")]
    config_path: PathBuf,
}

#[derive(clap::Args, Clone, Copy, Debug)]
struct SqlOverHttpArgs {
    /// How many connections to pool for each endpoint. Excess connections are discarded
    #[clap(long, default_value_t = 200)]
    sql_over_http_pool_max_total_conns: usize,

    /// How long pooled connections should remain idle for before closing
    #[clap(long, default_value = "5m", value_parser = humantime::parse_duration)]
    sql_over_http_idle_timeout: tokio::time::Duration,

    #[clap(long, default_value_t = 100)]
    sql_over_http_client_conn_threshold: u64,

    #[clap(long, default_value_t = 16)]
    sql_over_http_cancel_set_shards: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _logging_guard = proxy::logging::init().await?;
    let _panic_hook_guard = utils::logging::replace_panic_hook_with_tracing_panic_hook();
    let _sentry_guard = init_sentry(Some(GIT_VERSION.into()), &[]);

    Metrics::install(Arc::new(ThreadPoolMetrics::new(0)));

    info!("Version: {GIT_VERSION}");
    info!("Build_tag: {BUILD_TAG}");
    let neon_metrics = ::metrics::NeonMetrics::new(::metrics::BuildInfo {
        revision: GIT_VERSION,
        build_tag: BUILD_TAG,
    });

    let jemalloc = match proxy::jemalloc::MetricRecorder::new() {
        Ok(t) => Some(t),
        Err(e) => {
            tracing::error!(error = ?e, "could not start jemalloc metrics loop");
            None
        }
    };

    let args = LocalProxyCliArgs::parse();
    let config = build_config(&args)?;

    let metrics_listener = TcpListener::bind(args.metrics).await?.into_std()?;
    let http_listener = TcpListener::bind(args.http).await?;
    let shutdown = CancellationToken::new();

    // todo: should scale with CU
    let endpoint_rate_limiter = Arc::new(EndpointRateLimiter::new_with_shards(
        LeakyBucketConfig {
            rps: 10.0,
            max: 100.0,
        },
        16,
    ));

    refresh_config(args.config_path.clone()).await;

    let mut maintenance_tasks = JoinSet::new();
    maintenance_tasks.spawn(proxy::handle_signals(shutdown.clone(), move || {
        refresh_config(args.config_path.clone()).map(Ok)
    }));
    maintenance_tasks.spawn(proxy::http::health_server::task_main(
        metrics_listener,
        AppMetrics {
            jemalloc,
            neon_metrics,
            proxy: proxy::metrics::Metrics::get(),
        },
    ));

    let task = serverless::task_main(
        config,
        http_listener,
        shutdown.clone(),
        Arc::new(CancellationHandlerMain::new(
            Arc::new(DashMap::new()),
            None,
            proxy::metrics::CancellationSource::Local,
        )),
        endpoint_rate_limiter,
    );

    match futures::future::select(pin!(maintenance_tasks.join_next()), pin!(task)).await {
        // exit immediately on maintenance task completion
        Either::Left((Some(res), _)) => match proxy::flatten_err(res)? {},
        // exit with error immediately if all maintenance tasks have ceased (should be caught by branch above)
        Either::Left((None, _)) => bail!("no maintenance tasks running. invalid state"),
        // exit immediately on client task error
        Either::Right((res, _)) => res?,
    }

    Ok(())
}

/// ProxyConfig is created at proxy startup, and lives forever.
fn build_config(args: &LocalProxyCliArgs) -> anyhow::Result<&'static ProxyConfig> {
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
    let connect_compute_locks = ApiLocks::new(
        "connect_compute_lock",
        limiter,
        shards,
        timeout,
        epoch,
        &Metrics::get().proxy.connect_compute_lock,
    )?;

    let http_config = HttpConfig {
        accept_websockets: false,
        pool_options: GlobalConnPoolOptions {
            gc_epoch: Duration::from_secs(60),
            pool_shards: 2,
            idle_timeout: args.sql_over_http.sql_over_http_idle_timeout,
            opt_in: false,

            max_conns_per_endpoint: args.sql_over_http.sql_over_http_pool_max_total_conns,
            max_total_conns: args.sql_over_http.sql_over_http_pool_max_total_conns,
        },
        cancel_set: CancelSet::new(args.sql_over_http.sql_over_http_cancel_set_shards),
        client_conn_threshold: args.sql_over_http.sql_over_http_client_conn_threshold,
    };

    Ok(Box::leak(Box::new(ProxyConfig {
        tls_config: None,
        auth_backend: proxy::auth::Backend::Local(proxy::auth::backend::MaybeOwned::Owned(
            LocalBackend::new(args.compute),
        )),
        metric_collection: None,
        allow_self_signed_compute: false,
        http_config,
        authentication_config: AuthenticationConfig {
            thread_pool: ThreadPool::new(0),
            scram_protocol_timeout: Duration::from_secs(10),
            rate_limiter_enabled: false,
            rate_limiter: BucketRateLimiter::new(vec![]),
            rate_limit_ip_subnet: 64,
            ip_allowlist_check_enabled: true,
        },
        require_client_ip: false,
        handshake_timeout: Duration::from_secs(10),
        region: "local".into(),
        wake_compute_retry_config: RetryConfig::parse(RetryConfig::WAKE_COMPUTE_DEFAULT_VALUES)?,
        connect_compute_locks,
        connect_to_compute_retry_config: RetryConfig::parse(
            RetryConfig::CONNECT_TO_COMPUTE_DEFAULT_VALUES,
        )?,
    })))
}

async fn refresh_config(path: PathBuf) {
    match refresh_config_inner(&path).await {
        Ok(()) => {}
        Err(e) => {
            error!(error=?e, ?path, "could not read config file");
        }
    }
}

async fn refresh_config_inner(path: &Path) -> anyhow::Result<()> {
    let bytes = tokio::fs::read(&path).await?;
    let mut data: JwksRoleMapping = serde_json::from_slice(&bytes)?;

    let mut settings = None;

    for mapping in data.roles.values_mut() {
        for jwks in &mut mapping.jwks {
            ensure!(
                jwks.jwks_url.has_authority()
                    && (jwks.jwks_url.scheme() == "http" || jwks.jwks_url.scheme() == "https"),
                "Invalid JWKS url. Must be HTTP",
            );

            ensure!(
                jwks.jwks_url
                    .host()
                    .is_some_and(|h| h != url::Host::Domain("")),
                "Invalid JWKS url. No domain listed",
            );

            // clear username, password and ports
            jwks.jwks_url.set_username("").expect(
                "url can be a base and has a valid host and is not a file. should not error",
            );
            jwks.jwks_url.set_password(None).expect(
                "url can be a base and has a valid host and is not a file. should not error",
            );
            // local testing is hard if we need to have a specific restricted port
            if cfg!(not(feature = "testing")) {
                jwks.jwks_url.set_port(None).expect(
                    "url can be a base and has a valid host and is not a file. should not error",
                );
            }

            // clear query params
            jwks.jwks_url.set_fragment(None);
            jwks.jwks_url.query_pairs_mut().clear().finish();

            if jwks.jwks_url.scheme() != "https" {
                // local testing is hard if we need to set up https support.
                if cfg!(not(feature = "testing")) {
                    jwks.jwks_url
                        .set_scheme("https")
                        .expect("should not error to set the scheme to https if it was http");
                } else {
                    warn!(scheme = jwks.jwks_url.scheme(), "JWKS url is not HTTPS");
                }
            }

            let (pr, br) = settings.get_or_insert((jwks.project_id, jwks.branch_id));
            ensure!(
                *pr == jwks.project_id,
                "inconsistent project IDs configured"
            );
            ensure!(*br == jwks.branch_id, "inconsistent branch IDs configured");
        }
    }

    if let Some((project_id, branch_id)) = settings {
        JWKS_ROLE_MAP.store(Some(Arc::new(JwksRoleSettings {
            roles: data.roles,
            project_id,
            branch_id,
        })));
    }

    Ok(())
}
