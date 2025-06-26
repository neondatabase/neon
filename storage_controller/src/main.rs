use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use camino::Utf8PathBuf;

#[cfg(feature = "testing")]
use clap::ArgAction;
use clap::Parser;
use futures::future::OptionFuture;
use http_utils::tls_certs::ReloadingCertificateResolver;
use hyper0::Uri;
use metrics::BuildInfo;
use metrics::launch_timestamp::LaunchTimestamp;
use pageserver_api::config::PostHogConfig;
use reqwest::Certificate;
use storage_controller::http::make_router;
use storage_controller::metrics::preinitialize_metrics;
use storage_controller::persistence::Persistence;
use storage_controller::service::chaos_injector::ChaosInjector;
use storage_controller::service::feature_flag::FeatureFlagService;
use storage_controller::service::{
    Config, HEARTBEAT_INTERVAL_DEFAULT, LONG_RECONCILE_THRESHOLD_DEFAULT,
    MAX_OFFLINE_INTERVAL_DEFAULT, MAX_WARMING_UP_INTERVAL_DEFAULT,
    PRIORITY_RECONCILER_CONCURRENCY_DEFAULT, RECONCILER_CONCURRENCY_DEFAULT,
    SAFEKEEPER_RECONCILER_CONCURRENCY_DEFAULT, Service,
};
use tokio::signal::unix::SignalKind;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use utils::auth::{JwtAuth, SwappableJwtAuth};
use utils::logging::{self, LogFormat};
use utils::sentry_init::init_sentry;
use utils::{project_build_tag, project_git_version, tcp_listener};

project_git_version!(GIT_VERSION);
project_build_tag!(BUILD_TAG);

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Configure jemalloc to profile heap allocations by sampling stack traces every 2 MB (1 << 21).
/// This adds roughly 3% overhead for allocations on average, which is acceptable considering
/// performance-sensitive code will avoid allocations as far as possible anyway.
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:21\0";

const DEFAULT_SSL_KEY_FILE: &str = "server.key";
const DEFAULT_SSL_CERT_FILE: &str = "server.crt";
const DEFAULT_SSL_CERT_RELOAD_PERIOD: &str = "60s";

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
#[clap(group(
    clap::ArgGroup::new("listen-addresses")
        .required(true)
        .multiple(true)
        .args(&["listen", "listen_https"]),
))]
struct Cli {
    /// Host and port to listen HTTP on, like `127.0.0.1:1234`.
    /// At least one of ["listen", "listen_https"] should be specified.
    // TODO: Make this option dev-only when https is out everywhere.
    #[arg(short, long)]
    listen: Option<std::net::SocketAddr>,
    /// Host and port to listen HTTPS on, like `127.0.0.1:1234`.
    /// At least one of ["listen", "listen_https"] should be specified.
    #[arg(long)]
    listen_https: Option<std::net::SocketAddr>,

    /// Public key for JWT authentication of clients
    #[arg(long)]
    public_key: Option<String>,

    /// Token for authenticating this service with the pageservers it controls
    #[arg(long)]
    jwt_token: Option<String>,

    /// Token for authenticating this service with the safekeepers it controls
    #[arg(long)]
    safekeeper_jwt_token: Option<String>,

    /// Token for authenticating this service with the control plane, when calling
    /// the compute notification endpoint
    #[arg(long)]
    control_plane_jwt_token: Option<String>,

    #[arg(long)]
    peer_jwt_token: Option<String>,

    /// URL to control plane storage API prefix
    #[arg(long)]
    control_plane_url: Option<String>,

    /// URL to connect to postgres, like postgresql://localhost:1234/storage_controller
    #[arg(long)]
    database_url: Option<String>,

    /// Flag to enable dev mode, which permits running without auth
    #[arg(long, default_value = "false")]
    dev: bool,

    /// Grace period before marking unresponsive pageserver offline
    #[arg(long)]
    max_offline_interval: Option<humantime::Duration>,

    /// More tolerant grace period before marking unresponsive pagserver offline used
    /// around pageserver restarts
    #[arg(long)]
    max_warming_up_interval: Option<humantime::Duration>,

    /// Size threshold for automatically splitting shards (disabled by default)
    #[arg(long)]
    split_threshold: Option<u64>,

    /// Maximum number of shards during autosplits. 0 disables autosplits. Defaults
    /// to 16 as a safety to avoid too many shards by accident.
    #[arg(long, default_value = "16")]
    max_split_shards: u8,

    /// Size threshold for initial shard splits of unsharded tenants. 0 disables initial splits.
    #[arg(long)]
    initial_split_threshold: Option<u64>,

    /// Number of target shards for initial splits. 0 or 1 disables initial splits. Defaults to 2.
    #[arg(long, default_value = "2")]
    initial_split_shards: u8,

    /// Maximum number of normal-priority reconcilers that may run in parallel
    #[arg(long)]
    reconciler_concurrency: Option<usize>,

    /// Maximum number of high-priority reconcilers that may run in parallel
    #[arg(long)]
    priority_reconciler_concurrency: Option<usize>,

    /// Maximum number of safekeeper reconciliations that may run in parallel (per safekeeper)
    #[arg(long)]
    safekeeper_reconciler_concurrency: Option<usize>,

    /// Tenant API rate limit, as requests per second per tenant.
    #[arg(long, default_value = "10")]
    tenant_rate_limit: NonZeroU32,

    /// How long to wait for the initial database connection to be available.
    #[arg(long, default_value = "5s")]
    db_connect_timeout: humantime::Duration,

    #[arg(long, default_value = "false")]
    start_as_candidate: bool,

    // TODO: make this mandatory once the helm chart gets updated
    #[arg(long)]
    address_for_peers: Option<Uri>,

    /// `neon_local` sets this to the path of the neon_local repo dir.
    /// Only relevant for testing.
    // TODO: make `cfg(feature = "testing")`
    #[arg(long)]
    neon_local_repo_dir: Option<PathBuf>,

    /// Chaos testing: exercise tenant migrations
    #[arg(long)]
    chaos_interval: Option<humantime::Duration>,

    /// Chaos testing: exercise an immediate exit
    #[arg(long)]
    chaos_exit_crontab: Option<cron::Schedule>,

    /// Maximum acceptable lag for the secondary location while draining
    /// a pageserver
    #[arg(long)]
    max_secondary_lag_bytes: Option<u64>,

    /// Period with which to send heartbeats to registered nodes
    #[arg(long)]
    heartbeat_interval: Option<humantime::Duration>,

    #[arg(long)]
    long_reconcile_threshold: Option<humantime::Duration>,

    /// Flag to use https for requests to pageserver API.
    #[arg(long, default_value = "false")]
    use_https_pageserver_api: bool,

    // Whether to put timelines onto safekeepers
    #[arg(long, default_value = "false")]
    timelines_onto_safekeepers: bool,

    /// Flag to use https for requests to safekeeper API.
    #[arg(long, default_value = "false")]
    use_https_safekeeper_api: bool,

    /// Path to a file with certificate's private key for https API.
    #[arg(long, default_value = DEFAULT_SSL_KEY_FILE)]
    ssl_key_file: Utf8PathBuf,
    /// Path to a file with a X509 certificate for https API.
    #[arg(long, default_value = DEFAULT_SSL_CERT_FILE)]
    ssl_cert_file: Utf8PathBuf,
    /// Period to reload certificate and private key from files.
    #[arg(long, default_value = DEFAULT_SSL_CERT_RELOAD_PERIOD)]
    ssl_cert_reload_period: humantime::Duration,
    /// Trusted root CA certificates to use in https APIs.
    #[arg(long)]
    ssl_ca_file: Option<Utf8PathBuf>,

    /// Neon local specific flag. When set, ignore [`Cli::control_plane_url`] and deliver
    /// the compute notification directly (instead of via control plane).
    #[arg(long, default_value = "false")]
    use_local_compute_notifications: bool,

    /// Number of safekeepers to choose for a timeline when creating it.
    /// Safekeepers will be choosen from different availability zones.
    /// This option exists primarily for testing purposes.
    #[arg(long, default_value = "3", value_parser = clap::value_parser!(i64).range(1..))]
    timeline_safekeeper_count: i64,

    /// When set, actively checks and initiates heatmap downloads/uploads during reconciliation.
    /// This speed up migrations by avoiding the default wait for the heatmap download interval.
    /// Primarily useful for testing to reduce test execution time.
    #[cfg(feature = "testing")]
    #[arg(long, default_value = "true", action=ArgAction::Set)]
    kick_secondary_downloads: bool,
}

enum StrictMode {
    /// In strict mode, we will require that all secrets are loaded, i.e. security features
    /// may not be implicitly turned off by omitting secrets in the environment.
    Strict,
    /// In dev mode, secrets are optional, and omitting a particular secret will implicitly
    /// disable the auth related to it (e.g. no pageserver jwt key -> send unauthenticated
    /// requests, no public key -> don't authenticate incoming requests).
    Dev,
}

impl Default for StrictMode {
    fn default() -> Self {
        Self::Strict
    }
}

/// Secrets may either be provided on the command line (for testing), or loaded from AWS SecretManager: this
/// type encapsulates the logic to decide which and do the loading.
struct Secrets {
    database_url: String,
    public_key: Option<JwtAuth>,
    pageserver_jwt_token: Option<String>,
    safekeeper_jwt_token: Option<String>,
    control_plane_jwt_token: Option<String>,
    peer_jwt_token: Option<String>,
}

const POSTHOG_CONFIG_ENV: &str = "POSTHOG_CONFIG";

impl Secrets {
    const DATABASE_URL_ENV: &'static str = "DATABASE_URL";
    const PAGESERVER_JWT_TOKEN_ENV: &'static str = "PAGESERVER_JWT_TOKEN";
    const SAFEKEEPER_JWT_TOKEN_ENV: &'static str = "SAFEKEEPER_JWT_TOKEN";
    const CONTROL_PLANE_JWT_TOKEN_ENV: &'static str = "CONTROL_PLANE_JWT_TOKEN";
    const PEER_JWT_TOKEN_ENV: &'static str = "PEER_JWT_TOKEN";
    const PUBLIC_KEY_ENV: &'static str = "PUBLIC_KEY";

    /// Load secrets from, in order of preference:
    /// - CLI args if database URL is provided on the CLI
    /// - Environment variables if DATABASE_URL is set.
    async fn load(args: &Cli) -> anyhow::Result<Self> {
        let Some(database_url) = Self::load_secret(&args.database_url, Self::DATABASE_URL_ENV)
        else {
            anyhow::bail!(
                "Database URL is not set (set `--database-url`, or `DATABASE_URL` environment)"
            )
        };

        let public_key = match Self::load_secret(&args.public_key, Self::PUBLIC_KEY_ENV) {
            Some(v) => Some(JwtAuth::from_key(v).context("Loading public key")?),
            None => None,
        };

        let this = Self {
            database_url,
            public_key,
            pageserver_jwt_token: Self::load_secret(
                &args.jwt_token,
                Self::PAGESERVER_JWT_TOKEN_ENV,
            ),
            safekeeper_jwt_token: Self::load_secret(
                &args.safekeeper_jwt_token,
                Self::SAFEKEEPER_JWT_TOKEN_ENV,
            ),
            control_plane_jwt_token: Self::load_secret(
                &args.control_plane_jwt_token,
                Self::CONTROL_PLANE_JWT_TOKEN_ENV,
            ),
            peer_jwt_token: Self::load_secret(&args.peer_jwt_token, Self::PEER_JWT_TOKEN_ENV),
        };

        Ok(this)
    }

    fn load_secret(cli: &Option<String>, env_name: &str) -> Option<String> {
        if let Some(v) = cli {
            Some(v.clone())
        } else {
            std::env::var(env_name).ok()
        }
    }
}

fn main() -> anyhow::Result<()> {
    logging::init(
        LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stdout,
    )?;

    // log using tracing so we don't get confused output by default hook writing to stderr
    utils::logging::replace_panic_hook_with_tracing_panic_hook().forget();

    let _sentry_guard = init_sentry(Some(GIT_VERSION.into()), &[]);

    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        // let sentry send a message (and flush)
        // and trace the error
        hook(info);

        std::process::exit(1);
    }));

    tokio::runtime::Builder::new_current_thread()
        // We use spawn_blocking for database operations, so require approximately
        // as many blocking threads as we will open database connections.
        .max_blocking_threads(Persistence::MAX_CONNECTIONS as usize)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    let launch_ts = Box::leak(Box::new(LaunchTimestamp::generate()));

    preinitialize_metrics();

    let args = Cli::parse();
    tracing::info!(
        "version: {}, launch_timestamp: {}, build_tag {}",
        GIT_VERSION,
        launch_ts.to_string(),
        BUILD_TAG,
    );

    let build_info = BuildInfo {
        revision: GIT_VERSION,
        build_tag: BUILD_TAG,
    };

    let strict_mode = if args.dev {
        StrictMode::Dev
    } else {
        StrictMode::Strict
    };

    let secrets = Secrets::load(&args).await?;

    // Validate required secrets and arguments are provided in strict mode
    match strict_mode {
        StrictMode::Strict
            if (secrets.public_key.is_none()
                || secrets.pageserver_jwt_token.is_none()
                || secrets.control_plane_jwt_token.is_none()
                || secrets.safekeeper_jwt_token.is_none()) =>
        {
            // Production systems should always have secrets configured: if public_key was not set
            // then we would implicitly disable auth.
            anyhow::bail!(
                "Insecure config!  One or more secrets is not set.  This is only permitted in `--dev` mode"
            );
        }
        StrictMode::Strict if args.control_plane_url.is_none() => {
            // Production systems should always have a control plane URL set, to prevent falling
            // back to trying to use neon_local.
            anyhow::bail!(
                "`--control-plane-url` is not set: this is only permitted in `--dev` mode"
            );
        }
        StrictMode::Strict if args.use_local_compute_notifications => {
            anyhow::bail!("`--use-local-compute-notifications` is only permitted in `--dev` mode");
        }
        StrictMode::Strict if args.timeline_safekeeper_count < 3 => {
            anyhow::bail!(
                "Running with less than 3 safekeepers per timeline is only permitted in `--dev` mode"
            );
        }
        StrictMode::Strict => {
            tracing::info!("Starting in strict mode: configuration is OK.")
        }
        StrictMode::Dev => {
            tracing::warn!("Starting in dev mode: this may be an insecure configuration.")
        }
    }

    let ssl_ca_certs = match args.ssl_ca_file.as_ref() {
        Some(ssl_ca_file) => {
            tracing::info!("Using ssl root CA file: {ssl_ca_file:?}");
            let buf = tokio::fs::read(ssl_ca_file).await?;
            Certificate::from_pem_bundle(&buf)?
        }
        None => Vec::new(),
    };

    let posthog_config = if let Ok(json) = std::env::var(POSTHOG_CONFIG_ENV) {
        let res: Result<PostHogConfig, _> = serde_json::from_str(&json);
        if let Ok(config) = res {
            Some(config)
        } else {
            tracing::warn!("Invalid posthog config: {json}");
            None
        }
    } else {
        None
    };

    let config = Config {
        pageserver_jwt_token: secrets.pageserver_jwt_token,
        safekeeper_jwt_token: secrets.safekeeper_jwt_token,
        control_plane_jwt_token: secrets.control_plane_jwt_token,
        peer_jwt_token: secrets.peer_jwt_token,
        control_plane_url: args.control_plane_url,
        max_offline_interval: args
            .max_offline_interval
            .map(humantime::Duration::into)
            .unwrap_or(MAX_OFFLINE_INTERVAL_DEFAULT),
        max_warming_up_interval: args
            .max_warming_up_interval
            .map(humantime::Duration::into)
            .unwrap_or(MAX_WARMING_UP_INTERVAL_DEFAULT),
        reconciler_concurrency: args
            .reconciler_concurrency
            .unwrap_or(RECONCILER_CONCURRENCY_DEFAULT),
        priority_reconciler_concurrency: args
            .priority_reconciler_concurrency
            .unwrap_or(PRIORITY_RECONCILER_CONCURRENCY_DEFAULT),
        safekeeper_reconciler_concurrency: args
            .safekeeper_reconciler_concurrency
            .unwrap_or(SAFEKEEPER_RECONCILER_CONCURRENCY_DEFAULT),
        tenant_rate_limit: args.tenant_rate_limit,
        split_threshold: args.split_threshold,
        max_split_shards: args.max_split_shards,
        initial_split_threshold: args.initial_split_threshold,
        initial_split_shards: args.initial_split_shards,
        neon_local_repo_dir: args.neon_local_repo_dir,
        max_secondary_lag_bytes: args.max_secondary_lag_bytes,
        heartbeat_interval: args
            .heartbeat_interval
            .map(humantime::Duration::into)
            .unwrap_or(HEARTBEAT_INTERVAL_DEFAULT),
        long_reconcile_threshold: args
            .long_reconcile_threshold
            .map(humantime::Duration::into)
            .unwrap_or(LONG_RECONCILE_THRESHOLD_DEFAULT),
        address_for_peers: args.address_for_peers,
        start_as_candidate: args.start_as_candidate,
        use_https_pageserver_api: args.use_https_pageserver_api,
        use_https_safekeeper_api: args.use_https_safekeeper_api,
        ssl_ca_certs,
        timelines_onto_safekeepers: args.timelines_onto_safekeepers,
        use_local_compute_notifications: args.use_local_compute_notifications,
        timeline_safekeeper_count: args.timeline_safekeeper_count,
        posthog_config: posthog_config.clone(),
        #[cfg(feature = "testing")]
        kick_secondary_downloads: args.kick_secondary_downloads,
    };

    // Validate that we can connect to the database
    Persistence::await_connection(&secrets.database_url, args.db_connect_timeout.into()).await?;

    let persistence = Arc::new(Persistence::new(secrets.database_url).await);

    let service = Service::spawn(config, persistence.clone()).await?;

    let auth = secrets
        .public_key
        .map(|jwt_auth| Arc::new(SwappableJwtAuth::new(jwt_auth)));
    let router = make_router(service.clone(), auth, build_info)
        .build()
        .map_err(|err| anyhow!(err))?;
    let http_service =
        Arc::new(http_utils::RequestServiceBuilder::new(router).map_err(|err| anyhow!(err))?);

    let api_shutdown = CancellationToken::new();

    // Start HTTP server
    let http_server_task: OptionFuture<_> = match args.listen {
        Some(http_addr) => {
            let http_listener = tcp_listener::bind(http_addr)?;
            let http_server =
                http_utils::server::Server::new(Arc::clone(&http_service), http_listener, None)?;

            tracing::info!("Serving HTTP on {}", http_addr);
            Some(tokio::task::spawn(http_server.serve(api_shutdown.clone())))
        }
        None => None,
    }
    .into();

    // Start HTTPS server
    let https_server_task: OptionFuture<_> = match args.listen_https {
        Some(https_addr) => {
            let https_listener = tcp_listener::bind(https_addr)?;

            let resolver = ReloadingCertificateResolver::new(
                "main",
                &args.ssl_key_file,
                &args.ssl_cert_file,
                *args.ssl_cert_reload_period,
            )
            .await?;

            let server_config = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_cert_resolver(resolver);

            let tls_acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
            let https_server =
                http_utils::server::Server::new(http_service, https_listener, Some(tls_acceptor))?;

            tracing::info!("Serving HTTPS on {}", https_addr);
            Some(tokio::task::spawn(https_server.serve(api_shutdown.clone())))
        }
        None => None,
    }
    .into();

    let chaos_task = args.chaos_interval.map(|interval| {
        let service = service.clone();
        let cancel = CancellationToken::new();
        let cancel_bg = cancel.clone();
        let chaos_exit_crontab = args.chaos_exit_crontab;
        (
            tokio::task::spawn(
                async move {
                    let mut chaos_injector =
                        ChaosInjector::new(service, interval.into(), chaos_exit_crontab);
                    chaos_injector.run(cancel_bg).await
                }
                .instrument(tracing::info_span!("chaos_injector")),
            ),
            cancel,
        )
    });

    let feature_flag_task = if let Some(posthog_config) = posthog_config {
        let service = service.clone();
        let cancel = CancellationToken::new();
        let cancel_bg = cancel.clone();
        let task = tokio::task::spawn(
            async move {
                let feature_flag_service = FeatureFlagService::new(service, posthog_config);
                let feature_flag_service = Arc::new(feature_flag_service);
                feature_flag_service.run(cancel_bg).await
            }
            .instrument(tracing::info_span!("feature_flag_service")),
        );
        Some((task, cancel))
    } else {
        None
    };

    // Wait until we receive a signal
    let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt())?;
    let mut sigquit = tokio::signal::unix::signal(SignalKind::quit())?;
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())?;
    tokio::pin!(http_server_task, https_server_task);
    tokio::select! {
        _ = sigint.recv() => {},
        _ = sigterm.recv() => {},
        _ = sigquit.recv() => {},
        Some(err) = &mut http_server_task => {
            panic!("HTTP server task failed: {err:#?}");
        }
        Some(err) = &mut https_server_task => {
            panic!("HTTPS server task failed: {err:#?}");
        }
    }
    tracing::info!("Terminating on signal");

    // Stop HTTP and HTTPS servers first, so that we don't have to service requests
    // while shutting down Service.
    api_shutdown.cancel();

    // If the deadline is exceeded, we will fall through and shut down the service anyway,
    // any request handlers in flight will experience cancellation & their clients will
    // see a torn connection.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    match tokio::time::timeout_at(deadline, http_server_task).await {
        Ok(Some(Ok(_))) => tracing::info!("Joined HTTP server task"),
        Ok(Some(Err(e))) => tracing::error!("Error joining HTTP server task: {e}"),
        Ok(None) => {} // HTTP is disabled.
        Err(_) => tracing::warn!("Timed out joining HTTP server task"),
    }

    match tokio::time::timeout_at(deadline, https_server_task).await {
        Ok(Some(Ok(_))) => tracing::info!("Joined HTTPS server task"),
        Ok(Some(Err(e))) => tracing::error!("Error joining HTTPS server task: {e}"),
        Ok(None) => {} // HTTPS is disabled.
        Err(_) => tracing::warn!("Timed out joining HTTPS server task"),
    }

    // If we were injecting chaos, stop that so that we're not calling into Service while it shuts down
    if let Some((chaos_jh, chaos_cancel)) = chaos_task {
        chaos_cancel.cancel();
        chaos_jh.await.ok();
    }

    // If we were running the feature flag service, stop that so that we're not calling into Service while it shuts down
    if let Some((feature_flag_task, feature_flag_cancel)) = feature_flag_task {
        feature_flag_cancel.cancel();
        feature_flag_task.await.ok();
    }

    service.shutdown().await;
    tracing::info!("Service shutdown complete");

    std::process::exit(0);
}
