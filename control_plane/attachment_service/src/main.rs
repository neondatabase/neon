/// The attachment service mimics the aspects of the control plane API
/// that are required for a pageserver to operate.
///
/// This enables running & testing pageservers without a full-blown
/// deployment of the Neon cloud platform.
///
use anyhow::{anyhow, Context};
use attachment_service::http::make_router;
use attachment_service::metrics::preinitialize_metrics;
use attachment_service::persistence::Persistence;
use attachment_service::service::{Config, Service};
use aws_config::{self, BehaviorVersion, Region};
use camino::Utf8PathBuf;
use clap::Parser;
use diesel::Connection;
use metrics::launch_timestamp::LaunchTimestamp;
use std::sync::Arc;
use tokio::signal::unix::SignalKind;
use tokio_util::sync::CancellationToken;
use utils::auth::{JwtAuth, SwappableJwtAuth};
use utils::logging::{self, LogFormat};

use utils::{project_build_tag, project_git_version, tcp_listener};

project_git_version!(GIT_VERSION);
project_build_tag!(BUILD_TAG);

use diesel_migrations::{embed_migrations, EmbeddedMigrations};
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
struct Cli {
    /// Host and port to listen on, like `127.0.0.1:1234`
    #[arg(short, long)]
    listen: std::net::SocketAddr,

    /// Public key for JWT authentication of clients
    #[arg(long)]
    public_key: Option<String>,

    /// Token for authenticating this service with the pageservers it controls
    #[arg(long)]
    jwt_token: Option<String>,

    /// Token for authenticating this service with the control plane, when calling
    /// the compute notification endpoint
    #[arg(long)]
    control_plane_jwt_token: Option<String>,

    /// URL to control plane compute notification endpoint
    #[arg(long)]
    compute_hook_url: Option<String>,

    /// Path to the .json file to store state (will be created if it doesn't exist)
    #[arg(short, long)]
    path: Option<Utf8PathBuf>,

    /// URL to connect to postgres, like postgresql://localhost:1234/attachment_service
    #[arg(long)]
    database_url: Option<String>,
}

/// Secrets may either be provided on the command line (for testing), or loaded from AWS SecretManager: this
/// type encapsulates the logic to decide which and do the loading.
struct Secrets {
    database_url: String,
    public_key: Option<JwtAuth>,
    jwt_token: Option<String>,
    control_plane_jwt_token: Option<String>,
}

impl Secrets {
    const DATABASE_URL_SECRET: &'static str = "rds-neon-storage-controller-url";
    const PAGESERVER_JWT_TOKEN_SECRET: &'static str =
        "neon-storage-controller-pageserver-jwt-token";
    const CONTROL_PLANE_JWT_TOKEN_SECRET: &'static str =
        "neon-storage-controller-control-plane-jwt-token";
    const PUBLIC_KEY_SECRET: &'static str = "neon-storage-controller-public-key";

    async fn load(args: &Cli) -> anyhow::Result<Self> {
        match &args.database_url {
            Some(url) => Self::load_cli(url, args),
            None => Self::load_aws_sm().await,
        }
    }

    async fn load_aws_sm() -> anyhow::Result<Self> {
        let Ok(region) = std::env::var("AWS_REGION") else {
            anyhow::bail!("AWS_REGION is not set, cannot load secrets automatically: either set this, or use CLI args to supply secrets");
        };
        let config = aws_config::defaults(BehaviorVersion::v2023_11_09())
            .region(Region::new(region.clone()))
            .load()
            .await;

        let asm = aws_sdk_secretsmanager::Client::new(&config);

        let Some(database_url) = asm
            .get_secret_value()
            .secret_id(Self::DATABASE_URL_SECRET)
            .send()
            .await?
            .secret_string()
            .map(str::to_string)
        else {
            anyhow::bail!(
                "Database URL secret not found at {region}/{}",
                Self::DATABASE_URL_SECRET
            )
        };

        let jwt_token = asm
            .get_secret_value()
            .secret_id(Self::PAGESERVER_JWT_TOKEN_SECRET)
            .send()
            .await?
            .secret_string()
            .map(str::to_string);
        if jwt_token.is_none() {
            tracing::warn!("No pageserver JWT token set: this will only work if authentication is disabled on the pageserver");
        }

        let control_plane_jwt_token = asm
            .get_secret_value()
            .secret_id(Self::CONTROL_PLANE_JWT_TOKEN_SECRET)
            .send()
            .await?
            .secret_string()
            .map(str::to_string);
        if jwt_token.is_none() {
            tracing::warn!("No control plane JWT token set: this will only work if authentication is disabled on the pageserver");
        }

        let public_key = asm
            .get_secret_value()
            .secret_id(Self::PUBLIC_KEY_SECRET)
            .send()
            .await?
            .secret_string()
            .map(str::to_string);
        let public_key = match public_key {
            Some(key) => Some(JwtAuth::from_key(key)?),
            None => {
                tracing::warn!(
                    "No public key set: inccoming HTTP requests will not be authenticated"
                );
                None
            }
        };

        Ok(Self {
            database_url,
            public_key,
            jwt_token,
            control_plane_jwt_token,
        })
    }

    fn load_cli(database_url: &str, args: &Cli) -> anyhow::Result<Self> {
        let public_key = match &args.public_key {
            None => None,
            Some(key) => Some(JwtAuth::from_key(key.clone()).context("Loading public key")?),
        };
        Ok(Self {
            database_url: database_url.to_owned(),
            public_key,
            jwt_token: args.jwt_token.clone(),
            control_plane_jwt_token: args.control_plane_jwt_token.clone(),
        })
    }
}

/// Execute the diesel migrations that are built into this binary
async fn migration_run(database_url: &str) -> anyhow::Result<()> {
    use diesel::PgConnection;
    use diesel_migrations::{HarnessWithOutput, MigrationHarness};
    let mut conn = PgConnection::establish(database_url)?;

    HarnessWithOutput::write_to_stdout(&mut conn)
        .run_pending_migrations(MIGRATIONS)
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!(e))?;

    Ok(())
}

fn main() -> anyhow::Result<()> {
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

    logging::init(
        LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stdout,
    )?;

    preinitialize_metrics();

    let args = Cli::parse();
    tracing::info!(
        "version: {}, launch_timestamp: {}, build_tag {}, state at {}, listening on {}",
        GIT_VERSION,
        launch_ts.to_string(),
        BUILD_TAG,
        args.path.as_ref().unwrap_or(&Utf8PathBuf::from("<none>")),
        args.listen
    );

    let secrets = Secrets::load(&args).await?;

    let config = Config {
        jwt_token: secrets.jwt_token,
        control_plane_jwt_token: secrets.control_plane_jwt_token,
        compute_hook_url: args.compute_hook_url,
    };

    // After loading secrets & config, but before starting anything else, apply database migrations
    migration_run(&secrets.database_url)
        .await
        .context("Running database migrations")?;

    let json_path = args.path;
    let persistence = Arc::new(Persistence::new(secrets.database_url, json_path.clone()));

    let service = Service::spawn(config, persistence.clone()).await?;

    let http_listener = tcp_listener::bind(args.listen)?;

    let auth = secrets
        .public_key
        .map(|jwt_auth| Arc::new(SwappableJwtAuth::new(jwt_auth)));
    let router = make_router(service.clone(), auth)
        .build()
        .map_err(|err| anyhow!(err))?;
    let router_service = utils::http::RouterService::new(router).unwrap();

    // Start HTTP server
    let server_shutdown = CancellationToken::new();
    let server = hyper::Server::from_tcp(http_listener)?
        .serve(router_service)
        .with_graceful_shutdown({
            let server_shutdown = server_shutdown.clone();
            async move {
                server_shutdown.cancelled().await;
            }
        });
    tracing::info!("Serving on {0}", args.listen);
    let server_task = tokio::task::spawn(server);

    // Wait until we receive a signal
    let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt())?;
    let mut sigquit = tokio::signal::unix::signal(SignalKind::quit())?;
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())?;
    tokio::select! {
        _ = sigint.recv() => {},
        _ = sigterm.recv() => {},
        _ = sigquit.recv() => {},
    }
    tracing::info!("Terminating on signal");

    if json_path.is_some() {
        // Write out a JSON dump on shutdown: this is used in compat tests to avoid passing
        // full postgres dumps around.
        if let Err(e) = persistence.write_tenants_json().await {
            tracing::error!("Failed to write JSON on shutdown: {e}")
        }
    }

    // Stop HTTP server first, so that we don't have to service requests
    // while shutting down Service
    server_shutdown.cancel();
    if let Err(e) = server_task.await {
        tracing::error!("Error joining HTTP server task: {e}")
    }
    tracing::info!("Joined HTTP server task");

    service.shutdown().await;
    tracing::info!("Service shutdown complete");

    std::process::exit(0);
}
