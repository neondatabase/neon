/// The attachment service mimics the aspects of the control plane API
/// that are required for a pageserver to operate.
///
/// This enables running & testing pageservers without a full-blown
/// deployment of the Neon cloud platform.
///
use anyhow::anyhow;
use attachment_service::http::make_router;
use attachment_service::persistence::Persistence;
use attachment_service::service::{Config, Service};
use camino::Utf8PathBuf;
use clap::Parser;
use metrics::launch_timestamp::LaunchTimestamp;
use std::sync::Arc;
use utils::auth::{JwtAuth, SwappableJwtAuth};
use utils::logging::{self, LogFormat};
use utils::signals::{ShutdownSignals, Signal};

use utils::{project_build_tag, project_git_version, tcp_listener};

project_git_version!(GIT_VERSION);
project_build_tag!(BUILD_TAG);

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
struct Cli {
    /// Host and port to listen on, like `127.0.0.1:1234`
    #[arg(short, long)]
    listen: std::net::SocketAddr,

    /// Path to public key for JWT authentication of clients
    #[arg(long)]
    public_key: Option<camino::Utf8PathBuf>,

    /// Token for authenticating this service with the pageservers it controls
    #[arg(short, long)]
    jwt_token: Option<String>,

    /// Path to the .json file to store state (will be created if it doesn't exist)
    #[arg(short, long)]
    path: Utf8PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let launch_ts = Box::leak(Box::new(LaunchTimestamp::generate()));

    logging::init(
        LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stdout,
    )?;

    let args = Cli::parse();
    tracing::info!(
        "version: {}, launch_timestamp: {}, build_tag {}, state at {}, listening on {}",
        GIT_VERSION,
        launch_ts.to_string(),
        BUILD_TAG,
        args.path,
        args.listen
    );

    let config = Config {
        jwt_token: args.jwt_token,
    };

    let persistence = Arc::new(Persistence::spawn(&args.path).await);

    let service = Service::spawn(config, persistence).await?;

    let http_listener = tcp_listener::bind(args.listen)?;

    let auth = if let Some(public_key_path) = &args.public_key {
        let jwt_auth = JwtAuth::from_key_path(public_key_path)?;
        Some(Arc::new(SwappableJwtAuth::new(jwt_auth)))
    } else {
        None
    };
    let router = make_router(service, auth)
        .build()
        .map_err(|err| anyhow!(err))?;
    let service = utils::http::RouterService::new(router).unwrap();
    let server = hyper::Server::from_tcp(http_listener)?.serve(service);

    tracing::info!("Serving on {0}", args.listen);

    tokio::task::spawn(server);

    ShutdownSignals::handle(|signal| match signal {
        Signal::Interrupt | Signal::Terminate | Signal::Quit => {
            tracing::info!("Got {}. Terminating", signal.name());
            // We're just a test helper: no graceful shutdown.
            std::process::exit(0);
        }
    })?;

    Ok(())
}
