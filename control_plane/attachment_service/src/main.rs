/// The attachment service mimics the aspects of the control plane API
/// that are required for a pageserver to operate.
///
/// This enables running & testing pageservers without a full-blown
/// deployment of the Neon cloud platform.
///
use anyhow::anyhow;
use attachment_service::http::make_router;
use attachment_service::service::Service;
use clap::Parser;
use std::path::PathBuf;
use utils::logging::{self, LogFormat};
use utils::signals::{ShutdownSignals, Signal};

use utils::tcp_listener;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
struct Cli {
    /// Host and port to listen on, like `127.0.0.1:1234`
    #[arg(short, long)]
    listen: std::net::SocketAddr,

    /// Path to the .json file to store state (will be created if it doesn't exist)
    #[arg(short, long)]
    path: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init(
        LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stdout,
    )?;

    let args = Cli::parse();
    tracing::info!(
        "Starting, state at {}, listening on {}",
        args.path.to_string_lossy(),
        args.listen
    );

    let service = Service::spawn();

    let http_listener = tcp_listener::bind(args.listen)?;
    let router = make_router(service).build().map_err(|err| anyhow!(err))?;
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
