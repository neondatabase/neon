use anyhow::Context;
use axum::{
    extract::{ws::WebSocket, State, WebSocketUpgrade},
    response::Response,
};
use axum::{routing::get, Router, Server};
use std::{fmt::Debug, time::Duration};
use sysinfo::{RefreshKind, System, SystemExt};
use tokio::sync::broadcast;
use tracing::{error, info};

use runner::Runner;

// Code that interfaces with agent
pub mod dispatcher;
pub mod protocol;

pub mod cgroup;
pub mod filecache;
pub mod runner;

/// Arguments (previously command-line) to configure the monitor.
#[derive(Debug)]
pub struct Args {
    pub cgroup: Option<String>,
    pub pgconnstr: Option<String>,
    pub addr: String,
}

impl Args {
    pub fn addr(&self) -> &str {
        &self.addr
    }
}

/// The number of bytes in one mebibyte.
#[allow(non_upper_case_globals)]
const MiB: u64 = 1 << 20;

/// Convert a quantity in bytes to a quantity in mebibytes, generally for display
/// purposes. (Most calculations in this crate use bytes directly)
pub fn bytes_to_mebibytes(bytes: u64) -> f32 {
    (bytes as f32) / (MiB as f32)
}

pub fn get_total_system_memory() -> u64 {
    System::new_with_specifics(RefreshKind::new().with_memory()).total_memory()
}

/// Global app state for the Axum server
#[derive(Debug, Clone)]
pub struct ServerState {
    /// Used to close old connections.
    ///
    /// When a new connection is made, we send a message signalling to the old
    /// connection to close.
    pub sender: broadcast::Sender<()>,

    // The CLI args
    pub args: &'static Args,
}

/// The entrypoint to the binary.
///
/// Set up tracing, parse arguments, and start an http server.
pub async fn start(args: &'static Args) -> anyhow::Result<()> {
    // This channel is used to close old connections. When a new connection is
    // made, we send a message signalling to the old connection to close.
    let (sender, _) = tokio::sync::broadcast::channel::<()>(1);

    let app = Router::new()
        // This route gets upgraded to a websocket connection. We only support
        // connection at a time. We enforce this using global app state.
        // True indicates we are connected to someone and False indicates we can
        // receive connections.
        .route("/monitor", get(ws_handler))
        .with_state(ServerState { sender, args });

    let addr = args.addr();
    let bound = Server::try_bind(&addr.parse().expect("parsing address should not fail"))
        .with_context(|| format!("failed to bind to {addr}"))?;

    info!(addr, "server bound");

    bound
        .serve(app.into_make_service())
        .await
        .context("server exited")?;

    Ok(())
}

/// Handles incoming websocket connections.
///
/// If we are already to connected to an informant, we kill that old connection
/// and accept the new one.
#[tracing::instrument(name = "/monitor", skip(ws))]
pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<ServerState>) -> Response {
    // Kill the old monitor
    info!("closing old connection if there is one");
    let _ = state.sender.send(());

    // Start the new one. Wow, the cycle of death and rebirth
    let closer = state.sender.subscribe();
    ws.on_upgrade(|ws| start_monitor(ws, state.args, closer))
}

/// Starts the monitor. If startup fails or the monitor exits, an error will
/// be logged and our internal state will be reset to allow for new connections.
#[tracing::instrument(skip_all)]
async fn start_monitor(ws: WebSocket, args: &Args, kill: broadcast::Receiver<()>) {
    info!("accepted new websocket connection -> starting monitor");
    let monitor = tokio::time::timeout(
        Duration::from_secs(2),
        // Unwrap is safe because we initialize at the beginning of main
        Runner::new(Default::default(), args, ws, kill),
    )
    .await;
    let mut monitor = match monitor {
        Ok(Ok(monitor)) => monitor,
        Ok(Err(error)) => {
            error!(?error, "failed to create monitor");
            return;
        }
        Err(elapsed) => {
            error!(
                ?elapsed,
                "creating monitor timed out (probably waiting to receive protocol range)"
            );
            return;
        }
    };
    info!("connected to informant");

    match monitor.run().await {
        Ok(()) => info!("monitor was killed due to new connection"),
        Err(e) => error!(error = ?e, "monitor terminated by itself"),
    }
}
