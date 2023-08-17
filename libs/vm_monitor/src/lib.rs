#![cfg(target_os = "linux")]

use anyhow::Context;
use axum::{
    extract::{ws::WebSocket, State, WebSocketUpgrade},
    response::Response,
};
use axum::{routing::get, Router, Server};
use clap::Parser;
use futures::Future;
use std::{fmt::Debug, time::Duration};
use sysinfo::{RefreshKind, System, SystemExt};
use tokio::{sync::broadcast, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use runner::Runner;

// Code that interfaces with agent
pub mod dispatcher;
pub mod protocol;

pub mod cgroup;
pub mod filecache;
pub mod runner;

/// Arguments to configure the monitor.
#[derive(Debug, Parser)]
pub struct Args {
    #[arg(short, long)]
    pub cgroup: Option<String>,
    #[arg(short, long)]
    pub pgconnstr: Option<String>,
    #[arg(short, long)]
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

    /// Used to cancel all spawned threads in the monitor.
    pub token: CancellationToken,

    // The CLI args
    pub args: &'static Args,
}

/// Spawn a thread that may get cancelled by the provided [`CancellationToken`].
///
/// This is mainly meant to be called with futures that will be pending for a very
/// long time, or are not mean to return. If it is not desirable for the future to
/// ever resolve, such as in the case of [`cgroup::CgroupWatcher::watch`], the error can
/// be logged with `f`.
pub fn spawn_with_cancel<T, F>(
    token: CancellationToken,
    f: F,
    future: T,
) -> JoinHandle<Option<T::Output>>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
    F: FnOnce(&T::Output) + Send + 'static,
{
    tokio::spawn(async move {
        tokio::select! {
            _ = token.cancelled() => {
                info!("received global kill signal");
                None
            }
            res = future => {
                f(&res);
                Some(res)
            }
        }
    })
}

/// The entrypoint to the binary.
///
/// Set up tracing, parse arguments, and start an http server.
pub async fn start(args: &'static Args, token: CancellationToken) -> anyhow::Result<()> {
    // This channel is used to close old connections. When a new connection is
    // made, we send a message signalling to the old connection to close.
    let (sender, _) = tokio::sync::broadcast::channel::<()>(1);

    let app = Router::new()
        // This route gets upgraded to a websocket connection. We only support
        // connection at a time. We enforce this using global app state.
        // True indicates we are connected to someone and False indicates we can
        // receive connections.
        .route("/monitor", get(ws_handler))
        .with_state(ServerState {
            sender,
            token,
            args,
        });

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
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(ServerState {
        sender,
        token,
        args,
    }): State<ServerState>,
) -> Response {
    // Kill the old monitor
    info!("closing old connection if there is one");
    let _ = sender.send(());

    // Start the new one. Wow, the cycle of death and rebirth
    let closer = sender.subscribe();
    ws.on_upgrade(|ws| start_monitor(ws, args, closer, token))
}

/// Starts the monitor. If startup fails or the monitor exits, an error will
/// be logged and our internal state will be reset to allow for new connections.
#[tracing::instrument(skip_all)]
async fn start_monitor(
    ws: WebSocket,
    args: &Args,
    kill: broadcast::Receiver<()>,
    token: CancellationToken,
) {
    info!("accepted new websocket connection -> starting monitor");
    let timeout = Duration::from_secs(4);
    let monitor = tokio::time::timeout(
        timeout,
        // Unwrap is safe because we initialize at the beginning of main
        Runner::new(Default::default(), args, ws, kill, token),
    )
    .await;
    let mut monitor = match monitor {
        Ok(Ok(monitor)) => monitor,
        Ok(Err(error)) => {
            error!(?error, "failed to create monitor");
            return;
        }
        Err(_) => {
            error!(
                ?timeout,
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
