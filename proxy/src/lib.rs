use std::convert::Infallible;

use anyhow::{bail, Context};
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tracing::warn;

pub mod auth;
pub mod cache;
pub mod cancellation;
pub mod compute;
pub mod config;
pub mod console;
pub mod error;
pub mod http;
pub mod logging;
pub mod metrics;
pub mod parse;
pub mod protocol2;
pub mod proxy;
pub mod sasl;
pub mod scram;
pub mod stream;
pub mod url;
pub mod waiters;

/// Handle unix signals appropriately.
pub async fn handle_signals(token: CancellationToken) -> anyhow::Result<Infallible> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut hangup = signal(SignalKind::hangup())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;

    loop {
        tokio::select! {
            // Hangup is commonly used for config reload.
            _ = hangup.recv() => {
                warn!("received SIGHUP; config reload is not supported");
            }
            // Shut down the whole application.
            _ = interrupt.recv() => {
                warn!("received SIGINT, exiting immediately");
                bail!("interrupted");
            }
            _ = terminate.recv() => {
                warn!("received SIGTERM, shutting down once all existing connections have closed");
                token.cancel();
            }
        }
    }
}

/// Flattens `Result<Result<T>>` into `Result<T>`.
pub fn flatten_err<T>(r: Result<anyhow::Result<T>, JoinError>) -> anyhow::Result<T> {
    r.context("join error").and_then(|x| x)
}
