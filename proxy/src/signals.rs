use std::convert::Infallible;

use anyhow::bail;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Handle unix signals appropriately.
pub async fn handle<F>(
    token: CancellationToken,
    mut refresh_config: F,
) -> anyhow::Result<Infallible>
where
    F: FnMut(),
{
    use tokio::signal::unix::{signal, SignalKind};

    let mut hangup = signal(SignalKind::hangup())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;

    loop {
        tokio::select! {
            // Hangup is commonly used for config reload.
            _ = hangup.recv() => {
                info!("received SIGHUP");
                refresh_config();
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
