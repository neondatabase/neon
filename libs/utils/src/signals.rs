pub use signal_hook::consts::TERM_SIGNALS;
pub use signal_hook::consts::signal::*;
use signal_hook::iterator::Signals;
use tokio::signal::unix::{SignalKind, signal};
use tracing::info;

pub enum Signal {
    Quit,
    Interrupt,
    Terminate,
}

impl Signal {
    pub fn name(&self) -> &'static str {
        match self {
            Signal::Quit => "SIGQUIT",
            Signal::Interrupt => "SIGINT",
            Signal::Terminate => "SIGTERM",
        }
    }
}

pub struct ShutdownSignals;

impl ShutdownSignals {
    pub fn handle(mut handler: impl FnMut(Signal) -> anyhow::Result<()>) -> anyhow::Result<()> {
        for raw_signal in Signals::new(TERM_SIGNALS)?.into_iter() {
            let signal = match raw_signal {
                SIGINT => Signal::Interrupt,
                SIGTERM => Signal::Terminate,
                SIGQUIT => Signal::Quit,
                other => panic!("unknown signal: {other}"),
            };

            handler(signal)?;
        }

        Ok(())
    }
}

/// Runs in a loop since we want to be responsive to multiple signals
/// even after triggering shutdown (e.g. a SIGQUIT after a slow SIGTERM shutdown)
/// <https://github.com/neondatabase/neon/issues/9740>
pub async fn signal_handler(token: tokio_util::sync::CancellationToken) {
    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let mut sigquit = signal(SignalKind::quit()).unwrap();

    loop {
        let signal = tokio::select! {
            _ = sigquit.recv() => {
                info!("Got signal SIGQUIT. Terminating in immediate shutdown mode.");
                std::process::exit(111);
            }
            _ = sigint.recv() => "SIGINT",
            _ = sigterm.recv() => "SIGTERM",
        };

        if !token.is_cancelled() {
            info!("Got signal {signal}. Terminating gracefully in fast shutdown mode.");
            token.cancel();
        } else {
            info!("Got signal {signal}. Already shutting down.");
        }
    }
}
