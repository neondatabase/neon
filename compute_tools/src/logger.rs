use anyhow::Result;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;

/// Initialize `env_logger` using either `default_level` or
/// `RUST_LOG` environment variable as default log level.
pub fn init_logger(default_level: &str) -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_writer(std::io::stderr);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();

    Ok(())
}
