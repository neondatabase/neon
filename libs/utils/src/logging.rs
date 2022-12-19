use std::str::FromStr;

use anyhow::Context;
use strum_macros::{EnumString, EnumVariantNames};

pub use tracing_subscriber::DefaultSlabConfig;
pub use tracing_subscriber::SlabConfig;

#[derive(EnumString, EnumVariantNames, Eq, PartialEq, Debug, Clone, Copy)]
#[strum(serialize_all = "snake_case")]
pub enum LogFormat {
    Plain,
    Json,
}

impl LogFormat {
    pub fn from_config(s: &str) -> anyhow::Result<LogFormat> {
        use strum::VariantNames;
        LogFormat::from_str(s).with_context(|| {
            format!(
                "Unrecognized log format. Please specify one of: {:?}",
                LogFormat::VARIANTS
            )
        })
    }
}

pub fn init<S>(log_format: LogFormat) -> anyhow::Result<()>
where
    S: SlabConfig + 'static,
{
    let default_filter_str = "info";

    // We fall back to printing all spans at info-level or above if
    // the RUST_LOG environment variable is not set.
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter_str));

    let base_logger = tracing_subscriber::fmt()
        .with_slab_config::<S>()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_ansi(false)
        .with_writer(std::io::stdout);

    match log_format {
        LogFormat::Json => base_logger.json().init(),
        LogFormat::Plain => base_logger.init(),
    }

    Ok(())
}
