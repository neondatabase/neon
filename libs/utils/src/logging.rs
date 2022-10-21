use std::{
    fs::{File, OpenOptions},
    path::Path,
    str::FromStr,
};

use anyhow::{Context, Result};
use strum_macros::{EnumString, EnumVariantNames};

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
pub fn init(
    log_filename: impl AsRef<Path>,
    daemonize: bool,
    log_format: LogFormat,
) -> Result<File> {
    // Don't open the same file for output multiple times;
    // the different fds could overwrite each other's output.
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_filename)
        .with_context(|| format!("failed to open {:?}", log_filename.as_ref()))?;

    let default_filter_str = "info";

    // We fall back to printing all spans at info-level or above if
    // the RUST_LOG environment variable is not set.
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter_str));

    let x: File = log_file.try_clone().unwrap();
    let base_logger = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_ansi(false)
        .with_writer(move || -> Box<dyn std::io::Write> {
            // we are cloning and returning log file in order to allow redirecting daemonized stdout and stderr to it
            // if we do not use daemonization (e.g. in docker) it is better to log to stdout directly
            // for example to be in line with docker log command which expects logs comimg from stdout
            if daemonize {
                Box::new(x.try_clone().unwrap())
            } else {
                Box::new(std::io::stdout())
            }
        });

    match log_format {
        LogFormat::Json => base_logger.json().init(),
        LogFormat::Plain => base_logger.init(),
    }

    Ok(log_file)
}

// #[cfg(test)]
// Due to global logger, can't run tests in same process.
// So until there's a non-global one, the tests are in ../tests/ as separate files.
#[macro_export(local_inner_macros)]
macro_rules! test_init_file_logger {
    ($log_level:expr, $log_format:expr) => {{
        use std::str::FromStr;
        std::env::set_var("RUST_LOG", $log_level);

        let tmp_dir = tempfile::TempDir::new().unwrap();
        let log_file_path = tmp_dir.path().join("logfile");

        let log_format = $crate::logging::LogFormat::from_str($log_format).unwrap();
        let _log_file = $crate::logging::init(&log_file_path, true, log_format).unwrap();

        let log_file = std::fs::OpenOptions::new()
            .read(true)
            .open(&log_file_path)
            .unwrap();

        log_file
    }};
}
