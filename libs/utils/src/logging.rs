use std::{
    fs::{File, OpenOptions},
    path::Path,
};

use anyhow::{Context, Result};

pub fn init(log_filename: impl AsRef<Path>, daemonize: bool) -> Result<File> {
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

    let base_logger = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false) // don't include event targets
        .with_ansi(false); // don't use colors in log file;

    // we are cloning and returning log file in order to allow redirecting daemonized stdout and stderr to it
    // if we do not use daemonization (e.g. in docker) it is better to log to stdout directly
    // for example to be in line with docker log command which expects logs comimg from stdout
    if daemonize {
        let x = log_file.try_clone().unwrap();
        base_logger
            .with_writer(move || x.try_clone().unwrap())
            .init();
    } else {
        base_logger.init();
    }

    Ok(log_file)
}
