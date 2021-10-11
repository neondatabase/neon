use std::{
    fs::{File, OpenOptions},
    path::Path,
};

use anyhow::{Context, Result};

use tracing::subscriber::set_global_default;
use tracing_log::LogTracer;
use tracing_subscriber::fmt;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Registry};

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
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter_str));

    // we are cloning and returning log file in order to allow redirecting daemonized stdout and stderr to it
    // if we do not use daemonization (e.g. in docker) it is better to log to stdout directly
    // for example to be in line with docker log command which expects logs comimg from stdout
    //
    // TODO: perhaps use a more human-readable format when !daemonize
    if daemonize {
        let x = log_file.try_clone().unwrap();

        let fmt_layer = fmt::layer()
            .pretty()
            .with_target(false) // don't include event targets
            .with_ansi(false) // don't use colors in log file
            .with_writer(move || x.try_clone().unwrap());
        let subscriber = Registry::default().with(env_filter).with(fmt_layer);

        set_global_default(subscriber).expect("Failed to set subscriber");
    } else {
        let fmt_layer = fmt::layer().with_target(false); // don't include event targets
        let subscriber = Registry::default().with(env_filter).with(fmt_layer);

        set_global_default(subscriber).expect("Failed to set subscriber");
    }

    // Redirect all `log`'s events to our subscriber
    LogTracer::init().expect("Failed to set logger");

    Ok(log_file)
}
