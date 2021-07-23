use crate::PageServerConf;

use anyhow::{Context, Result};
use slog::{Drain, FnValue};
use std::fs::{File, OpenOptions};

pub fn init_logging(
    _conf: &PageServerConf,
    log_filename: &str,
) -> Result<(slog_scope::GlobalLoggerGuard, File)> {
    // Don't open the same file for output multiple times;
    // the different fds could overwrite each other's output.
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_filename)
        .with_context(|| format!("failed to open {:?}", &log_filename))?;

    let logger_file = log_file.try_clone().unwrap();

    let decorator = slog_term::PlainSyncDecorator::new(logger_file);
    let drain = slog_term::FullFormat::new(decorator).build();
    let drain = slog::Filter::new(drain, |record: &slog::Record| {
        if record.level().is_at_least(slog::Level::Info) {
            return true;
        }
        false
    });
    let drain = std::sync::Mutex::new(drain).fuse();
    let logger = slog::Logger::root(
        drain,
        slog::o!(
            "location" =>
                FnValue(move |record| {
                    format!("{}, {}:{}",
                            record.module(),
                            record.file(),
                            record.line()
                    )
                }
                )
        ),
    );
    Ok((slog_scope::set_global_logger(logger), log_file))
}
