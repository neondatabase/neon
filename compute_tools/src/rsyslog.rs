use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use std::{fs::OpenOptions, io::Write};

use anyhow::{Context, Result};
use tracing::{error, info, instrument, warn};

fn get_rsyslog_pid() -> Option<String> {
    let output = Command::new("pgrep")
        .arg("rsyslogd")
        .output()
        .expect("Failed to execute pgrep");

    if !output.stdout.is_empty() {
        let pid = std::str::from_utf8(&output.stdout)
            .expect("Invalid UTF-8 in process output")
            .trim()
            .to_string();
        Some(pid)
    } else {
        None
    }
}

// Restart rsyslogd to apply the new configuration.
// This is necessary, because there is no other way to reload the rsyslog configuration.
//
// Rsyslogd shouldn't lose any messages, because of the restart,
// because it tracks the last read position in the log files
// and will continue reading from that position.
// TODO: test it properly
//
fn restart_rsyslog() -> Result<()> {
    let old_pid = get_rsyslog_pid().context("rsyslogd is not running")?;
    info!("rsyslogd is running with pid: {}, restart it", old_pid);

    // kill it to restart
    let _ = Command::new("pkill")
        .arg("rsyslogd")
        .output()
        .context("Failed to stop rsyslogd")?;

    Ok(())
}

pub fn configure_audit_rsyslog(
    log_directory: String,
    tag: &str,
    remote_endpoint: &str,
) -> Result<()> {
    let config_content: String = format!(
        include_str!("config_template/compute_audit_rsyslog_template.conf"),
        log_directory = log_directory,
        tag = tag,
        remote_endpoint = remote_endpoint
    );

    info!("rsyslog config_content: {}", config_content);

    let rsyslog_conf_path = "/etc/rsyslog.d/compute_audit_rsyslog.conf";
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(rsyslog_conf_path)?;

    file.write_all(config_content.as_bytes())?;

    info!(
        "rsyslog configuration file {} added successfully. Starting rsyslogd",
        rsyslog_conf_path
    );

    // start the service, using the configuration
    restart_rsyslog()?;

    Ok(())
}

#[instrument(skip_all)]
async fn pgaudit_gc_main_loop(log_directory: String) -> Result<()> {
    info!("running pgaudit GC main loop");
    loop {
        // Check log_directory for old pgaudit logs and delete them.
        // New log files are checked every 5 minutes, as set in pgaudit.log_rotation_age
        // Find files that were not modified in the last 15 minutes and delete them.
        // This should be enough time for rsyslog to process the logs and for us to catch the alerts.
        //
        // In case of a very high load, we might need to adjust this value and pgaudit.log_rotation_age.
        //
        // TODO: add some smarter logic to delete the files that are fully streamed according to rsyslog
        // imfile-state files, but for now just do a simple GC to avoid filling up the disk.
        let _ = Command::new("find")
            .arg(&log_directory)
            .arg("-name")
            .arg("audit*.log")
            .arg("-mmin")
            .arg("+15")
            .arg("-delete")
            .output()?;

        // also collect the metric for the size of the log directory
        async fn get_log_files_size(path: &Path) -> Result<u64> {
            let mut total_size = 0;

            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let entry_path = entry.path();

                if entry_path.is_file() && entry_path.to_string_lossy().ends_with("log") {
                    total_size += entry.metadata()?.len();
                }
            }

            Ok(total_size)
        }

        let log_directory_size = get_log_files_size(Path::new(&log_directory))
            .await
            .unwrap_or_else(|e| {
                warn!("Failed to get log directory size: {}", e);
                0
            });
        crate::metrics::AUDIT_LOG_DIR_SIZE.set(log_directory_size as f64);
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

// launch pgaudit GC thread to clean up the old pgaudit logs stored in the log_directory
pub fn launch_pgaudit_gc(log_directory: String) {
    tokio::spawn(async move {
        if let Err(e) = pgaudit_gc_main_loop(log_directory).await {
            error!("pgaudit GC main loop failed: {}", e);
        }
    });
}
