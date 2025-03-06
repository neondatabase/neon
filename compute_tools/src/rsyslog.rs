use std::process::Command;
use std::{fs::OpenOptions, io::Write};

use anyhow::{Context, Result};
use tracing::info;

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
    info!(
        "rsyslogd is already running with pid: {}, restart it",
        old_pid
    );

    // kill it to restart
    let _ = Command::new("pkill")
        .arg("rsyslogd")
        .output()
        .context("Failed to stop rsyslogd")?;

    // debug only
    if let Some(pid) = get_rsyslog_pid() {
        info!("rsyslogd is alive with pid: {}", pid);
    }

    Ok(())
}

pub fn configure_and_start_rsyslog(
    log_directory: &str,
    tag: &str,
    remote_endpoint: &str,
) -> Result<()> {
    let config_content: String = format!(
        include_str!("config_template/compute_rsyslog_template.conf"),
        log_directory = log_directory,
        tag = tag,
        remote_endpoint = remote_endpoint
    );

    info!("rsyslog config_content: {}", config_content);

    let rsyslog_conf_path = "/etc/rsyslog.d/hipaa_rsyslog.conf";
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(rsyslog_conf_path)?;

    file.write_all(config_content.as_bytes())?;

    info!("rsyslog configuration added successfully. Starting rsyslogd");

    // start the service, using the configuration
    restart_rsyslog()?;

    Ok(())
}
