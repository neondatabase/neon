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

// Start rsyslogd with the specified configuration file
// If it is already running, do nothing.
fn start_rsyslog(rsyslog_conf_path: &str) -> Result<()> {
    let pid = get_rsyslog_pid();
    if let Some(pid) = pid {
        info!("rsyslogd is already running with pid: {}", pid);
        return Ok(());
    }

    let _ = Command::new("/usr/sbin/rsyslogd")
        .arg("-f")
        .arg(rsyslog_conf_path)
        .arg("-i")
        .arg("/var/run/rsyslogd/rsyslogd.pid")
        .output()
        .context("Failed to start rsyslogd")?;

    // Check that rsyslogd is running
    if let Some(pid) = get_rsyslog_pid() {
        info!("rsyslogd started successfully with pid: {}", pid);
    } else {
        return Err(anyhow::anyhow!("Failed to start rsyslogd"));
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

    let rsyslog_conf_path = "/etc/compute_rsyslog.conf";
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(rsyslog_conf_path)?;

    file.write_all(config_content.as_bytes())?;

    info!("rsyslog configuration added successfully. Starting rsyslogd");

    // start the service, using the configuration
    start_rsyslog(rsyslog_conf_path)?;

    Ok(())
}
