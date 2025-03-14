use std::io::ErrorKind;
use std::process::Command;
use std::{fs::OpenOptions, io::Write};

use anyhow::{Context, Result, anyhow};
use tracing::info;

const POSTGRES_LOGS_CONF_PATH: &str = "/etc/rsyslog.d/postgres_logs.conf";

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
    log_directory: &str,
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

/// Configuration for enabling Postgres logs forwarding from rsyslogd
pub struct PostgresLogsRsyslogConfig<'a> {
    pub host: Option<&'a str>,
}

impl<'a> PostgresLogsRsyslogConfig<'a> {
    pub fn new(host: Option<&'a str>) -> Self {
        Self { host }
    }

    pub fn build(&self) -> Result<String> {
        match self.host {
            Some(host) => {
                if let Some((target, port)) = host.split_once(":") {
                    Ok(format!(
                        include_str!(
                            "config_template/compute_rsyslog_postgres_export_template.conf"
                        ),
                        logs_export_target = target,
                        logs_export_port = port,
                    ))
                } else {
                    Err(anyhow!("Invalid host format for Postgres logs export"))
                }
            }
            None => Ok("".to_string()),
        }
    }

    fn current_config() -> Result<String> {
        let config_content = match std::fs::read_to_string(POSTGRES_LOGS_CONF_PATH) {
            Ok(c) => c,
            Err(err) if err.kind() == ErrorKind::NotFound => String::new(),
            Err(err) => return Err(err.into()),
        };
        Ok(config_content)
    }

    /// Returns the default host for otel collector that receives Postgres logs
    pub fn default_host(project_id: &str) -> String {
        format!(
            "config-{}-collector.neon-telemetry.svc.cluster.local:10514",
            project_id
        )
    }
}

pub fn configure_postgres_logs_export(conf: PostgresLogsRsyslogConfig) -> Result<()> {
    let new_config = conf.build()?;
    let current_config = PostgresLogsRsyslogConfig::current_config()?;

    if new_config == current_config {
        info!("postgres logs rsyslog configuration is up-to-date");
        return Ok(());
    }

    // When new config is empty we can simply remove the configuration file.
    if new_config.is_empty() {
        info!("removing rsyslog config file: {}", POSTGRES_LOGS_CONF_PATH);
        match std::fs::remove_file(POSTGRES_LOGS_CONF_PATH) {
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        restart_rsyslog()?;
        return Ok(());
    }

    info!(
        "configuring rsyslog for postgres logs export to: {:?}",
        conf.host
    );

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(POSTGRES_LOGS_CONF_PATH)?;
    file.write_all(new_config.as_bytes())?;

    info!(
        "rsyslog configuration file {} added successfully. Starting rsyslogd",
        POSTGRES_LOGS_CONF_PATH
    );

    restart_rsyslog()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::rsyslog::PostgresLogsRsyslogConfig;

    #[test]
    fn test_postgres_logs_config() {
        {
            // Verify empty config
            let conf = PostgresLogsRsyslogConfig::new(None);
            let res = conf.build();
            assert!(res.is_ok());
            let conf_str = res.unwrap();
            assert_eq!(&conf_str, "");
        }

        {
            // Verify config
            let conf = PostgresLogsRsyslogConfig::new(Some("collector.cvc.local:514"));
            let res = conf.build();
            assert!(res.is_ok());
            let conf_str = res.unwrap();
            assert!(conf_str.contains("omfwd"));
            assert!(conf_str.contains(r#"target="collector.cvc.local""#));
            assert!(conf_str.contains(r#"port="514""#));
        }

        {
            // Verify invalid config
            let conf = PostgresLogsRsyslogConfig::new(Some("invalid"));
            let res = conf.build();
            assert!(res.is_err());
        }

        {
            // Verify config with default host
            let host = PostgresLogsRsyslogConfig::default_host("shy-breeze-123");
            let conf = PostgresLogsRsyslogConfig::new(Some(&host));
            let res = conf.build();
            assert!(res.is_ok());
            let conf_str = res.unwrap();
            assert!(conf_str.contains(r#"shy-breeze-123"#));
            assert!(conf_str.contains(r#"port="10514""#));
        }
    }
}
