use std::fs;
use std::io::ErrorKind;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use std::{fs::OpenOptions, io::Write};
use url::{Host, Url};

use anyhow::{Context, Result, anyhow};
use hostname_validator;
use tracing::{error, info, instrument, warn};

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

fn wait_for_rsyslog_pid() -> Result<String, anyhow::Error> {
    const MAX_WAIT: Duration = Duration::from_secs(5);
    const INITIAL_SLEEP: Duration = Duration::from_millis(2);

    let mut sleep_duration = INITIAL_SLEEP;
    let start = std::time::Instant::now();
    let mut attempts = 1;

    for attempt in 1.. {
        attempts = attempt;
        match get_rsyslog_pid() {
            Some(pid) => return Ok(pid),
            None => {
                if start.elapsed() >= MAX_WAIT {
                    break;
                }
                info!(
                    "rsyslogd is not running, attempt {}. Sleeping for {} ms",
                    attempt,
                    sleep_duration.as_millis()
                );
                std::thread::sleep(sleep_duration);
                sleep_duration *= 2;
            }
        }
    }

    Err(anyhow::anyhow!(
        "rsyslogd is not running after waiting for {} seconds and {} attempts",
        attempts,
        start.elapsed().as_secs()
    ))
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
    // kill it to restart
    let _ = Command::new("pkill")
        .arg("rsyslogd")
        .output()
        .context("Failed to restart rsyslogd")?;

    // ensure rsyslogd is running
    wait_for_rsyslog_pid()?;

    Ok(())
}

fn parse_audit_syslog_address(
    remote_plain_endpoint: &str,
    remote_tls_endpoint: &str,
) -> Result<(String, u16, String)> {
    let tls;
    let remote_endpoint = if !remote_tls_endpoint.is_empty() {
        tls = "true".to_string();
        remote_tls_endpoint
    } else {
        tls = "false".to_string();
        remote_plain_endpoint
    };
    // Urlify the remote_endpoint, so parsing can be done with url::Url.
    let url_str = format!("http://{remote_endpoint}");
    let url = Url::parse(&url_str).map_err(|err| {
        anyhow!("Error parsing {remote_endpoint}, expected host:port, got {err:?}")
    })?;

    let is_valid = url.scheme() == "http"
        && url.path() == "/"
        && url.query().is_none()
        && url.fragment().is_none()
        && url.username() == ""
        && url.password().is_none();

    if !is_valid {
        return Err(anyhow!(
            "Invalid address format {remote_endpoint}, expected host:port"
        ));
    }
    let host = match url.host() {
        Some(Host::Domain(h)) if hostname_validator::is_valid(h) => h.to_string(),
        Some(Host::Ipv4(ip4)) => ip4.to_string(),
        Some(Host::Ipv6(ip6)) => ip6.to_string(),
        _ => return Err(anyhow!("Invalid host")),
    };
    let port = url
        .port()
        .ok_or_else(|| anyhow!("Invalid port in {remote_endpoint}"))?;

    Ok((host, port, tls))
}

fn generate_audit_rsyslog_config(
    log_directory: String,
    endpoint_id: &str,
    project_id: &str,
    remote_syslog_host: &str,
    remote_syslog_port: u16,
    remote_syslog_tls: &str,
) -> String {
    format!(
        include_str!("config_template/compute_audit_rsyslog_template.conf"),
        log_directory = log_directory,
        endpoint_id = endpoint_id,
        project_id = project_id,
        remote_syslog_host = remote_syslog_host,
        remote_syslog_port = remote_syslog_port,
        remote_syslog_tls = remote_syslog_tls
    )
}

pub fn configure_audit_rsyslog(
    log_directory: String,
    endpoint_id: &str,
    project_id: &str,
    remote_endpoint: &str,
    remote_tls_endpoint: &str,
) -> Result<()> {
    let (remote_syslog_host, remote_syslog_port, remote_syslog_tls) =
        parse_audit_syslog_address(remote_endpoint, remote_tls_endpoint).unwrap();
    let config_content = generate_audit_rsyslog_config(
        log_directory,
        endpoint_id,
        project_id,
        &remote_syslog_host,
        remote_syslog_port,
        &remote_syslog_tls,
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
}

/// Writes rsyslogd configuration for Postgres logs export and restarts rsyslog.
pub fn configure_postgres_logs_export(conf: PostgresLogsRsyslogConfig) -> Result<()> {
    let new_config = conf.build()?;
    let current_config = PostgresLogsRsyslogConfig::current_config()?;

    if new_config == current_config {
        info!("postgres logs rsyslog configuration is up-to-date");
        return Ok(());
    }

    // Nothing to configure
    if new_config.is_empty() {
        // When the configuration is removed, PostgreSQL will stop sending data
        // to the files watched by rsyslog, so restarting rsyslog is more effort
        // than just ignoring this change.
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

#[cfg(test)]
mod tests {
    use crate::rsyslog::PostgresLogsRsyslogConfig;

    use super::{generate_audit_rsyslog_config, parse_audit_syslog_address};

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
    }

    #[test]
    fn test_parse_audit_syslog_address() {
        {
            // host:port format (plaintext)
            let parsed = parse_audit_syslog_address("collector.host.tld:5555", "");
            assert!(parsed.is_ok());
            assert_eq!(
                parsed.unwrap(),
                (
                    String::from("collector.host.tld"),
                    5555,
                    String::from("false")
                )
            );
        }

        {
            // host:port format with ipv4 ip address (plaintext)
            let parsed = parse_audit_syslog_address("10.0.0.1:5555", "");
            assert!(parsed.is_ok());
            assert_eq!(
                parsed.unwrap(),
                (String::from("10.0.0.1"), 5555, String::from("false"))
            );
        }

        {
            // host:port format with ipv6 ip address (plaintext)
            let parsed =
                parse_audit_syslog_address("[7e60:82ed:cb2e:d617:f904:f395:aaca:e252]:5555", "");
            assert_eq!(
                parsed.unwrap(),
                (
                    String::from("7e60:82ed:cb2e:d617:f904:f395:aaca:e252"),
                    5555,
                    String::from("false")
                )
            );
        }

        {
            // Only TLS host:port defined
            let parsed = parse_audit_syslog_address("", "tls.host.tld:5556");
            assert_eq!(
                parsed.unwrap(),
                (String::from("tls.host.tld"), 5556, String::from("true"))
            );
        }

        {
            // tls host should take precedence, when both defined
            let parsed = parse_audit_syslog_address("plaintext.host.tld:5555", "tls.host.tld:5556");
            assert_eq!(
                parsed.unwrap(),
                (String::from("tls.host.tld"), 5556, String::from("true"))
            );
        }

        {
            // host without port (plaintext)
            let parsed = parse_audit_syslog_address("collector.host.tld", "");
            assert!(parsed.is_err());
        }

        {
            // port without host
            let parsed = parse_audit_syslog_address(":5555", "");
            assert!(parsed.is_err());
        }

        {
            // valid host with invalid port
            let parsed = parse_audit_syslog_address("collector.host.tld:90001", "");
            assert!(parsed.is_err());
        }

        {
            // invalid hostname with valid port
            let parsed = parse_audit_syslog_address("-collector.host.tld:5555", "");
            assert!(parsed.is_err());
        }

        {
            // parse error
            let parsed = parse_audit_syslog_address("collector.host.tld:::5555", "");
            assert!(parsed.is_err());
        }
    }

    #[test]
    fn test_generate_audit_rsyslog_config() {
        {
            // plaintext version
            let log_directory = "/tmp/log".to_string();
            let endpoint_id = "ep-test-endpoint-id";
            let project_id = "test-project-id";
            let remote_syslog_host = "collector.host.tld";
            let remote_syslog_port = 5555;
            let remote_syslog_tls = "false";

            let conf_str = generate_audit_rsyslog_config(
                log_directory,
                endpoint_id,
                project_id,
                remote_syslog_host,
                remote_syslog_port,
                remote_syslog_tls,
            );

            assert!(conf_str.contains(r#"set $.remote_syslog_tls = "false";"#));
            assert!(conf_str.contains(r#"type="omfwd""#));
            assert!(conf_str.contains(r#"target="collector.host.tld""#));
            assert!(conf_str.contains(r#"port="5555""#));
            assert!(conf_str.contains(r#"StreamDriverPermittedPeers="collector.host.tld""#));
        }

        {
            // TLS version
            let log_directory = "/tmp/log".to_string();
            let endpoint_id = "ep-test-endpoint-id";
            let project_id = "test-project-id";
            let remote_syslog_host = "collector.host.tld";
            let remote_syslog_port = 5556;
            let remote_syslog_tls = "true";

            let conf_str = generate_audit_rsyslog_config(
                log_directory,
                endpoint_id,
                project_id,
                remote_syslog_host,
                remote_syslog_port,
                remote_syslog_tls,
            );

            assert!(conf_str.contains(r#"set $.remote_syslog_tls = "true";"#));
            assert!(conf_str.contains(r#"type="omfwd""#));
            assert!(conf_str.contains(r#"target="collector.host.tld""#));
            assert!(conf_str.contains(r#"port="5556""#));
            assert!(conf_str.contains(r#"StreamDriverPermittedPeers="collector.host.tld""#));
        }
    }
}
