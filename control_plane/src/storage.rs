use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;
use std::{io, result, thread};

use anyhow::{bail, Context};
use nix::errno::Errno;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use pageserver::http::models::{TenantConfigRequest, TenantCreateRequest, TimelineCreateRequest};
use pageserver::timelines::TimelineInfo;
use postgres::{Config, NoTls};
use reqwest::blocking::{Client, RequestBuilder, Response};
use reqwest::{IntoUrl, Method};
use thiserror::Error;
use utils::{
    connstring::connection_address,
    http::error::HttpErrorBody,
    lsn::Lsn,
    postgres_backend::AuthType,
    zid::{TenantId, ZTimelineId},
};

use crate::local_env::LocalEnv;
use crate::{fill_aws_secrets_vars, fill_rust_env_vars, read_pidfile};
use pageserver::tenant_mgr::TenantInfo;

#[derive(Error, Debug)]
pub enum PageserverHttpError {
    #[error("Reqwest error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("Error: {0}")]
    Response(String),
}

type Result<T> = result::Result<T, PageserverHttpError>;

pub trait ResponseErrorMessageExt: Sized {
    fn error_from_body(self) -> Result<Self>;
}

impl ResponseErrorMessageExt for Response {
    fn error_from_body(self) -> Result<Self> {
        let status = self.status();
        if !(status.is_client_error() || status.is_server_error()) {
            return Ok(self);
        }

        // reqwest do not export it's error construction utility functions, so lets craft the message ourselves
        let url = self.url().to_owned();
        Err(PageserverHttpError::Response(
            match self.json::<HttpErrorBody>() {
                Ok(err_body) => format!("Error: {}", err_body.msg),
                Err(_) => format!("Http error ({}) at {}.", status.as_u16(), url),
            },
        ))
    }
}

//
// Control routines for pageserver.
//
// Used in CLI and tests.
//
#[derive(Debug)]
pub struct PageServerNode {
    pub pg_connection_config: Config,
    pub env: LocalEnv,
    pub http_client: Client,
    pub http_base_url: String,
}

impl PageServerNode {
    pub fn from_env(env: &LocalEnv) -> PageServerNode {
        let password = if env.pageserver.auth_type == AuthType::ZenithJWT {
            &env.pageserver.auth_token
        } else {
            ""
        };

        Self {
            pg_connection_config: Self::pageserver_connection_config(
                password,
                &env.pageserver.listen_pg_addr,
            ),
            env: env.clone(),
            http_client: Client::new(),
            http_base_url: format!("http://{}/v1", env.pageserver.listen_http_addr),
        }
    }

    /// Construct libpq connection string for connecting to the pageserver.
    fn pageserver_connection_config(password: &str, listen_addr: &str) -> Config {
        format!("postgresql://no_user:{}@{}/no_db", password, listen_addr)
            .parse()
            .unwrap()
    }

    pub fn init(
        &self,
        create_tenant: Option<TenantId>,
        initial_timeline_id: Option<ZTimelineId>,
        config_overrides: &[&str],
    ) -> anyhow::Result<ZTimelineId> {
        let mut cmd = Command::new(self.env.pageserver_bin()?);

        let id = format!("id={}", self.env.pageserver.id);

        // FIXME: the paths should be shell-escaped to handle paths with spaces, quotas etc.
        let base_data_dir_param = self.env.base_data_dir.display().to_string();
        let pg_distrib_dir_param =
            format!("pg_distrib_dir='{}'", self.env.pg_distrib_dir.display());
        let authg_type_param = format!("auth_type='{}'", self.env.pageserver.auth_type);
        let listen_http_addr_param = format!(
            "listen_http_addr='{}'",
            self.env.pageserver.listen_http_addr
        );
        let listen_pg_addr_param =
            format!("listen_pg_addr='{}'", self.env.pageserver.listen_pg_addr);
        let broker_endpoints_param = format!(
            "broker_endpoints=[{}]",
            self.env
                .etcd_broker
                .broker_endpoints
                .iter()
                .map(|url| format!("'{url}'"))
                .collect::<Vec<_>>()
                .join(",")
        );
        let mut args = Vec::with_capacity(20);

        args.push("--init");
        args.extend(["-D", &base_data_dir_param]);
        args.extend(["-c", &pg_distrib_dir_param]);
        args.extend(["-c", &authg_type_param]);
        args.extend(["-c", &listen_http_addr_param]);
        args.extend(["-c", &listen_pg_addr_param]);
        args.extend(["-c", &broker_endpoints_param]);
        args.extend(["-c", &id]);

        let broker_etcd_prefix_param = self
            .env
            .etcd_broker
            .broker_etcd_prefix
            .as_ref()
            .map(|prefix| format!("broker_etcd_prefix='{prefix}'"));
        if let Some(broker_etcd_prefix_param) = broker_etcd_prefix_param.as_deref() {
            args.extend(["-c", broker_etcd_prefix_param]);
        }

        for config_override in config_overrides {
            args.extend(["-c", config_override]);
        }

        if self.env.pageserver.auth_type != AuthType::Trust {
            args.extend([
                "-c",
                "auth_validation_public_key_path='auth_public_key.pem'",
            ]);
        }

        let create_tenant = create_tenant.map(|id| id.to_string());
        if let Some(tenant_id) = create_tenant.as_deref() {
            args.extend(["--create-tenant", tenant_id])
        }

        let initial_timeline_id = initial_timeline_id.unwrap_or_else(ZTimelineId::generate);
        let initial_timeline_id_string = initial_timeline_id.to_string();
        args.extend(["--initial-timeline-id", &initial_timeline_id_string]);

        let cmd_with_args = cmd.args(args);
        let init_output = fill_rust_env_vars(cmd_with_args)
            .output()
            .with_context(|| {
                format!("failed to init pageserver with command {:?}", cmd_with_args)
            })?;

        if !init_output.status.success() {
            bail!(
                "init invocation failed, {}\nStdout: {}\nStderr: {}",
                init_output.status,
                String::from_utf8_lossy(&init_output.stdout),
                String::from_utf8_lossy(&init_output.stderr)
            );
        }

        // echo the captured output of the init command
        println!("{}", String::from_utf8_lossy(&init_output.stdout));

        Ok(initial_timeline_id)
    }

    pub fn repo_path(&self) -> PathBuf {
        self.env.pageserver_data_dir()
    }

    pub fn pid_file(&self) -> PathBuf {
        self.repo_path().join("pageserver.pid")
    }

    pub fn start(&self, config_overrides: &[&str]) -> anyhow::Result<()> {
        print!(
            "Starting pageserver at '{}' in '{}'",
            connection_address(&self.pg_connection_config),
            self.repo_path().display()
        );
        io::stdout().flush().unwrap();

        let repo_path = self.repo_path();
        let mut args = vec!["-D", repo_path.to_str().unwrap()];

        for config_override in config_overrides {
            args.extend(["-c", config_override]);
        }

        let mut cmd = Command::new(self.env.pageserver_bin()?);
        let mut filled_cmd = fill_rust_env_vars(cmd.args(&args).arg("--daemonize"));
        filled_cmd = fill_aws_secrets_vars(filled_cmd);

        if !filled_cmd.status()?.success() {
            bail!(
                "Pageserver failed to start. See '{}' for details.",
                self.repo_path().join("pageserver.log").display()
            );
        }

        // It takes a while for the page server to start up. Wait until it is
        // open for business.
        const RETRIES: i8 = 15;
        for retries in 1..RETRIES {
            match self.check_status() {
                Ok(_) => {
                    println!("\nPageserver started");
                    return Ok(());
                }
                Err(err) => {
                    match err {
                        PageserverHttpError::Transport(err) => {
                            if err.is_connect() && retries < 5 {
                                print!(".");
                                io::stdout().flush().unwrap();
                            } else {
                                if retries == 5 {
                                    println!() // put a line break after dots for second message
                                }
                                println!(
                                    "Pageserver not responding yet, err {} retrying ({})...",
                                    err, retries
                                );
                            }
                        }
                        PageserverHttpError::Response(msg) => {
                            bail!("pageserver failed to start: {} ", msg)
                        }
                    }
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
        bail!("pageserver failed to start in {} seconds", RETRIES);
    }

    ///
    /// Stop the server.
    ///
    /// If 'immediate' is true, we use SIGQUIT, killing the process immediately.
    /// Otherwise we use SIGTERM, triggering a clean shutdown
    ///
    /// If the server is not running, returns success
    ///
    pub fn stop(&self, immediate: bool) -> anyhow::Result<()> {
        let pid_file = self.pid_file();
        if !pid_file.exists() {
            println!("Pageserver is already stopped");
            return Ok(());
        }
        let pid = Pid::from_raw(read_pidfile(&pid_file)?);

        let sig = if immediate {
            print!("Stopping pageserver immediately..");
            Signal::SIGQUIT
        } else {
            print!("Stopping pageserver gracefully..");
            Signal::SIGTERM
        };
        io::stdout().flush().unwrap();
        match kill(pid, sig) {
            Ok(_) => (),
            Err(Errno::ESRCH) => {
                println!(
                    "Pageserver with pid {} does not exist, but a PID file was found",
                    pid
                );
                return Ok(());
            }
            Err(err) => bail!(
                "Failed to send signal to pageserver with pid {}: {}",
                pid,
                err.desc()
            ),
        }

        let address = connection_address(&self.pg_connection_config);

        // TODO Remove this "timeout" and handle it on caller side instead.
        // Shutting down may take a long time,
        // if pageserver checkpoints a lot of data
        let mut tcp_stopped = false;
        for _ in 0..100 {
            if !tcp_stopped {
                if let Err(err) = TcpStream::connect(&address) {
                    tcp_stopped = true;
                    if err.kind() != io::ErrorKind::ConnectionRefused {
                        eprintln!("\nPageserver connection failed with error: {err}");
                    }
                }
            }
            if tcp_stopped {
                // Also check status on the HTTP port

                match self.check_status() {
                    Err(PageserverHttpError::Transport(err)) if err.is_connect() => {
                        println!("done!");
                        return Ok(());
                    }
                    Err(err) => {
                        eprintln!("\nPageserver status check failed with error: {err}");
                        return Ok(());
                    }
                    Ok(()) => {
                        // keep waiting
                    }
                }
            }
            print!(".");
            io::stdout().flush().unwrap();
            thread::sleep(Duration::from_secs(1));
        }

        bail!("Failed to stop pageserver with pid {}", pid);
    }

    pub fn page_server_psql(&self, sql: &str) -> Vec<postgres::SimpleQueryMessage> {
        let mut client = self.pg_connection_config.connect(NoTls).unwrap();

        println!("Pageserver query: '{}'", sql);
        client.simple_query(sql).unwrap()
    }

    pub fn page_server_psql_client(&self) -> result::Result<postgres::Client, postgres::Error> {
        self.pg_connection_config.connect(NoTls)
    }

    fn http_request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        let mut builder = self.http_client.request(method, url);
        if self.env.pageserver.auth_type == AuthType::ZenithJWT {
            builder = builder.bearer_auth(&self.env.pageserver.auth_token)
        }
        builder
    }

    pub fn check_status(&self) -> Result<()> {
        self.http_request(Method::GET, format!("{}/status", self.http_base_url))
            .send()?
            .error_from_body()?;
        Ok(())
    }

    pub fn tenant_list(&self) -> Result<Vec<TenantInfo>> {
        Ok(self
            .http_request(Method::GET, format!("{}/tenant", self.http_base_url))
            .send()?
            .error_from_body()?
            .json()?)
    }

    pub fn tenant_create(
        &self,
        new_tenant_id: Option<TenantId>,
        settings: HashMap<&str, &str>,
    ) -> anyhow::Result<Option<TenantId>> {
        let tenant_id_string = self
            .http_request(Method::POST, format!("{}/tenant", self.http_base_url))
            .json(&TenantCreateRequest {
                new_tenant_id,
                checkpoint_distance: settings
                    .get("checkpoint_distance")
                    .map(|x| x.parse::<u64>())
                    .transpose()?,
                compaction_target_size: settings
                    .get("compaction_target_size")
                    .map(|x| x.parse::<u64>())
                    .transpose()?,
                compaction_period: settings.get("compaction_period").map(|x| x.to_string()),
                compaction_threshold: settings
                    .get("compaction_threshold")
                    .map(|x| x.parse::<usize>())
                    .transpose()?,
                gc_horizon: settings
                    .get("gc_horizon")
                    .map(|x| x.parse::<u64>())
                    .transpose()?,
                gc_period: settings.get("gc_period").map(|x| x.to_string()),
                image_creation_threshold: settings
                    .get("image_creation_threshold")
                    .map(|x| x.parse::<usize>())
                    .transpose()?,
                pitr_interval: settings.get("pitr_interval").map(|x| x.to_string()),
            })
            .send()?
            .error_from_body()?
            .json::<Option<String>>()?;

        tenant_id_string
            .map(|id| {
                id.parse().with_context(|| {
                    format!(
                        "Failed to parse tennat creation response as tenant id: {}",
                        id
                    )
                })
            })
            .transpose()
    }

    pub fn tenant_config(&self, tenant_id: TenantId, settings: HashMap<&str, &str>) -> Result<()> {
        self.http_request(Method::PUT, format!("{}/tenant/config", self.http_base_url))
            .json(&TenantConfigRequest {
                tenant_id,
                checkpoint_distance: settings
                    .get("checkpoint_distance")
                    .map(|x| x.parse::<u64>().unwrap()),
                compaction_target_size: settings
                    .get("compaction_target_size")
                    .map(|x| x.parse::<u64>().unwrap()),
                compaction_period: settings.get("compaction_period").map(|x| x.to_string()),
                compaction_threshold: settings
                    .get("compaction_threshold")
                    .map(|x| x.parse::<usize>().unwrap()),
                gc_horizon: settings
                    .get("gc_horizon")
                    .map(|x| x.parse::<u64>().unwrap()),
                gc_period: settings.get("gc_period").map(|x| x.to_string()),
                image_creation_threshold: settings
                    .get("image_creation_threshold")
                    .map(|x| x.parse::<usize>().unwrap()),
                pitr_interval: settings.get("pitr_interval").map(|x| x.to_string()),
            })
            .send()?
            .error_from_body()?;

        Ok(())
    }

    pub fn timeline_list(&self, tenant_id: &TenantId) -> anyhow::Result<Vec<TimelineInfo>> {
        let timeline_infos: Vec<TimelineInfo> = self
            .http_request(
                Method::GET,
                format!("{}/tenant/{}/timeline", self.http_base_url, tenant_id),
            )
            .send()?
            .error_from_body()?
            .json()?;

        Ok(timeline_infos)
    }

    pub fn timeline_create(
        &self,
        tenant_id: TenantId,
        new_timeline_id: Option<ZTimelineId>,
        ancestor_start_lsn: Option<Lsn>,
        ancestor_timeline_id: Option<ZTimelineId>,
    ) -> anyhow::Result<Option<TimelineInfo>> {
        let timeline_info_response = self
            .http_request(
                Method::POST,
                format!("{}/tenant/{}/timeline", self.http_base_url, tenant_id),
            )
            .json(&TimelineCreateRequest {
                new_timeline_id,
                ancestor_start_lsn,
                ancestor_timeline_id,
            })
            .send()?
            .error_from_body()?
            .json::<Option<TimelineInfo>>()?;

        Ok(timeline_info_response)
    }
}
