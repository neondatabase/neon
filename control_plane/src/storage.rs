use std::io::Write;
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;
use std::{io, result, thread};

use anyhow::bail;
use nix::errno::Errno;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use pageserver::http::models::{BranchCreateRequest, TenantCreateRequest};
use postgres::{Config, NoTls};
use reqwest::blocking::{Client, RequestBuilder, Response};
use reqwest::{IntoUrl, Method};
use thiserror::Error;
use zenith_utils::http::error::HttpErrorBody;
use zenith_utils::postgres_backend::AuthType;
use zenith_utils::zid::ZTenantId;

use crate::local_env::LocalEnv;
use crate::read_pidfile;
use pageserver::branches::BranchInfo;
use pageserver::tenant_mgr::TenantInfo;
use zenith_utils::connstring::connection_address;

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

    pub fn init(&self, create_tenant: Option<&str>) -> anyhow::Result<()> {
        let mut cmd = Command::new(self.env.pageserver_bin()?);
        let var = "LLVM_PROFILE_FILE";
        if let Some(val) = std::env::var_os(var) {
            cmd.env(var, val);
        }

        // FIXME: the paths should be shell-escaped to handle paths with spaces, quotas etc.
        let mut args = vec![
            "--init".to_string(),
            "-D".to_string(),
            self.env.base_data_dir.display().to_string(),
            "-c".to_string(),
            format!("pg_distrib_dir='{}'", self.env.pg_distrib_dir.display()),
            "-c".to_string(),
            format!("auth_type='{}'", self.env.pageserver.auth_type),
            "-c".to_string(),
            format!(
                "listen_http_addr='{}'",
                self.env.pageserver.listen_http_addr
            ),
            "-c".to_string(),
            format!("listen_pg_addr='{}'", self.env.pageserver.listen_pg_addr),
        ];

        if self.env.pageserver.auth_type != AuthType::Trust {
            args.extend([
                "-c".to_string(),
                "auth_validation_public_key_path='auth_public_key.pem'".to_string(),
            ]);
        }

        if let Some(tenantid) = create_tenant {
            args.extend(["--create-tenant".to_string(), tenantid.to_string()])
        }

        let status = cmd
            .args(args)
            .env_clear()
            .env("RUST_BACKTRACE", "1")
            .status()
            .expect("pageserver init failed");

        if !status.success() {
            bail!("pageserver init failed");
        }

        Ok(())
    }

    pub fn repo_path(&self) -> PathBuf {
        self.env.pageserver_data_dir()
    }

    pub fn pid_file(&self) -> PathBuf {
        self.repo_path().join("pageserver.pid")
    }

    pub fn start(&self) -> anyhow::Result<()> {
        print!(
            "Starting pageserver at '{}' in '{}'",
            connection_address(&self.pg_connection_config),
            self.repo_path().display()
        );
        io::stdout().flush().unwrap();

        let mut cmd = Command::new(self.env.pageserver_bin()?);
        cmd.args(&["-D", self.repo_path().to_str().unwrap()])
            .arg("--daemonize")
            .env_clear()
            .env("RUST_BACKTRACE", "1");

        let var = "LLVM_PROFILE_FILE";
        if let Some(val) = std::env::var_os(var) {
            cmd.env(var, val);
        }

        if !cmd.status()?.success() {
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
            println!("Stop pageserver immediately");
            Signal::SIGQUIT
        } else {
            println!("Stop pageserver gracefully");
            Signal::SIGTERM
        };
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
        for _ in 0..100 {
            if let Err(_e) = TcpStream::connect(&address) {
                println!("Pageserver stopped receiving connections");

                //Now check status
                match self.check_status() {
                    Ok(_) => {
                        println!("Pageserver status is OK. Wait a bit.");
                        thread::sleep(Duration::from_secs(1));
                    }
                    Err(err) => {
                        println!("Pageserver status is: {}", err);
                        return Ok(());
                    }
                }
            } else {
                println!("Pageserver still receives connections");
                thread::sleep(Duration::from_secs(1));
            }
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
        self.http_request(Method::GET, format!("{}/{}", self.http_base_url, "status"))
            .send()?
            .error_from_body()?;
        Ok(())
    }

    pub fn tenant_list(&self) -> Result<Vec<TenantInfo>> {
        Ok(self
            .http_request(Method::GET, format!("{}/{}", self.http_base_url, "tenant"))
            .send()?
            .error_from_body()?
            .json()?)
    }

    pub fn tenant_create(&self, tenantid: ZTenantId) -> Result<()> {
        Ok(self
            .http_request(Method::POST, format!("{}/{}", self.http_base_url, "tenant"))
            .json(&TenantCreateRequest {
                tenant_id: tenantid,
            })
            .send()?
            .error_from_body()?
            .json()?)
    }

    pub fn branch_list(&self, tenantid: &ZTenantId) -> Result<Vec<BranchInfo>> {
        Ok(self
            .http_request(
                Method::GET,
                format!("{}/branch/{}", self.http_base_url, tenantid),
            )
            .send()?
            .error_from_body()?
            .json()?)
    }

    pub fn branch_create(
        &self,
        branch_name: &str,
        startpoint: &str,
        tenantid: &ZTenantId,
    ) -> Result<BranchInfo> {
        Ok(self
            .http_request(Method::POST, format!("{}/branch", self.http_base_url))
            .json(&BranchCreateRequest {
                tenant_id: tenantid.to_owned(),
                name: branch_name.to_owned(),
                start_point: startpoint.to_owned(),
            })
            .send()?
            .error_from_body()?
            .json()?)
    }

    pub fn branch_get_by_name(
        &self,
        tenantid: &ZTenantId,
        branch_name: &str,
    ) -> Result<BranchInfo> {
        Ok(self
            .http_request(
                Method::GET,
                format!("{}/branch/{}/{}", self.http_base_url, tenantid, branch_name),
            )
            .send()?
            .error_for_status()?
            .json()?)
    }
}
