use std::collections::HashMap;
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Result};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use pageserver::http::models::{BranchCreateRequest, TenantCreateRequest};
use postgres::{Config, NoTls};
use reqwest::blocking::{Client, RequestBuilder};
use reqwest::{IntoUrl, Method, StatusCode};
use zenith_utils::postgres_backend::AuthType;
use zenith_utils::zid::ZTenantId;

use crate::local_env::LocalEnv;
use crate::read_pidfile;
use pageserver::branches::BranchInfo;
use zenith_utils::connstring::connection_address;

const HTTP_BASE_URL: &str = "http://127.0.0.1:9898/v1";

//
// Control routines for pageserver.
//
// Used in CLI and tests.
//
#[derive(Debug)]
pub struct PageServerNode {
    pub kill_on_exit: bool,
    pub pg_connection_config: Config,
    pub env: LocalEnv,
    pub http_client: Client,
    pub http_base_url: String,
}

impl PageServerNode {
    pub fn from_env(env: &LocalEnv) -> PageServerNode {
        let password = if env.auth_type == AuthType::ZenithJWT {
            &env.auth_token
        } else {
            ""
        };

        PageServerNode {
            kill_on_exit: false,
            pg_connection_config: Self::default_config(password), // default
            env: env.clone(),
            http_client: Client::new(),
            http_base_url: HTTP_BASE_URL.to_owned(),
        }
    }

    fn default_config(password: &str) -> Config {
        format!("postgresql://no_user:{}@localhost:64000/no_db", password)
            .parse()
            .unwrap()
    }

    pub fn init(
        &self,
        create_tenant: Option<&str>,
        enable_auth: bool,
        repository_format: Option<&str>,
    ) -> Result<()> {
        let mut cmd = Command::new(self.env.pageserver_bin()?);
        let mut args = vec![
            "--init",
            "-D",
            self.env.base_data_dir.to_str().unwrap(),
            "--postgres-distrib",
            self.env.pg_distrib_dir.to_str().unwrap(),
        ];

        if enable_auth {
            args.extend(&["--auth-validation-public-key-path", "auth_public_key.pem"]);
            args.extend(&["--auth-type", "ZenithJWT"]);
        }

        if let Some(repo_format) = repository_format {
            args.extend(&["--repository-format", repo_format]);
        }

        create_tenant.map(|tenantid| args.extend(&["--create-tenant", tenantid]));
        let status = cmd
            .args(args)
            .env_clear()
            .env("RUST_BACKTRACE", "1")
            .status()
            .expect("pageserver init failed");

        if status.success() {
            Ok(())
        } else {
            Err(anyhow!("pageserver init failed"))
        }
    }

    pub fn repo_path(&self) -> PathBuf {
        self.env.pageserver_data_dir()
    }

    pub fn pid_file(&self) -> PathBuf {
        self.repo_path().join("pageserver.pid")
    }

    pub fn start(&self) -> Result<()> {
        println!(
            "Starting pageserver at '{}' in {}",
            connection_address(&self.pg_connection_config),
            self.repo_path().display()
        );

        let mut cmd = Command::new(self.env.pageserver_bin()?);
        cmd.args(&["-D", self.repo_path().to_str().unwrap()])
            .arg("-d")
            .env_clear()
            .env("RUST_BACKTRACE", "1");

        if !cmd.status()?.success() {
            bail!(
                "Pageserver failed to start. See '{}' for details.",
                self.repo_path().join("pageserver.log").display()
            );
        }

        // It takes a while for the page server to start up. Wait until it is
        // open for business.
        for retries in 1..15 {
            match self.check_status() {
                Ok(_) => {
                    println!("Pageserver started");
                    return Ok(());
                }
                Err(err) => {
                    println!(
                        "Pageserver not responding yet, err {} retrying ({})...",
                        err, retries
                    );
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
        bail!("pageserver failed to start");
    }

    pub fn stop(&self) -> Result<()> {
        let pid = read_pidfile(&self.pid_file())?;
        let pid = Pid::from_raw(pid);
        if kill(pid, Signal::SIGTERM).is_err() {
            bail!("Failed to kill pageserver with pid {}", pid);
        }

        // wait for pageserver stop
        let address = connection_address(&self.pg_connection_config);
        for _ in 0..5 {
            let stream = TcpStream::connect(&address);
            thread::sleep(Duration::from_secs(1));
            if let Err(_e) = stream {
                println!("Pageserver stopped");
                return Ok(());
            }
            println!("Stopping pageserver on {}", address);
        }

        bail!("Failed to stop pageserver with pid {}", pid);
    }

    pub fn page_server_psql(&self, sql: &str) -> Vec<postgres::SimpleQueryMessage> {
        let mut client = self.pg_connection_config.connect(NoTls).unwrap();

        println!("Pageserver query: '{}'", sql);
        client.simple_query(sql).unwrap()
    }

    pub fn page_server_psql_client(&self) -> Result<postgres::Client, postgres::Error> {
        self.pg_connection_config.connect(NoTls)
    }

    fn http_request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        let mut builder = self.http_client.request(method, url);
        if self.env.auth_type == AuthType::ZenithJWT {
            builder = builder.bearer_auth(&self.env.auth_token)
        }
        builder
    }

    pub fn check_status(&self) -> Result<()> {
        let status = self
            .http_request(Method::GET, format!("{}/{}", self.http_base_url, "status"))
            .send()?
            .status();
        ensure!(
            status == StatusCode::OK,
            format!("got unexpected response status {}", status)
        );
        Ok(())
    }

    pub fn tenant_list(&self) -> Result<Vec<String>> {
        Ok(self
            .http_request(Method::GET, format!("{}/{}", self.http_base_url, "tenant"))
            .send()?
            .error_for_status()?
            .json()?)
    }

    pub fn tenant_create(&self, tenantid: ZTenantId) -> Result<()> {
        Ok(self
            .http_request(Method::POST, format!("{}/{}", self.http_base_url, "tenant"))
            .json(&TenantCreateRequest {
                tenant_id: tenantid,
            })
            .send()?
            .error_for_status()?
            .json()?)
    }

    pub fn branch_list(&self, tenantid: &ZTenantId) -> Result<Vec<BranchInfo>> {
        Ok(self
            .http_request(
                Method::GET,
                format!("{}/branch/{}", self.http_base_url, tenantid),
            )
            .send()?
            .error_for_status()?
            .json()?)
    }

    pub fn branch_create(
        &self,
        branch_name: &str,
        startpoint: &str,
        tenantid: &ZTenantId,
    ) -> Result<BranchInfo> {
        Ok(self
            .http_request(Method::POST, format!("{}/{}", self.http_base_url, "branch"))
            .json(&BranchCreateRequest {
                tenant_id: tenantid.to_owned(),
                name: branch_name.to_owned(),
                start_point: startpoint.to_owned(),
            })
            .send()?
            .error_for_status()?
            .json()?)
    }

    // TODO: make this a separate request type and avoid loading all the branches
    pub fn branch_get_by_name(
        &self,
        tenantid: &ZTenantId,
        branch_name: &str,
    ) -> Result<BranchInfo> {
        let branch_infos = self.branch_list(tenantid)?;
        let branch_by_name: Result<HashMap<String, BranchInfo>> = branch_infos
            .into_iter()
            .map(|branch_info| Ok((branch_info.name.clone(), branch_info)))
            .collect();
        let branch_by_name = branch_by_name?;

        let branch = branch_by_name
            .get(branch_name)
            .ok_or_else(|| anyhow!("Branch {} not found", branch_name))?;

        Ok(branch.clone())
    }
}

impl Drop for PageServerNode {
    fn drop(&mut self) {
        if self.kill_on_exit {
            let _ = self.stop();
        }
    }
}
