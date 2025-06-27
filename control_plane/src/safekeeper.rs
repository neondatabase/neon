//! Code to manage safekeepers
//!
//! In the local test environment, the data for each safekeeper is stored in
//!
//! ```text
//!   .neon/safekeepers/<safekeeper id>
//! ```
use std::error::Error as _;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use std::{io, result};

use anyhow::Context;
use camino::Utf8PathBuf;
use postgres_connection::PgConnectionConfig;
use safekeeper_api::models::TimelineCreateRequest;
use safekeeper_client::mgmt_api;
use thiserror::Error;
use utils::auth::{Claims, Scope};
use utils::id::NodeId;

use crate::background_process;
use crate::local_env::{LocalEnv, SafekeeperConf};

#[derive(Error, Debug)]
pub enum SafekeeperHttpError {
    #[error("request error: {0}{}", .0.source().map(|e| format!(": {e}")).unwrap_or_default())]
    Transport(#[from] reqwest::Error),

    #[error("Error: {0}")]
    Response(String),
}

type Result<T> = result::Result<T, SafekeeperHttpError>;

fn err_from_client_err(err: mgmt_api::Error) -> SafekeeperHttpError {
    use mgmt_api::Error::*;
    match err {
        ApiError(_, str) => SafekeeperHttpError::Response(str),
        Cancelled => SafekeeperHttpError::Response("Cancelled".to_owned()),
        ReceiveBody(err) => SafekeeperHttpError::Transport(err),
        ReceiveErrorBody(err) => SafekeeperHttpError::Response(err),
        Timeout(str) => SafekeeperHttpError::Response(format!("timeout: {str}")),
    }
}

//
// Control routines for safekeeper.
//
// Used in CLI and tests.
//
#[derive(Debug)]
pub struct SafekeeperNode {
    pub id: NodeId,

    pub conf: SafekeeperConf,

    pub pg_connection_config: PgConnectionConfig,
    pub env: LocalEnv,
    pub http_client: mgmt_api::Client,
    pub listen_addr: String,
}

impl SafekeeperNode {
    pub fn from_env(env: &LocalEnv, conf: &SafekeeperConf) -> SafekeeperNode {
        let listen_addr = if let Some(ref listen_addr) = conf.listen_addr {
            listen_addr.clone()
        } else {
            "127.0.0.1".to_string()
        };
        let jwt = None;
        let http_base_url = format!("http://{}:{}", listen_addr, conf.http_port);
        SafekeeperNode {
            id: conf.id,
            conf: conf.clone(),
            pg_connection_config: Self::safekeeper_connection_config(&listen_addr, conf.pg_port),
            env: env.clone(),
            http_client: mgmt_api::Client::new(env.create_http_client(), http_base_url, jwt),
            listen_addr,
        }
    }

    /// Construct libpq connection string for connecting to this safekeeper.
    fn safekeeper_connection_config(addr: &str, port: u16) -> PgConnectionConfig {
        PgConnectionConfig::new_host_port(url::Host::parse(addr).unwrap(), port)
    }

    pub fn datadir_path_by_id(env: &LocalEnv, sk_id: NodeId) -> PathBuf {
        env.safekeeper_data_dir(&format!("sk{sk_id}"))
    }

    pub fn datadir_path(&self) -> PathBuf {
        SafekeeperNode::datadir_path_by_id(&self.env, self.id)
    }

    pub fn pid_file(&self) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(self.datadir_path().join("safekeeper.pid"))
            .expect("non-Unicode path")
    }

    /// Initializes a safekeeper node by creating all necessary files,
    /// e.g. SSL certificates and JWT token file.
    pub fn initialize(&self) -> anyhow::Result<()> {
        if self.env.generate_local_ssl_certs {
            self.env.generate_ssl_cert(
                &self.datadir_path().join("server.crt"),
                &self.datadir_path().join("server.key"),
            )?;
        }

        // Generate a token file for authentication with other safekeepers
        if self.conf.auth_enabled {
            let token = self
                .env
                .generate_auth_token(&Claims::new(None, Scope::SafekeeperData))?;

            let token_path = self.datadir_path().join("peer_jwt_token");
            std::fs::write(token_path, token)?;
        }

        Ok(())
    }

    pub async fn start(
        &self,
        extra_opts: &[String],
        retry_timeout: &Duration,
    ) -> anyhow::Result<()> {
        print!(
            "Starting safekeeper at '{}' in '{}', retrying for {:?}",
            self.pg_connection_config.raw_address(),
            self.datadir_path().display(),
            retry_timeout,
        );
        io::stdout().flush().unwrap();

        let listen_pg = format!("{}:{}", self.listen_addr, self.conf.pg_port);
        let listen_http = format!("{}:{}", self.listen_addr, self.conf.http_port);
        let id = self.id;
        let datadir = self.datadir_path();

        let id_string = id.to_string();
        // TODO: add availability_zone to the config.
        // Right now we just specify any value here and use it to check metrics in tests.
        let availability_zone = format!("sk-{id_string}");

        let mut args = vec![
            "-D".to_owned(),
            datadir
                .to_str()
                .with_context(|| {
                    format!("Datadir path {datadir:?} cannot be represented as a unicode string")
                })?
                .to_owned(),
            "--id".to_owned(),
            id_string,
            "--listen-pg".to_owned(),
            listen_pg,
            "--listen-http".to_owned(),
            listen_http,
            "--availability-zone".to_owned(),
            availability_zone,
        ];
        if let Some(pg_tenant_only_port) = self.conf.pg_tenant_only_port {
            let listen_pg_tenant_only = format!("{}:{}", self.listen_addr, pg_tenant_only_port);
            args.extend(["--listen-pg-tenant-only".to_owned(), listen_pg_tenant_only]);
        }
        if !self.conf.sync {
            args.push("--no-sync".to_owned());
        }

        let broker_endpoint = format!("{}", self.env.broker.client_url());
        args.extend(["--broker-endpoint".to_owned(), broker_endpoint]);

        let mut backup_threads = String::new();
        if let Some(threads) = self.conf.backup_threads {
            backup_threads = threads.to_string();
            args.extend(["--backup-threads".to_owned(), backup_threads]);
        } else {
            drop(backup_threads);
        }

        if let Some(ref remote_storage) = self.conf.remote_storage {
            args.extend(["--remote-storage".to_owned(), remote_storage.clone()]);
        }

        let key_path = self.env.base_data_dir.join("auth_public_key.pem");
        if self.conf.auth_enabled {
            let key_path_string = key_path
                .to_str()
                .with_context(|| {
                    format!("Key path {key_path:?} cannot be represented as a unicode string")
                })?
                .to_owned();
            args.extend([
                "--pg-auth-public-key-path".to_owned(),
                key_path_string.clone(),
            ]);
            args.extend([
                "--pg-tenant-only-auth-public-key-path".to_owned(),
                key_path_string.clone(),
            ]);
            args.extend([
                "--http-auth-public-key-path".to_owned(),
                key_path_string.clone(),
            ]);
        }

        if let Some(https_port) = self.conf.https_port {
            args.extend([
                "--listen-https".to_owned(),
                format!("{}:{}", self.listen_addr, https_port),
            ]);
        }
        if let Some(ssl_ca_file) = self.env.ssl_ca_cert_path() {
            args.push(format!("--ssl-ca-file={}", ssl_ca_file.to_str().unwrap()));
        }

        if self.conf.auth_enabled {
            let token_path = self.datadir_path().join("peer_jwt_token");
            let token_path_str = token_path
                .to_str()
                .with_context(|| {
                    format!("Token path {token_path:?} cannot be represented as a unicode string")
                })?
                .to_owned();
            args.extend(["--auth-token-path".to_owned(), token_path_str]);
        }

        args.extend_from_slice(extra_opts);

        let env_variables = Vec::new();
        background_process::start_process(
            &format!("safekeeper-{id}"),
            &datadir,
            &self.env.safekeeper_bin(),
            &args,
            env_variables,
            background_process::InitialPidFile::Expect(self.pid_file()),
            retry_timeout,
            || async {
                match self.check_status().await {
                    Ok(()) => Ok(true),
                    Err(SafekeeperHttpError::Transport(_)) => Ok(false),
                    Err(e) => Err(anyhow::anyhow!("Failed to check node status: {e}")),
                }
            },
        )
        .await
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
        background_process::stop_process(
            immediate,
            &format!("safekeeper {}", self.id),
            &self.pid_file(),
        )
    }

    pub async fn check_status(&self) -> Result<()> {
        self.http_client
            .status()
            .await
            .map_err(err_from_client_err)?;
        Ok(())
    }

    pub async fn create_timeline(&self, req: &TimelineCreateRequest) -> Result<()> {
        self.http_client
            .create_timeline(req)
            .await
            .map_err(err_from_client_err)?;
        Ok(())
    }
}
