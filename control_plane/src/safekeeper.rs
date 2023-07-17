//! Code to manage safekeepers
//!
//! In the local test environment, the data for each safekeeper is stored in
//!
//! ```text
//!   .neon/safekeepers/<safekeeper id>
//! ```
use std::io::Write;
use std::path::PathBuf;
use std::process::Child;
use std::{io, result};

use anyhow::Context;
use postgres_connection::PgConnectionConfig;
use reqwest::blocking::{Client, RequestBuilder, Response};
use reqwest::{IntoUrl, Method};
use thiserror::Error;
use utils::{http::error::HttpErrorBody, id::NodeId};

use crate::{
    background_process,
    local_env::{LocalEnv, SafekeeperConf},
};

#[derive(Error, Debug)]
pub enum SafekeeperHttpError {
    #[error("Reqwest error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("Error: {0}")]
    Response(String),
}

type Result<T> = result::Result<T, SafekeeperHttpError>;

pub trait ResponseErrorMessageExt: Sized {
    fn error_from_body(self) -> Result<Self>;
}

impl ResponseErrorMessageExt for Response {
    fn error_from_body(self) -> Result<Self> {
        let status = self.status();
        if !(status.is_client_error() || status.is_server_error()) {
            return Ok(self);
        }

        // reqwest does not export its error construction utility functions, so let's craft the message ourselves
        let url = self.url().to_owned();
        Err(SafekeeperHttpError::Response(
            match self.json::<HttpErrorBody>() {
                Ok(err_body) => format!("Error: {}", err_body.msg),
                Err(_) => format!("Http error ({}) at {}.", status.as_u16(), url),
            },
        ))
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
    pub http_client: Client,
    pub http_base_url: String,
}

impl SafekeeperNode {
    pub fn from_env(env: &LocalEnv, conf: &SafekeeperConf) -> SafekeeperNode {
        SafekeeperNode {
            id: conf.id,
            conf: conf.clone(),
            pg_connection_config: Self::safekeeper_connection_config(conf.pg_port),
            env: env.clone(),
            http_client: Client::new(),
            http_base_url: format!("http://127.0.0.1:{}/v1", conf.http_port),
        }
    }

    /// Construct libpq connection string for connecting to this safekeeper.
    fn safekeeper_connection_config(port: u16) -> PgConnectionConfig {
        PgConnectionConfig::new_host_port(url::Host::parse("127.0.0.1").unwrap(), port)
    }

    pub fn datadir_path_by_id(env: &LocalEnv, sk_id: NodeId) -> PathBuf {
        env.safekeeper_data_dir(&format!("sk{sk_id}"))
    }

    pub fn datadir_path(&self) -> PathBuf {
        SafekeeperNode::datadir_path_by_id(&self.env, self.id)
    }

    pub fn pid_file(&self) -> PathBuf {
        self.datadir_path().join("safekeeper.pid")
    }

    pub fn start(&self) -> anyhow::Result<Child> {
        print!(
            "Starting safekeeper at '{}' in '{}'",
            self.pg_connection_config.raw_address(),
            self.datadir_path().display()
        );
        io::stdout().flush().unwrap();

        let listen_pg = format!("127.0.0.1:{}", self.conf.pg_port);
        let listen_http = format!("127.0.0.1:{}", self.conf.http_port);
        let id = self.id;
        let datadir = self.datadir_path();

        let id_string = id.to_string();
        // TODO: add availability_zone to the config.
        // Right now we just specify any value here and use it to check metrics in tests.
        let availability_zone = format!("sk-{}", id_string);

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
            let listen_pg_tenant_only = format!("127.0.0.1:{}", pg_tenant_only_port);
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
            args.extend([
                "--auth-validation-public-key-path".to_owned(),
                key_path
                    .to_str()
                    .with_context(|| {
                        format!("Key path {key_path:?} cannot be represented as a unicode string")
                    })?
                    .to_owned(),
            ]);
        }

        background_process::start_process(
            &format!("safekeeper-{id}"),
            &datadir,
            &self.env.safekeeper_bin(),
            &args,
            [],
            background_process::InitialPidFile::Expect(&self.pid_file()),
            || match self.check_status() {
                Ok(()) => Ok(true),
                Err(SafekeeperHttpError::Transport(_)) => Ok(false),
                Err(e) => Err(anyhow::anyhow!("Failed to check node status: {e}")),
            },
        )
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

    fn http_request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        // TODO: authentication
        //if self.env.auth_type == AuthType::NeonJWT {
        //    builder = builder.bearer_auth(&self.env.safekeeper_auth_token)
        //}
        self.http_client.request(method, url)
    }

    pub fn check_status(&self) -> Result<()> {
        self.http_request(Method::GET, format!("{}/{}", self.http_base_url, "status"))
            .send()?
            .error_from_body()?;
        Ok(())
    }
}
