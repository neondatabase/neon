use std::io::Write;
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use std::{io, result, thread};

use anyhow::bail;
use nix::errno::Errno;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use postgres::Config;
use reqwest::blocking::{Client, RequestBuilder, Response};
use reqwest::{IntoUrl, Method};
use thiserror::Error;
use zenith_utils::http::error::HttpErrorBody;
use zenith_utils::zid::ZNodeId;

use crate::local_env::{LocalEnv, SafekeeperConf};
use crate::storage::PageServerNode;
use crate::{fill_rust_env_vars, read_pidfile};
use zenith_utils::connstring::connection_address;

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

        // reqwest do not export it's error construction utility functions, so lets craft the message ourselves
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
    pub id: ZNodeId,

    pub conf: SafekeeperConf,

    pub pg_connection_config: Config,
    pub env: LocalEnv,
    pub http_client: Client,
    pub http_base_url: String,

    pub pageserver: Arc<PageServerNode>,
}

impl SafekeeperNode {
    pub fn from_env(env: &LocalEnv, conf: &SafekeeperConf) -> SafekeeperNode {
        let pageserver = Arc::new(PageServerNode::from_env(env));

        println!("initializing for sk {} for {}", conf.id, conf.http_port);

        SafekeeperNode {
            id: conf.id,
            conf: conf.clone(),
            pg_connection_config: Self::safekeeper_connection_config(conf.pg_port),
            env: env.clone(),
            http_client: Client::new(),
            http_base_url: format!("http://127.0.0.1:{}/v1", conf.http_port),
            pageserver,
        }
    }

    /// Construct libpq connection string for connecting to this safekeeper.
    fn safekeeper_connection_config(port: u16) -> Config {
        // TODO safekeeper authentication not implemented yet
        format!("postgresql://no_user@127.0.0.1:{}/no_db", port)
            .parse()
            .unwrap()
    }

    pub fn datadir_path_by_id(env: &LocalEnv, sk_id: ZNodeId) -> PathBuf {
        env.safekeeper_data_dir(format!("sk{}", sk_id).as_ref())
    }

    pub fn datadir_path(&self) -> PathBuf {
        SafekeeperNode::datadir_path_by_id(&self.env, self.id)
    }

    pub fn pid_file(&self) -> PathBuf {
        self.datadir_path().join("safekeeper.pid")
    }

    pub fn start(&self) -> anyhow::Result<()> {
        print!(
            "Starting safekeeper at '{}' in '{}'",
            connection_address(&self.pg_connection_config),
            self.datadir_path().display()
        );
        io::stdout().flush().unwrap();

        let listen_pg = format!("127.0.0.1:{}", self.conf.pg_port);
        let listen_http = format!("127.0.0.1:{}", self.conf.http_port);

        let mut cmd = Command::new(self.env.safekeeper_bin()?);
        fill_rust_env_vars(
            cmd.args(&["-D", self.datadir_path().to_str().unwrap()])
                .args(&["--id", self.id.to_string().as_ref()])
                .args(&["--listen-pg", &listen_pg])
                .args(&["--listen-http", &listen_http])
                .args(&["--recall", "1 second"])
                .arg("--daemonize"),
        );
        if !self.conf.sync {
            cmd.arg("--no-sync");
        }

        if !cmd.status()?.success() {
            bail!(
                "Safekeeper failed to start. See '{}' for details.",
                self.datadir_path().join("safekeeper.log").display()
            );
        }

        // It takes a while for the safekeeper to start up. Wait until it is
        // open for business.
        const RETRIES: i8 = 15;
        for retries in 1..RETRIES {
            match self.check_status() {
                Ok(_) => {
                    println!("\nSafekeeper started");
                    return Ok(());
                }
                Err(err) => {
                    match err {
                        SafekeeperHttpError::Transport(err) => {
                            if err.is_connect() && retries < 5 {
                                print!(".");
                                io::stdout().flush().unwrap();
                            } else {
                                if retries == 5 {
                                    println!() // put a line break after dots for second message
                                }
                                println!(
                                    "Safekeeper not responding yet, err {} retrying ({})...",
                                    err, retries
                                );
                            }
                        }
                        SafekeeperHttpError::Response(msg) => {
                            bail!("safekeeper failed to start: {} ", msg)
                        }
                    }
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
        bail!("safekeeper failed to start in {} seconds", RETRIES);
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
            println!("Safekeeper {} is already stopped", self.id);
            return Ok(());
        }
        let pid = read_pidfile(&pid_file)?;
        let pid = Pid::from_raw(pid);

        let sig = if immediate {
            println!("Stop safekeeper immediately");
            Signal::SIGQUIT
        } else {
            println!("Stop safekeeper gracefully");
            Signal::SIGTERM
        };
        match kill(pid, sig) {
            Ok(_) => (),
            Err(Errno::ESRCH) => {
                println!(
                    "Safekeeper with pid {} does not exist, but a PID file was found",
                    pid
                );
                return Ok(());
            }
            Err(err) => bail!(
                "Failed to send signal to safekeeper with pid {}: {}",
                pid,
                err.desc()
            ),
        }

        let address = connection_address(&self.pg_connection_config);

        // TODO Remove this "timeout" and handle it on caller side instead.
        // Shutting down may take a long time,
        // if safekeeper flushes a lot of data
        for _ in 0..100 {
            if let Err(_e) = TcpStream::connect(&address) {
                println!("Safekeeper stopped receiving connections");

                //Now check status
                match self.check_status() {
                    Ok(_) => {
                        println!("Safekeeper status is OK. Wait a bit.");
                        thread::sleep(Duration::from_secs(1));
                    }
                    Err(err) => {
                        println!("Safekeeper status is: {}", err);
                        return Ok(());
                    }
                }
            } else {
                println!("Safekeeper still receives connections");
                thread::sleep(Duration::from_secs(1));
            }
        }

        bail!("Failed to stop safekeeper with pid {}", pid);
    }

    fn http_request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        // TODO: authentication
        //if self.env.auth_type == AuthType::ZenithJWT {
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
