//
use std::path::PathBuf;
use std::time::Duration;

use std::env;
use zenith_utils::zid::ZTimelineId;

pub mod http;
pub mod json_ctrl;
pub mod receive_wal;
pub mod replication;
pub mod s3_offload;
pub mod safekeeper;
pub mod send_wal;
pub mod timeline;
pub mod upgrade;
pub mod wal_service;

pub mod defaults {
    use const_format::formatcp;

    pub const DEFAULT_PG_LISTEN_PORT: u16 = 5454;
    pub const DEFAULT_PG_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_PG_LISTEN_PORT}");

    pub const DEFAULT_HTTP_LISTEN_PORT: u16 = 7676;
    pub const DEFAULT_HTTP_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_HTTP_LISTEN_PORT}");
}

#[derive(Debug, Clone)]
pub struct SafeKeeperConf {
    // Repository directory, relative to current working directory.
    // Normally, the safekeeper changes the current working directory
    // to the repository, and 'workdir' is always '.'. But we don't do
    // that during unit testing, because the current directory is global
    // to the process but different unit tests work on different
    // data directories to avoid clashing with each other.
    pub workdir: PathBuf,

    pub daemonize: bool,
    pub no_sync: bool,
    pub listen_pg_addr: String,
    pub listen_http_addr: String,
    pub pageserver_addr: Option<String>,
    // TODO (create issue) this is temporary, until protocol between PG<->SK<->PS rework
    pub pageserver_auth_token: Option<String>,
    pub ttl: Option<Duration>,
    pub recall_period: Option<Duration>,
}

impl SafeKeeperConf {
    pub fn timeline_dir(&self, timelineid: &ZTimelineId) -> PathBuf {
        self.workdir.join(timelineid.to_string())
    }
}

impl Default for SafeKeeperConf {
    fn default() -> Self {
        SafeKeeperConf {
            // Always set to './'. We will chdir into the directory specified on the
            // command line, so that when the server is running, all paths are relative
            // to that.
            workdir: PathBuf::from("./"),
            daemonize: false,
            no_sync: false,
            pageserver_addr: None,
            listen_pg_addr: defaults::DEFAULT_PG_LISTEN_ADDR.to_string(),
            listen_http_addr: defaults::DEFAULT_PG_LISTEN_ADDR.to_string(),
            ttl: None,
            recall_period: None,
            pageserver_auth_token: env::var("PAGESERVER_AUTH_TOKEN").ok(),
        }
    }
}
