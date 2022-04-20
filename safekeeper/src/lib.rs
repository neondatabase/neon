//
use std::path::PathBuf;
use std::time::Duration;
use url::Url;

use utils::zid::{ZNodeId, ZTenantTimelineId};

pub mod broker;
pub mod callmemaybe;
pub mod control_file;
pub mod control_file_upgrade;
pub mod handler;
pub mod http;
pub mod json_ctrl;
pub mod receive_wal;
pub mod s3_offload;
pub mod safekeeper;
pub mod send_wal;
pub mod timeline;
pub mod wal_service;
pub mod wal_storage;

pub mod defaults {
    use const_format::formatcp;
    use std::time::Duration;

    pub const DEFAULT_PG_LISTEN_PORT: u16 = 5454;
    pub const DEFAULT_PG_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_PG_LISTEN_PORT}");

    pub const DEFAULT_HTTP_LISTEN_PORT: u16 = 7676;
    pub const DEFAULT_HTTP_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_HTTP_LISTEN_PORT}");
    pub const DEFAULT_RECALL_PERIOD: Duration = Duration::from_secs(1);
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
    pub ttl: Option<Duration>,
    pub recall_period: Duration,
    pub my_id: ZNodeId,
    pub broker_endpoints: Option<Vec<Url>>,
}

impl SafeKeeperConf {
    pub fn timeline_dir(&self, zttid: &ZTenantTimelineId) -> PathBuf {
        self.workdir
            .join(zttid.tenant_id.to_string())
            .join(zttid.timeline_id.to_string())
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
            listen_pg_addr: defaults::DEFAULT_PG_LISTEN_ADDR.to_string(),
            listen_http_addr: defaults::DEFAULT_HTTP_LISTEN_ADDR.to_string(),
            ttl: None,
            recall_period: defaults::DEFAULT_RECALL_PERIOD,
            my_id: ZNodeId(0),
            broker_endpoints: None,
        }
    }
}
