use defaults::DEFAULT_WAL_BACKUP_RUNTIME_THREADS;
//
use remote_storage::RemoteStorageConfig;
use std::path::PathBuf;
use std::time::Duration;
use url::Url;

use utils::zid::{NodeId, TenantId, ZTenantTimelineId};

pub mod broker;
pub mod control_file;
pub mod control_file_upgrade;
pub mod handler;
pub mod http;
pub mod json_ctrl;
pub mod metrics;
pub mod receive_wal;
pub mod remove_wal;
pub mod safekeeper;
pub mod send_wal;
pub mod timeline;
pub mod wal_backup;
pub mod wal_service;
pub mod wal_storage;

pub mod defaults {
    use const_format::formatcp;
    use std::time::Duration;

    pub const DEFAULT_PG_LISTEN_PORT: u16 = 5454;
    pub const DEFAULT_PG_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_PG_LISTEN_PORT}");

    pub const DEFAULT_HTTP_LISTEN_PORT: u16 = 7676;
    pub const DEFAULT_HTTP_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_HTTP_LISTEN_PORT}");
    pub const DEFAULT_RECALL_PERIOD: Duration = Duration::from_secs(10);
    pub const DEFAULT_WAL_BACKUP_RUNTIME_THREADS: usize = 8;
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
    pub recall_period: Duration,
    pub remote_storage: Option<RemoteStorageConfig>,
    pub backup_runtime_threads: usize,
    pub wal_backup_enabled: bool,
    pub my_id: NodeId,
    pub broker_endpoints: Vec<Url>,
    pub broker_etcd_prefix: String,
}

impl SafeKeeperConf {
    pub fn tenant_dir(&self, tenant_id: &TenantId) -> PathBuf {
        self.workdir.join(tenant_id.to_string())
    }

    pub fn timeline_dir(&self, zttid: &ZTenantTimelineId) -> PathBuf {
        self.tenant_dir(&zttid.tenant_id)
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
            remote_storage: None,
            recall_period: defaults::DEFAULT_RECALL_PERIOD,
            my_id: NodeId(0),
            broker_endpoints: Vec::new(),
            broker_etcd_prefix: etcd_broker::DEFAULT_NEON_BROKER_ETCD_PREFIX.to_string(),
            backup_runtime_threads: DEFAULT_WAL_BACKUP_RUNTIME_THREADS,
            wal_backup_enabled: true,
        }
    }
}
