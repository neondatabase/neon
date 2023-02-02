use remote_storage::RemoteStorageConfig;
use std::path::PathBuf;
use std::time::Duration;
use storage_broker::Uri;

use utils::id::{NodeId, TenantId, TenantTimelineId};

mod auth;
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

mod timelines_global_map;
use std::sync::Arc;
pub use timelines_global_map::GlobalTimelines;
use utils::auth::JwtAuth;

pub mod defaults {
    pub use safekeeper_api::{
        DEFAULT_HTTP_LISTEN_ADDR, DEFAULT_HTTP_LISTEN_PORT, DEFAULT_PG_LISTEN_ADDR,
        DEFAULT_PG_LISTEN_PORT,
    };

    pub const DEFAULT_WAL_BACKUP_RUNTIME_THREADS: usize = 8;
    pub const DEFAULT_HEARTBEAT_TIMEOUT: &str = "5000ms";
    pub const DEFAULT_MAX_OFFLOADER_LAG_BYTES: u64 = 128 * (1 << 20);
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
    pub my_id: NodeId,
    pub listen_pg_addr: String,
    pub listen_http_addr: String,
    pub no_sync: bool,
    pub broker_endpoint: Uri,
    pub broker_keepalive_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub remote_storage: Option<RemoteStorageConfig>,
    pub max_offloader_lag_bytes: u64,
    pub backup_runtime_threads: Option<usize>,
    pub wal_backup_enabled: bool,
    pub auth: Option<Arc<JwtAuth>>,
}

impl SafeKeeperConf {
    pub fn tenant_dir(&self, tenant_id: &TenantId) -> PathBuf {
        self.workdir.join(tenant_id.to_string())
    }

    pub fn timeline_dir(&self, ttid: &TenantTimelineId) -> PathBuf {
        self.tenant_dir(&ttid.tenant_id)
            .join(ttid.timeline_id.to_string())
    }
}

impl SafeKeeperConf {
    #[cfg(test)]
    fn dummy() -> Self {
        SafeKeeperConf {
            workdir: PathBuf::from("./"),
            no_sync: false,
            listen_pg_addr: defaults::DEFAULT_PG_LISTEN_ADDR.to_string(),
            listen_http_addr: defaults::DEFAULT_HTTP_LISTEN_ADDR.to_string(),
            remote_storage: None,
            my_id: NodeId(0),
            broker_endpoint: storage_broker::DEFAULT_ENDPOINT
                .parse()
                .expect("failed to parse default broker endpoint"),
            broker_keepalive_interval: Duration::from_secs(5),
            backup_runtime_threads: None,
            wal_backup_enabled: true,
            auth: None,
            heartbeat_timeout: Duration::new(5, 0),
            max_offloader_lag_bytes: defaults::DEFAULT_MAX_OFFLOADER_LAG_BYTES,
        }
    }
}
