//
use std::path::PathBuf;
use std::time::Duration;

pub mod http;
pub mod json_ctrl;
pub mod receive_wal;
pub mod replication;
pub mod s3_offload;
pub mod safekeeper;
pub mod send_wal;
pub mod timeline;
pub mod wal_service;

pub mod defaults {
    use const_format::formatcp;

    pub const DEFAULT_PG_LISTEN_PORT: u16 = 5454;
    pub const DEFAULT_PG_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_PG_LISTEN_PORT}");

    pub const DEFAULT_HTTP_LISTEN_PORT: u16 = 7676;
    pub const DEFAULT_HTTP_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_HTTP_LISTEN_PORT}");
}

#[derive(Debug, Clone)]
pub struct WalAcceptorConf {
    pub data_dir: PathBuf,
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
