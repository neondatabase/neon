//
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

mod pq_protocol;
pub mod s3_offload;
pub mod wal_service;

use crate::pq_protocol::SystemId;

#[derive(Debug, Clone)]
pub struct WalAcceptorConf {
    pub data_dir: PathBuf,
    pub systemid: SystemId,
    pub daemonize: bool,
    pub no_sync: bool,
    pub listen_addr: SocketAddr,
    pub pageserver_addr: Option<SocketAddr>,
    pub ttl: Option<Duration>,
}
