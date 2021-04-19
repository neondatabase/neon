//
use std::net::SocketAddr;
use std::path::PathBuf;

mod pq_protocol;
pub mod wal_service;
pub mod xlog_utils;

#[derive(Debug, Clone)]
pub struct WalAcceptorConf {
    pub data_dir: PathBuf,
    pub daemonize: bool,
    pub no_sync: bool,
    pub listen_addr: SocketAddr,
    pub pageserver_addr: Option<SocketAddr>,
}
