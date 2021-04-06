//
use std::net::SocketAddr;
use std::path::PathBuf;

pub mod wal_service;
pub mod xlog_utils;
mod pq_protocol;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct WalAcceptorConf {
    pub data_dir: PathBuf,
    pub daemonize: bool,
    pub no_sync: bool,
    pub listen_addr: SocketAddr,
}
