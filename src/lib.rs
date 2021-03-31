use std::net::SocketAddr;
use std::path::PathBuf;

#[allow(dead_code)]
pub mod control_plane;

pub mod page_cache;
pub mod page_service;
pub mod wal_service;
pub mod restore_s3;
pub mod waldecoder;
pub mod walreceiver;
pub mod walredo;
pub mod tui;
pub mod tui_event;
pub mod pq_protocol;
pub mod xlog_utils;
mod tui_logger;

#[allow(dead_code)]
#[derive(Clone)]
pub struct PageServerConf {
    pub data_dir: PathBuf,
    pub daemonize: bool,
    pub interactive: bool,
    pub wal_producer_connstr: String,
    pub listen_addr: SocketAddr,
    pub skip_recovery: bool,
}

#[allow(dead_code)]
#[derive(Debug,Clone)]
pub struct WalAcceptorConf {
    pub data_dir: PathBuf,
    pub no_sync: bool,
    pub listen_addr: SocketAddr,
}
