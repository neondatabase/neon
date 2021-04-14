use std::net::SocketAddr;
use std::path::PathBuf;

pub mod page_cache;
pub mod page_service;
pub mod restore_datadir;
pub mod restore_s3;
pub mod tui;
pub mod tui_event;
mod tui_logger;
pub mod waldecoder;
pub mod walreceiver;
pub mod walredo;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PageServerConf {
    pub data_dir: PathBuf,
    pub daemonize: bool,
    pub interactive: bool,
    pub wal_producer_connstr: Option<String>,
    pub listen_addr: SocketAddr,
    pub restore_from: String,
}
