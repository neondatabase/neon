use std::net::IpAddr;

#[allow(dead_code)]
pub mod control_plane;

pub mod page_cache;
pub mod page_service;
pub mod restore_s3;
pub mod waldecoder;
pub mod walreceiver;
pub mod walredo;

pub struct PageServerConf {
    pub data_dir: String,
    pub daemonize: bool,
    pub wal_producer_ip: IpAddr,
    pub wal_producer_port: u32,
    pub skip_recovery: bool,
}
