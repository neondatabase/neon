use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

pub mod basebackup;
pub mod page_cache;
pub mod page_service;
pub mod pg_constants;
pub mod restore_local_repo;
pub mod tui;
pub mod tui_event;
mod tui_logger;
pub mod waldecoder;
pub mod walreceiver;
pub mod walredo;

#[derive(Debug, Clone)]
pub struct PageServerConf {
    pub daemonize: bool,
    pub interactive: bool,
    pub listen_addr: SocketAddr,
    pub gc_horizon: u64,
    pub gc_period: Duration,
}

// Zenith Timeline ID is a 32-byte random ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ZTimelineId([u8; 16]);

impl FromStr for ZTimelineId {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<ZTimelineId, Self::Err> {
        let timelineid = hex::decode(s)?;

        let mut buf: [u8; 16] = [0u8; 16];
        buf.copy_from_slice(timelineid.as_slice());
        Ok(ZTimelineId(buf))
    }
}

impl ZTimelineId {
    pub fn from(b: [u8; 16]) -> ZTimelineId {
        ZTimelineId(b)
    }

    pub fn get_from_buf(buf: &mut dyn bytes::Buf) -> ZTimelineId {
        let mut arr = [0u8; 16];
        buf.copy_to_slice(&mut arr);
        ZTimelineId::from(arr)
    }

    pub fn as_arr(&self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Display for ZTimelineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

pub fn zenith_repo_dir() -> PathBuf {
    // Find repository path
    match std::env::var_os("ZENITH_REPO_DIR") {
        Some(val) => PathBuf::from(val.to_str().unwrap()),
        None => ".zenith".into(),
    }
}
