use std::net::SocketAddr;

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
pub mod basebackup;

#[derive(Debug, Clone)]
pub struct PageServerConf {
    pub daemonize: bool,
    pub interactive: bool,
    pub listen_addr: SocketAddr,
}

// Zenith Timeline ID is a 32-byte random ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ZTimelineId([u8; 16]);

impl ZTimelineId {

    pub fn from_str(s: &str) -> Result<ZTimelineId, hex::FromHexError> {
        let timelineid = hex::decode(s)?;

        let mut buf: [u8; 16] = [0u8; 16];
        buf.copy_from_slice(timelineid.as_slice());
        Ok(ZTimelineId(buf))
    }

    pub fn from(b: [u8; 16]) -> ZTimelineId {
        ZTimelineId(b)
    }

    pub fn to_str(self: &ZTimelineId) -> String {
         hex::encode(self.0)
    }
}

impl std::fmt::Display for ZTimelineId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.to_str())
    }
}
