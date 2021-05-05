use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

pub mod basebackup;
pub mod page_cache;
pub mod page_service;
pub mod repository;
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

/// Zenith Timeline ID is a 128-bit random ID.
///
/// Zenith timeline IDs are different from PostgreSQL timeline
/// IDs. They serve a similar purpose though: they differentiate
/// between different "histories" of the same cluster.  However,
/// PostgreSQL timeline IDs are a bit cumbersome, because they are only
/// 32-bits wide, and they must be in ascending order in any given
/// timeline history.  Those limitations mean that we cannot generate a
/// new PostgreSQL timeline ID by just generating a random number. And
/// that in turn is problematic for the "pull/push" workflow, where you
/// have a local copy of a zenith repository, and you periodically sync
/// the local changes with a remote server. When you work "detached"
/// from the remote server, you cannot create a PostgreSQL timeline ID
/// that's guaranteed to be different from all existing timelines in
/// the remote server. For example, if two people are having a clone of
/// the repository on their laptops, and they both create a new branch
/// with different name. What timeline ID would they assign to their
/// branches? If they pick the same one, and later try to push the
/// branches to the same remote server, they will get mixed up.
///
/// To avoid those issues, Zenith has its own concept of timelines that
/// is separate from PostgreSQL timelines, and doesn't have those
/// limitations. A zenith timeline is identified by a 128-bit ID, which
/// is usually printed out as a hex string.
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
