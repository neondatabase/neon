use hex::FromHex;
use hex::FromHexError;
use rand::Rng;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use zenith_utils::zid::ZTimelineId;

pub mod callmemaybe;
pub mod handler;
pub mod http;
pub mod json_ctrl;
pub mod receive_wal;
pub mod s3_offload;
pub mod safekeeper;
pub mod send_wal;
pub mod timeline;
pub mod upgrade;
pub mod wal_service;

pub mod defaults {
    use const_format::formatcp;
    use std::time::Duration;

    pub const DEFAULT_PG_LISTEN_PORT: u16 = 5454;
    pub const DEFAULT_PG_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_PG_LISTEN_PORT}");

    pub const DEFAULT_HTTP_LISTEN_PORT: u16 = 7676;
    pub const DEFAULT_HTTP_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_HTTP_LISTEN_PORT}");
    pub const DEFAULT_RECALL_PERIOD: Duration = Duration::from_secs(1);
}

#[derive(Debug, Clone)]
pub struct SafeKeeperConf {
    // Repository directory, relative to current working directory.
    // Normally, the safekeeper changes the current working directory
    // to the repository, and 'workdir' is always '.'. But we don't do
    // that during unit testing, because the current directory is global
    // to the process but different unit tests work on different
    // data directories to avoid clashing with each other.
    pub workdir: PathBuf,

    pub daemonize: bool,
    pub no_sync: bool,
    pub listen_pg_addr: String,
    pub listen_http_addr: String,
    pub ttl: Option<Duration>,
    pub recall_period: Duration,
}

impl SafeKeeperConf {
    pub fn timeline_dir(&self, timelineid: &ZTimelineId) -> PathBuf {
        self.workdir.join(timelineid.to_string())
    }
}

impl Default for SafeKeeperConf {
    fn default() -> Self {
        SafeKeeperConf {
            // Always set to './'. We will chdir into the directory specified on the
            // command line, so that when the server is running, all paths are relative
            // to that.
            workdir: PathBuf::from("./"),
            daemonize: false,
            no_sync: false,
            listen_pg_addr: defaults::DEFAULT_PG_LISTEN_ADDR.to_string(),
            listen_http_addr: defaults::DEFAULT_HTTP_LISTEN_ADDR.to_string(),
            ttl: None,
            recall_period: defaults::DEFAULT_RECALL_PERIOD,
        }
    }
}

/// Safekeeper unique 64 bit ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SafekeeperId(pub [u8; 8]);

impl SafekeeperId {
    pub fn generate() -> Self {
        let mut tli_buf = [0u8; 8];
        rand::thread_rng().fill(&mut tli_buf);
        SafekeeperId(tli_buf)
    }

    pub fn from_slice<T: AsRef<[u8]>>(data: T) -> Result<Self, FromHexError> {
        let decoded = <[u8; 8]>::from_hex(data)?;
        Ok(Self(decoded))
    }
}

// Display the ID as hex.
impl fmt::Display for SafekeeperId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

/// This safekeeper ID.
pub static mut MY_ID: SafekeeperId = SafekeeperId([0u8; 8]);
