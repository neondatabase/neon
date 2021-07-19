use hex::FromHex;
use rand::Rng;
use serde::{Deserialize, Serialize};

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

pub mod basebackup;
pub mod branches;
pub mod logger;
pub mod object_key;
pub mod object_repository;
pub mod object_store;
pub mod page_cache;
pub mod page_service;
pub mod repository;
pub mod restore_local_repo;
pub mod rocksdb_storage;
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
    pub listen_addr: String,
    pub gc_horizon: u64,
    pub gc_period: Duration,
    pub superuser: String,

    // Repository directory, relative to current working directory.
    // Normally, the page server changes the current working directory
    // to the repository, and 'workdir' is always '.'. But we don't do
    // that during unit testing, because the current directory is global
    // to the process but different unit tests work on different
    // repositories.
    pub workdir: PathBuf,

    pub pg_distrib_dir: PathBuf,
}

impl PageServerConf {
    //
    // Repository paths, relative to workdir.
    //

    fn tenants_path(&self) -> PathBuf {
        self.workdir.join("tenants")
    }

    fn tenant_path(&self, tenantid: &ZTenantId) -> PathBuf {
        self.tenants_path().join(tenantid.to_string())
    }

    fn tags_path(&self, tenantid: &ZTenantId) -> PathBuf {
        self.tenant_path(tenantid).join("refs").join("tags")
    }

    fn tag_path(&self, tag_name: &str, tenantid: &ZTenantId) -> PathBuf {
        self.tags_path(tenantid).join(tag_name)
    }

    fn branches_path(&self, tenantid: &ZTenantId) -> PathBuf {
        self.tenant_path(tenantid).join("refs").join("branches")
    }

    fn branch_path(&self, branch_name: &str, tenantid: &ZTenantId) -> PathBuf {
        self.branches_path(tenantid).join(branch_name)
    }

    fn timelines_path(&self, tenantid: &ZTenantId) -> PathBuf {
        self.tenant_path(tenantid).join("timelines")
    }

    fn timeline_path(&self, timelineid: &ZTimelineId, tenantid: &ZTenantId) -> PathBuf {
        self.timelines_path(tenantid).join(timelineid.to_string())
    }

    fn ancestor_path(&self, timelineid: &ZTimelineId, tenantid: &ZTenantId) -> PathBuf {
        self.timeline_path(timelineid, tenantid).join("ancestor")
    }

    fn wal_dir_path(&self, timelineid: &ZTimelineId, tenantid: &ZTenantId) -> PathBuf {
        self.timeline_path(timelineid, tenantid).join("wal")
    }

    //
    // Postgres distribution paths
    //

    pub fn pg_bin_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("bin")
    }

    pub fn pg_lib_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("lib")
    }
}

// Zenith ID is a 128-bit random ID.
// Used to represent various identifiers. Provides handy utility methods and impls.
// TODO (LizardWizzard) figure out best way to remove boiler plate with trait impls caused by newtype pattern
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
struct ZId([u8; 16]);

impl ZId {
    pub fn get_from_buf(buf: &mut dyn bytes::Buf) -> ZId {
        let mut arr = [0u8; 16];
        buf.copy_to_slice(&mut arr);
        ZId::from(arr)
    }

    pub fn as_arr(&self) -> [u8; 16] {
        self.0
    }

    pub fn generate() -> Self {
        let mut tli_buf = [0u8; 16];
        rand::thread_rng().fill(&mut tli_buf);
        ZId::from(tli_buf)
    }
}

impl FromStr for ZId {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<ZId, Self::Err> {
        Self::from_hex(s)
    }
}

// this is needed for pretty serialization and deserialization of ZId's using serde integration with hex crate
impl FromHex for ZId {
    type Error = hex::FromHexError;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        let mut buf: [u8; 16] = [0u8; 16];
        hex::decode_to_slice(hex, &mut buf)?;
        Ok(ZId(buf))
    }
}

impl AsRef<[u8]> for ZId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<[u8; 16]> for ZId {
    fn from(b: [u8; 16]) -> Self {
        ZId(b)
    }
}

impl fmt::Display for ZId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ZTimelineId(ZId);

impl FromStr for ZTimelineId {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<ZTimelineId, Self::Err> {
        let value = ZId::from_str(s)?;
        Ok(ZTimelineId(value))
    }
}

impl From<[u8; 16]> for ZTimelineId {
    fn from(b: [u8; 16]) -> Self {
        ZTimelineId(ZId::from(b))
    }
}

impl ZTimelineId {
    pub fn get_from_buf(buf: &mut dyn bytes::Buf) -> ZTimelineId {
        ZTimelineId(ZId::get_from_buf(buf))
    }

    pub fn as_arr(&self) -> [u8; 16] {
        self.0.as_arr()
    }

    pub fn generate() -> Self {
        ZTimelineId(ZId::generate())
    }
}

impl fmt::Display for ZTimelineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// Zenith Tenant Id represents identifiar of a particular tenant.
// Is used for distinguishing requests and data belonging to different users.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct ZTenantId(ZId);

impl FromStr for ZTenantId {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<ZTenantId, Self::Err> {
        let value = ZId::from_str(s)?;
        Ok(ZTenantId(value))
    }
}

impl From<[u8; 16]> for ZTenantId {
    fn from(b: [u8; 16]) -> Self {
        ZTenantId(ZId::from(b))
    }
}

impl FromHex for ZTenantId {
    type Error = hex::FromHexError;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        Ok(ZTenantId(ZId::from_hex(hex)?))
    }
}

impl AsRef<[u8]> for ZTenantId {
    fn as_ref(&self) -> &[u8] {
        &self.0 .0
    }
}

impl ZTenantId {
    pub fn get_from_buf(buf: &mut dyn bytes::Buf) -> ZTenantId {
        ZTenantId(ZId::get_from_buf(buf))
    }

    pub fn as_arr(&self) -> [u8; 16] {
        self.0.as_arr()
    }

    pub fn generate() -> Self {
        ZTenantId(ZId::generate())
    }
}

impl fmt::Display for ZTenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
