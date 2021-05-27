use crate::repository::{BufferTag, RelTag};
use crate::ZTimelineId;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::iter::Iterator;
use zenith_utils::lsn::Lsn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectKey {
    pub timeline: ZTimelineId,
    pub buf_tag: BufferTag,
}

///
/// Low-level storage abstraction.
///
/// All the data in the repository is stored in a key-value store. This trait
/// abstracts the details of the key-value store.
///
/// A simple key-value store would support just GET and PUT operations with
/// a key, but the upper layer needs slightly complicated read operations
///
/// The most frequently used function is 'object_versions'. It is used
/// to look up a page version. It is LSN aware, in that the caller
/// specifies an LSN, and the function returns all values for that
/// block with the same or older LSN.
///
pub trait ObjectStore: Send + Sync {
    ///
    /// Store a value with given key.
    ///
    fn put(&self, key: &ObjectKey, lsn: Lsn, value: &[u8]) -> Result<()>;

    /// Read entry with the exact given key.
    ///
    /// This is used for retrieving metadata with special key that doesn't
    /// correspond to any real relation.
    fn get(&self, key: &ObjectKey, lsn: Lsn) -> Result<Vec<u8>>;

    /// Iterate through all page versions of one object.
    ///
    /// Returns all page versions in descending LSN order, along with the LSN
    /// of each page version.
    fn object_versions<'a>(
        &'a self,
        key: &ObjectKey,
        lsn: Lsn,
    ) -> Result<Box<dyn Iterator<Item = (Lsn, Vec<u8>)> + 'a>>;

    /// Iterate through all keys with given tablespace and database ID, and LSN <= 'lsn'.
    ///
    /// This is used to implement 'create database'
    fn list_rels(
        &self,
        timelineid: ZTimelineId,
        spcnode: u32,
        dbnode: u32,
        lsn: Lsn,
    ) -> Result<HashSet<RelTag>>;
}
