//! Low-level key-value storage abstraction.
//!
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

/// A single version of an Object.
pub struct ObjectVersion {
    pub lsn: Lsn,
    pub value: Box<[u8]>,
}

/// An iterator over `ObjectVersion`
pub type ObjectsIter<'a> = dyn Iterator<Item = ObjectVersion> + 'a;

/// A single version of an Object, and its tag.
///
/// This is basically a flattened form of `(BufferTag, ObjectVersion)`.
pub struct ObjectAny {
    pub buf_tag: BufferTag,
    pub lsn: Lsn,
    pub value: Box<[u8]>,
}

/// An iterator over `ObjectAny`
pub type AllObjectsIter<'a> = dyn Iterator<Item = Result<ObjectAny>> + 'a;

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
    fn object_versions(&self, key: &ObjectKey, lsn: Lsn) -> Result<Box<ObjectsIter>>;

    /// Iterate through versions of all objects in a timeline.
    ///
    /// Returns objects in increasing key-version order.
    /// Returns all versions up to and including the specified LSN.
    fn objects(&self, timeline: ZTimelineId, lsn: Lsn) -> Result<Box<AllObjectsIter>>;

    /// Iterate through all keys with given tablespace and database ID, and LSN <= 'lsn'.
    /// Both dbnode and spcnode can be InvalidId (0) which means get all relations in tablespace/cluster
    ///
    /// This is used to implement 'create database'
    fn list_rels(
        &self,
        timelineid: ZTimelineId,
        spcnode: u32,
        dbnode: u32,
        lsn: Lsn,
    ) -> Result<HashSet<RelTag>>;

    /// Unlink object (used by GC). This mehod may actually delete object or just mark it for deletion.
    fn unlink(&self, key: &ObjectKey, lsn: Lsn) -> Result<()>;
}
