use crate::walrecord::ZenithWalRecord;
use crate::CheckpointConfig;
use anyhow::{bail, Result};
use byteorder::{ByteOrder, BE};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::future::Future;
use std::ops::{AddAssign, Range};
use std::sync::{Arc, RwLockReadGuard};
use std::time::Duration;
use utils::{
    lsn::{Lsn, RecordLsn},
    zid::ZTimelineId,
};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
/// Key used in the Repository kv-store.
///
/// The Repository treats this as an opaque struct, but see the code in pgdatadir_mapping.rs
/// for what we actually store in these fields.
pub struct Key {
    pub field1: u8,
    pub field2: u32,
    pub field3: u32,
    pub field4: u32,
    pub field5: u8,
    pub field6: u32,
}

pub const KEY_SIZE: usize = 18;

impl Key {
    pub fn next(&self) -> Key {
        self.add(1)
    }

    pub fn add(&self, x: u32) -> Key {
        let mut key = *self;

        let r = key.field6.overflowing_add(x);
        key.field6 = r.0;
        if r.1 {
            let r = key.field5.overflowing_add(1);
            key.field5 = r.0;
            if r.1 {
                let r = key.field4.overflowing_add(1);
                key.field4 = r.0;
                if r.1 {
                    let r = key.field3.overflowing_add(1);
                    key.field3 = r.0;
                    if r.1 {
                        let r = key.field2.overflowing_add(1);
                        key.field2 = r.0;
                        if r.1 {
                            let r = key.field1.overflowing_add(1);
                            key.field1 = r.0;
                            assert!(!r.1);
                        }
                    }
                }
            }
        }
        key
    }

    pub fn from_slice(b: &[u8]) -> Self {
        Key {
            field1: b[0],
            field2: u32::from_be_bytes(b[1..5].try_into().unwrap()),
            field3: u32::from_be_bytes(b[5..9].try_into().unwrap()),
            field4: u32::from_be_bytes(b[9..13].try_into().unwrap()),
            field5: b[13],
            field6: u32::from_be_bytes(b[14..18].try_into().unwrap()),
        }
    }

    pub fn write_to_byte_slice(&self, buf: &mut [u8]) {
        buf[0] = self.field1;
        BE::write_u32(&mut buf[1..5], self.field2);
        BE::write_u32(&mut buf[5..9], self.field3);
        BE::write_u32(&mut buf[9..13], self.field4);
        buf[13] = self.field5;
        BE::write_u32(&mut buf[14..18], self.field6);
    }
}

pub fn key_range_size(key_range: &Range<Key>) -> u32 {
    let start = key_range.start;
    let end = key_range.end;

    if end.field1 != start.field1
        || end.field2 != start.field2
        || end.field3 != start.field3
        || end.field4 != start.field4
    {
        return u32::MAX;
    }

    let start = (start.field5 as u64) << 32 | start.field6 as u64;
    let end = (end.field5 as u64) << 32 | end.field6 as u64;

    let diff = end - start;
    if diff > u32::MAX as u64 {
        u32::MAX
    } else {
        diff as u32
    }
}

pub fn singleton_range(key: Key) -> Range<Key> {
    key..key.next()
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02X}{:08X}{:08X}{:08X}{:02X}{:08X}",
            self.field1, self.field2, self.field3, self.field4, self.field5, self.field6
        )
    }
}

impl Key {
    pub const MIN: Key = Key {
        field1: u8::MIN,
        field2: u32::MIN,
        field3: u32::MIN,
        field4: u32::MIN,
        field5: u8::MIN,
        field6: u32::MIN,
    };
    pub const MAX: Key = Key {
        field1: u8::MAX,
        field2: u32::MAX,
        field3: u32::MAX,
        field4: u32::MAX,
        field5: u8::MAX,
        field6: u32::MAX,
    };

    pub fn from_hex(s: &str) -> Result<Self> {
        if s.len() != 36 {
            bail!("parse error");
        }
        Ok(Key {
            field1: u8::from_str_radix(&s[0..2], 16)?,
            field2: u32::from_str_radix(&s[2..10], 16)?,
            field3: u32::from_str_radix(&s[10..18], 16)?,
            field4: u32::from_str_radix(&s[18..26], 16)?,
            field5: u8::from_str_radix(&s[26..28], 16)?,
            field6: u32::from_str_radix(&s[28..36], 16)?,
        })
    }
}

/// A 'value' stored for a one Key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    /// An Image value contains a full copy of the value
    Image(Bytes),
    /// A WalRecord value contains a WAL record that needs to be
    /// replayed get the full value. Replaying the WAL record
    /// might need a previous version of the value (if will_init()
    /// returns false), or it may be replayed stand-alone (true).
    WalRecord(ZenithWalRecord),
}

impl Value {
    pub fn is_image(&self) -> bool {
        matches!(self, Value::Image(_))
    }

    pub fn will_init(&self) -> bool {
        match self {
            Value::Image(_) => true,
            Value::WalRecord(rec) => rec.will_init(),
        }
    }
}

///
/// A repository corresponds to one .neon directory. One repository holds multiple
/// timelines, forked off from the same initial call to 'initdb'.
///
/// There is exactly one implementation of this trait, LayeredRepository. This trait exist
/// mostly for documentation purposes, to make it clear which functions are considered public,
/// and which ones are implementation details of the storage format. Most code passes around
/// concrete RepositoryImpl and TimelineImpl structs, but code outside layered_repository.rs
/// should refrain from calling functions that are not listed in this trait.
///
pub trait Repository: Send + Sync {
    type Timeline: crate::DatadirTimeline;

    /// Get Timeline handle for given zenith timeline ID.
    /// This function is idempotent. It doesn't change internal state in any way.
    fn get_timeline(&self, timelineid: ZTimelineId) -> Option<Arc<Self::Timeline>>;

    /// Lists timelines the repository contains.
    /// Up to repository's implementation to omit certain timelines that ar not considered ready for use.
    fn list_timelines(&self) -> Vec<Arc<Self::Timeline>>;

    /// Create a new, empty timeline. The caller is responsible for loading data into it
    /// Initdb lsn is provided for timeline impl to be able to perform checks for some operations against it.
    fn create_empty_timeline(
        &self,
        timeline_id: ZTimelineId,
        initdb_lsn: Lsn,
    ) -> Result<Arc<Self::Timeline>>;

    /// Branch a timeline
    fn branch_timeline(
        &self,
        src: ZTimelineId,
        dst: ZTimelineId,
        start_lsn: Option<Lsn>,
    ) -> Result<Arc<Self::Timeline>>;

    /// Permanently delete a timeline, from local disk and from remote storage
    fn delete_timeline(&self, timeline_id: ZTimelineId) -> anyhow::Result<()>;

    /// Flush all data to disk.
    ///
    /// this is used at graceful shutdown.
    fn checkpoint(&self) -> Result<()>;

    /// perform one garbage collection iteration, removing old data files from disk.
    /// this function is periodically called by gc thread.
    /// also it can be explicitly requested through page server api 'do_gc' command.
    ///
    /// 'timelineid' specifies the timeline to GC, or None for all.
    /// `horizon` specifies delta from last lsn to preserve all object versions (pitr interval).
    /// `checkpoint_before_gc` parameter is used to force compaction of storage before GC
    /// to make tests more deterministic.
    /// TODO Do we still need it or we can call checkpoint explicitly in tests where needed?
    fn gc_iteration(
        &self,
        timelineid: Option<ZTimelineId>,
        horizon: u64,
        pitr: Duration,
        checkpoint_before_gc: bool,
    ) -> Result<GcResult>;

    /// Perform one compaction iteration.
    /// This function is periodically called by compactor thread.
    /// Also it can be explicitly requested per timeline through page server
    /// api's 'compact' command.
    fn compaction_iteration(&self) -> Result<()>;
}

///
/// Result of performing GC
///
#[derive(Default)]
pub struct GcResult {
    pub layers_total: u64,
    pub layers_needed_by_cutoff: u64,
    pub layers_needed_by_pitr: u64,
    pub layers_needed_by_branches: u64,
    pub layers_not_updated: u64,
    pub layers_removed: u64, // # of layer files removed because they have been made obsolete by newer ondisk files.

    pub elapsed: Duration,
}

impl AddAssign for GcResult {
    fn add_assign(&mut self, other: Self) {
        self.layers_total += other.layers_total;
        self.layers_needed_by_pitr += other.layers_needed_by_pitr;
        self.layers_needed_by_cutoff += other.layers_needed_by_cutoff;
        self.layers_needed_by_branches += other.layers_needed_by_branches;
        self.layers_not_updated += other.layers_not_updated;
        self.layers_removed += other.layers_removed;

        self.elapsed += other.elapsed;
    }
}

pub trait Timeline: Send + Sync {
    //------------------------------------------------------------------------------
    // Public GET functions
    //------------------------------------------------------------------------------

    ///
    /// Wait until WAL has been received and processed up to this LSN.
    ///
    /// You should call this before any of the other get_* or list_* functions. Calling
    /// those functions with an LSN that has been processed yet is an error.
    ///
    fn wait_lsn(&self, lsn: Lsn) -> Result<()>;

    /// Lock and get timeline's GC cuttof
    fn get_latest_gc_cutoff_lsn(&self) -> RwLockReadGuard<Lsn>;

    /// Look up given page version.
    ///
    /// It's possible that we are missing some layer files locally that are needed
    /// to reconstruct the page version. In that case, this returns
    /// PageReconstructError::NeedDownload, with a future that can be used to download
    /// the missing file.
    ///
    /// This function is synchronous, but it can block on local disk I/O or waiting for
    /// locks. But it will not block waiting for remote storage, that's why the NeedDownload
    /// return error exists.
    ///
    /// NOTE: It is considered an error to 'get' a key that doesn't exist. The abstraction
    /// above this needs to store suitable metadata to track what data exists with
    /// what keys, in separate metadata entries. If a non-existent key is requested,
    /// the Repository implementation may incorrectly return a value from an ancestor
    /// branch, for example, or waste a lot of cycles chasing the non-existing key.
    ///
    fn get(&self, key: Key, lsn: Lsn) -> Result<Bytes, PageReconstructError>;

    /// Get the ancestor's timeline id
    fn get_ancestor_timeline_id(&self) -> Option<ZTimelineId>;

    /// Get the LSN where this branch was created
    fn get_ancestor_lsn(&self) -> Lsn;

    /// Atomically get both last and prev.
    fn get_last_record_rlsn(&self) -> RecordLsn;

    /// Get last or prev record separately. Same as get_last_record_rlsn().last/prev.
    fn get_last_record_lsn(&self) -> Lsn;

    fn get_prev_record_lsn(&self) -> Lsn;

    fn get_disk_consistent_lsn(&self) -> Lsn;

    fn get_remote_consistent_lsn(&self) -> Option<Lsn>;

    // Mutate the timeline with a [`TimelineWriter`].
    //
    // FIXME: This ought to return &'a TimelineWriter, where TimelineWriter
    // is a generic type in this trait. But that doesn't currently work in
    // Rust: https://rust-lang.github.io/rfcs/1598-generic_associated_types.html
    // So for now, this function is not part of the trait. In practice, all callers
    // have a reference to the concrete TimelineImpl type, and can call the writer
    // function directly from TimelineImpl.
    //
    //fn writer<'a>(&'a self) -> Box<dyn TimelineWriter + 'a>;

    ///
    /// Flush to disk all data that was written with the put_* functions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL. We don't
    /// know anything about them here in the repository.
    fn checkpoint(&self, cconf: CheckpointConfig) -> Result<()>;

    ///
    /// Check that it is valid to request operations with that lsn.
    fn check_lsn_is_in_scope(
        &self,
        lsn: Lsn,
        latest_gc_cutoff_lsn: &RwLockReadGuard<Lsn>,
    ) -> Result<()>;

    /// Get the physical size of the timeline at the latest LSN
    fn get_physical_size(&self) -> u64;
    /// Get the physical size of the timeline at the latest LSN non incrementally
    fn get_physical_size_non_incremental(&self) -> Result<u64>;
}

/// An error happened in a get() operation.
#[derive(thiserror::Error)]
pub enum PageReconstructError {
    #[error(transparent)]
    Other(#[from] anyhow::Error), // source and Display delegate to anyhow::Error

    #[error(transparent)]
    WalRedo(#[from] crate::walredo::WalRedoError),

    /// A layer file is missing locally. You can call the returned Future to
    /// download the missing layer, and after that finishes, the operation will
    /// probably succeed if you retry it. (It can return NeedDownload again, if
    /// another layer needs to be downloaded.).
    #[error("layer file needs to be downloaded")]
    NeedDownload(std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send + Sync>>),
}

impl fmt::Debug for PageReconstructError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            PageReconstructError::Other(err) => err.fmt(f),
            PageReconstructError::WalRedo(err) => err.fmt(f),
            // TODO: print more info about the missing layer
            PageReconstructError::NeedDownload(_) => write!(f, "need to download a layer"),
        }
    }
}

///
/// Run a function that can return PageReconstructError, downloading the missing
/// file and retrying if needed.
///
pub async fn retry_get<F, T>(f: F) -> Result<T, anyhow::Error>
where
    F: Send + Fn() -> Result<T, PageReconstructError>,
    T: Send,
{
    loop {
        let result = f();
        let future = match result {
            Err(PageReconstructError::NeedDownload(future)) => {
                tracing::error!("RETRYING");
                future
            }
            _ => return Ok(result?),
        };
        future.await?;
    }
}

/// Various functions to mutate the timeline.
// TODO Currently, Deref is used to allow easy access to read methods from this trait.
// This is probably considered a bad practice in Rust and should be fixed eventually,
// but will cause large code changes.
pub trait TimelineWriter {
    /// Put a new page version that can be constructed from a WAL record
    ///
    /// This will implicitly extend the relation, if the page is beyond the
    /// current end-of-file.
    fn put(&self, key: Key, lsn: Lsn, value: &Value) -> Result<()>;

    fn delete(&self, key_range: Range<Key>, lsn: Lsn) -> Result<()>;

    /// Track the end of the latest digested WAL record.
    ///
    /// Call this after you have finished writing all the WAL up to 'lsn'.
    ///
    /// 'lsn' must be aligned. This wakes up any wait_lsn() callers waiting for
    /// the 'lsn' or anything older. The previous last record LSN is stored alongside
    /// the latest and can be read.
    fn finish_write(&self, lsn: Lsn);

    fn update_current_logical_size(&self, delta: isize);
}
