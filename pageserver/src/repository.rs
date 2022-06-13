use crate::layered_repository::metadata::TimelineMetadata;
use crate::storage_sync::index::RemoteIndex;
use crate::walrecord::ZenithWalRecord;
use crate::CheckpointConfig;
use anyhow::{bail, Result};
use byteorder::{ByteOrder, BE};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
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

#[derive(Clone, Copy, Debug)]
pub enum TimelineSyncStatusUpdate {
    Downloaded,
}

impl Display for TimelineSyncStatusUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TimelineSyncStatusUpdate::Downloaded => "Downloaded",
        };
        f.write_str(s)
    }
}

///
/// A repository corresponds to one .neon directory. One repository holds multiple
/// timelines, forked off from the same initial call to 'initdb'.
pub trait Repository: Send + Sync {
    type Timeline: Timeline;

    /// Updates timeline based on the `TimelineSyncStatusUpdate`, received from the remote storage synchronization.
    /// See [`crate::remote_storage`] for more details about the synchronization.
    fn apply_timeline_remote_sync_status_update(
        &self,
        timeline_id: ZTimelineId,
        timeline_sync_status_update: TimelineSyncStatusUpdate,
    ) -> Result<()>;

    /// Get Timeline handle for given zenith timeline ID.
    /// This function is idempotent. It doesn't change internal state in any way.
    fn get_timeline(&self, timelineid: ZTimelineId) -> Option<RepositoryTimeline<Self::Timeline>>;

    /// Get Timeline handle for locally available timeline. Load it into memory if it is not loaded.
    fn get_timeline_load(&self, timelineid: ZTimelineId) -> Result<Arc<Self::Timeline>>;

    /// Lists timelines the repository contains.
    /// Up to repository's implementation to omit certain timelines that ar not considered ready for use.
    fn list_timelines(&self) -> Vec<(ZTimelineId, RepositoryTimeline<Self::Timeline>)>;

    /// Create a new, empty timeline. The caller is responsible for loading data into it
    /// Initdb lsn is provided for timeline impl to be able to perform checks for some operations against it.
    fn create_empty_timeline(
        &self,
        timelineid: ZTimelineId,
        initdb_lsn: Lsn,
    ) -> Result<Arc<Self::Timeline>>;

    /// Branch a timeline
    fn branch_timeline(&self, src: ZTimelineId, dst: ZTimelineId, start_lsn: Lsn) -> Result<()>;

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

    /// detaches timeline-related in-memory data.
    fn detach_timeline(&self, timeline_id: ZTimelineId) -> Result<()>;

    // Allows to retrieve remote timeline index from the repo. Used in walreceiver to grab remote consistent lsn.
    fn get_remote_index(&self) -> &RemoteIndex;
}

/// A timeline, that belongs to the current repository.
pub enum RepositoryTimeline<T> {
    /// Timeline, with its files present locally in pageserver's working directory.
    /// Loaded into pageserver's memory and ready to be used.
    Loaded(Arc<T>),

    /// All the data is available locally, but not loaded into memory, so loading have to be done before actually using the timeline
    Unloaded {
        // It is ok to keep metadata here, because it is not changed when timeline is unloaded.
        // FIXME can s3 sync actually change it? It can change it when timeline is in awaiting download state.
        //  but we currently do not download something for the timeline once it is local (even if there are new checkpoints) is it correct?
        // also it is not that good to keep TimelineMetadata here, because it is layered repo implementation detail
        metadata: TimelineMetadata,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalTimelineState {
    // timeline is loaded into memory (with layer map and all the bits),
    Loaded,
    // timeline is on disk locally and ready to be loaded into memory.
    Unloaded,
}

impl<'a, T> From<&'a RepositoryTimeline<T>> for LocalTimelineState {
    fn from(local_timeline_entry: &'a RepositoryTimeline<T>) -> Self {
        match local_timeline_entry {
            RepositoryTimeline::Loaded(_) => LocalTimelineState::Loaded,
            RepositoryTimeline::Unloaded { .. } => LocalTimelineState::Unloaded,
        }
    }
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
    /// NOTE: It is considered an error to 'get' a key that doesn't exist. The abstraction
    /// above this needs to store suitable metadata to track what data exists with
    /// what keys, in separate metadata entries. If a non-existent key is requested,
    /// the Repository implementation may incorrectly return a value from an ancestor
    /// branch, for example, or waste a lot of cycles chasing the non-existing key.
    ///
    fn get(&self, key: Key, lsn: Lsn) -> Result<Bytes>;

    /// Get the ancestor's timeline id
    fn get_ancestor_timeline_id(&self) -> Option<ZTimelineId>;

    /// Get the LSN where this branch was created
    fn get_ancestor_lsn(&self) -> Lsn;

    //------------------------------------------------------------------------------
    // Public PUT functions, to update the repository with new page versions.
    //
    // These are called by the WAL receiver to digest WAL records.
    //------------------------------------------------------------------------------
    /// Atomically get both last and prev.
    fn get_last_record_rlsn(&self) -> RecordLsn;

    /// Get last or prev record separately. Same as get_last_record_rlsn().last/prev.
    fn get_last_record_lsn(&self) -> Lsn;

    fn get_prev_record_lsn(&self) -> Lsn;

    fn get_disk_consistent_lsn(&self) -> Lsn;

    /// Mutate the timeline with a [`TimelineWriter`].
    ///
    /// FIXME: This ought to return &'a TimelineWriter, where TimelineWriter
    /// is a generic type in this trait. But that doesn't currently work in
    /// Rust: https://rust-lang.github.io/rfcs/1598-generic_associated_types.html
    fn writer<'a>(&'a self) -> Box<dyn TimelineWriter + 'a>;

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
}

/// Various functions to mutate the timeline.
// TODO Currently, Deref is used to allow easy access to read methods from this trait.
// This is probably considered a bad practice in Rust and should be fixed eventually,
// but will cause large code changes.
pub trait TimelineWriter<'a> {
    /// Put a new page version that can be constructed from a WAL record
    ///
    /// This will implicitly extend the relation, if the page is beyond the
    /// current end-of-file.
    fn put(&self, key: Key, lsn: Lsn, value: Value) -> Result<()>;

    fn delete(&self, key_range: Range<Key>, lsn: Lsn) -> Result<()>;

    /// Track the end of the latest digested WAL record.
    ///
    /// Call this after you have finished writing all the WAL up to 'lsn'.
    ///
    /// 'lsn' must be aligned. This wakes up any wait_lsn() callers waiting for
    /// the 'lsn' or anything older. The previous last record LSN is stored alongside
    /// the latest and can be read.
    fn finish_write(&self, lsn: Lsn);
}

#[cfg(test)]
pub mod repo_harness {
    use bytes::BytesMut;
    use lazy_static::lazy_static;
    use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
    use std::{fs, path::PathBuf};

    use crate::RepositoryImpl;
    use crate::{
        config::PageServerConf,
        layered_repository::LayeredRepository,
        walredo::{WalRedoError, WalRedoManager},
    };

    use super::*;
    use crate::tenant_config::{TenantConf, TenantConfOpt};
    use hex_literal::hex;
    use utils::zid::ZTenantId;

    pub const TIMELINE_ID: ZTimelineId =
        ZTimelineId::from_array(hex!("11223344556677881122334455667788"));
    pub const NEW_TIMELINE_ID: ZTimelineId =
        ZTimelineId::from_array(hex!("AA223344556677881122334455667788"));

    /// Convenience function to create a page image with given string as the only content
    #[allow(non_snake_case)]
    pub fn TEST_IMG(s: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        buf.resize(64, 0);

        buf.freeze()
    }

    lazy_static! {
        static ref LOCK: RwLock<()> = RwLock::new(());
    }

    impl From<TenantConf> for TenantConfOpt {
        fn from(tenant_conf: TenantConf) -> Self {
            Self {
                checkpoint_distance: Some(tenant_conf.checkpoint_distance),
                compaction_target_size: Some(tenant_conf.compaction_target_size),
                compaction_period: Some(tenant_conf.compaction_period),
                compaction_threshold: Some(tenant_conf.compaction_threshold),
                gc_horizon: Some(tenant_conf.gc_horizon),
                gc_period: Some(tenant_conf.gc_period),
                image_creation_threshold: Some(tenant_conf.image_creation_threshold),
                pitr_interval: Some(tenant_conf.pitr_interval),
                walreceiver_connect_timeout: Some(tenant_conf.walreceiver_connect_timeout),
                lagging_wal_timeout: Some(tenant_conf.lagging_wal_timeout),
                max_lsn_wal_lag: Some(tenant_conf.max_lsn_wal_lag),
            }
        }
    }

    pub struct RepoHarness<'a> {
        pub conf: &'static PageServerConf,
        pub tenant_conf: TenantConf,
        pub tenant_id: ZTenantId,

        pub lock_guard: (
            Option<RwLockReadGuard<'a, ()>>,
            Option<RwLockWriteGuard<'a, ()>>,
        ),
    }

    impl<'a> RepoHarness<'a> {
        pub fn create(test_name: &'static str) -> Result<Self> {
            Self::create_internal(test_name, false)
        }
        pub fn create_exclusive(test_name: &'static str) -> Result<Self> {
            Self::create_internal(test_name, true)
        }
        fn create_internal(test_name: &'static str, exclusive: bool) -> Result<Self> {
            let lock_guard = if exclusive {
                (None, Some(LOCK.write().unwrap()))
            } else {
                (Some(LOCK.read().unwrap()), None)
            };

            let repo_dir = PageServerConf::test_repo_dir(test_name);
            let _ = fs::remove_dir_all(&repo_dir);
            fs::create_dir_all(&repo_dir)?;

            let conf = PageServerConf::dummy_conf(repo_dir);
            // Make a static copy of the config. This can never be free'd, but that's
            // OK in a test.
            let conf: &'static PageServerConf = Box::leak(Box::new(conf));

            let tenant_conf = TenantConf::dummy_conf();

            let tenant_id = ZTenantId::generate();
            fs::create_dir_all(conf.tenant_path(&tenant_id))?;
            fs::create_dir_all(conf.timelines_path(&tenant_id))?;

            Ok(Self {
                conf,
                tenant_conf,
                tenant_id,
                lock_guard,
            })
        }

        pub fn load(&self) -> RepositoryImpl {
            self.try_load().expect("failed to load test repo")
        }

        pub fn try_load(&self) -> Result<RepositoryImpl> {
            let walredo_mgr = Arc::new(TestRedoManager);

            let repo = LayeredRepository::new(
                self.conf,
                TenantConfOpt::from(self.tenant_conf),
                walredo_mgr,
                self.tenant_id,
                RemoteIndex::empty(),
                false,
            );
            // populate repo with locally available timelines
            for timeline_dir_entry in fs::read_dir(self.conf.timelines_path(&self.tenant_id))
                .expect("should be able to read timelines dir")
            {
                let timeline_dir_entry = timeline_dir_entry.unwrap();
                let timeline_id: ZTimelineId = timeline_dir_entry
                    .path()
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .parse()
                    .unwrap();

                repo.apply_timeline_remote_sync_status_update(
                    timeline_id,
                    TimelineSyncStatusUpdate::Downloaded,
                )?;
            }

            Ok(repo)
        }

        pub fn timeline_path(&self, timeline_id: &ZTimelineId) -> PathBuf {
            self.conf.timeline_path(timeline_id, &self.tenant_id)
        }
    }

    // Mock WAL redo manager that doesn't do much
    pub struct TestRedoManager;

    impl WalRedoManager for TestRedoManager {
        fn request_redo(
            &self,
            key: Key,
            lsn: Lsn,
            base_img: Option<Bytes>,
            records: Vec<(Lsn, ZenithWalRecord)>,
        ) -> Result<Bytes, WalRedoError> {
            let s = format!(
                "redo for {} to get to {}, with {} and {} records",
                key,
                lsn,
                if base_img.is_some() {
                    "base image"
                } else {
                    "no base image"
                },
                records.len()
            );
            println!("{}", s);

            Ok(TEST_IMG(&s))
        }
    }
}

///
/// Tests that should work the same with any Repository/Timeline implementation.
///
#[allow(clippy::bool_assert_comparison)]
#[cfg(test)]
mod tests {
    use super::repo_harness::*;
    use super::*;
    //use postgres_ffi::{pg_constants, xlog_utils::SIZEOF_CHECKPOINT};
    //use std::sync::Arc;
    use bytes::BytesMut;
    use hex_literal::hex;
    use lazy_static::lazy_static;

    lazy_static! {
        static ref TEST_KEY: Key = Key::from_slice(&hex!("112222222233333333444444445500000001"));
    }

    #[test]
    fn test_basic() -> Result<()> {
        let repo = RepoHarness::create("test_basic")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x10), Value::Image(TEST_IMG("foo at 0x10")))?;
        writer.finish_write(Lsn(0x10));
        drop(writer);

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x20), Value::Image(TEST_IMG("foo at 0x20")))?;
        writer.finish_write(Lsn(0x20));
        drop(writer);

        assert_eq!(tline.get(*TEST_KEY, Lsn(0x10))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x1f))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x20))?, TEST_IMG("foo at 0x20"));

        Ok(())
    }

    /// Convenience function to create a page image with given string as the only content
    pub fn test_value(s: &str) -> Value {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        Value::Image(buf.freeze())
    }

    ///
    /// Test branch creation
    ///
    #[test]
    fn test_branch() -> Result<()> {
        let repo = RepoHarness::create("test_branch")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        let writer = tline.writer();
        use std::str::from_utf8;

        #[allow(non_snake_case)]
        let TEST_KEY_A: Key = Key::from_hex("112222222233333333444444445500000001").unwrap();
        #[allow(non_snake_case)]
        let TEST_KEY_B: Key = Key::from_hex("112222222233333333444444445500000002").unwrap();

        // Insert a value on the timeline
        writer.put(TEST_KEY_A, Lsn(0x20), test_value("foo at 0x20"))?;
        writer.put(TEST_KEY_B, Lsn(0x20), test_value("foobar at 0x20"))?;
        writer.finish_write(Lsn(0x20));

        writer.put(TEST_KEY_A, Lsn(0x30), test_value("foo at 0x30"))?;
        writer.finish_write(Lsn(0x30));
        writer.put(TEST_KEY_A, Lsn(0x40), test_value("foo at 0x40"))?;
        writer.finish_write(Lsn(0x40));

        //assert_current_logical_size(&tline, Lsn(0x40));

        // Branch the history, modify relation differently on the new timeline
        repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Lsn(0x30))?;
        let newtline = repo
            .get_timeline_load(NEW_TIMELINE_ID)
            .expect("Should have a local timeline");
        let new_writer = newtline.writer();
        new_writer.put(TEST_KEY_A, Lsn(0x40), test_value("bar at 0x40"))?;
        new_writer.finish_write(Lsn(0x40));

        // Check page contents on both branches
        assert_eq!(
            from_utf8(&tline.get(TEST_KEY_A, Lsn(0x40))?)?,
            "foo at 0x40"
        );
        assert_eq!(
            from_utf8(&newtline.get(TEST_KEY_A, Lsn(0x40))?)?,
            "bar at 0x40"
        );
        assert_eq!(
            from_utf8(&newtline.get(TEST_KEY_B, Lsn(0x40))?)?,
            "foobar at 0x20"
        );

        //assert_current_logical_size(&tline, Lsn(0x40));

        Ok(())
    }

    fn make_some_layers<T: Timeline>(tline: &T, start_lsn: Lsn) -> Result<()> {
        let mut lsn = start_lsn;
        #[allow(non_snake_case)]
        {
            let writer = tline.writer();
            // Create a relation on the timeline
            writer.put(
                *TEST_KEY,
                lsn,
                Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
            lsn += 0x10;
            writer.put(
                *TEST_KEY,
                lsn,
                Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
            lsn += 0x10;
        }
        tline.checkpoint(CheckpointConfig::Forced)?;
        {
            let writer = tline.writer();
            writer.put(
                *TEST_KEY,
                lsn,
                Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
            lsn += 0x10;
            writer.put(
                *TEST_KEY,
                lsn,
                Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
        }
        tline.checkpoint(CheckpointConfig::Forced)
    }

    #[test]
    fn test_prohibit_branch_creation_on_garbage_collected_data() -> Result<()> {
        let repo =
            RepoHarness::create("test_prohibit_branch_creation_on_garbage_collected_data")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        make_some_layers(tline.as_ref(), Lsn(0x20))?;

        // this removes layers before lsn 40 (50 minus 10), so there are two remaining layers, image and delta for 31-50
        // FIXME: this doesn't actually remove any layer currently, given how the checkpointing
        // and compaction works. But it does set the 'cutoff' point so that the cross check
        // below should fail.
        repo.gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)?;

        // try to branch at lsn 25, should fail because we already garbage collected the data
        match repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Lsn(0x25)) {
            Ok(_) => panic!("branching should have failed"),
            Err(err) => {
                assert!(err.to_string().contains("invalid branch start lsn"));
                assert!(err
                    .source()
                    .unwrap()
                    .to_string()
                    .contains("we might've already garbage collected needed data"))
            }
        }

        Ok(())
    }

    #[test]
    fn test_prohibit_branch_creation_on_pre_initdb_lsn() -> Result<()> {
        let repo = RepoHarness::create("test_prohibit_branch_creation_on_pre_initdb_lsn")?.load();

        repo.create_empty_timeline(TIMELINE_ID, Lsn(0x50))?;
        // try to branch at lsn 0x25, should fail because initdb lsn is 0x50
        match repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Lsn(0x25)) {
            Ok(_) => panic!("branching should have failed"),
            Err(err) => {
                assert!(&err.to_string().contains("invalid branch start lsn"));
                assert!(&err
                    .source()
                    .unwrap()
                    .to_string()
                    .contains("is earlier than latest GC horizon"));
            }
        }

        Ok(())
    }

    /*
    // FIXME: This currently fails to error out. Calling GC doesn't currently
    // remove the old value, we'd need to work a little harder
    #[test]
    fn test_prohibit_get_for_garbage_collected_data() -> Result<()> {
        let repo =
            RepoHarness::create("test_prohibit_get_for_garbage_collected_data")?
            .load();

        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        make_some_layers(tline.as_ref(), Lsn(0x20))?;

        repo.gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)?;
        let latest_gc_cutoff_lsn = tline.get_latest_gc_cutoff_lsn();
        assert!(*latest_gc_cutoff_lsn > Lsn(0x25));
        match tline.get(*TEST_KEY, Lsn(0x25)) {
            Ok(_) => panic!("request for page should have failed"),
            Err(err) => assert!(err.to_string().contains("not found at")),
        }
        Ok(())
    }
     */

    #[test]
    fn test_retain_data_in_parent_which_is_needed_for_child() -> Result<()> {
        let repo =
            RepoHarness::create("test_retain_data_in_parent_which_is_needed_for_child")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        make_some_layers(tline.as_ref(), Lsn(0x20))?;

        repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Lsn(0x40))?;
        let newtline = repo
            .get_timeline_load(NEW_TIMELINE_ID)
            .expect("Should have a local timeline");
        // this removes layers before lsn 40 (50 minus 10), so there are two remaining layers, image and delta for 31-50
        repo.gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)?;
        assert!(newtline.get(*TEST_KEY, Lsn(0x25)).is_ok());

        Ok(())
    }
    #[test]
    fn test_parent_keeps_data_forever_after_branching() -> Result<()> {
        let repo = RepoHarness::create("test_parent_keeps_data_forever_after_branching")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        make_some_layers(tline.as_ref(), Lsn(0x20))?;

        repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Lsn(0x40))?;
        let newtline = repo
            .get_timeline_load(NEW_TIMELINE_ID)
            .expect("Should have a local timeline");

        make_some_layers(newtline.as_ref(), Lsn(0x60))?;

        // run gc on parent
        repo.gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)?;

        // Check that the data is still accessible on the branch.
        assert_eq!(
            newtline.get(*TEST_KEY, Lsn(0x50))?,
            TEST_IMG(&format!("foo at {}", Lsn(0x40)))
        );

        Ok(())
    }

    #[test]
    fn timeline_load() -> Result<()> {
        const TEST_NAME: &str = "timeline_load";
        let harness = RepoHarness::create(TEST_NAME)?;
        {
            let repo = harness.load();
            let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0x8000))?;
            make_some_layers(tline.as_ref(), Lsn(0x8000))?;
            tline.checkpoint(CheckpointConfig::Forced)?;
        }

        let repo = harness.load();
        let tline = repo
            .get_timeline(TIMELINE_ID)
            .expect("cannot load timeline");
        assert!(matches!(tline, RepositoryTimeline::Unloaded { .. }));

        assert!(repo.get_timeline_load(TIMELINE_ID).is_ok());

        let tline = repo
            .get_timeline(TIMELINE_ID)
            .expect("cannot load timeline");
        assert!(matches!(tline, RepositoryTimeline::Loaded(_)));

        Ok(())
    }

    #[test]
    fn timeline_load_with_ancestor() -> Result<()> {
        const TEST_NAME: &str = "timeline_load_with_ancestor";
        let harness = RepoHarness::create(TEST_NAME)?;
        // create two timelines
        {
            let repo = harness.load();
            let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

            make_some_layers(tline.as_ref(), Lsn(0x20))?;
            tline.checkpoint(CheckpointConfig::Forced)?;

            repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Lsn(0x40))?;

            let newtline = repo
                .get_timeline_load(NEW_TIMELINE_ID)
                .expect("Should have a local timeline");

            make_some_layers(newtline.as_ref(), Lsn(0x60))?;
            tline.checkpoint(CheckpointConfig::Forced)?;
        }

        // check that both of them are initially unloaded
        let repo = harness.load();
        {
            let tline = repo.get_timeline(TIMELINE_ID).expect("cannot get timeline");
            assert!(matches!(tline, RepositoryTimeline::Unloaded { .. }));

            let tline = repo
                .get_timeline(NEW_TIMELINE_ID)
                .expect("cannot get timeline");
            assert!(matches!(tline, RepositoryTimeline::Unloaded { .. }));
        }
        // load only child timeline
        let _ = repo
            .get_timeline_load(NEW_TIMELINE_ID)
            .expect("cannot load timeline");

        // check that both, child and ancestor are loaded
        let tline = repo
            .get_timeline(NEW_TIMELINE_ID)
            .expect("cannot get timeline");
        assert!(matches!(tline, RepositoryTimeline::Loaded(_)));

        let tline = repo.get_timeline(TIMELINE_ID).expect("cannot get timeline");
        assert!(matches!(tline, RepositoryTimeline::Loaded(_)));

        Ok(())
    }
}
