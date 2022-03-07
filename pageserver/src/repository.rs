use crate::walrecord::ZenithWalRecord;
use crate::CheckpointConfig;
use anyhow::{bail, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{AddAssign, Range};
use std::sync::{Arc, RwLockReadGuard};
use std::time::Duration;
use zenith_utils::lsn::{Lsn, RecordLsn};
use zenith_utils::zid::ZTimelineId;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
/// Key used in the Repository kv-store.
///
/// The Repository treates this as an opaque struct, but see the code in pgdatadir_mapping.rs
/// for what we actually store in these fields.
pub struct Key {
    pub field1: u8,
    pub field2: u32,
    pub field3: u32,
    pub field4: u32,
    pub field5: u8,
    pub field6: u32,
}

impl Key {

    pub fn next(&self) -> Key {
        let mut key = self.clone();

        let x = key.field6.overflowing_add(1);
        key.field6 = x.0;
        if x.1 {
            let x = key.field5.overflowing_add(1);
            key.field5 = x.0;
            if x.1 {
                let x = key.field4.overflowing_add(1);
                key.field4 = x.0;
                if x.1 {
                    let x = key.field3.overflowing_add(1);
                    key.field3 = x.0;
                    if x.1 {
                        let x = key.field2.overflowing_add(1);
                        key.field2 = x.0;
                        if x.1 {
                            let x = key.field1.overflowing_add(1);
                            key.field1 = x.0;
                            assert!(!x.1);
                        }
                    }
                }
            }
        }
        key
    }
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

    pub fn to_prefix_128(&self) -> u128 {
        assert!(self.field1 & 0xf0 == 0);
        (self.field1 as u128) << 124
            | (self.field2 as u128) << 92
            | (self.field3 as u128) << 60
            | (self.field4 as u128) << 28
            | (self.field5 as u128) << 20
            | (self.field6 as u128) >> 12
    }
}

//
// There are two kinds of values: incremental and non-incremental
//
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Image(Bytes),
    WalRecord(ZenithWalRecord),
}

impl Value {
    pub fn is_image(&self) -> bool {
        matches!(self, Value::Image(_))
    }
}

///
/// A repository corresponds to one .zenith directory. One repository holds multiple
/// timelines, forked off from the same initial call to 'initdb'.
pub trait Repository: Send + Sync {
    type Timeline: Timeline;

    fn detach_timeline(&self, timeline_id: ZTimelineId) -> Result<()>;

    /// Updates timeline based on the new sync state, received from the remote storage synchronization.
    /// See [`crate::remote_storage`] for more details about the synchronization.
    fn set_timeline_state(
        &self,
        timeline_id: ZTimelineId,
        new_state: TimelineSyncState,
    ) -> Result<()>;

    /// Gets current synchronization state of the timeline.
    /// See [`crate::remote_storage`] for more details about the synchronization.
    fn get_timeline_state(&self, timeline_id: ZTimelineId) -> Option<TimelineSyncState>;

    /// Get Timeline handle for given zenith timeline ID.
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<RepositoryTimeline<Self::Timeline>>;

    /// Create a new, empty timeline. The caller is responsible for loading data into it
    /// Initdb lsn is provided for timeline impl to be able to perform checks for some operations against it.
    fn create_empty_timeline(
        &self,
        timelineid: ZTimelineId,
        initdb_lsn: Lsn,
    ) -> Result<Arc<Self::Timeline>>;

    /// Branch a timeline
    fn branch_timeline(&self, src: ZTimelineId, dst: ZTimelineId, start_lsn: Lsn) -> Result<()>;

    /// perform one garbage collection iteration, removing old data files from disk.
    /// this function is periodically called by gc thread.
    /// also it can be explicitly requested through page server api 'do_gc' command.
    ///
    /// 'timelineid' specifies the timeline to GC, or None for all.
    /// `horizon` specifies delta from last lsn to preserve all object versions (pitr interval).
    /// `checkpoint_before_gc` parameter is used to force compaction of storage before CG
    /// to make tests more deterministic.
    /// TODO Do we still need it or we can call checkpoint explicitly in tests where needed?
    fn gc_iteration(
        &self,
        timelineid: Option<ZTimelineId>,
        horizon: u64,
        checkpoint_before_gc: bool,
    ) -> Result<GcResult>;

    /// perform one checkpoint iteration, flushing in-memory data on disk.
    /// this function is periodically called by checkponter thread.
    fn checkpoint_iteration(&self, cconf: CheckpointConfig) -> Result<()>;
}

/// A timeline, that belongs to the current repository.
pub enum RepositoryTimeline<T> {
    /// Timeline, with its files present locally in pageserver's working directory.
    /// Loaded into pageserver's memory and ready to be used.
    Local(Arc<T>),
    /// Timeline, found on the pageserver's remote storage, but not yet downloaded locally.
    Remote {
        id: ZTimelineId,
        /// metadata contents of the latest successfully uploaded checkpoint
        disk_consistent_lsn: Lsn,
    },
}

impl<T> RepositoryTimeline<T> {
    pub fn local_timeline(&self) -> Option<Arc<T>> {
        if let Self::Local(local_timeline) = self {
            Some(Arc::clone(local_timeline))
        } else {
            None
        }
    }
}

/// A state of the timeline synchronization with the remote storage.
/// Contains `disk_consistent_lsn` of the corresponding remote timeline (latest checkpoint's disk_consistent_lsn).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TimelineSyncState {
    /// No further downloads from the remote storage are needed.
    /// The timeline state is up-to-date or ahead of the remote storage one,
    /// ready to be used in any pageserver operation.
    Ready(Lsn),
    /// Timeline is scheduled for downloading, but its current local state is not up to date with the remote storage.
    /// The timeline is not ready to be used in any pageserver operations, otherwise it might diverge its local state from the remote version,
    /// making it impossible to sync it further.
    AwaitsDownload(Lsn),
    /// Timeline was not in the pageserver's local working directory, but was found on the remote storage, ready to be downloaded.
    /// Cannot be used in any pageserver operations due to complete absence locally.
    CloudOnly(Lsn),
    /// Timeline was evicted from the pageserver's local working directory due to conflicting remote and local states or too many errors during the synchronization.
    /// Such timelines cannot have their state synchronized further and may not have the data about remote timeline's disk_consistent_lsn, since eviction may happen
    /// due to errors before the remote timeline contents is known.
    Evicted(Option<Lsn>),
}

impl TimelineSyncState {
    pub fn remote_disk_consistent_lsn(&self) -> Option<Lsn> {
        Some(match self {
            TimelineSyncState::Evicted(None) => return None,
            TimelineSyncState::Ready(lsn) => lsn,
            TimelineSyncState::AwaitsDownload(lsn) => lsn,
            TimelineSyncState::CloudOnly(lsn) => lsn,
            TimelineSyncState::Evicted(Some(lsn)) => lsn,
        })
        .copied()
    }
}

///
/// Result of performing GC
///
#[derive(Default)]
pub struct GcResult {
    pub layers_total: u64,
    pub layers_needed_by_cutoff: u64,
    pub layers_needed_by_branches: u64,
    pub layers_not_updated: u64,
    pub layers_removed: u64, // # of layer files removed because they have been made obsolete by newer ondisk files.

    pub elapsed: Duration,
}

impl AddAssign for GcResult {
    fn add_assign(&mut self, other: Self) {
        self.layers_total += other.layers_total;
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
    /// NOTE: It is considerd an error to 'get' a key that doesn't exist. The abstraction
    /// above this needs to store suitable metadata to track what data exists with
    /// what keys, in separate metadata entries. If a non-existent key is requested,
    /// the Repository implementation may incorrectly return a value from an ancestore
    /// branch, for exampel, or waste a lot of cycles chasing the non-existing key.
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

    fn create_images(&self, threshold: usize) -> Result<()>;

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

    /// Track end of the latest digested WAL record.
    ///
    /// Advance requires aligned LSN as an argument and would wake wait_lsn() callers.
    /// Previous last record LSN is stored alongside the latest and can be read.
    fn advance_last_record_lsn(&self, lsn: Lsn);
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
        layered_repository::{LayeredRepository, TIMELINES_SEGMENT_NAME},
        walredo::{WalRedoError, WalRedoManager},
    };

    use super::*;
    use hex_literal::hex;
    use zenith_utils::zid::ZTenantId;

    pub const TIMELINE_ID: ZTimelineId =
        ZTimelineId::from_array(hex!("11223344556677881122334455667788"));
    pub const NEW_TIMELINE_ID: ZTimelineId =
        ZTimelineId::from_array(hex!("AA223344556677881122334455667788"));

    /// Convenience function to create a page image with given string as the only content
    #[allow(non_snake_case)]
    pub fn TEST_IMG(s: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        buf.resize(8192, 0);

        buf.freeze()
    }

    lazy_static! {
        static ref LOCK: RwLock<()> = RwLock::new(());
    }

    pub struct RepoHarness<'a> {
        pub conf: &'static PageServerConf,
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
            fs::create_dir_all(&repo_dir.join(TIMELINES_SEGMENT_NAME))?;

            let conf = PageServerConf::dummy_conf(repo_dir);
            // Make a static copy of the config. This can never be free'd, but that's
            // OK in a test.
            let conf: &'static PageServerConf = Box::leak(Box::new(conf));

            let tenant_id = ZTenantId::generate();
            fs::create_dir_all(conf.tenant_path(&tenant_id))?;
            fs::create_dir_all(conf.branches_path(&tenant_id))?;

            Ok(Self {
                conf,
                tenant_id,
                lock_guard,
            })
        }

        pub fn load(&self) -> RepositoryImpl {
            let walredo_mgr = Arc::new(TestRedoManager);

            LayeredRepository::new(self.conf, walredo_mgr, self.tenant_id, false)
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

    #[test]
    fn test_basic() -> Result<()> {
        let repo = RepoHarness::create("test_basic")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        #[allow(non_snake_case)]
        let TEST_KEY: Key = Key::from_hex("112222222233333333444444445500000001").unwrap();

        let writer = tline.writer();
        writer.put(TEST_KEY, Lsn(0x10), Value::Image(TEST_IMG("foo at 0x10")))?;
        writer.advance_last_record_lsn(Lsn(0x10));
        drop(writer);

        let writer = tline.writer();
        writer.put(TEST_KEY, Lsn(0x20), Value::Image(TEST_IMG("foo at 0x20")))?;
        writer.advance_last_record_lsn(Lsn(0x20));
        drop(writer);

        assert_eq!(tline.get(TEST_KEY, Lsn(0x10))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(TEST_KEY, Lsn(0x1f))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(TEST_KEY, Lsn(0x20))?, TEST_IMG("foo at 0x20"));

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
        writer.advance_last_record_lsn(Lsn(0x20));

        writer.put(TEST_KEY_A, Lsn(0x30), test_value("foo at 0x30"))?;
        writer.advance_last_record_lsn(Lsn(0x30));
        writer.put(TEST_KEY_A, Lsn(0x40), test_value("foo at 0x40"))?;
        writer.advance_last_record_lsn(Lsn(0x40));

        //assert_current_logical_size(&tline, Lsn(0x40));

        // Branch the history, modify relation differently on the new timeline
        repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Lsn(0x30))?;
        let newtline = match repo.get_timeline(NEW_TIMELINE_ID)?.local_timeline() {
            Some(timeline) => timeline,
            None => panic!("Should have a local timeline"),
        };
        let new_writer = newtline.writer();
        new_writer.put(TEST_KEY_A, Lsn(0x40), test_value("bar at 0x40"))?;
        new_writer.advance_last_record_lsn(Lsn(0x40));

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

    /* // FIXME: Garbage collection is broken
        #[test]
        fn test_prohibit_branch_creation_on_garbage_collected_data() -> Result<()> {
            let repo =
                RepoHarness::create("test_prohibit_branch_creation_on_garbage_collected_data")?.load_page_repo();

            let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
            make_some_layers(&tline, Lsn(0x20))?;

            // this removes layers before lsn 40 (50 minus 10), so there are two remaining layers, image and delta for 31-50
            repo.gc_iteration(Some(TIMELINE_ID), 0x10, false)?;

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
            let repo = RepoHarness::create("test_prohibit_branch_creation_on_pre_initdb_lsn")?.load_page_repo();

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

        #[test]
        fn test_prohibit_get_page_at_lsn_for_garbage_collected_pages() -> Result<()> {
            let repo =
                RepoHarness::create("test_prohibit_get_page_at_lsn_for_garbage_collected_pages")?
                    .load_page_repo();

            let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
            make_some_layers(&tline, Lsn(0x20))?;

            repo.gc_iteration(Some(TIMELINE_ID), 0x10, false)?;
            let latest_gc_cutoff_lsn = tline.get_latest_gc_cutoff_lsn();
            assert!(*latest_gc_cutoff_lsn > Lsn(0x25));
            // FIXME: GC is currently disabled, so this still works
            /*
            match tline.get_page_at_lsn(TESTREL_A, 0, Lsn(0x25)) {
                Ok(_) => panic!("request for page should have failed"),
                Err(err) => assert!(err.to_string().contains("not found at")),
            }
             */
            Ok(())
        }

        #[test]
        fn test_retain_data_in_parent_which_is_needed_for_child() -> Result<()> {
            let repo =
                RepoHarness::create("test_retain_data_in_parent_which_is_needed_for_child")?.load_page_repo();
            let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
            make_some_layers(&tline, Lsn(0x20))?;

            repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Lsn(0x40))?;
            let newtline = match repo.get_timeline(NEW_TIMELINE_ID)?.local_timeline() {
                Some(timeline) => timeline,
                None => panic!("Should have a local timeline"),
            };

            // this removes layers before lsn 40 (50 minus 10), so there are two remaining layers, image and delta for 31-50
            repo.gc_iteration(Some(TIMELINE_ID), 0x10, false)?;
            assert!(newtline.get_page_at_lsn(TESTREL_A, 0, Lsn(0x25)).is_ok());

            Ok(())
        }

        #[test]
        fn test_parent_keeps_data_forever_after_branching() -> Result<()> {
            let harness = RepoHarness::create("test_parent_keeps_data_forever_after_branching")?;
            let repo = harness.load_page_repo();
            let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
            make_some_layers(&tline, Lsn(0x20))?;

            repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Lsn(0x40))?;
            let newtline = match repo.get_timeline(NEW_TIMELINE_ID)?.local_timeline() {
                Some(timeline) => timeline,
                None => panic!("Should have a local timeline"),
            };

            make_some_layers(&newtline, Lsn(0x60))?;

            // run gc on parent
            repo.gc_iteration(Some(TIMELINE_ID), 0x10, false)?;

            // Check that the data is still accessible on the branch.
            assert_eq!(
                newtline.get_page_at_lsn(TESTREL_A, 0, Lsn(0x50))?,
                TEST_IMG(&format!("foo blk 0 at {}", Lsn(0x40)))
            );

            Ok(())
        }

    */
}
