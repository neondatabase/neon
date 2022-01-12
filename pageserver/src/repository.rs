use crate::relish::*;
use crate::walrecord::MultiXactMember;
use crate::CheckpointConfig;
use anyhow::Result;
use bytes::Bytes;
use postgres_ffi::{MultiXactId, MultiXactOffset, TransactionId};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::ops::{AddAssign, Deref};
use std::sync::Arc;
use std::time::Duration;
use zenith_utils::lsn::{Lsn, RecordLsn};
use zenith_utils::zid::ZTimelineId;

/// Block number within a relish. This matches PostgreSQL's BlockNumber type.
pub type BlockNumber = u32;

///
/// A repository corresponds to one .zenith directory. One repository holds multiple
/// timelines, forked off from the same initial call to 'initdb'.
pub trait Repository: Send + Sync {
    fn shutdown(&self) -> Result<()>;

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
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<RepositoryTimeline>;

    /// Create a new, empty timeline. The caller is responsible for loading data into it
    /// Initdb lsn is provided for timeline impl to be able to perform checks for some operations against it.
    fn create_empty_timeline(
        &self,
        timelineid: ZTimelineId,
        initdb_lsn: Lsn,
    ) -> Result<Arc<dyn Timeline>>;

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
pub enum RepositoryTimeline {
    /// Timeline, with its files present locally in pageserver's working directory.
    /// Loaded into pageserver's memory and ready to be used.
    Local(Arc<dyn Timeline>),
    /// Timeline, found on the pageserver's remote storage, but not yet downloaded locally.
    Remote {
        id: ZTimelineId,
        /// metadata contents of the latest successfully uploaded checkpoint
        disk_consistent_lsn: Lsn,
        awaits_download: bool,
    },
}

impl RepositoryTimeline {
    pub fn local_timeline(&self) -> Option<Arc<dyn Timeline>> {
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
    pub ondisk_relfiles_total: u64,
    pub ondisk_relfiles_needed_by_cutoff: u64,
    pub ondisk_relfiles_needed_by_branches: u64,
    pub ondisk_relfiles_not_updated: u64,
    pub ondisk_relfiles_needed_as_tombstone: u64,
    pub ondisk_relfiles_removed: u64, // # of layer files removed because they have been made obsolete by newer ondisk files.
    pub ondisk_relfiles_dropped: u64, // # of layer files removed because the relation was dropped

    pub ondisk_nonrelfiles_total: u64,
    pub ondisk_nonrelfiles_needed_by_cutoff: u64,
    pub ondisk_nonrelfiles_needed_by_branches: u64,
    pub ondisk_nonrelfiles_not_updated: u64,
    pub ondisk_nonrelfiles_needed_as_tombstone: u64,
    pub ondisk_nonrelfiles_removed: u64, // # of layer files removed because they have been made obsolete by newer ondisk files.
    pub ondisk_nonrelfiles_dropped: u64, // # of layer files removed because the relation was dropped

    pub elapsed: Duration,
}

impl AddAssign for GcResult {
    fn add_assign(&mut self, other: Self) {
        self.ondisk_relfiles_total += other.ondisk_relfiles_total;
        self.ondisk_relfiles_needed_by_cutoff += other.ondisk_relfiles_needed_by_cutoff;
        self.ondisk_relfiles_needed_by_branches += other.ondisk_relfiles_needed_by_branches;
        self.ondisk_relfiles_not_updated += other.ondisk_relfiles_not_updated;
        self.ondisk_relfiles_needed_as_tombstone += other.ondisk_relfiles_needed_as_tombstone;
        self.ondisk_relfiles_removed += other.ondisk_relfiles_removed;
        self.ondisk_relfiles_dropped += other.ondisk_relfiles_dropped;

        self.ondisk_nonrelfiles_total += other.ondisk_nonrelfiles_total;
        self.ondisk_nonrelfiles_needed_by_cutoff += other.ondisk_nonrelfiles_needed_by_cutoff;
        self.ondisk_nonrelfiles_needed_by_branches += other.ondisk_nonrelfiles_needed_by_branches;
        self.ondisk_nonrelfiles_not_updated += other.ondisk_nonrelfiles_not_updated;
        self.ondisk_nonrelfiles_needed_as_tombstone += other.ondisk_nonrelfiles_needed_as_tombstone;
        self.ondisk_nonrelfiles_removed += other.ondisk_nonrelfiles_removed;
        self.ondisk_nonrelfiles_dropped += other.ondisk_nonrelfiles_dropped;

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

    /// Look up given page version.
    fn get_page_at_lsn(&self, tag: RelishTag, blknum: BlockNumber, lsn: Lsn) -> Result<Bytes>;

    /// Get size of a relish
    fn get_relish_size(&self, tag: RelishTag, lsn: Lsn) -> Result<Option<BlockNumber>>;

    /// Does relation exist?
    fn get_rel_exists(&self, tag: RelishTag, lsn: Lsn) -> Result<bool>;

    /// Get a list of all existing relations
    /// Pass RelTag to get relation objects or None to get nonrels.
    fn list_relishes(&self, tag: Option<RelTag>, lsn: Lsn) -> Result<HashSet<RelishTag>>;

    /// Get a list of all existing relations in given tablespace and database.
    fn list_rels(&self, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelishTag>>;

    /// Get a list of all existing non-relational objects
    fn list_nonrels(&self, lsn: Lsn) -> Result<HashSet<RelishTag>>;

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
    fn get_start_lsn(&self) -> Lsn;
    fn get_disk_consistent_lsn(&self) -> Lsn;

    /// Mutate the timeline with a [`TimelineWriter`].
    fn writer<'a>(&'a self) -> Box<dyn TimelineWriter + 'a>;

    ///
    /// Flush to disk all data that was written with the put_* functions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL. We don't
    /// know anything about them here in the repository.
    fn checkpoint(&self, cconf: CheckpointConfig) -> Result<()>;

    ///
    /// Check that it is valid to request operations with that lsn.
    fn check_lsn_is_in_scope(&self, lsn: Lsn) -> Result<()>;

    /// Retrieve current logical size of the timeline
    ///
    /// NOTE: counted incrementally, includes ancestors,
    /// doesnt support TwoPhase relishes yet
    fn get_current_logical_size(&self) -> usize;

    /// Does the same as get_current_logical_size but counted on demand.
    /// Used in tests to ensure thet incremental and non incremental variants match.
    fn get_current_logical_size_non_incremental(&self, lsn: Lsn) -> Result<usize>;

    /// An escape hatch to allow "casting" a generic Timeline to LayeredTimeline.
    fn upgrade_to_layered_timeline(&self) -> &crate::layered_repository::LayeredTimeline;
}

/// Various functions to mutate the timeline.
// TODO Currently, Deref is used to allow easy access to read methods from this trait.
// This is probably considered a bad practice in Rust and should be fixed eventually,
// but will cause large code changes.
pub trait TimelineWriter: Deref<Target = dyn Timeline> {
    /// Put a new page version that can be constructed from a WAL record
    ///
    /// This will implicitly extend the relation, if the page is beyond the
    /// current end-of-file.
    fn put_wal_record(
        &self,
        lsn: Lsn,
        tag: RelishTag,
        blknum: BlockNumber,
        rec: ZenithWalRecord,
    ) -> Result<()>;

    /// Like put_wal_record, but with ready-made image of the page.
    fn put_page_image(
        &self,
        tag: RelishTag,
        blknum: BlockNumber,
        lsn: Lsn,
        img: Bytes,
    ) -> Result<()>;

    /// Truncate relation
    fn put_truncation(&self, rel: RelishTag, lsn: Lsn, nblocks: BlockNumber) -> Result<()>;

    /// This method is used for marking dropped relations and truncated SLRU files and aborted two phase records
    fn drop_relish(&self, tag: RelishTag, lsn: Lsn) -> Result<()>;

    /// Track end of the latest digested WAL record.
    ///
    /// Advance requires aligned LSN as an argument and would wake wait_lsn() callers.
    /// Previous last record LSN is stored alongside the latest and can be read.
    fn advance_last_record_lsn(&self, lsn: Lsn);
}

/// Each update to a page is represented by a ZenithWalRecord. It can be a wrapper
/// around a PostgreSQL WAL record, or a custom zenith-specific "record".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ZenithWalRecord {
    /// Native PostgreSQL WAL record
    Postgres { will_init: bool, rec: Bytes },

    /// Set bits in heap visibility map. (heap blkno, flag bits to clear)
    ClearVisibilityMapFlags { heap_blkno: u32, flags: u8 },
    /// Mark transaction IDs as committed on a CLOG page
    ClogSetCommitted { xids: Vec<TransactionId> },
    /// Mark transaction IDs as aborted on a CLOG page
    ClogSetAborted { xids: Vec<TransactionId> },
    /// Extend multixact offsets SLRU
    MultixactOffsetCreate {
        mid: MultiXactId,
        moff: MultiXactOffset,
    },
    /// Extend multixact members SLRU.
    MultixactMembersCreate {
        moff: MultiXactOffset,
        members: Vec<MultiXactMember>,
    },
}

impl ZenithWalRecord {
    /// Does replaying this WAL record initialize the page from scratch, or does
    /// it need to be applied over the previous image of the page?
    pub fn will_init(&self) -> bool {
        match self {
            ZenithWalRecord::Postgres { will_init, rec: _ } => *will_init,

            // None of the special zenith record types currently initialize the page
            _ => false,
        }
    }
}

#[cfg(test)]
pub mod repo_harness {
    use bytes::BytesMut;
    use std::{fs, path::PathBuf};

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

    pub struct RepoHarness {
        pub conf: &'static PageServerConf,
        pub tenant_id: ZTenantId,
    }

    impl RepoHarness {
        pub fn create(test_name: &'static str) -> Result<Self> {
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

            Ok(Self { conf, tenant_id })
        }

        pub fn load(&self) -> Box<dyn Repository> {
            let walredo_mgr = Arc::new(TestRedoManager);

            Box::new(LayeredRepository::new(
                self.conf,
                walredo_mgr,
                self.tenant_id,
                false,
            ))
        }

        pub fn timeline_path(&self, timeline_id: &ZTimelineId) -> PathBuf {
            self.conf.timeline_path(timeline_id, &self.tenant_id)
        }
    }

    // Mock WAL redo manager that doesn't do much
    struct TestRedoManager;

    impl WalRedoManager for TestRedoManager {
        fn request_redo(
            &self,
            rel: RelishTag,
            blknum: BlockNumber,
            lsn: Lsn,
            base_img: Option<Bytes>,
            records: Vec<(Lsn, ZenithWalRecord)>,
        ) -> Result<Bytes, WalRedoError> {
            let s = format!(
                "redo for {} blk {} to get to {}, with {} and {} records",
                rel,
                blknum,
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
    use crate::layered_repository::metadata::METADATA_FILE_NAME;

    use super::repo_harness::*;
    use super::*;
    use postgres_ffi::{pg_constants, xlog_utils::SIZEOF_CHECKPOINT};
    use std::fs;

    /// Arbitrary relation tag, for testing.
    const TESTREL_A_REL_TAG: RelTag = RelTag {
        spcnode: 0,
        dbnode: 111,
        relnode: 1000,
        forknum: 0,
    };
    const TESTREL_A: RelishTag = RelishTag::Relation(TESTREL_A_REL_TAG);
    const TESTREL_B: RelishTag = RelishTag::Relation(RelTag {
        spcnode: 0,
        dbnode: 111,
        relnode: 1001,
        forknum: 0,
    });

    fn assert_current_logical_size(timeline: &Arc<dyn Timeline>, lsn: Lsn) {
        let incremental = timeline.get_current_logical_size();
        let non_incremental = timeline
            .get_current_logical_size_non_incremental(lsn)
            .unwrap();
        assert_eq!(incremental, non_incremental);
    }

    static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);
    static ZERO_CHECKPOINT: Bytes = Bytes::from_static(&[0u8; SIZEOF_CHECKPOINT]);

    #[test]
    fn test_relsize() -> Result<()> {
        let repo = RepoHarness::create("test_relsize")?.load();
        // get_timeline() with non-existent timeline id should fail
        //repo.get_timeline("11223344556677881122334455667788");

        // Create timeline to work on
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        let writer = tline.writer();

        writer.put_page_image(TESTREL_A, 0, Lsn(0x20), TEST_IMG("foo blk 0 at 2"))?;
        writer.put_page_image(TESTREL_A, 0, Lsn(0x20), TEST_IMG("foo blk 0 at 2"))?;
        writer.put_page_image(TESTREL_A, 0, Lsn(0x30), TEST_IMG("foo blk 0 at 3"))?;
        writer.put_page_image(TESTREL_A, 1, Lsn(0x40), TEST_IMG("foo blk 1 at 4"))?;
        writer.put_page_image(TESTREL_A, 2, Lsn(0x50), TEST_IMG("foo blk 2 at 5"))?;

        writer.advance_last_record_lsn(Lsn(0x50));

        assert_current_logical_size(&tline, Lsn(0x50));

        // The relation was created at LSN 2, not visible at LSN 1 yet.
        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x10))?, false);
        assert!(tline.get_relish_size(TESTREL_A, Lsn(0x10))?.is_none());

        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x20))?, true);
        assert_eq!(tline.get_relish_size(TESTREL_A, Lsn(0x20))?.unwrap(), 1);
        assert_eq!(tline.get_relish_size(TESTREL_A, Lsn(0x50))?.unwrap(), 3);

        // Check page contents at each LSN
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 0, Lsn(0x20))?,
            TEST_IMG("foo blk 0 at 2")
        );

        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 0, Lsn(0x30))?,
            TEST_IMG("foo blk 0 at 3")
        );

        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 0, Lsn(0x40))?,
            TEST_IMG("foo blk 0 at 3")
        );
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 1, Lsn(0x40))?,
            TEST_IMG("foo blk 1 at 4")
        );

        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 0, Lsn(0x50))?,
            TEST_IMG("foo blk 0 at 3")
        );
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 1, Lsn(0x50))?,
            TEST_IMG("foo blk 1 at 4")
        );
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 2, Lsn(0x50))?,
            TEST_IMG("foo blk 2 at 5")
        );

        // Truncate last block
        writer.put_truncation(TESTREL_A, Lsn(0x60), 2)?;
        writer.advance_last_record_lsn(Lsn(0x60));
        assert_current_logical_size(&tline, Lsn(0x60));

        // Check reported size and contents after truncation
        assert_eq!(tline.get_relish_size(TESTREL_A, Lsn(0x60))?.unwrap(), 2);
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 0, Lsn(0x60))?,
            TEST_IMG("foo blk 0 at 3")
        );
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 1, Lsn(0x60))?,
            TEST_IMG("foo blk 1 at 4")
        );

        // should still see the truncated block with older LSN
        assert_eq!(tline.get_relish_size(TESTREL_A, Lsn(0x50))?.unwrap(), 3);
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 2, Lsn(0x50))?,
            TEST_IMG("foo blk 2 at 5")
        );

        // Truncate to zero length
        writer.put_truncation(TESTREL_A, Lsn(0x68), 0)?;
        writer.advance_last_record_lsn(Lsn(0x68));
        assert_eq!(tline.get_relish_size(TESTREL_A, Lsn(0x68))?.unwrap(), 0);

        // Extend from 0 to 2 blocks, leaving a gap
        writer.put_page_image(TESTREL_A, 1, Lsn(0x70), TEST_IMG("foo blk 1"))?;
        writer.advance_last_record_lsn(Lsn(0x70));
        assert_eq!(tline.get_relish_size(TESTREL_A, Lsn(0x70))?.unwrap(), 2);
        assert_eq!(tline.get_page_at_lsn(TESTREL_A, 0, Lsn(0x70))?, ZERO_PAGE);
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 1, Lsn(0x70))?,
            TEST_IMG("foo blk 1")
        );

        // Extend a lot more, leaving a big gap that spans across segments
        // FIXME: This is currently broken, see https://github.com/zenithdb/zenith/issues/500
        /*
        tline.put_page_image(TESTREL_A, 1500, Lsn(0x80), TEST_IMG("foo blk 1500"))?;
        tline.advance_last_record_lsn(Lsn(0x80));
        assert_eq!(tline.get_relish_size(TESTREL_A, Lsn(0x80))?.unwrap(), 1501);
        for blk in 2..1500 {
            assert_eq!(
                tline.get_page_at_lsn(TESTREL_A, blk, Lsn(0x80))?,
                ZERO_PAGE);
        }
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 1500, Lsn(0x80))?,
            TEST_IMG("foo blk 1500"));
         */

        Ok(())
    }

    // Test what happens if we dropped a relation
    // and then created it again within the same layer.
    #[test]
    fn test_drop_extend() -> Result<()> {
        let repo = RepoHarness::create("test_drop_extend")?.load();

        // Create timeline to work on
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        let writer = tline.writer();

        writer.put_page_image(TESTREL_A, 0, Lsn(0x20), TEST_IMG("foo blk 0 at 2"))?;
        writer.advance_last_record_lsn(Lsn(0x20));

        // Check that rel exists and size is correct
        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x20))?, true);
        assert_eq!(tline.get_relish_size(TESTREL_A, Lsn(0x20))?.unwrap(), 1);

        // Drop relish
        writer.drop_relish(TESTREL_A, Lsn(0x30))?;
        writer.advance_last_record_lsn(Lsn(0x30));

        // Check that rel is not visible anymore
        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x30))?, false);
        assert!(tline.get_relish_size(TESTREL_A, Lsn(0x30))?.is_none());

        // Extend it again
        writer.put_page_image(TESTREL_A, 0, Lsn(0x40), TEST_IMG("foo blk 0 at 4"))?;
        writer.advance_last_record_lsn(Lsn(0x40));

        // Check that rel exists and size is correct
        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x40))?, true);
        assert_eq!(tline.get_relish_size(TESTREL_A, Lsn(0x40))?.unwrap(), 1);

        Ok(())
    }

    // Test what happens if we truncated a relation
    // so that one of its segments was dropped
    // and then extended it again within the same layer.
    #[test]
    fn test_truncate_extend() -> Result<()> {
        let repo = RepoHarness::create("test_truncate_extend")?.load();

        // Create timeline to work on
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        let writer = tline.writer();

        //from storage_layer.rs
        const RELISH_SEG_SIZE: u32 = 10 * 1024 * 1024 / 8192;
        let relsize = RELISH_SEG_SIZE * 2;

        // Create relation with relsize blocks
        for blkno in 0..relsize {
            let lsn = Lsn(0x20);
            let data = format!("foo blk {} at {}", blkno, lsn);
            writer.put_page_image(TESTREL_A, blkno, lsn, TEST_IMG(&data))?;
        }

        writer.advance_last_record_lsn(Lsn(0x20));

        // The relation was created at LSN 2, not visible at LSN 1 yet.
        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x10))?, false);
        assert!(tline.get_relish_size(TESTREL_A, Lsn(0x10))?.is_none());

        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x20))?, true);
        assert_eq!(
            tline.get_relish_size(TESTREL_A, Lsn(0x20))?.unwrap(),
            relsize
        );

        // Check relation content
        for blkno in 0..relsize {
            let lsn = Lsn(0x20);
            let data = format!("foo blk {} at {}", blkno, lsn);
            assert_eq!(
                tline.get_page_at_lsn(TESTREL_A, blkno, lsn)?,
                TEST_IMG(&data)
            );
        }

        // Truncate relation so that second segment was dropped
        // - only leave one page
        writer.put_truncation(TESTREL_A, Lsn(0x60), 1)?;
        writer.advance_last_record_lsn(Lsn(0x60));

        // Check reported size and contents after truncation
        assert_eq!(tline.get_relish_size(TESTREL_A, Lsn(0x60))?.unwrap(), 1);

        for blkno in 0..1 {
            let lsn = Lsn(0x20);
            let data = format!("foo blk {} at {}", blkno, lsn);
            assert_eq!(
                tline.get_page_at_lsn(TESTREL_A, blkno, Lsn(0x60))?,
                TEST_IMG(&data)
            );
        }

        // should still see all blocks with older LSN
        assert_eq!(
            tline.get_relish_size(TESTREL_A, Lsn(0x50))?.unwrap(),
            relsize
        );
        for blkno in 0..relsize {
            let lsn = Lsn(0x20);
            let data = format!("foo blk {} at {}", blkno, lsn);
            assert_eq!(
                tline.get_page_at_lsn(TESTREL_A, blkno, Lsn(0x50))?,
                TEST_IMG(&data)
            );
        }

        // Extend relation again.
        // Add enough blocks to create second segment
        for blkno in 0..relsize {
            let lsn = Lsn(0x80);
            let data = format!("foo blk {} at {}", blkno, lsn);
            writer.put_page_image(TESTREL_A, blkno, lsn, TEST_IMG(&data))?;
        }
        writer.advance_last_record_lsn(Lsn(0x80));

        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x80))?, true);
        assert_eq!(
            tline.get_relish_size(TESTREL_A, Lsn(0x80))?.unwrap(),
            relsize
        );
        // Check relation content
        for blkno in 0..relsize {
            let lsn = Lsn(0x80);
            let data = format!("foo blk {} at {}", blkno, lsn);
            assert_eq!(
                tline.get_page_at_lsn(TESTREL_A, blkno, Lsn(0x80))?,
                TEST_IMG(&data)
            );
        }

        Ok(())
    }

    /// Test get_relsize() and truncation with a file larger than 1 GB, so that it's
    /// split into multiple 1 GB segments in Postgres.
    #[test]
    fn test_large_rel() -> Result<()> {
        let repo = RepoHarness::create("test_large_rel")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        let writer = tline.writer();

        let mut lsn = 0x10;
        for blknum in 0..pg_constants::RELSEG_SIZE + 1 {
            let img = TEST_IMG(&format!("foo blk {} at {}", blknum, Lsn(lsn)));
            lsn += 0x10;
            writer.put_page_image(TESTREL_A, blknum as BlockNumber, Lsn(lsn), img)?;
        }
        writer.advance_last_record_lsn(Lsn(lsn));

        assert_current_logical_size(&tline, Lsn(lsn));

        assert_eq!(
            tline.get_relish_size(TESTREL_A, Lsn(lsn))?.unwrap(),
            pg_constants::RELSEG_SIZE + 1
        );

        // Truncate one block
        lsn += 0x10;
        writer.put_truncation(TESTREL_A, Lsn(lsn), pg_constants::RELSEG_SIZE)?;
        writer.advance_last_record_lsn(Lsn(lsn));
        assert_eq!(
            tline.get_relish_size(TESTREL_A, Lsn(lsn))?.unwrap(),
            pg_constants::RELSEG_SIZE
        );
        assert_current_logical_size(&tline, Lsn(lsn));

        // Truncate another block
        lsn += 0x10;
        writer.put_truncation(TESTREL_A, Lsn(lsn), pg_constants::RELSEG_SIZE - 1)?;
        writer.advance_last_record_lsn(Lsn(lsn));
        assert_eq!(
            tline.get_relish_size(TESTREL_A, Lsn(lsn))?.unwrap(),
            pg_constants::RELSEG_SIZE - 1
        );
        assert_current_logical_size(&tline, Lsn(lsn));

        // Truncate to 1500, and then truncate all the way down to 0, one block at a time
        // This tests the behavior at segment boundaries
        let mut size: i32 = 3000;
        while size >= 0 {
            lsn += 0x10;
            writer.put_truncation(TESTREL_A, Lsn(lsn), size as BlockNumber)?;
            writer.advance_last_record_lsn(Lsn(lsn));
            assert_eq!(
                tline.get_relish_size(TESTREL_A, Lsn(lsn))?.unwrap(),
                size as BlockNumber
            );

            size -= 1;
        }
        assert_current_logical_size(&tline, Lsn(lsn));

        Ok(())
    }

    ///
    /// Test list_rels() function, with branches and dropped relations
    ///
    #[test]
    fn test_list_rels_drop() -> Result<()> {
        let repo = RepoHarness::create("test_list_rels_drop")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        let writer = tline.writer();
        const TESTDB: u32 = 111;

        // Import initial dummy checkpoint record, otherwise the get_timeline() call
        // after branching fails below
        writer.put_page_image(RelishTag::Checkpoint, 0, Lsn(0x10), ZERO_CHECKPOINT.clone())?;

        // Create a relation on the timeline
        writer.put_page_image(TESTREL_A, 0, Lsn(0x20), TEST_IMG("foo blk 0 at 2"))?;

        writer.advance_last_record_lsn(Lsn(0x30));

        // Check that list_rels() lists it after LSN 2, but no before it
        assert!(!tline.list_rels(0, TESTDB, Lsn(0x10))?.contains(&TESTREL_A));
        assert!(tline.list_rels(0, TESTDB, Lsn(0x20))?.contains(&TESTREL_A));
        assert!(tline.list_rels(0, TESTDB, Lsn(0x30))?.contains(&TESTREL_A));

        // Create a branch, check that the relation is visible there
        repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Lsn(0x30))?;
        let newtline = match repo.get_timeline(NEW_TIMELINE_ID)?.local_timeline() {
            Some(timeline) => timeline,
            None => panic!("Should have a local timeline"),
        };
        let new_writer = newtline.writer();

        assert!(newtline
            .list_rels(0, TESTDB, Lsn(0x30))?
            .contains(&TESTREL_A));

        // Drop it on the branch
        new_writer.drop_relish(TESTREL_A, Lsn(0x40))?;
        new_writer.advance_last_record_lsn(Lsn(0x40));

        drop(new_writer);

        // Check that it's no longer listed on the branch after the point where it was dropped
        assert!(newtline
            .list_rels(0, TESTDB, Lsn(0x30))?
            .contains(&TESTREL_A));
        assert!(!newtline
            .list_rels(0, TESTDB, Lsn(0x40))?
            .contains(&TESTREL_A));

        // Run checkpoint and garbage collection and check that it's still not visible
        newtline.checkpoint(CheckpointConfig::Forced)?;
        repo.gc_iteration(Some(NEW_TIMELINE_ID), 0, true)?;

        assert!(!newtline
            .list_rels(0, TESTDB, Lsn(0x40))?
            .contains(&TESTREL_A));

        Ok(())
    }

    ///
    /// Test branch creation
    ///
    #[test]
    fn test_branch() -> Result<()> {
        let repo = RepoHarness::create("test_branch")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        let writer = tline.writer();

        // Import initial dummy checkpoint record, otherwise the get_timeline() call
        // after branching fails below
        writer.put_page_image(RelishTag::Checkpoint, 0, Lsn(0x10), ZERO_CHECKPOINT.clone())?;

        // Create a relation on the timeline
        writer.put_page_image(TESTREL_A, 0, Lsn(0x20), TEST_IMG("foo blk 0 at 2"))?;
        writer.put_page_image(TESTREL_A, 0, Lsn(0x30), TEST_IMG("foo blk 0 at 3"))?;
        writer.put_page_image(TESTREL_A, 0, Lsn(0x40), TEST_IMG("foo blk 0 at 4"))?;

        // Create another relation
        writer.put_page_image(TESTREL_B, 0, Lsn(0x20), TEST_IMG("foobar blk 0 at 2"))?;

        writer.advance_last_record_lsn(Lsn(0x40));
        assert_current_logical_size(&tline, Lsn(0x40));

        // Branch the history, modify relation differently on the new timeline
        repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Lsn(0x30))?;
        let newtline = match repo.get_timeline(NEW_TIMELINE_ID)?.local_timeline() {
            Some(timeline) => timeline,
            None => panic!("Should have a local timeline"),
        };
        let new_writer = newtline.writer();

        new_writer.put_page_image(TESTREL_A, 0, Lsn(0x40), TEST_IMG("bar blk 0 at 4"))?;
        new_writer.advance_last_record_lsn(Lsn(0x40));

        // Check page contents on both branches
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 0, Lsn(0x40))?,
            TEST_IMG("foo blk 0 at 4")
        );

        assert_eq!(
            newtline.get_page_at_lsn(TESTREL_A, 0, Lsn(0x40))?,
            TEST_IMG("bar blk 0 at 4")
        );

        assert_eq!(
            newtline.get_page_at_lsn(TESTREL_B, 0, Lsn(0x40))?,
            TEST_IMG("foobar blk 0 at 2")
        );

        assert_eq!(newtline.get_relish_size(TESTREL_B, Lsn(0x40))?.unwrap(), 1);

        assert_current_logical_size(&tline, Lsn(0x40));

        Ok(())
    }

    fn make_some_layers(tline: &Arc<dyn Timeline>, start_lsn: Lsn) -> Result<()> {
        let mut lsn = start_lsn;
        {
            let writer = tline.writer();
            // Create a relation on the timeline
            writer.put_page_image(
                TESTREL_A,
                0,
                lsn,
                TEST_IMG(&format!("foo blk 0 at {}", lsn)),
            )?;
            lsn += 0x10;
            writer.put_page_image(
                TESTREL_A,
                0,
                lsn,
                TEST_IMG(&format!("foo blk 0 at {}", lsn)),
            )?;
            writer.advance_last_record_lsn(lsn);
        }
        tline.checkpoint(CheckpointConfig::Forced)?;
        {
            let writer = tline.writer();
            lsn += 0x10;
            writer.put_page_image(
                TESTREL_A,
                0,
                lsn,
                TEST_IMG(&format!("foo blk 0 at {}", lsn)),
            )?;
            lsn += 0x10;
            writer.put_page_image(
                TESTREL_A,
                0,
                lsn,
                TEST_IMG(&format!("foo blk 0 at {}", lsn)),
            )?;
            writer.advance_last_record_lsn(lsn);
        }
        tline.checkpoint(CheckpointConfig::Forced)
    }

    #[test]
    fn test_prohibit_branch_creation_on_garbage_collected_data() -> Result<()> {
        let repo =
            RepoHarness::create("test_prohibit_branch_creation_on_garbage_collected_data")?.load();

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
                    .contains("is earlier than initdb lsn"));
            }
        }

        Ok(())
    }

    #[test]
    fn test_prohibit_get_page_at_lsn_for_garbage_collected_pages() -> Result<()> {
        let repo =
            RepoHarness::create("test_prohibit_get_page_at_lsn_for_garbage_collected_pages")?
                .load();

        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        make_some_layers(&tline, Lsn(0x20))?;

        repo.gc_iteration(Some(TIMELINE_ID), 0x10, false)?;

        match tline.get_page_at_lsn(TESTREL_A, 0, Lsn(0x25)) {
            Ok(_) => panic!("request for page should have failed"),
            Err(err) => assert!(err
                .to_string()
                .contains("tried to request a page version that was garbage collected")),
        }
        Ok(())
    }

    #[test]
    fn test_retain_data_in_parent_which_is_needed_for_child() -> Result<()> {
        let repo =
            RepoHarness::create("test_retain_data_in_parent_which_is_needed_for_child")?.load();
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
        let repo = harness.load();
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

        // check that the layer in parent before the branching point is still there
        let tline_dir = harness.conf.timeline_path(&TIMELINE_ID, &harness.tenant_id);

        let expected_image_layer_path = tline_dir.join(format!(
            "rel_{}_{}_{}_{}_{}_{:016X}_{:016X}",
            TESTREL_A_REL_TAG.spcnode,
            TESTREL_A_REL_TAG.dbnode,
            TESTREL_A_REL_TAG.relnode,
            TESTREL_A_REL_TAG.forknum,
            0, // seg is 0
            0x20,
            0x30,
        ));
        assert!(fs::metadata(&expected_image_layer_path).is_ok());

        Ok(())
    }

    #[test]
    fn test_read_beyond_eof() -> Result<()> {
        let harness = RepoHarness::create("test_read_beyond_eof")?;
        let repo = harness.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        make_some_layers(&tline, Lsn(0x20))?;
        {
            let writer = tline.writer();
            writer.put_page_image(
                TESTREL_A,
                0,
                Lsn(0x60),
                TEST_IMG(&format!("foo blk 0 at {}", Lsn(0x50))),
            )?;
            writer.advance_last_record_lsn(Lsn(0x60));
        }

        // Test read before rel creation. Should error out.
        assert!(tline.get_page_at_lsn(TESTREL_A, 1, Lsn(0x10)).is_err());

        // Read block beyond end of relation at different points in time.
        // These reads should fall into different delta, image, and in-memory layers.
        assert_eq!(tline.get_page_at_lsn(TESTREL_A, 1, Lsn(0x20))?, ZERO_PAGE);
        assert_eq!(tline.get_page_at_lsn(TESTREL_A, 1, Lsn(0x25))?, ZERO_PAGE);
        assert_eq!(tline.get_page_at_lsn(TESTREL_A, 1, Lsn(0x30))?, ZERO_PAGE);
        assert_eq!(tline.get_page_at_lsn(TESTREL_A, 1, Lsn(0x35))?, ZERO_PAGE);
        assert_eq!(tline.get_page_at_lsn(TESTREL_A, 1, Lsn(0x40))?, ZERO_PAGE);
        assert_eq!(tline.get_page_at_lsn(TESTREL_A, 1, Lsn(0x45))?, ZERO_PAGE);
        assert_eq!(tline.get_page_at_lsn(TESTREL_A, 1, Lsn(0x50))?, ZERO_PAGE);
        assert_eq!(tline.get_page_at_lsn(TESTREL_A, 1, Lsn(0x55))?, ZERO_PAGE);
        assert_eq!(tline.get_page_at_lsn(TESTREL_A, 1, Lsn(0x60))?, ZERO_PAGE);

        // Test on an in-memory layer with no preceding layer
        {
            let writer = tline.writer();
            writer.put_page_image(
                TESTREL_B,
                0,
                Lsn(0x70),
                TEST_IMG(&format!("foo blk 0 at {}", Lsn(0x70))),
            )?;
            writer.advance_last_record_lsn(Lsn(0x70));
        }
        assert_eq!(tline.get_page_at_lsn(TESTREL_B, 1, Lsn(0x70))?, ZERO_PAGE);

        Ok(())
    }

    #[test]
    fn corrupt_metadata() -> Result<()> {
        const TEST_NAME: &str = "corrupt_metadata";
        let harness = RepoHarness::create(TEST_NAME)?;
        let repo = harness.load();

        repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        drop(repo);

        let metadata_path = harness.timeline_path(&TIMELINE_ID).join(METADATA_FILE_NAME);

        assert!(metadata_path.is_file());

        let mut metadata_bytes = std::fs::read(&metadata_path)?;
        assert_eq!(metadata_bytes.len(), 512);
        metadata_bytes[512 - 4 - 2] ^= 1;
        std::fs::write(metadata_path, metadata_bytes)?;

        let new_repo = harness.load();
        let err = new_repo.get_timeline(TIMELINE_ID).err().unwrap();
        assert_eq!(err.to_string(), "failed to load metadata");
        assert_eq!(
            err.source().unwrap().to_string(),
            "metadata checksum mismatch"
        );

        Ok(())
    }

    #[test]
    fn future_layerfiles() -> Result<()> {
        const TEST_NAME: &str = "future_layerfiles";
        let harness = RepoHarness::create(TEST_NAME)?;
        let repo = harness.load();

        // Create a timeline with disk_consistent_lsn = 8000
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0x8000))?;
        let writer = tline.writer();
        writer.advance_last_record_lsn(Lsn(0x8000));
        drop(writer);
        repo.checkpoint_iteration(CheckpointConfig::Forced)?;
        drop(repo);

        let timeline_path = harness.timeline_path(&TIMELINE_ID);

        let make_empty_file = |filename: &str| -> std::io::Result<()> {
            let path = timeline_path.join(filename);

            assert!(!path.exists());
            std::fs::write(&path, &[])?;

            Ok(())
        };

        // Helper function to check that a relation file exists, and a corresponding
        // <filename>.0.old file does not.
        let assert_exists = |filename: &str| {
            let path = timeline_path.join(filename);
            assert!(path.exists(), "file {} was removed", filename);

            // Check that there is no .old file
            let backup_path = timeline_path.join(format!("{}.0.old", filename));
            assert!(
                !backup_path.exists(),
                "unexpected backup file {}",
                backup_path.display()
            );
        };

        // Helper function to check that a relation file does *not* exists, and a corresponding
        // <filename>.<num>.old file does.
        let assert_is_renamed = |filename: &str, num: u32| {
            let path = timeline_path.join(filename);
            assert!(
                !path.exists(),
                "file {} was not removed as expected",
                filename
            );

            let backup_path = timeline_path.join(format!("{}.{}.old", filename, num));
            assert!(
                backup_path.exists(),
                "backup file {} was not created",
                backup_path.display()
            );
        };

        // These files are considered to be in the future and will be renamed out
        // of the way
        let future_filenames = vec![
            format!("pg_control_0_{:016X}", 0x8001),
            format!("pg_control_0_{:016X}_{:016X}", 0x8001, 0x8008),
        ];
        // But these are not:
        let past_filenames = vec![
            format!("pg_control_0_{:016X}", 0x8000),
            format!("pg_control_0_{:016X}_{:016X}", 0x7000, 0x8001),
        ];

        for filename in future_filenames.iter().chain(past_filenames.iter()) {
            make_empty_file(filename)?;
        }

        // Load the timeline. This will cause the files in the "future" to be renamed
        // away.
        let new_repo = harness.load();
        new_repo.get_timeline(TIMELINE_ID).unwrap();
        drop(new_repo);

        for filename in future_filenames.iter() {
            assert_is_renamed(filename, 0);
        }
        for filename in past_filenames.iter() {
            assert_exists(filename);
        }

        // Create the future files again, and load again. They should be renamed to
        // *.1.old this time.
        for filename in future_filenames.iter() {
            make_empty_file(filename)?;
        }

        let new_repo = harness.load();
        new_repo.get_timeline(TIMELINE_ID).unwrap();
        drop(new_repo);

        for filename in future_filenames.iter() {
            assert_is_renamed(filename, 0);
            assert_is_renamed(filename, 1);
        }
        for filename in past_filenames.iter() {
            assert_exists(filename);
        }

        Ok(())
    }
}
