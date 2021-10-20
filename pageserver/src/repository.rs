use crate::relish::*;
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::ops::{AddAssign, Deref};
use std::sync::Arc;
use std::time::Duration;
use zenith_utils::lsn::{Lsn, RecordLsn};
use zenith_utils::zid::ZTimelineId;

///
/// A repository corresponds to one .zenith directory. One repository holds multiple
/// timelines, forked off from the same initial call to 'initdb'.
pub trait Repository: Send + Sync {
    fn shutdown(&self) -> Result<()>;

    /// Get Timeline handle for given zenith timeline ID.
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>>;

    /// Create a new, empty timeline. The caller is responsible for loading data into it
    fn create_empty_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>>;

    /// Branch a timeline
    fn branch_timeline(&self, src: ZTimelineId, dst: ZTimelineId, start_lsn: Lsn) -> Result<()>;

    /// perform one garbage collection iteration.
    /// garbage collection is periodically performed by gc thread,
    /// but it can be explicitly requested through page server api.
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
    fn get_page_at_lsn(&self, tag: RelishTag, blknum: u32, lsn: Lsn) -> Result<Bytes>;

    /// Get size of a relish
    fn get_relish_size(&self, tag: RelishTag, lsn: Lsn) -> Result<Option<u32>>;

    /// Does relation exist?
    fn get_rel_exists(&self, tag: RelishTag, lsn: Lsn) -> Result<bool>;

    /// Get a list of all existing relations
    /// Pass RelTag to get relation objects or None to get nonrels.
    fn list_relishes(&self, tag: Option<RelTag>, lsn: Lsn) -> Result<HashSet<RelishTag>>;

    /// Get a list of all existing relations in given tablespace and database.
    fn list_rels(&self, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelishTag>>;

    /// Get a list of all existing non-relational objects
    fn list_nonrels(&self, lsn: Lsn) -> Result<HashSet<RelishTag>>;

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

    /// Mutate the timeline with a [`TimelineWriter`].
    fn writer<'a>(&'a self) -> Box<dyn TimelineWriter + 'a>;

    ///
    /// Flush to disk all data that was written with the put_* functions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL. We don't
    /// know anything about them here in the repository.
    fn checkpoint(&self) -> Result<()>;

    /// Retrieve current logical size of the timeline
    ///
    /// NOTE: counted incrementally, includes ancestors,
    /// doesnt support TwoPhase relishes yet
    fn get_current_logical_size(&self) -> usize;

    /// Does the same as get_current_logical_size but counted on demand.
    /// Used in tests to ensure thet incremental and non incremental variants match.
    fn get_current_logical_size_non_incremental(&self, lsn: Lsn) -> Result<usize>;
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
    fn put_wal_record(&self, lsn: Lsn, tag: RelishTag, blknum: u32, rec: WALRecord) -> Result<()>;

    /// Like put_wal_record, but with ready-made image of the page.
    fn put_page_image(&self, tag: RelishTag, blknum: u32, lsn: Lsn, img: Bytes) -> Result<()>;

    /// Truncate relation
    fn put_truncation(&self, rel: RelishTag, lsn: Lsn, nblocks: u32) -> Result<()>;

    /// This method is used for marking dropped relations and truncated SLRU files and aborted two phase records
    fn drop_relish(&self, tag: RelishTag, lsn: Lsn) -> Result<()>;

    /// Track end of the latest digested WAL record.
    ///
    /// Advance requires aligned LSN as an argument and would wake wait_lsn() callers.
    /// Previous last record LSN is stored alongside the latest and can be read.
    fn advance_last_record_lsn(&self, lsn: Lsn);
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WALRecord {
    pub will_init: bool,
    pub rec: Bytes,
    // Remember the offset of main_data in rec,
    // so that we don't have to parse the record again.
    // If record has no main_data, this offset equals rec.len().
    pub main_data_offset: u32,
}

impl WALRecord {
    pub fn pack(&self, buf: &mut BytesMut) {
        buf.put_u8(self.will_init as u8);
        buf.put_u32(self.main_data_offset);
        buf.put_u32(self.rec.len() as u32);
        buf.put_slice(&self.rec[..]);
    }
    pub fn unpack(buf: &mut Bytes) -> WALRecord {
        let will_init = buf.get_u8() != 0;
        let main_data_offset = buf.get_u32();
        let rec_len = buf.get_u32() as usize;
        let rec = buf.split_to(rec_len);
        WALRecord {
            will_init,
            rec,
            main_data_offset,
        }
    }
}

///
/// Tests that should work the same with any Repository/Timeline implementation.
///
#[allow(clippy::bool_assert_comparison)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::layered_repository::{LayeredRepository, METADATA_FILE_NAME};
    use crate::walredo::{WalRedoError, WalRedoManager};
    use crate::PageServerConf;
    use hex_literal::hex;
    use postgres_ffi::pg_constants;
    use postgres_ffi::xlog_utils::SIZEOF_CHECKPOINT;
    use std::fs;
    use std::path::PathBuf;
    use zenith_utils::zid::ZTenantId;

    const TIMELINE_ID: ZTimelineId =
        ZTimelineId::from_array(hex!("11223344556677881122334455667788"));
    const NEW_TIMELINE_ID: ZTimelineId =
        ZTimelineId::from_array(hex!("AA223344556677881122334455667788"));

    /// Arbitrary relation tag, for testing.
    const TESTREL_A: RelishTag = RelishTag::Relation(RelTag {
        spcnode: 0,
        dbnode: 111,
        relnode: 1000,
        forknum: 0,
    });
    const TESTREL_B: RelishTag = RelishTag::Relation(RelTag {
        spcnode: 0,
        dbnode: 111,
        relnode: 1001,
        forknum: 0,
    });

    /// Convenience function to create a page image with given string as the only content
    #[allow(non_snake_case)]
    fn TEST_IMG(s: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        buf.resize(8192, 0);

        buf.freeze()
    }

    fn assert_current_logical_size(timeline: &Arc<dyn Timeline>, lsn: Lsn) {
        let incremental = timeline.get_current_logical_size();
        let non_incremental = timeline
            .get_current_logical_size_non_incremental(lsn)
            .unwrap();
        assert_eq!(incremental, non_incremental);
    }

    static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);
    static ZERO_CHECKPOINT: Bytes = Bytes::from_static(&[0u8; SIZEOF_CHECKPOINT]);

    struct RepoHarness {
        conf: &'static PageServerConf,
        tenant_id: ZTenantId,
    }

    impl RepoHarness {
        fn create(test_name: &'static str) -> Result<Self> {
            let repo_dir = PageServerConf::test_repo_dir(test_name);
            let _ = fs::remove_dir_all(&repo_dir);
            fs::create_dir_all(&repo_dir)?;
            fs::create_dir_all(&repo_dir.join("timelines"))?;

            let conf = PageServerConf::dummy_conf(repo_dir);
            // Make a static copy of the config. This can never be free'd, but that's
            // OK in a test.
            let conf: &'static PageServerConf = Box::leak(Box::new(conf));

            let tenant_id = ZTenantId::generate();
            fs::create_dir_all(conf.tenant_path(&tenant_id))?;

            Ok(Self { conf, tenant_id })
        }

        fn load(&self) -> Box<dyn Repository> {
            let walredo_mgr = Arc::new(TestRedoManager);

            Box::new(LayeredRepository::new(
                self.conf,
                walredo_mgr,
                self.tenant_id,
                false,
            ))
        }

        fn timeline_path(&self, timeline_id: &ZTimelineId) -> PathBuf {
            self.conf.timeline_path(timeline_id, &self.tenant_id)
        }
    }

    #[test]
    fn test_relsize() -> Result<()> {
        let repo = RepoHarness::create("test_relsize")?.load();
        // get_timeline() with non-existent timeline id should fail
        //repo.get_timeline("11223344556677881122334455667788");

        // Create timeline to work on
        let tline = repo.create_empty_timeline(TIMELINE_ID)?;
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
        let tline = repo.create_empty_timeline(TIMELINE_ID)?;
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
        let tline = repo.create_empty_timeline(TIMELINE_ID)?;
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
        let tline = repo.create_empty_timeline(TIMELINE_ID)?;
        let writer = tline.writer();

        let mut lsn = 0x10;
        for blknum in 0..pg_constants::RELSEG_SIZE + 1 {
            let img = TEST_IMG(&format!("foo blk {} at {}", blknum, Lsn(lsn)));
            lsn += 0x10;
            writer.put_page_image(TESTREL_A, blknum as u32, Lsn(lsn), img)?;
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
            writer.put_truncation(TESTREL_A, Lsn(lsn), size as u32)?;
            writer.advance_last_record_lsn(Lsn(lsn));
            assert_eq!(
                tline.get_relish_size(TESTREL_A, Lsn(lsn))?.unwrap(),
                size as u32
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
        let tline = repo.create_empty_timeline(TIMELINE_ID)?;
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
        let newtline = repo.get_timeline(NEW_TIMELINE_ID)?;
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
        newtline.checkpoint()?;
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
        let tline = repo.create_empty_timeline(TIMELINE_ID)?;
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
        let newtline = repo.get_timeline(NEW_TIMELINE_ID)?;
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

    #[test]
    fn corrupt_metadata() -> Result<()> {
        const TEST_NAME: &str = "corrupt_metadata";
        let harness = RepoHarness::create(TEST_NAME)?;
        let repo = harness.load();

        repo.create_empty_timeline(TIMELINE_ID)?;
        drop(repo);

        let metadata_path = harness.timeline_path(&TIMELINE_ID).join(METADATA_FILE_NAME);

        assert!(metadata_path.is_file());

        let mut metadata_bytes = std::fs::read(&metadata_path)?;
        assert_eq!(metadata_bytes.len(), 512);
        metadata_bytes[512 - 4 - 2] ^= 1;
        std::fs::write(metadata_path, metadata_bytes)?;

        let new_repo = harness.load();
        let err = new_repo.get_timeline(TIMELINE_ID).err().unwrap();
        assert!(err.to_string().contains("checksum"));

        Ok(())
    }

    #[test]
    fn future_layerfiles() -> Result<()> {
        const TEST_NAME: &str = "future_layerfiles";
        let harness = RepoHarness::create(TEST_NAME)?;
        let repo = harness.load();

        repo.create_empty_timeline(TIMELINE_ID)?;
        drop(repo);

        let timeline_path = harness.timeline_path(&TIMELINE_ID);

        let make_empty_file = |filename: &str| -> std::io::Result<()> {
            let path = timeline_path.join(filename);

            assert!(!path.exists());
            std::fs::write(&path, &[])?;

            Ok(())
        };

        let image_filename = format!("pg_control_0_{:016X}", 8000);
        let delta_filename = format!("pg_control_0_{:016X}_{:016X}", 8000, 8008);

        make_empty_file(&image_filename)?;
        make_empty_file(&delta_filename)?;

        let new_repo = harness.load();
        new_repo.get_timeline(TIMELINE_ID).unwrap();
        drop(new_repo);

        let check_old = |filename: &str, num: u32| {
            let path = timeline_path.join(filename);
            assert!(!path.exists());

            let backup_path = timeline_path.join(format!("{}.{}.old", filename, num));
            assert!(backup_path.exists());
        };

        check_old(&image_filename, 0);
        check_old(&delta_filename, 0);

        make_empty_file(&image_filename)?;
        make_empty_file(&delta_filename)?;

        let new_repo = harness.load();
        new_repo.get_timeline(TIMELINE_ID).unwrap();
        drop(new_repo);

        check_old(&image_filename, 0);
        check_old(&delta_filename, 0);
        check_old(&image_filename, 1);
        check_old(&delta_filename, 1);

        Ok(())
    }

    // Mock WAL redo manager that doesn't do much
    struct TestRedoManager;

    impl WalRedoManager for TestRedoManager {
        fn request_redo(
            &self,
            rel: RelishTag,
            blknum: u32,
            lsn: Lsn,
            base_img: Option<Bytes>,
            records: Vec<(Lsn, WALRecord)>,
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
