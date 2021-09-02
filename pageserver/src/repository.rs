use crate::relish::*;
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::ops::AddAssign;
use std::sync::Arc;
use std::time::Duration;
use zenith_utils::lsn::{Lsn, RecordLsn};
use zenith_utils::zid::ZTimelineId;

///
/// A repository corresponds to one .zenith directory. One repository holds multiple
/// timelines, forked off from the same initial call to 'initdb'.
pub trait Repository: Send + Sync {
    /// Get Timeline handle for given zenith timeline ID.
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>>;

    /// Create a new, empty timeline. The caller is responsible for loading data into it
    fn create_empty_timeline(
        &self,
        timelineid: ZTimelineId,
        start_lsn: Lsn,
    ) -> Result<Arc<dyn Timeline>>;

    /// Branch a timeline
    fn branch_timeline(&self, src: ZTimelineId, dst: ZTimelineId, start_lsn: Lsn) -> Result<()>;

    /// perform one garbage collection iteration.
    /// garbage collection is periodically performed by gc thread,
    /// but it can be explicitly requested through page server api.
    ///
    /// 'timelineid' specifies the timeline to GC, or None for all.
    /// `horizon` specifies delta from last lsn to preserve all object versions (pitr interval).
    /// `compact` parameter is used to force compaction of storage.
    /// some storage implementation are based on lsm tree and require periodic merge (compaction).
    /// usually storage implementation determines itself when compaction should be performed.
    /// but for gc tests it way be useful to force compaction just after completion of gc iteration
    /// to make sure that all detected garbage is removed.
    /// so right now `compact` is set to true when gc explicitly requested through page srver api,
    /// and is st to false in gc threads which infinitely repeats gc iterations in loop.
    fn gc_iteration(
        &self,
        timelineid: Option<ZTimelineId>,
        horizon: u64,
        compact: bool,
    ) -> Result<GcResult>;

    // TODO get timelines?
    //fn get_stats(&self) -> RepositoryStats;
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
    pub ondisk_relfiles_removed: u64, // # of layer files removed because they have been made obsolete by newer ondisk files.
    pub ondisk_relfiles_dropped: u64, // # of layer files removed because the relation was dropped

    pub ondisk_nonrelfiles_total: u64,
    pub ondisk_nonrelfiles_needed_by_cutoff: u64,
    pub ondisk_nonrelfiles_needed_by_branches: u64,
    pub ondisk_nonrelfiles_not_updated: u64,
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
        self.ondisk_relfiles_removed += other.ondisk_relfiles_removed;
        self.ondisk_relfiles_dropped += other.ondisk_relfiles_dropped;

        self.ondisk_nonrelfiles_total += other.ondisk_nonrelfiles_total;
        self.ondisk_nonrelfiles_needed_by_cutoff += other.ondisk_nonrelfiles_needed_by_cutoff;
        self.ondisk_nonrelfiles_needed_by_branches += other.ondisk_nonrelfiles_needed_by_branches;
        self.ondisk_nonrelfiles_not_updated += other.ondisk_nonrelfiles_not_updated;
        self.ondisk_nonrelfiles_removed += other.ondisk_nonrelfiles_removed;
        self.ondisk_nonrelfiles_dropped += other.ondisk_nonrelfiles_dropped;

        self.elapsed += other.elapsed;
    }
}

pub trait Timeline: Send + Sync {
    //------------------------------------------------------------------------------
    // Public GET functions
    //------------------------------------------------------------------------------

    /// Look up given page in the cache.
    fn get_page_at_lsn(&self, tag: RelishTag, blknum: u32, lsn: Lsn) -> Result<Bytes>;

    /// Look up given page in the cache.
    fn get_page_at_lsn_nowait(&self, tag: RelishTag, blknum: u32, lsn: Lsn) -> Result<Bytes>;

    /// Get size of a relish
    fn get_relish_size(&self, tag: RelishTag, lsn: Lsn) -> Result<Option<u32>>;

    /// Does relation exist?
    fn get_rel_exists(&self, tag: RelishTag, lsn: Lsn) -> Result<bool>;

    /// Get a list of all distinct relations in given tablespace and database.
    fn list_rels(&self, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelTag>>;

    /// Get a list of non-relational objects
    fn list_nonrels(&self, lsn: Lsn) -> Result<HashSet<RelishTag>>;

    //------------------------------------------------------------------------------
    // Public PUT functions, to update the repository with new page versions.
    //
    // These are called by the WAL receiver to digest WAL records.
    //------------------------------------------------------------------------------

    /// Put a new page version that can be constructed from a WAL record
    ///
    /// This will implicitly extend the relation, if the page is beyond the
    /// current end-of-file.
    fn put_wal_record(&self, tag: RelishTag, blknum: u32, rec: WALRecord) -> Result<()>;

    /// Like put_wal_record, but with ready-made image of the page.
    fn put_page_image(&self, tag: RelishTag, blknum: u32, lsn: Lsn, img: Bytes) -> Result<()>;

    /// Truncate relation
    fn put_truncation(&self, rel: RelishTag, lsn: Lsn, nblocks: u32) -> Result<()>;

    /// Unlink relish.
    /// This method is used for marking dropped relations and truncated SLRU segments
    fn put_unlink(&self, tag: RelishTag, lsn: Lsn) -> Result<()>;

    /// Track end of the latest digested WAL record.
    ///
    /// Advance requires aligned LSN as an argument and would wake wait_lsn() callers.
    /// Previous last record LSN is stored alongside the latest and can be read.
    fn advance_last_record_lsn(&self, lsn: Lsn);
    /// Atomically get both last and prev.
    fn get_last_record_rlsn(&self) -> RecordLsn;
    /// Get last or prev record separately. Same as get_last_record_rlsn().last/prev.
    fn get_last_record_lsn(&self) -> Lsn;
    fn get_prev_record_lsn(&self) -> Lsn;

    ///
    /// Flush to disk all data that was written with the put_* functions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL. We don't
    /// know anything about them here in the repository.
    fn checkpoint(&self) -> Result<()>;
}

#[derive(Clone)]
pub struct RepositoryStats {
    pub num_entries: Lsn,
    pub num_page_images: Lsn,
    pub num_wal_records: Lsn,
    pub num_getpage_requests: Lsn,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WALRecord {
    pub lsn: Lsn, // LSN at the *end* of the record
    pub will_init: bool,
    pub rec: Bytes,
    // Remember the offset of main_data in rec,
    // so that we don't have to parse the record again.
    // If record has no main_data, this offset equals rec.len().
    pub main_data_offset: u32,
}

impl WALRecord {
    pub fn pack(&self, buf: &mut BytesMut) {
        buf.put_u64(self.lsn.0);
        buf.put_u8(self.will_init as u8);
        buf.put_u32(self.main_data_offset);
        buf.put_u32(self.rec.len() as u32);
        buf.put_slice(&self.rec[..]);
    }
    pub fn unpack(buf: &mut Bytes) -> WALRecord {
        let lsn = Lsn::from(buf.get_u64());
        let will_init = buf.get_u8() != 0;
        let main_data_offset = buf.get_u32();
        let mut dst = vec![0u8; buf.get_u32() as usize];
        buf.copy_to_slice(&mut dst);
        WALRecord {
            lsn,
            will_init,
            rec: Bytes::from(dst),
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
    use crate::layered_repository::LayeredRepository;
    use crate::walredo::{WalRedoError, WalRedoManager};
    use crate::PageServerConf;
    use postgres_ffi::pg_constants;
    use std::fs;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::time::Duration;
    use zenith_utils::postgres_backend::AuthType;
    use zenith_utils::zid::ZTenantId;

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

    static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

    fn get_test_repo(test_name: &str) -> Result<Box<dyn Repository>> {
        let repo_dir = PathBuf::from(format!("../tmp_check/test_{}", test_name));
        let _ = fs::remove_dir_all(&repo_dir);
        fs::create_dir_all(&repo_dir)?;
        fs::create_dir_all(&repo_dir.join("timelines"))?;

        let conf = PageServerConf {
            daemonize: false,
            gc_horizon: 64 * 1024 * 1024,
            gc_period: Duration::from_secs(10),
            listen_addr: "127.0.0.1:5430".to_string(),
            http_endpoint_addr: "127.0.0.1:9898".to_string(),
            superuser: "zenith_admin".to_string(),
            workdir: repo_dir,
            pg_distrib_dir: "".into(),
            auth_type: AuthType::Trust,
            auth_validation_public_key_path: None,
        };
        // Make a static copy of the config. This can never be free'd, but that's
        // OK in a test.
        let conf: &'static PageServerConf = Box::leak(Box::new(conf));
        let tenantid = ZTenantId::generate();
        fs::create_dir_all(conf.tenant_path(&tenantid)).unwrap();

        let walredo_mgr = TestRedoManager {};

        let repo = Box::new(LayeredRepository::new(
            conf,
            Arc::new(walredo_mgr),
            tenantid,
        ));

        Ok(repo)
    }

    #[test]
    fn test_relsize() -> Result<()> {
        let repo = get_test_repo("test_relsize")?;
        // get_timeline() with non-existent timeline id should fail
        //repo.get_timeline("11223344556677881122334455667788");

        // Create timeline to work on
        let timelineid = ZTimelineId::from_str("11223344556677881122334455667788").unwrap();
        let tline = repo.create_empty_timeline(timelineid, Lsn(0x00))?;

        tline.put_page_image(TESTREL_A, 0, Lsn(0x20), TEST_IMG("foo blk 0 at 2"))?;
        tline.put_page_image(TESTREL_A, 0, Lsn(0x20), TEST_IMG("foo blk 0 at 2"))?;
        tline.put_page_image(TESTREL_A, 0, Lsn(0x30), TEST_IMG("foo blk 0 at 3"))?;
        tline.put_page_image(TESTREL_A, 1, Lsn(0x40), TEST_IMG("foo blk 1 at 4"))?;
        tline.put_page_image(TESTREL_A, 2, Lsn(0x50), TEST_IMG("foo blk 2 at 5"))?;

        tline.advance_last_record_lsn(Lsn(0x50));

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
        tline.put_truncation(TESTREL_A, Lsn(0x60), 2)?;
        tline.advance_last_record_lsn(Lsn(0x60));

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
        tline.put_truncation(TESTREL_A, Lsn(0x60), 0)?;
        tline.advance_last_record_lsn(Lsn(0x60));
        assert_eq!(tline.get_relish_size(TESTREL_A, Lsn(0x60))?.unwrap(), 0);

        // Extend from 0 to 2 blocks, leaving a gap
        tline.put_page_image(TESTREL_A, 1, Lsn(0x70), TEST_IMG("foo blk 1"))?;
        tline.advance_last_record_lsn(Lsn(0x70));
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

    /// Test get_relsize() and truncation with a file larger than 1 GB, so that it's
    /// split into multiple 1 GB segments in Postgres.
    #[test]
    fn test_large_rel() -> Result<()> {
        let repo = get_test_repo("test_large_rel")?;
        let timelineid = ZTimelineId::from_str("11223344556677881122334455667788").unwrap();
        let tline = repo.create_empty_timeline(timelineid, Lsn(0x00))?;

        let mut lsn = 0x10;
        for blknum in 0..pg_constants::RELSEG_SIZE + 1 {
            let img = TEST_IMG(&format!("foo blk {} at {}", blknum, Lsn(lsn)));
            lsn += 0x10;
            tline.put_page_image(TESTREL_A, blknum as u32, Lsn(lsn), img)?;
        }
        tline.advance_last_record_lsn(Lsn(lsn));

        assert_eq!(
            tline.get_relish_size(TESTREL_A, Lsn(lsn))?.unwrap(),
            pg_constants::RELSEG_SIZE + 1
        );

        // Truncate one block
        lsn += 0x10;
        tline.put_truncation(TESTREL_A, Lsn(lsn), pg_constants::RELSEG_SIZE)?;
        tline.advance_last_record_lsn(Lsn(lsn));
        assert_eq!(
            tline.get_relish_size(TESTREL_A, Lsn(lsn))?.unwrap(),
            pg_constants::RELSEG_SIZE
        );

        // Truncate another block
        lsn += 0x10;
        tline.put_truncation(TESTREL_A, Lsn(lsn), pg_constants::RELSEG_SIZE - 1)?;
        tline.advance_last_record_lsn(Lsn(lsn));
        assert_eq!(
            tline.get_relish_size(TESTREL_A, Lsn(lsn))?.unwrap(),
            pg_constants::RELSEG_SIZE - 1
        );

        // Truncate to 1500, and then truncate all the way down to 0, one block at a time
        // This tests the behavior at segment boundaries
        let mut size: i32 = 3000;
        while size >= 0 {
            lsn += 0x10;
            tline.put_truncation(TESTREL_A, Lsn(lsn), size as u32)?;
            tline.advance_last_record_lsn(Lsn(lsn));
            assert_eq!(
                tline.get_relish_size(TESTREL_A, Lsn(lsn))?.unwrap(),
                size as u32
            );

            size -= 1;
        }

        Ok(())
    }

    ///
    /// Test branch creation
    ///
    #[test]
    fn test_branch() -> Result<()> {
        let repo = get_test_repo("test_branch")?;
        let timelineid = ZTimelineId::from_str("11223344556677881122334455667788").unwrap();
        let tline = repo.create_empty_timeline(timelineid, Lsn(0x00))?;

        // Import initial dummy checkpoint record, otherwise the get_timeline() call
        // after branching fails below
        tline.put_page_image(RelishTag::Checkpoint, 0, Lsn(0x10), ZERO_PAGE.clone())?;

        // Create a relation on the timeline
        tline.put_page_image(TESTREL_A, 0, Lsn(0x20), TEST_IMG("foo blk 0 at 2"))?;
        tline.put_page_image(TESTREL_A, 0, Lsn(0x30), TEST_IMG("foo blk 0 at 3"))?;
        tline.put_page_image(TESTREL_A, 0, Lsn(0x40), TEST_IMG("foo blk 0 at 4"))?;

        // Create another relation
        tline.put_page_image(TESTREL_B, 0, Lsn(0x20), TEST_IMG("foobar blk 0 at 2"))?;

        tline.advance_last_record_lsn(Lsn(0x40));

        // Branch the history, modify relation differently on the new timeline
        let newtimelineid = ZTimelineId::from_str("AA223344556677881122334455667788").unwrap();
        repo.branch_timeline(timelineid, newtimelineid, Lsn(0x30))?;
        let newtline = repo.get_timeline(newtimelineid)?;

        newtline.put_page_image(TESTREL_A, 0, Lsn(0x40), TEST_IMG("bar blk 0 at 4"))?;
        newtline.advance_last_record_lsn(Lsn(0x40));

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

        Ok(())
    }

    // Mock WAL redo manager that doesn't do much
    struct TestRedoManager {}

    impl WalRedoManager for TestRedoManager {
        fn request_redo(
            &self,
            rel: RelishTag,
            blknum: u32,
            lsn: Lsn,
            base_img: Option<Bytes>,
            records: Vec<WALRecord>,
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
