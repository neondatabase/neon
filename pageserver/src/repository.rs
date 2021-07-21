use crate::object_key::*;
use crate::relish::*;
use crate::ZTimelineId;
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use postgres_ffi::nonrelfile_utils::transaction_id_get_status;
use postgres_ffi::pg_constants;
use postgres_ffi::TransactionId;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::iter::Iterator;
use std::ops::AddAssign;
use std::sync::Arc;
use std::time::Duration;
use zenith_utils::lsn::Lsn;

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

    // TODO get timelines?
    //fn get_stats(&self) -> RepositoryStats;
}

///
/// Result of performing GC
///
#[derive(Default)]
pub struct GcResult {
    // FIXME: These counters make sense for the ObjectRepository. They are not used
    // by the LayeredRepository.
    pub n_relations: u64,
    pub inspected: u64,
    pub truncated: u64,
    pub deleted: u64,
    pub prep_deleted: u64, // 2PC prepare
    pub slru_deleted: u64, // SLRU (clog, multixact)
    pub chkp_deleted: u64, // Checkpoints
    pub dropped: u64,

    // These are used for the LayeredRepository instead
    pub snapshot_relfiles_total: u64,
    pub snapshot_relfiles_needed_by_cutoff: u64,
    pub snapshot_relfiles_needed_by_branches: u64,
    pub snapshot_relfiles_not_updated: u64,
    pub snapshot_relfiles_removed: u64, // # of snapshot files removed because they have been made obsolete by newer snapshot files.
    pub snapshot_relfiles_dropped: u64, // # of snapshot files removed because the relation was dropped

    pub snapshot_nonrelfiles_total: u64,
    pub snapshot_nonrelfiles_needed_by_cutoff: u64,
    pub snapshot_nonrelfiles_needed_by_branches: u64,
    pub snapshot_nonrelfiles_not_updated: u64,
    pub snapshot_nonrelfiles_removed: u64, // # of snapshot files removed because they have been made obsolete by newer snapshot files.
    pub snapshot_nonrelfiles_dropped: u64, // # of snapshot files removed because the relation was dropped

    pub elapsed: Duration,
}

impl AddAssign for GcResult {
    fn add_assign(&mut self, other: Self) {
        self.n_relations += other.n_relations;
        self.truncated += other.truncated;
        self.deleted += other.deleted;
        self.dropped += other.dropped;

        self.snapshot_relfiles_total += other.snapshot_relfiles_total;
        self.snapshot_relfiles_needed_by_cutoff += other.snapshot_relfiles_needed_by_cutoff;
        self.snapshot_relfiles_needed_by_branches += other.snapshot_relfiles_needed_by_branches;
        self.snapshot_relfiles_not_updated += other.snapshot_relfiles_not_updated;
        self.snapshot_relfiles_removed += other.snapshot_relfiles_removed;
        self.snapshot_relfiles_dropped += other.snapshot_relfiles_dropped;

        self.snapshot_nonrelfiles_total += other.snapshot_nonrelfiles_total;
        self.snapshot_nonrelfiles_needed_by_cutoff += other.snapshot_nonrelfiles_needed_by_cutoff;
        self.snapshot_nonrelfiles_needed_by_branches +=
            other.snapshot_nonrelfiles_needed_by_branches;
        self.snapshot_nonrelfiles_not_updated += other.snapshot_nonrelfiles_not_updated;
        self.snapshot_nonrelfiles_removed += other.snapshot_nonrelfiles_removed;
        self.snapshot_nonrelfiles_dropped += other.snapshot_nonrelfiles_dropped;

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

    /// Get size of relation
    fn get_rel_size(&self, tag: RelishTag, lsn: Lsn) -> Result<u32>;

    /// Does relation exist?
    fn get_rel_exists(&self, tag: RelishTag, lsn: Lsn) -> Result<bool>;

    /// Get a list of all distinct relations in given tablespace and database.
    fn list_rels(&self, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelTag>>;

    /// Get a list of non-relational objects
    fn list_nonrels<'a>(&'a self, lsn: Lsn) -> Result<HashSet<RelishTag>>;

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
    fn put_page_image(
        &self,
        tag: RelishTag,
        blknum: u32,
        lsn: Lsn,
        img: Bytes,
        update_meta: bool,
    ) -> Result<()>;

    /// Truncate relation
    fn put_truncation(&self, rel: RelishTag, lsn: Lsn, nblocks: u32) -> Result<()>;

    /// Unlink relation. This method is used for marking dropped relations.
    fn put_unlink(&self, tag: RelishTag, lsn: Lsn) -> Result<()>;

    /// Put raw data
    fn put_raw_data(&self, tag: ObjectTag, lsn: Lsn, data: &[u8]) -> Result<()>;

    // Get object tag greater or equal than specified
    fn get_next_tag(&self, tag: ObjectTag) -> Result<Option<ObjectTag>>;

    /// Remember the all WAL before the given LSN has been processed.
    ///
    /// The WAL receiver calls this after the put_* functions, to indicate that
    /// all WAL before this point has been digested. Before that, if you call
    /// GET on an earlier LSN, it will block.
    fn advance_last_valid_lsn(&self, lsn: Lsn);
    fn get_last_valid_lsn(&self) -> Lsn;
    fn init_valid_lsn(&self, lsn: Lsn);

    /// Like `advance_last_valid_lsn`, but this always points to the end of
    /// a WAL record, not in the middle of one.
    ///
    /// This must be <= last valid LSN. This is tracked separately from last
    /// valid LSN, so that the WAL receiver knows where to restart streaming.
    fn advance_last_record_lsn(&self, lsn: Lsn);
    fn get_last_record_lsn(&self) -> Lsn;

    // Like `advance_last_record_lsn`, but points to the start position of last record
    fn get_prev_record_lsn(&self) -> Lsn;

    ///
    /// Flush to disk all data that was written with the put_* functions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL. We don't
    /// know anything about them here in the repository.
    fn checkpoint(&self) -> Result<()>;

    /// Events for all relations in the timeline.
    /// Contains updates from start up to the last valid LSN
    /// at time of history() call. This lsn can be read via the lsn() function.
    ///
    /// Relation size is increased implicitly and decreased with Truncate updates.
    // TODO ordering guarantee?
    fn history<'a>(&'a self) -> Result<Box<dyn History + 'a>>;

    /// Perform one garbage collection iteration.
    /// Garbage collection is periodically performed by GC thread,
    /// but it can be explicitly requested through page server API.
    ///
    /// `horizon` specifies delta from last LSN to preserve all object versions (PITR interval).
    /// `compact` parameter is used to force compaction of storage.
    /// Some storage implementation are based on LSM tree and require periodic merge (compaction).
    /// Usually storage implementation determines itself when compaction should be performed.
    /// But for GC tests it way be useful to force compaction just after completion of GC iteration
    /// to make sure that all detected garbage is removed.
    /// So right now `compact` is set to true when GC explicitly requested through page srver API,
    /// and is st to false in GC threads which infinitely repeats GC iterations in loop.
    fn gc_iteration(&self, horizon: u64, compact: bool) -> Result<GcResult>;

    // Check transaction status
    fn get_tx_status(&self, xid: TransactionId, lsn: Lsn) -> anyhow::Result<u8> {
        let pageno = xid / pg_constants::CLOG_XACTS_PER_PAGE;
        let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
        let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;

        let clog_page = self.get_page_at_lsn(
            RelishTag::Slru {
                slru: SlruKind::Clog,
                segno,
            },
            rpageno,
            lsn,
        )?;
        let status = transaction_id_get_status(xid, &clog_page[..]);
        Ok(status)
    }
}

pub trait History: Iterator<Item = Result<Modification>> {
    /// The last_valid_lsn at the time of history() call.
    fn lsn(&self) -> Lsn;
}

//
// Structure representing any update operation of object storage.
// It is used to copy object storage content in PUSH method.
//
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Modification {
    pub tag: ObjectTag,
    pub lsn: Lsn,
    pub data: Vec<u8>,
}

impl Modification {
    pub fn new(entry: (ObjectTag, Lsn, Vec<u8>)) -> Modification {
        Modification {
            tag: entry.0,
            lsn: entry.1,
            data: entry.2,
        }
    }
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::layered_repository::LayeredRepository;
    use crate::object_repository::ObjectRepository;
    use crate::object_repository::{ObjectValue, PageEntry, RelationSizeEntry};
    use crate::rocksdb_storage::RocksObjectStore;
    use crate::walredo::{WalRedoError, WalRedoManager};
    use crate::{PageServerConf, RepositoryFormat, ZTenantId};
    use postgres_ffi::pg_constants;
    use std::fs;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::time::Duration;
    use zenith_utils::bin_ser::BeSer;

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

    fn get_test_repo(
        test_name: &str,
        repository_format: RepositoryFormat,
    ) -> Result<Box<dyn Repository>> {
        let repo_dir = PathBuf::from(format!("../tmp_check/test_{}", test_name));
        let _ = fs::remove_dir_all(&repo_dir);
        fs::create_dir_all(&repo_dir)?;
        fs::create_dir_all(&repo_dir.join("timelines"))?;

        let conf = PageServerConf {
            daemonize: false,
            gc_horizon: 64 * 1024 * 1024,
            gc_period: Duration::from_secs(10),
            listen_addr: "127.0.0.1:5430".to_string(),
            superuser: "zenith_admin".to_string(),
            workdir: repo_dir,
            pg_distrib_dir: "".into(),
            repository_format,
        };
        // Make a static copy of the config. This can never be free'd, but that's
        // OK in a test.
        let conf: &'static PageServerConf = Box::leak(Box::new(conf));
        let tenantid = ZTenantId::generate();
        fs::create_dir_all(conf.tenant_path(&tenantid)).unwrap();

        let walredo_mgr = TestRedoManager {};

        let repo: Box<dyn Repository + Sync + Send> = match conf.repository_format {
            RepositoryFormat::Layered => {
                Box::new(LayeredRepository::new(conf, Arc::new(walredo_mgr)))
            }
            RepositoryFormat::RocksDb => {
                let obj_store = RocksObjectStore::create(conf, &tenantid)?;

                Box::new(ObjectRepository::new(
                    conf,
                    Arc::new(obj_store),
                    Arc::new(walredo_mgr),
                    tenantid
                ))
            }
        };

        Ok(repo)
    }

    /// Test get_relsize() and truncation.
    #[test]
    fn test_relsize_rocksdb() -> Result<()> {
        let repo = get_test_repo("test_relsize_rocksdb", RepositoryFormat::RocksDb)?;
        test_relsize(&*repo)
    }

    #[test]
    fn test_relsize_layered() -> Result<()> {
        let repo = get_test_repo("test_relsize_layered", RepositoryFormat::Layered)?;
        test_relsize(&*repo)
    }

    fn test_relsize(repo: &dyn Repository) -> Result<()> {
        // get_timeline() with non-existent timeline id should fail
        //repo.get_timeline("11223344556677881122334455667788");

        // Create timeline to work on
        let timelineid = ZTimelineId::from_str("11223344556677881122334455667788").unwrap();
        let tline = repo.create_empty_timeline(timelineid, Lsn(0))?;

        tline.init_valid_lsn(Lsn(1));
        tline.put_page_image(TESTREL_A, 0, Lsn(2), TEST_IMG("foo blk 0 at 2"), true)?;
        tline.put_page_image(TESTREL_A, 0, Lsn(2), TEST_IMG("foo blk 0 at 2"), true)?;
        tline.put_page_image(TESTREL_A, 0, Lsn(3), TEST_IMG("foo blk 0 at 3"), true)?;
        tline.put_page_image(TESTREL_A, 1, Lsn(4), TEST_IMG("foo blk 1 at 4"), true)?;
        tline.put_page_image(TESTREL_A, 2, Lsn(5), TEST_IMG("foo blk 2 at 5"), true)?;

        tline.advance_last_valid_lsn(Lsn(5));

        // The relation was created at LSN 2, not visible at LSN 1 yet.
        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(1))?, false);
        assert!(tline.get_rel_size(TESTREL_A, Lsn(1)).is_err());

        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(2))?, true);
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(2))?, 1);
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(5))?, 3);

        // Check page contents at each LSN
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 0, Lsn(2))?,
            TEST_IMG("foo blk 0 at 2")
        );

        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 0, Lsn(3))?,
            TEST_IMG("foo blk 0 at 3")
        );

        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 0, Lsn(4))?,
            TEST_IMG("foo blk 0 at 3")
        );
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 1, Lsn(4))?,
            TEST_IMG("foo blk 1 at 4")
        );

        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 0, Lsn(5))?,
            TEST_IMG("foo blk 0 at 3")
        );
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 1, Lsn(5))?,
            TEST_IMG("foo blk 1 at 4")
        );
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 2, Lsn(5))?,
            TEST_IMG("foo blk 2 at 5")
        );

        // Truncate last block
        tline.put_truncation(TESTREL_A, Lsn(6), 2)?;
        tline.advance_last_valid_lsn(Lsn(6));

        // Check reported size and contents after truncation
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(6))?, 2);
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 0, Lsn(6))?,
            TEST_IMG("foo blk 0 at 3")
        );
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 1, Lsn(6))?,
            TEST_IMG("foo blk 1 at 4")
        );

        // should still see the truncated block with older LSN
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(5))?, 3);
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 2, Lsn(5))?,
            TEST_IMG("foo blk 2 at 5")
        );

        Ok(())
    }

    /// Test get_relsize() and truncation with a file larger than 1 GB, so that it's
    /// split into multiple 1 GB segments in Postgres.
    ///
    /// This isn't very interesting with the RocksDb implementation, as we don't pay
    /// any attention to Postgres segment boundaries there.
    #[test]
    fn test_large_rel_rocksdb() -> Result<()> {
        let repo = get_test_repo("test_large_rel_rocksdb", RepositoryFormat::RocksDb)?;
        test_large_rel(&*repo)
    }

    #[test]
    fn test_large_rel_layered() -> Result<()> {
        let repo = get_test_repo("test_large_rel_layered", RepositoryFormat::Layered)?;
        test_large_rel(&*repo)
    }

    fn test_large_rel(repo: &dyn Repository) -> Result<()> {
        let timelineid = ZTimelineId::from_str("11223344556677881122334455667788").unwrap();
        let tline = repo.create_empty_timeline(timelineid, Lsn(0))?;

        tline.init_valid_lsn(Lsn(1));

        let mut lsn = 1;
        for blknum in 0..pg_constants::RELSEG_SIZE + 1 {
            let img = TEST_IMG(&format!("foo blk {} at {}", blknum, Lsn(lsn)));
            lsn += 1;
            tline.put_page_image(TESTREL_A, blknum as u32, Lsn(lsn), img, true)?;
        }
        tline.advance_last_valid_lsn(Lsn(lsn));

        assert_eq!(
            tline.get_rel_size(TESTREL_A, Lsn(lsn))?,
            pg_constants::RELSEG_SIZE + 1
        );

        // Truncate one block
        lsn += 1;
        tline.put_truncation(TESTREL_A, Lsn(lsn), pg_constants::RELSEG_SIZE)?;
        tline.advance_last_valid_lsn(Lsn(lsn));
        assert_eq!(
            tline.get_rel_size(TESTREL_A, Lsn(lsn))?,
            pg_constants::RELSEG_SIZE
        );

        // Truncate another block
        lsn += 1;
        tline.put_truncation(TESTREL_A, Lsn(lsn), pg_constants::RELSEG_SIZE - 1)?;
        tline.advance_last_valid_lsn(Lsn(lsn));
        assert_eq!(
            tline.get_rel_size(TESTREL_A, Lsn(lsn))?,
            pg_constants::RELSEG_SIZE - 1
        );

        Ok(())
    }

    fn skip_nonrel_objects<'a>(
        snapshot: Box<dyn History + 'a>,
    ) -> Result<impl Iterator<Item = <dyn History as Iterator>::Item> + 'a> {
        Ok(snapshot.skip_while(|r| match r {
            Ok(m) => match m.tag {
                ObjectTag::RelationMetadata(_) => false,
                _ => true,
            },
            _ => panic!("Iteration error"),
        }))
    }

    #[test]
    fn test_branch_rocksdb() -> Result<()> {
        let repo = get_test_repo("test_branch_rocksdb", RepositoryFormat::RocksDb)?;
        test_branch(&*repo)
    }

    #[test]
    fn test_branch_layered() -> Result<()> {
        let repo = get_test_repo("test_branch_layered", RepositoryFormat::Layered)?;
        test_branch(&*repo)
    }

    ///
    /// Test branch creation
    ///
    fn test_branch(repo: &dyn Repository) -> Result<()> {
        let timelineid = ZTimelineId::from_str("11223344556677881122334455667788").unwrap();
        let tline = repo.create_empty_timeline(timelineid, Lsn(0))?;

        // Import initial dummy checkpoint record, otherwise the get_timeline() call
        // after branching fails below
        tline.put_page_image(RelishTag::Checkpoint, 0, Lsn(1), ZERO_PAGE.clone(), false)?;

        // Create a relation on the timeline
        tline.init_valid_lsn(Lsn(1));
        tline.put_page_image(TESTREL_A, 0, Lsn(2), TEST_IMG("foo blk 0 at 2"), true)?;
        tline.put_page_image(TESTREL_A, 0, Lsn(3), TEST_IMG("foo blk 0 at 3"), true)?;
        tline.put_page_image(TESTREL_A, 0, Lsn(4), TEST_IMG("foo blk 0 at 4"), true)?;

        // Create another relation
        tline.put_page_image(TESTREL_B, 0, Lsn(2), TEST_IMG("foobar blk 0 at 2"), true)?;

        tline.advance_last_valid_lsn(Lsn(4));

        // Branch the history, modify relation differently on the new timeline
        let newtimelineid = ZTimelineId::from_str("AA223344556677881122334455667788").unwrap();
        repo.branch_timeline(timelineid, newtimelineid, Lsn(3))?;
        let newtline = repo.get_timeline(newtimelineid)?;

        newtline.put_page_image(TESTREL_A, 0, Lsn(4), TEST_IMG("bar blk 0 at 4"), true)?;
        newtline.advance_last_valid_lsn(Lsn(4));

        // Check page contents on both branches
        assert_eq!(
            tline.get_page_at_lsn(TESTREL_A, 0, Lsn(4))?,
            TEST_IMG("foo blk 0 at 4")
        );

        assert_eq!(
            newtline.get_page_at_lsn(TESTREL_A, 0, Lsn(4))?,
            TEST_IMG("bar blk 0 at 4")
        );

        assert_eq!(
            newtline.get_page_at_lsn(TESTREL_B, 0, Lsn(4))?,
            TEST_IMG("foobar blk 0 at 2")
        );

        assert_eq!(newtline.get_rel_size(TESTREL_B, Lsn(4))?, 1);

        Ok(())
    }

    #[test]
    fn test_history_rocksdb() -> Result<()> {
        let repo = get_test_repo("test_history_rocksdb", RepositoryFormat::RocksDb)?;
        test_history(&*repo)
    }
    #[test]
    // TODO: This doesn't work with the layered storage, the functions needed for push/pull
    // functionality haven't been implemented yet.
    #[ignore]
    fn test_history_layered() -> Result<()> {
        let repo = get_test_repo("test_history_layered", RepositoryFormat::Layered)?;
        test_history(&*repo)
    }
    fn test_history(repo: &dyn Repository) -> Result<()> {
        let timelineid = ZTimelineId::from_str("11223344556677881122334455667788").unwrap();
        let tline = repo.create_empty_timeline(timelineid, Lsn(0))?;

        let snapshot = tline.history()?;
        assert_eq!(snapshot.lsn(), Lsn(0));
        let mut snapshot = skip_nonrel_objects(snapshot)?;
        assert_eq!(None, snapshot.next().transpose()?);

        // add a page and advance the last valid LSN
        let rel = TESTREL_A;
        tline.put_page_image(rel, 1, Lsn(1), TEST_IMG("blk 1 @ lsn 1"), true)?;
        tline.advance_last_valid_lsn(Lsn(1));

        let expected_page = Modification {
            tag: ObjectTag::Buffer(rel, 1),
            lsn: Lsn(1),
            data: ObjectValue::ser(&ObjectValue::Page(PageEntry::Page(TEST_IMG(
                "blk 1 @ lsn 1",
            ))))?,
        };
        let expected_init_size = Modification {
            tag: ObjectTag::RelationMetadata(rel),
            lsn: Lsn(1),
            data: ObjectValue::ser(&ObjectValue::RelationSize(RelationSizeEntry::Size(2)))?,
        };
        let expected_trunc_size = Modification {
            tag: ObjectTag::RelationMetadata(rel),
            lsn: Lsn(2),
            data: ObjectValue::ser(&ObjectValue::RelationSize(RelationSizeEntry::Size(0)))?,
        };

        let snapshot = tline.history()?;
        assert_eq!(snapshot.lsn(), Lsn(1));
        let mut snapshot = skip_nonrel_objects(snapshot)?;
        assert_eq!(
            Some(&expected_init_size),
            snapshot.next().transpose()?.as_ref()
        );
        assert_eq!(Some(&expected_page), snapshot.next().transpose()?.as_ref());
        assert_eq!(None, snapshot.next().transpose()?);

        // truncate to zero, but don't advance the last valid LSN
        tline.put_truncation(rel, Lsn(2), 0)?;
        let snapshot = tline.history()?;
        assert_eq!(snapshot.lsn(), Lsn(1));
        let mut snapshot = skip_nonrel_objects(snapshot)?;
        assert_eq!(
            Some(&expected_init_size),
            snapshot.next().transpose()?.as_ref()
        );
        assert_eq!(Some(&expected_page), snapshot.next().transpose()?.as_ref());
        assert_eq!(None, snapshot.next().transpose()?);

        // advance the last valid LSN and the truncation should be observable
        tline.advance_last_valid_lsn(Lsn(2));
        let snapshot = tline.history()?;
        assert_eq!(snapshot.lsn(), Lsn(2));
        let mut snapshot = skip_nonrel_objects(snapshot)?;
        assert_eq!(
            Some(&expected_init_size),
            snapshot.next().transpose()?.as_ref()
        );
        assert_eq!(
            Some(&expected_trunc_size),
            snapshot.next().transpose()?.as_ref()
        );
        assert_eq!(Some(&expected_page), snapshot.next().transpose()?.as_ref());
        assert_eq!(None, snapshot.next().transpose()?);

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
