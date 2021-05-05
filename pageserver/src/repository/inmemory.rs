//!
//! Zenith repository implementation that's backed by full base backups (called snapshots)
//! of the PostgreSQL cluster, and the WAL. All the WAL on a timeline is loaded into memory
//! at startup, hence the name "inmemory.rs".
//!

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use log::*;

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::repository::{BufferTag, RelTag, Repository, Timeline, WALRecord};
use crate::waldecoder::{DecodedWALRecord, Oid, XlCreateDatabase, XlSmgrTruncate};
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::ZTimelineId;

use zenith_utils::lsn::{AtomicLsn, Lsn};
use zenith_utils::seqwait::SeqWait;
use postgres_ffi::pg_constants;

mod relfile;

use relfile::RelFileEntry;

// Timeout when waiting or WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(60);

///
/// Repository consists of multiple timelines. Keep them in a hash table.
///
pub struct InMemoryRepository {
    conf: PageServerConf,
    timelines: Mutex<HashMap<ZTimelineId, Arc<InMemoryTimeline>>>,
}

/// Public interface
impl Repository for InMemoryRepository {
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<InMemoryTimeline>> {
        let timelines = self.timelines.lock().unwrap();

        match timelines.get(&timelineid) {
            Some(timeline) => Ok(Arc::clone(&timeline)),
            None => bail!("timeline not found"),
        }
    }

    fn get_or_restore_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<InMemoryTimeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        match timelines.get(&timelineid) {
            Some(timeline) => Ok(timeline.clone()),
            None => {
                let timeline = InMemoryTimeline::new(&self.conf, timelineid);

                // FIXME load on demand
                crate::restore_local_repo::restore_timeline(
                    &self.conf, &timeline, timelineid, false,
                )?;

                let timeline_rc = Arc::new(timeline);
                timelines.insert(timelineid, timeline_rc.clone());
                Ok(timeline_rc)
            }
        }
    }
}

/// Private functions
impl InMemoryRepository {
    pub fn new(conf: &PageServerConf) -> InMemoryRepository {
        InMemoryRepository {
            conf: conf.clone(),
            timelines: Mutex::new(HashMap::new()),
        }
    }
}

pub struct InMemoryTimeline {
    timelineid: ZTimelineId,

    relfiles: Mutex<HashMap<RelTag, Arc<RelFileEntry>>>,

    // WAL redo manager
    walredo_mgr: WalRedoManager,

    // What page versions do we hold in the repository? If we get a
    // request > last_valid_lsn, we need to wait until we receive all
    // the WAL up to the request. The SeqWait provides functions for
    // that. TODO: If we get a request for an old LSN, such that the
    // versions have already been garbage collected away, we should
    // throw an error, but we don't track that currently.
    //
    // last_record_lsn points to the end of last processed WAL record.
    // It can lag behind last_valid_lsn, if the WAL receiver has
    // received some WAL after the end of last record, but not the
    // whole next record yet. In the page cache, we care about
    // last_valid_lsn, but if the WAL receiver needs to restart the
    // streaming, it needs to restart at the end of last record, so we
    // track them separately. last_record_lsn should perhaps be in
    // walreceiver.rs instead of here, but it seems convenient to keep
    // all three values together.
    //
    last_valid_lsn: SeqWait<Lsn>,
    last_record_lsn: AtomicLsn,

    // Counters, for metrics collection.
    pub num_entries: AtomicU64,
    pub num_page_images: AtomicU64,
    pub num_wal_records: AtomicU64,
    pub num_getpage_requests: AtomicU64,
}

impl Timeline for InMemoryTimeline {
    /// Look up given page in the cache.
    fn get_page_at_lsn(&self, tag: BufferTag, lsn: Lsn) -> Result<Bytes> {
        debug!("get_page_at_lsn: {:?} at {}", tag, lsn);
        self.wait_lsn(lsn)?;
        self.get_relfile(tag.rel)
            .get_page_at_lsn(&self.walredo_mgr, tag.blknum, lsn)
    }

    fn get_relsize(&self, rel: RelTag, lsn: Lsn) -> Result<u32> {
        self.wait_lsn(lsn)?;
        let result = self.get_relfile(rel).get_relsize(lsn);
        debug!("get_relsize: {:?} at {} -> {:?}", rel, lsn, result);
        result
    }
    fn get_relsize_exists(&self, rel: RelTag, lsn: Lsn) -> Result<bool> {
        self.wait_lsn(lsn)?;
        let result = self.get_relfile(rel).exists(lsn);

        debug!("get_relsize_exists: {:?} at {} -> {:?}", rel, lsn, result);
        result
    }

    fn put_wal_record(&self, tag: BufferTag, rec: WALRecord) {
        debug!("put_wal_record: {:?} at {:?}", tag, rec);
        self.get_relfile(tag.rel).put_wal_record(tag.blknum, rec);
    }

    fn put_truncation(&self, rel: RelTag, lsn: Lsn, relsize: u32) -> anyhow::Result<()> {
        debug!("put_truncation: {:?} at {}", relsize, lsn);
        self.get_relfile(rel).put_truncation(lsn, relsize)
    }

    fn put_page_image(&self, tag: BufferTag, lsn: Lsn, img: Bytes) {
        debug!("put_page_image: {:?} at {}", tag, lsn);
        self.get_relfile(tag.rel)
            .put_page_image(tag.blknum, lsn, img)
    }

    fn put_create_database(&self, _lsn: Lsn, _db_id: Oid, _tablespace_id: Oid, _src_db_id: Oid, _src_tablespace_id: Oid) -> Result<()> {

        // TODO: Make a copy of everything in the old DB under new DB id
        bail!("CREATE DATABASE not implemented");
    }

    // Process a WAL record. We make a separate copy of it for every block it modifies.
    fn save_decoded_record(
        &self,
        decoded: DecodedWALRecord,
        recdata: Bytes,
        lsn: Lsn) -> anyhow::Result<()>
    {
        for blk in decoded.blocks.iter() {
            let tag = BufferTag {
                rel: RelTag {
                    spcnode: blk.rnode_spcnode,
                    dbnode: blk.rnode_dbnode,
                    relnode: blk.rnode_relnode,
                    forknum: blk.forknum as u8,
                },
                blknum: blk.blkno,
            };

            let rec = WALRecord {
                lsn,
                will_init: blk.will_init || blk.apply_image,
                rec: recdata.clone(),
                main_data_offset: decoded.main_data_offset as u32,
            };

            self.put_wal_record(tag, rec);
        }
        // include truncate wal record in all pages
        if decoded.xl_rmid == pg_constants::RM_SMGR_ID
            && (decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK)
                == pg_constants::XLOG_SMGR_TRUNCATE
        {
            let truncate = XlSmgrTruncate::decode(&decoded);
            if (truncate.flags & pg_constants::SMGR_TRUNCATE_HEAP) != 0 {
                let rel = RelTag {
                    spcnode: truncate.rnode.spcnode,
                    dbnode: truncate.rnode.dbnode,
                    relnode: truncate.rnode.relnode,
                    forknum: pg_constants::MAIN_FORKNUM,
                };
                self.put_truncation(rel, lsn, truncate.blkno)?;
            }
        } else if decoded.xl_rmid == pg_constants::RM_DBASE_ID
            && (decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK)
                == pg_constants::XLOG_DBASE_CREATE
        {
            let createdb = XlCreateDatabase::decode(&decoded);
            self.put_create_database(
                lsn,
                createdb.db_id,
                createdb.tablespace_id,
                createdb.src_db_id,
                createdb.src_tablespace_id,
            )?;
        }
        // Now that this record has been handled, let the page cache know that
        // it is up-to-date to this LSN
        self.advance_last_record_lsn(lsn);
        Ok(())
    }

    /// Remember that WAL has been received and added to the page cache up to the given LSN
    fn advance_last_valid_lsn(&self, lsn: Lsn) {
        let old = self.last_valid_lsn.advance(lsn);

        // Can't move backwards.
        if lsn < old {
            warn!(
                "attempted to move last valid LSN backwards (was {}, new {})",
                old, lsn
            );
        }
    }

    fn init_valid_lsn(&self, lsn: Lsn) {
        let old = self.last_valid_lsn.advance(lsn);
        assert!(old == Lsn(0));
        let old = self.last_record_lsn.fetch_max(lsn);
        assert!(old == Lsn(0));
    }

    fn get_last_valid_lsn(&self) -> Lsn {
        self.last_valid_lsn.load()
    }

    ///
    /// Remember the (end of) last valid WAL record remembered in the page cache.
    ///
    /// NOTE: this updates last_valid_lsn as well.
    ///
    fn advance_last_record_lsn(&self, lsn: Lsn) {
        // Can't move backwards.
        let old = self.last_record_lsn.fetch_max(lsn);
        assert!(old <= lsn);

        // Also advance last_valid_lsn
        let old = self.last_valid_lsn.advance(lsn);
        // Can't move backwards.
        if lsn < old {
            warn!(
                "attempted to move last record LSN backwards (was {}, new {})",
                old, lsn
            );
        }
    }

    fn get_last_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load()
    }
}

impl InMemoryTimeline {
    fn new(conf: &PageServerConf, timelineid: ZTimelineId) -> InMemoryTimeline {
        InMemoryTimeline {
            timelineid,
            relfiles: Mutex::new(HashMap::new()),

            walredo_mgr: WalRedoManager::new(conf, timelineid),

            last_valid_lsn: SeqWait::new(Lsn(0)),
            last_record_lsn: AtomicLsn::new(0),

            num_entries: AtomicU64::new(0),
            num_page_images: AtomicU64::new(0),
            num_wal_records: AtomicU64::new(0),
            num_getpage_requests: AtomicU64::new(0),
        }
    }

    ///
    /// Get a handle to a RelFileEntry
    ///
    fn get_relfile(&self, tag: RelTag) -> Arc<RelFileEntry> {
        // First, look up the relfile
        let mut relfiles = self.relfiles.lock().unwrap();
        if let Some(relentry) = relfiles.get(&tag) {
            relentry.clone()
        } else {
            // No RelFileEntry for this relation yet. Create one.
            let relentry = Arc::new(RelFileEntry::new(self.timelineid, tag));
            relfiles.insert(tag, relentry.clone());

            relentry
        }
    }

    ///
    /// Wait until WAL has been received up to the given LSN.
    ///
    fn wait_lsn(&self, mut lsn: Lsn) -> anyhow::Result<Lsn> {
        // When invalid LSN is requested, it means "don't wait, return latest version of the page"
        // This is necessary for bootstrap.
        if lsn == Lsn(0) {
            let last_valid_lsn = self.last_valid_lsn.load();
            trace!(
                "walreceiver doesn't work yet last_valid_lsn {}, requested {}",
                last_valid_lsn,
                lsn
            );
            lsn = last_valid_lsn;
        }

        self.last_valid_lsn
            .wait_for_timeout(lsn, TIMEOUT)
            .with_context(|| {
                format!(
                    "Timed out while waiting for WAL record at LSN {} to arrive",
                    lsn
                )
            })?;

        Ok(lsn)
    }
}
