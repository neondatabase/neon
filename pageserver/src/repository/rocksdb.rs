//
// A Repository holds all the different page versions and WAL records
//
// This implementation uses RocksDB to store WAL wal records and
// full page images, keyed by the RelFileNode, blocknumber, and the
// LSN.

use crate::repository::{
    BufferTag, RelTag, Repository, RepositoryIterator, RepositoryKey, Timeline, WALRecord,
};
use crate::restore_local_repo::restore_timeline;
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::ZTimelineId;
// use crate::PageServerConf;
// use crate::branches;
use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::*;
use postgres_ffi::*;
use std::cmp::min;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use zenith_utils::lsn::{AtomicLsn, Lsn};
use zenith_utils::seqwait::SeqWait;

// Timeout when waiting or WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(600);

pub struct RocksRepository {
    conf: &'static PageServerConf,
    timelines: Mutex<HashMap<ZTimelineId, Arc<RocksTimeline>>>,

    walredo_mgr: Arc<dyn WalRedoManager>,
}

pub struct RocksTimeline {
    // RocksDB handle
    db: rocksdb::DB,

    // WAL redo manager
    walredo_mgr: Arc<dyn WalRedoManager>,

    // What page versions do we hold in the cache? If we get a request > last_valid_lsn,
    // we need to wait until we receive all the WAL up to the request. The SeqWait
    // provides functions for that. TODO: If we get a request for an old LSN, such that
    // the versions have already been garbage collected away, we should throw an error,
    // but we don't track that currently.
    //
    // last_record_lsn points to the end of last processed WAL record.
    // It can lag behind last_valid_lsn, if the WAL receiver has received some WAL
    // after the end of last record, but not the whole next record yet. In the
    // page cache, we care about last_valid_lsn, but if the WAL receiver needs to
    // restart the streaming, it needs to restart at the end of last record, so
    // we track them separately. last_record_lsn should perhaps be in
    // walreceiver.rs instead of here, but it seems convenient to keep all three
    // values together.
    //
    last_valid_lsn: SeqWait<Lsn>,
    last_record_lsn: AtomicLsn,

    // Counters, for metrics collection.
    pub num_entries: AtomicU64,
    pub num_page_images: AtomicU64,
    pub num_wal_records: AtomicU64,
    pub num_getpage_requests: AtomicU64,
}

//
// We store two kinds of entries in the repository:
//
// 1. Ready-made images of the block
// 2. WAL records, to be applied on top of the "previous" entry
//
// Some WAL records will initialize the page from scratch. For such records,
// the 'will_init' flag is set. They don't need the previous page image before
// applying. The 'will_init' flag is set for records containing a full-page image,
// and for records with the BKPBLOCK_WILL_INIT flag. These differ from PageImages
// stored directly in the cache entry in that you still need to run the WAL redo
// routine to generate the page image.
//

#[derive(Debug)]
enum CacheEntryContent {
    PageImage(Bytes),
    WALRecord(WALRecord),
    Truncation,
    Drop,
}

// The serialized representation of a CacheEntryContent begins with
// single byte that indicates what kind of entry it is. There is also
// an UNUSED_VERSION_FLAG that is not represented in the CacheEntryContent
// at all, you must peek into the first byte of the serialized representation
// to read it.
const CONTENT_PAGE_IMAGE: u8 = 0u8;
const CONTENT_WAL_RECORD: u8 = 1u8;
const CONTENT_TRUNCATION: u8 = 2u8;
const CONTENT_DROP: u8 = 3u8;

const CONTENT_KIND_MASK: u8 = 3u8; // bitmask that covers the above

const UNUSED_VERSION_FLAG: u8 = 4u8;

impl CacheEntryContent {
    pub fn pack(&self, buf: &mut BytesMut) {
        match self {
            CacheEntryContent::PageImage(image) => {
                buf.put_u8(CONTENT_PAGE_IMAGE);
                buf.put_u16(image.len() as u16);
                buf.put_slice(&image[..]);
            }
            CacheEntryContent::WALRecord(rec) => {
                buf.put_u8(CONTENT_WAL_RECORD);
                rec.pack(buf);
            }
            CacheEntryContent::Truncation => {
                buf.put_u8(CONTENT_TRUNCATION);
            }
            CacheEntryContent::Drop => {
                buf.put_u8(CONTENT_DROP);
            }
        }
    }
    pub fn unpack(buf: &mut Bytes) -> CacheEntryContent {
        let kind = buf.get_u8() & CONTENT_KIND_MASK;

        match kind {
            CONTENT_PAGE_IMAGE => {
                let len = buf.get_u16() as usize;
                let mut dst = vec![0u8; len];
                buf.copy_to_slice(&mut dst);
                CacheEntryContent::PageImage(Bytes::from(dst))
            }
            CONTENT_WAL_RECORD => CacheEntryContent::WALRecord(WALRecord::unpack(buf)),
            CONTENT_TRUNCATION => CacheEntryContent::Truncation,
            CONTENT_DROP => CacheEntryContent::Drop,
            _ => unreachable!(),
        }
    }

    fn from_slice(slice: &[u8]) -> Self {
        let mut buf = Bytes::copy_from_slice(slice);
        Self::unpack(&mut buf)
    }

    fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        self.pack(&mut buf);
        buf
    }
}

impl RocksRepository {
    pub fn new(
        conf: &'static PageServerConf,
        walredo_mgr: Arc<dyn WalRedoManager>,
    ) -> RocksRepository {
        RocksRepository {
            conf,
            timelines: Mutex::new(HashMap::new()),
            walredo_mgr,
        }
    }
}
struct RocksIterator<'a> {
    iter: rocksdb::DBRawIterator<'a>,
}

impl<'a> RepositoryIterator for RocksIterator<'a> {
    fn next(&mut self) {
        self.iter.next()
    }
    fn prev(&mut self) {
        self.iter.prev()
    }
    fn valid(&self) -> bool {
        self.iter.valid()
    }
    fn first(&mut self, key: &RepositoryKey) {
        self.iter.seek(key.to_bytes())
    }
    fn last(&mut self, key: &RepositoryKey) {
        self.iter.seek_for_prev(key.to_bytes())
    }
    fn key(&self) -> RepositoryKey {
        RepositoryKey::from_slice(self.iter.key().unwrap())
    }
    fn value(&self) -> &[u8] {
        self.iter.value().unwrap()
    }
}

// Get handle to a given timeline. It is assumed to already exist.
impl Repository for RocksRepository {
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>> {
        let timelines = self.timelines.lock().unwrap();

        match timelines.get(&timelineid) {
            Some(timeline) => Ok(timeline.clone()),
            None => bail!("timeline not found"),
        }
    }

    fn get_or_restore_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        match timelines.get(&timelineid) {
            Some(timeline) => Ok(timeline.clone()),
            None => {
                let timeline = RocksTimeline::new(self.conf, timelineid, self.walredo_mgr.clone());

                restore_timeline(self.conf, &timeline, timelineid)?;

                let timeline_rc = Arc::new(timeline);

                timelines.insert(timelineid, timeline_rc.clone());

                if self.conf.gc_horizon != 0 {
                    let timeline_rc_copy = timeline_rc.clone();
                    let conf = self.conf;
                    let _gc_thread = thread::Builder::new()
                        .name("Garbage collection thread".into())
                        .spawn(move || {
                            // FIXME
                            timeline_rc_copy.do_gc(conf).expect("GC thread died");
                        })
                        .unwrap();
                }
                Ok(timeline_rc)
            }
        }
    }

    #[cfg(test)]
    fn create_empty_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        let timeline = RocksTimeline::new(&self.conf, timelineid, self.walredo_mgr.clone());

        let timeline_rc = Arc::new(timeline);
        let r = timelines.insert(timelineid, timeline_rc.clone());
        assert!(r.is_none());

        // don't start the garbage collector for unit tests, either.

        Ok(timeline_rc)
    }
}

impl RocksTimeline {
    fn open_rocksdb(conf: &PageServerConf, timelineid: ZTimelineId) -> rocksdb::DB {
        let path = conf.timeline_path(timelineid);
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_use_fsync(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_compaction_filter("ttl", move |_level: u32, key: &[u8], val: &[u8]| {
            if (val[0] & UNUSED_VERSION_FLAG) != 0 {
                trace!("Delete unused version {:?}", RepositoryKey::from_slice(key));
                rocksdb::compaction_filter::Decision::Remove
            } else {
                rocksdb::compaction_filter::Decision::Keep
            }
        });
        rocksdb::DB::open(&opts, &path).unwrap()
    }

    fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        walredo_mgr: Arc<dyn WalRedoManager>,
    ) -> RocksTimeline {
        RocksTimeline {
            db: RocksTimeline::open_rocksdb(conf, timelineid),

            walredo_mgr,

            last_valid_lsn: SeqWait::new(Lsn(0)),
            last_record_lsn: AtomicLsn::new(0),

            num_entries: AtomicU64::new(0),
            num_page_images: AtomicU64::new(0),
            num_wal_records: AtomicU64::new(0),
            num_getpage_requests: AtomicU64::new(0),
        }
    }
}

impl RocksTimeline {
    ///
    /// Collect all the WAL records that are needed to reconstruct a page
    /// image for the given cache entry.
    ///
    /// Returns an old page image (if any), and a vector of WAL records to apply
    /// over it.
    ///
    fn collect_records_for_apply(
        &self,
        tag: BufferTag,
        lsn: Lsn,
    ) -> (Option<Bytes>, Vec<WALRecord>) {
        let key = RepositoryKey { tag, lsn };
        let mut base_img: Option<Bytes> = None;
        let mut records: Vec<WALRecord> = Vec::new();

        let mut iter = self.iterator();
        iter.last(&key);

        // Scan backwards, collecting the WAL records, until we hit an
        // old page image.
        while iter.valid() {
            let key = iter.key();
            if key.tag != tag {
                break;
            }
            let content = CacheEntryContent::from_slice(iter.value());
            if let CacheEntryContent::PageImage(img) = content {
                // We have a base image. No need to dig deeper into the list of
                // records
                base_img = Some(img);
                break;
            } else if let CacheEntryContent::WALRecord(rec) = content {
                records.push(rec.clone());
                // If this WAL record initializes the page, no need to dig deeper.
                if rec.will_init {
                    break;
                }
            } else {
                panic!("no base image and no WAL record on cache entry");
            }
            iter.prev();
        }
        records.reverse();
        (base_img, records)
    }

    // Internal functions

    //
    // Internal function to get relation size at given LSN.
    //
    // The caller must ensure that WAL has been received up to 'lsn'.
    //
    fn relsize_get_nowait(&self, rel: RelTag, lsn: Lsn) -> Result<u32> {
        assert!(lsn <= self.last_valid_lsn.load());

        let mut key = RepositoryKey {
            tag: BufferTag {
                rel,
                blknum: u32::MAX,
            },
            lsn,
        };
        let mut iter = self.iterator();
        loop {
            iter.last(&key);
            if iter.valid() {
                let thiskey = iter.key();
                if thiskey.tag.rel == rel {
                    // Ignore entries with later LSNs.
                    if thiskey.lsn > lsn {
                        key.tag.blknum = thiskey.tag.blknum;
                        continue;
                    }

                    let content = CacheEntryContent::from_slice(iter.value());
                    if let CacheEntryContent::Truncation = content {
                        if thiskey.tag.blknum > 0 {
                            key.tag.blknum = thiskey.tag.blknum - 1;
                            continue;
                        }
                        break;
                    }
                    let relsize = thiskey.tag.blknum + 1;
                    debug!("Size of relation {} at {} is {}", rel, lsn, relsize);
                    return Ok(relsize);
                }
            }
            break;
        }
        debug!("Size of relation {} at {} is zero", rel, lsn);
        Ok(0)
    }

    ///
    /// Drop relations with all its forks or non-relational file
    ///
    fn drop(&self, tag: BufferTag) -> Result<()> {
        let mut iter = self.iterator();
        let mut key = RepositoryKey {
            tag,
            lsn: Lsn(u64::MAX),
        };
        if tag.rel.forknum == pg_constants::MAIN_FORKNUM {
            // if it is relation then remove all its blocks in all forks
            key.tag.blknum = u32::MAX;
            key.tag.rel.forknum = pg_constants::INIT_FORKNUM;
        } else {
            assert!(tag.rel.forknum > pg_constants::INIT_FORKNUM);
        }
        debug!("Drop relation {:?}", tag);
        iter.last(&key);
        while iter.valid() {
            let key = iter.key();
            if key.tag.rel.relnode != tag.rel.relnode
                || key.tag.rel.spcnode != tag.rel.spcnode
                || key.tag.rel.dbnode != tag.rel.dbnode
                || (key.tag.rel.forknum != tag.rel.forknum
                    && tag.rel.forknum != pg_constants::MAIN_FORKNUM)
            {
                // no more entries belonging to this relation or file
                break;
            }
            let v = iter.value();
            if (v[0] & UNUSED_VERSION_FLAG) == 0 {
                let mut v = v.to_owned();
                v[0] |= UNUSED_VERSION_FLAG;
                self.db.put(key.to_bytes(), &v[..])?;
            } else {
                // already marked for deletion
                break;
            }
            iter.prev();
        }
        Ok(())
    }

    fn do_gc(&self, conf: &'static PageServerConf) -> Result<Bytes> {
        loop {
            thread::sleep(conf.gc_period);
            trace!("Start GC iteration");
            let last_lsn = self.get_last_valid_lsn();

            // checked_sub() returns None on overflow.
            if let Some(horizon) = last_lsn.checked_sub(conf.gc_horizon) {
                let mut maxkey = RepositoryKey {
                    tag: BufferTag {
                        rel: RelTag {
                            spcnode: u32::MAX,
                            dbnode: u32::MAX,
                            relnode: u32::MAX,
                            forknum: u8::MAX,
                        },
                        blknum: u32::MAX,
                    },
                    lsn: Lsn::MAX,
                };
                let now = Instant::now();
                let mut reconstructed = 0u64;
                let mut truncated = 0u64;
                let mut inspected = 0u64;
                let mut deleted = 0u64;
                loop {
                    let mut iter = self.iterator();
                    iter.last(&maxkey);
                    if iter.valid() {
                        let key = iter.key();
                        let v = iter.value();
                        let flag = v[0];
                        let last_lsn = key.lsn;

                        inspected += 1;

                        // Construct boundaries for old records cleanup
                        maxkey.tag = key.tag;
                        maxkey.lsn = min(horizon, last_lsn); // do not remove last version

                        let mut minkey = maxkey.clone();
                        minkey.lsn = Lsn(0); // first version

                        if (flag & CONTENT_KIND_MASK) == CONTENT_DROP {
                            // If drop record in over the horizon then delete all entries from repository
                            if last_lsn < horizon {
                                self.drop(key.tag)?;
                            }
                            maxkey = minkey;
                            continue;
                        }

                        // Special handling of PREPARE transaction WAL record.
                        if last_lsn < horizon
                            && key.tag.rel.forknum == pg_constants::PG_TWOPHASE_FORKNUM
                        {
                            let xid = key.tag.blknum;
                            // We can not remove information about uncompleted prepared transaction
                            // even if it is over horizon
                            if self.get_tx_status(xid, horizon)?
                                != pg_constants::TRANSACTION_STATUS_IN_PROGRESS
                            {
                                if (v[0] & UNUSED_VERSION_FLAG) == 0 {
                                    let mut v = v.to_owned();
                                    v[0] |= UNUSED_VERSION_FLAG;
                                    self.db.put(key.to_bytes(), &v[..])?;
                                    deleted += 1;
                                }
                            }
                            maxkey = minkey;
                            continue;
                        }
                        // reconstruct most recent page version
                        if (v[0] & CONTENT_KIND_MASK) == CONTENT_WAL_RECORD {
                            // force reconstruction of most recent page version
                            let (base_img, records) =
                                self.collect_records_for_apply(key.tag, key.lsn);

                            trace!(
                                "Reconstruct most recent page {} blk {} at {} from {} records",
                                key.tag.rel,
                                key.tag.blknum,
                                key.lsn,
                                records.len()
                            );

                            let new_img = self
                                .walredo_mgr
                                .request_redo(key.tag, key.lsn, base_img, records)?;
                            self.put_page_image(key.tag, key.lsn, new_img.clone())?;

                            reconstructed += 1;
                        }

                        iter.last(&maxkey);
                        if iter.valid() {
                            // do not remove last version
                            if last_lsn > horizon {
                                // locate most recent record before horizon
                                let key = iter.key();
                                if key.tag == maxkey.tag {
                                    let v = iter.value();
                                    if (v[0] & CONTENT_KIND_MASK) == CONTENT_WAL_RECORD {
                                        let (base_img, records) =
                                            self.collect_records_for_apply(key.tag, key.lsn);
                                        trace!("Reconstruct horizon page {} blk {} at {} from {} records",
                                              key.tag.rel, key.tag.blknum, key.lsn, records.len());
                                        let new_img = self
                                            .walredo_mgr
                                            .request_redo(key.tag, key.lsn, base_img, records)?;
                                        self.put_page_image(key.tag, key.lsn, new_img.clone())?;

                                        truncated += 1;
                                    } else {
                                        trace!(
                                            "Keeping horizon page {} blk {} at {}",
                                            key.tag.rel,
                                            key.tag.blknum,
                                            key.lsn
                                        );
                                    }
                                }
                            } else {
                                trace!(
                                    "Last page {} blk {} at {}, horizon {}",
                                    key.tag.rel,
                                    key.tag.blknum,
                                    key.lsn,
                                    horizon
                                );
                            }
                            // remove records prior to horizon
                            loop {
                                iter.prev();
                                if !iter.valid() {
                                    break;
                                }
                                let key = iter.key();
                                if key.tag != maxkey.tag {
                                    break;
                                }
                                let v = iter.value();
                                if (v[0] & UNUSED_VERSION_FLAG) == 0 {
                                    let mut v = v.to_owned();
                                    v[0] |= UNUSED_VERSION_FLAG;
                                    self.db.put(key.to_bytes(), &v[..])?;
                                    deleted += 1;
                                    trace!(
                                        "deleted: {} blk {} at {}",
                                        key.tag.rel,
                                        key.tag.blknum,
                                        key.lsn
                                    );
                                } else {
                                    break;
                                }
                            }
                        }
                        maxkey = minkey;
                    } else {
                        break;
                    }
                }
                info!("Garbage collection completed in {:?}:\n{} version chains inspected, {} pages reconstructed, {} version histories truncated, {} versions deleted",
					  now.elapsed(), inspected, reconstructed, truncated, deleted);
            }
        }
    }
}

impl Timeline for RocksTimeline {
    // Public GET interface functions

    ///
    /// GetPage@LSN
    ///
    /// Returns an 8k page image
    ///
    fn get_page_at_lsn(&self, tag: BufferTag, req_lsn: Lsn) -> Result<Bytes> {
        self.num_getpage_requests.fetch_add(1, Ordering::Relaxed);
        const ZERO_PAGE: [u8; 8192] = [0u8; 8192];

        let lsn = self.wait_lsn(req_lsn)?;

        // Look up cache entry. If it's a page image, return that. If it's a WAL record,
        // ask the WAL redo service to reconstruct the page image from the WAL records.
        let key = RepositoryKey { tag, lsn };

        let mut iter = self.iterator();
        iter.last(&key);

        if iter.valid() {
            let key = iter.key();
            if key.tag == tag {
                let content = CacheEntryContent::from_slice(iter.value());
                let page_img: Bytes;
                if let CacheEntryContent::PageImage(img) = content {
                    page_img = img;
                } else if let CacheEntryContent::WALRecord(_rec) = content {
                    // Request the WAL redo manager to apply the WAL records for us.
                    let (base_img, records) = self.collect_records_for_apply(tag, lsn);
                    page_img = self.walredo_mgr.request_redo(tag, lsn, base_img, records)?;

                    self.put_page_image(tag, lsn, page_img.clone())?;
                } else if let CacheEntryContent::Truncation = content {
                    // Because FSM pages may be not up-to-date we can request truncated page
                    return Ok(Bytes::from_static(&ZERO_PAGE));
                } else {
                    // No base image, and no WAL record. Huh?
                    bail!(
                        "no page image or WAL record for requested page {} blk {} at lsn {}({}), content={:?}",
                        tag.rel,
                        tag.blknum,
                        req_lsn,
                        lsn,
						content
                    );
                }
                // FIXME: assumes little-endian. Only used for the debugging log though
                let page_lsn_hi =
                    u32::from_le_bytes(page_img.get(0..4).unwrap().try_into().unwrap());
                let page_lsn_lo =
                    u32::from_le_bytes(page_img.get(4..8).unwrap().try_into().unwrap());
                debug!(
                    "Returning page with LSN {:X}/{:X} for {} blk {}",
                    page_lsn_hi, page_lsn_lo, tag.rel, tag.blknum
                );
                return Ok(page_img);
            }
        }
        debug!(
            "Page {} blk {} at {}({}) not found",
            tag.rel, tag.blknum, req_lsn, lsn
        );
        Ok(Bytes::from_static(&ZERO_PAGE))
        /* return Err("could not find page image")?; */
    }

    ///
    /// Get size of relation at given LSN.
    ///
    fn get_relsize(&self, rel: RelTag, lsn: Lsn) -> Result<u32> {
        let lsn = self.wait_lsn(lsn)?;
        self.relsize_get_nowait(rel, lsn)
    }

    ///
    /// Does relation exist at given LSN?
    ///
    /// FIXME: this actually returns true, if the relation exists at *any* LSN
    fn get_relsize_exists(&self, rel: RelTag, req_lsn: Lsn) -> Result<bool> {
        let lsn = self.wait_lsn(req_lsn)?;

        let key = RepositoryKey {
            tag: BufferTag {
                rel,
                blknum: u32::MAX,
            },
            lsn,
        };
        let mut iter = self.iterator();
        iter.last(&key);
        if iter.valid() {
            let key = iter.key();
            if key.tag.rel == rel {
                debug!("Relation {} exists at {}", rel, lsn);
                return Ok(true);
            }
        }
        debug!("Relation {} doesn't exist at {}", rel, lsn);
        Ok(false)
    }

    // Other public functions, for updating the repository.
    // These are used by the WAL receiver and WAL redo.

    ///
    /// Adds a WAL record to the repository
    ///
    fn put_wal_record(&self, tag: BufferTag, rec: WALRecord) -> Result<()> {
        let lsn = rec.lsn;
        let key = RepositoryKey { tag, lsn };

        let content = CacheEntryContent::WALRecord(rec);

        let _res = self.db.put(key.to_bytes(), content.to_bytes())?;
        trace!(
            "put_wal_record rel {} blk {} at {}",
            tag.rel,
            tag.blknum,
            lsn
        );

        self.num_entries.fetch_add(1, Ordering::Relaxed);
        self.num_wal_records.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    ///
    /// Put drop record which should completely delete relation with all its forks
    /// or non-relational file from repository
    ///
    fn put_drop(&self, tag: BufferTag, lsn: Lsn) -> Result<()> {
        let key = RepositoryKey { tag, lsn };
        let content = CacheEntryContent::Drop;
        let _res = self.db.put(key.to_bytes(), content.to_bytes())?;
        Ok(())
    }

    ///
    /// Adds a relation-wide WAL record (like truncate) to the repository,
    /// associating it with all pages started with specified block number
    ///
    fn put_truncation(&self, rel: RelTag, lsn: Lsn, nblocks: u32) -> Result<()> {
        // What was the size of the relation before this record?
        let last_lsn = self.last_valid_lsn.load();
        let old_rel_size = self.relsize_get_nowait(rel, last_lsn)?;

        let content = CacheEntryContent::Truncation;
        // set new relation size
        trace!("Truncate relation {} to {} blocks at {}", rel, nblocks, lsn);

        for blknum in nblocks..old_rel_size {
            let key = RepositoryKey {
                tag: BufferTag { rel, blknum },
                lsn,
            };
            trace!("put_wal_record lsn: {}", key.lsn);
            let _res = self.db.put(key.to_bytes(), content.to_bytes())?;
        }
        let n = (old_rel_size - nblocks) as u64;
        self.num_entries.fetch_add(n, Ordering::Relaxed);
        self.num_wal_records.fetch_add(n, Ordering::Relaxed);
        Ok(())
    }

    ///
    /// Get page image at particular LSN
    ///
    fn get_page_image(&self, tag: BufferTag, lsn: Lsn) -> Result<Option<Bytes>> {
        let key = RepositoryKey { tag, lsn };
        if let Some(bytes) = self.db.get(key.to_bytes())? {
            let content = CacheEntryContent::from_slice(&bytes);
            if let CacheEntryContent::PageImage(img) = content {
                return Ok(Some(img));
            }
        }
        Ok(None)
    }

    ///
    /// Memorize a full image of a page version
    ///
    fn put_page_image(&self, tag: BufferTag, lsn: Lsn, img: Bytes) -> Result<()> {
        let key = RepositoryKey { tag, lsn };
        let content = CacheEntryContent::PageImage(img);

        trace!("put_wal_record lsn: {}", key.lsn);
        let _res = self.db.put(key.to_bytes(), content.to_bytes())?;

        trace!(
            "put_page_image rel {} blk {} at {}",
            tag.rel,
            tag.blknum,
            lsn
        );
        self.num_page_images.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn iterator(&self) -> Box<dyn RepositoryIterator + '_> {
        Box::new(RocksIterator {
            iter: self.db.raw_iterator(),
        })
    }

    fn put_raw_data(&self, key: RepositoryKey, data: &[u8]) -> Result<()> {
        self.db.put(key.to_bytes(), data)?;
        Ok(())
    }

    /// Remember that WAL has been received and added to the timeline up to the given LSN
    fn advance_last_valid_lsn(&self, lsn: Lsn) {
        let lsn = Lsn((lsn.0 + 7) & !7); // align position on 8 bytes
        let old = self.last_valid_lsn.advance(lsn);

        // Can't move backwards.
        if lsn < old {
            warn!(
                "attempted to move last valid LSN backwards (was {}, new {})",
                old, lsn
            );
        }
    }

    ///
    /// Remember the (end of) last valid WAL record remembered for the timeline.
    ///
    /// NOTE: this updates last_valid_lsn as well.
    ///
    fn advance_last_record_lsn(&self, lsn: Lsn) {
        let lsn = Lsn((lsn.0 + 7) & !7); // align position on 8 bytes
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

    fn init_valid_lsn(&self, lsn: Lsn) {
        let old = self.last_valid_lsn.advance(lsn);
        assert!(old == Lsn(0));
        let old = self.last_record_lsn.fetch_max(lsn);
        assert!(old == Lsn(0));
    }

    fn get_last_valid_lsn(&self) -> Lsn {
        self.last_valid_lsn.load()
    }

    //
    // Wait until WAL has been received up to the given LSN.
    //
    fn wait_lsn(&self, lsn: Lsn) -> Result<Lsn> {
        let mut lsn = lsn;
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
        //trace!("Start waiting for LSN {}, valid LSN is {}", lsn,  self.last_valid_lsn.load());
        self.last_valid_lsn
            .wait_for_timeout(lsn, TIMEOUT)
            .with_context(|| {
                format!(
                    "Timed out while waiting for WAL record at LSN {} to arrive",
                    lsn
                )
            })?;
        //trace!("Stop waiting for LSN {}, valid LSN is {}", lsn,  self.last_valid_lsn.load());

        Ok(lsn)
    }

    //
    // Get statistics to be displayed in the user interface.
    //
    // FIXME
    /*
    fn get_stats(&self) -> TimelineStats {
        TimelineStats {
            num_entries: self.num_entries.load(Ordering::Relaxed),
            num_page_images: self.num_page_images.load(Ordering::Relaxed),
            num_wal_records: self.num_wal_records.load(Ordering::Relaxed),
            num_getpage_requests: self.num_getpage_requests.load(Ordering::Relaxed),
        }
    }
    */
}
