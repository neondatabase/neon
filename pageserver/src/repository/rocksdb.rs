//
// A Repository holds all the different page versions and WAL records
//
// This implementation uses RocksDB to store WAL wal records and
// full page images, keyed by the RelFileNode, blocknumber, and the
// LSN.

use crate::repository::{BufferTag, RelTag, Repository, Timeline, WALRecord};
use crate::restore_local_repo::import_timeline_wal;
use crate::waldecoder::Oid;
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::ZTimelineId;
use anyhow::{anyhow, bail, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::*;
use postgres_ffi::pg_constants;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::collections::HashMap;
use std::convert::TryInto;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use zenith_utils::bin_ser::BeSer;
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
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
struct CacheKey {
    pub tag: BufferTag,
    pub lsn: Lsn,
}

//
// In addition to those per-page entries, the 'last_valid_lsn' and 'last_record_lsn'
// values are also persisted in the rocskdb repository. They are stored with CacheKeys
// with ROCKSDB_SPECIAL_FORKNUM, and 'blknum' indicates which value it is. The
// rest of the key fields are zero. We use a CacheKey as the key for these too,
// so that whenever we iterate through keys in the repository, we can safely parse
// the key blob as CacheKey without checking for these special values first.
//
// FIXME: This is quite a similar concept to the special entries created by
// `BufferTag::fork` function. Merge them somehow? These special keys are specific
// to the rocksb implementation, not exposed to the rest of the system, but the
// other special forks created by `BufferTag::fork` are also used elsewhere.
//
impl CacheKey {
    const fn special(id: u32) -> CacheKey {
        CacheKey {
            tag: BufferTag {
                rel: RelTag {
                    forknum: pg_constants::ROCKSDB_SPECIAL_FORKNUM,
                    spcnode: 0,
                    dbnode: 0,
                    relnode: 0,
                },
                blknum: id,
            },
            lsn: Lsn(0),
        }
    }

    fn is_special(&self) -> bool {
        self.tag.rel.forknum == pg_constants::ROCKSDB_SPECIAL_FORKNUM
    }
}

static LAST_VALID_LSN_KEY: CacheKey = CacheKey::special(0);
static LAST_VALID_RECORD_LSN_KEY: CacheKey = CacheKey::special(1);

enum CacheEntryContent {
    PageImage(Bytes),
    WALRecord(WALRecord),
    Truncation,
}

// The serialized representation of a CacheEntryContent begins with
// single byte that indicates what kind of entry it is. There is also
// an UNUSED_VERSION_FLAG that is not represented in the CacheEntryContent
// at all, you must peek into the first byte of the serialized representation
// to read it.
const CONTENT_PAGE_IMAGE: u8 = 1u8;
const CONTENT_WAL_RECORD: u8 = 2u8;
const CONTENT_TRUNCATION: u8 = 3u8;

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

impl RocksRepository {
    fn get_rocks_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<RocksTimeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        match timelines.get(&timelineid) {
            Some(timeline) => Ok(timeline.clone()),
            None => {
                let timeline =
                    RocksTimeline::open(self.conf, timelineid, self.walredo_mgr.clone())?;

                // Load any new WAL after the last checkpoint into the repository.
                info!(
                    "Loading WAL for timeline {} starting at {}",
                    timelineid,
                    timeline.get_last_record_lsn()
                );
                let wal_dir = self.conf.timeline_path(timelineid).join("wal");
                import_timeline_wal(&wal_dir, &timeline, timeline.get_last_record_lsn())?;

                let timeline_rc = Arc::new(timeline);

                if self.conf.gc_horizon != 0 {
                    RocksTimeline::launch_gc_thread(self.conf, timeline_rc.clone());
                }

                timelines.insert(timelineid, timeline_rc.clone());

                Ok(timeline_rc)
            }
        }
    }
}

// Get handle to a given timeline. It is assumed to already exist.
impl Repository for RocksRepository {
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>> {
        Ok(self.get_rocks_timeline(timelineid)?)
    }

    fn create_empty_timeline(
        &self,
        timelineid: ZTimelineId,
        start_lsn: Lsn,
    ) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        let timeline =
            RocksTimeline::create(&self.conf, timelineid, self.walredo_mgr.clone(), start_lsn)?;

        let timeline_rc = Arc::new(timeline);
        let r = timelines.insert(timelineid, timeline_rc.clone());
        assert!(r.is_none());

        // don't start the garbage collector for unit tests, either.

        Ok(timeline_rc)
    }

    fn branch_timeline(&self, src: ZTimelineId, dst: ZTimelineId, at_lsn: Lsn) -> Result<()> {
        let src_timeline = self.get_rocks_timeline(src)?;

        info!("branching at {}", at_lsn);

        let dst_timeline =
            RocksTimeline::create(&self.conf, dst, self.walredo_mgr.clone(), at_lsn)?;

        // Copy all entries <= LSN
        //
        // This is very inefficient, a far cry from the promise of cheap copy-on-write
        // branching. But it will do for now.
        let mut iter = src_timeline.db.raw_iterator();
        iter.seek_to_first();
        while iter.valid() {
            let k = iter.key().unwrap();
            let key = CacheKey::des(k)?;

            if !key.is_special() && key.lsn <= at_lsn {
                let v = iter.value().unwrap();
                dst_timeline.db.put(k, v)?;
            }
            iter.next();
        }
        Ok(())
    }
}

impl RocksTimeline {
    /// common options used by `open` and `create`
    fn get_rocksdb_opts() -> rocksdb::Options {
        let mut opts = rocksdb::Options::default();
        opts.set_use_fsync(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_compaction_filter("ttl", move |_level: u32, _key: &[u8], val: &[u8]| {
            if (val[0] & UNUSED_VERSION_FLAG) != 0 {
                rocksdb::compaction_filter::Decision::Remove
            } else {
                rocksdb::compaction_filter::Decision::Keep
            }
        });
        opts
    }

    /// Open a RocksDB database, and load the last valid and record LSNs into memory.
    fn open(
        conf: &PageServerConf,
        timelineid: ZTimelineId,
        walredo_mgr: Arc<dyn WalRedoManager>,
    ) -> Result<RocksTimeline> {
        let path = conf.timeline_path(timelineid);
        let db = rocksdb::DB::open(&RocksTimeline::get_rocksdb_opts(), path)?;

        // Load these into memory
        let lsnstr = db
            .get(LAST_VALID_LSN_KEY.ser()?)
            .with_context(|| "last_valid_lsn not found in repository")?
            .ok_or(anyhow!("empty last_valid_lsn"))?;
        let last_valid_lsn = Lsn::from_str(std::str::from_utf8(&lsnstr)?)?;
        let lsnstr = db
            .get(LAST_VALID_RECORD_LSN_KEY.ser()?)
            .with_context(|| "last_record_lsn not found in repository")?
            .ok_or(anyhow!("empty last_record_lsn"))?;
        let last_record_lsn = Lsn::from_str(std::str::from_utf8(&lsnstr)?)?;

        let timeline = RocksTimeline {
            db,
            walredo_mgr,

            last_valid_lsn: SeqWait::new(last_valid_lsn),
            last_record_lsn: AtomicLsn::new(last_record_lsn.0),

            num_entries: AtomicU64::new(0),
            num_page_images: AtomicU64::new(0),
            num_wal_records: AtomicU64::new(0),
            num_getpage_requests: AtomicU64::new(0),
        };
        Ok(timeline)
    }

    /// Create a new RocksDB database. It is initally empty, except for the last
    /// valid and last record LSNs, which are set to 'start_lsn'.
    fn create(
        conf: &PageServerConf,
        timelineid: ZTimelineId,
        walredo_mgr: Arc<dyn WalRedoManager>,
        start_lsn: Lsn,
    ) -> Result<RocksTimeline> {
        let path = conf.timeline_path(timelineid);
        let mut opts = RocksTimeline::get_rocksdb_opts();
        opts.create_if_missing(true);
        opts.set_error_if_exists(true);
        let db = rocksdb::DB::open(&opts, path)?;

        let timeline = RocksTimeline {
            db,
            walredo_mgr,

            last_valid_lsn: SeqWait::new(start_lsn),
            last_record_lsn: AtomicLsn::new(start_lsn.0),

            num_entries: AtomicU64::new(0),
            num_page_images: AtomicU64::new(0),
            num_wal_records: AtomicU64::new(0),
            num_getpage_requests: AtomicU64::new(0),
        };
        // Write the initial last_valid/record_lsn values
        timeline.checkpoint()?;
        Ok(timeline)
    }

    fn launch_gc_thread(conf: &'static PageServerConf, timeline_rc: Arc<RocksTimeline>) {
        let timeline_rc_copy = timeline_rc.clone();
        let _gc_thread = thread::Builder::new()
            .name("Garbage collection thread".into())
            .spawn(move || {
                // FIXME
                timeline_rc_copy.do_gc(conf).expect("GC thread died");
            })
            .unwrap();
    }

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
        let key = CacheKey { tag, lsn };
        let mut base_img: Option<Bytes> = None;
        let mut records: Vec<WALRecord> = Vec::new();

        let mut iter = self.db.raw_iterator();
        let serialized_key = key.ser().expect("serialize CacheKey should always succeed");
        iter.seek_for_prev(serialized_key);

        // Scan backwards, collecting the WAL records, until we hit an
        // old page image.
        while iter.valid() {
            let key = CacheKey::des(iter.key().unwrap()).unwrap();
            if key.tag != tag {
                break;
            }
            let content = CacheEntryContent::from_slice(iter.value().unwrap());
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

        let mut key = CacheKey {
            tag: BufferTag {
                rel,
                blknum: u32::MAX,
            },
            lsn,
        };
        let mut iter = self.db.raw_iterator();
        loop {
            iter.seek_for_prev(key.ser()?);
            if iter.valid() {
                let thiskey = CacheKey::des(iter.key().unwrap())?;
                if thiskey.tag.rel == rel {
                    // Ignore entries with later LSNs.
                    if thiskey.lsn > lsn {
                        key.tag.blknum = thiskey.tag.blknum;
                        continue;
                    }

                    let content = CacheEntryContent::from_slice(iter.value().unwrap());
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

    fn do_gc(&self, conf: &'static PageServerConf) -> Result<Bytes> {
        loop {
            thread::sleep(conf.gc_period);
            let last_lsn = self.get_last_valid_lsn();

            // checked_sub() returns None on overflow.
            if let Some(horizon) = last_lsn.checked_sub(conf.gc_horizon) {
                let mut maxkey = CacheKey {
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
                    let mut iter = self.db.raw_iterator();
                    iter.seek_for_prev(maxkey.ser()?);
                    if iter.valid() {
                        let key = CacheKey::des(iter.key().unwrap())?;
                        let v = iter.value().unwrap();

                        inspected += 1;

                        // Construct boundaries for old records cleanup
                        maxkey.tag = key.tag;
                        let last_lsn = key.lsn;
                        maxkey.lsn = min(horizon, last_lsn); // do not remove last version

                        let mut minkey = maxkey.clone();
                        minkey.lsn = Lsn(0); // first version

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
                            self.put_page_image(key.tag, key.lsn, new_img.clone());

                            reconstructed += 1;
                        }

                        iter.seek_for_prev(maxkey.ser()?);
                        if iter.valid() {
                            // do not remove last version
                            if last_lsn > horizon {
                                // locate most recent record before horizon
                                let key = CacheKey::des(iter.key().unwrap())?;
                                if key.tag == maxkey.tag {
                                    let v = iter.value().unwrap();
                                    if (v[0] & CONTENT_KIND_MASK) == CONTENT_WAL_RECORD {
                                        let (base_img, records) =
                                            self.collect_records_for_apply(key.tag, key.lsn);
                                        trace!("Reconstruct horizon page {} blk {} at {} from {} records",
                                              key.tag.rel, key.tag.blknum, key.lsn, records.len());
                                        let new_img = self
                                            .walredo_mgr
                                            .request_redo(key.tag, key.lsn, base_img, records)?;
                                        self.put_page_image(key.tag, key.lsn, new_img.clone());

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
                                let key = CacheKey::des(iter.key().unwrap())?;
                                if key.tag != maxkey.tag {
                                    break;
                                }
                                let v = iter.value().unwrap();
                                if (v[0] & UNUSED_VERSION_FLAG) == 0 {
                                    let mut v = v.to_owned();
                                    v[0] |= UNUSED_VERSION_FLAG;
                                    self.db.put(key.ser()?, &v[..])?;
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

    //
    // Wait until WAL has been received up to the given LSN.
    //
    fn wait_lsn(&self, mut lsn: Lsn) -> Result<Lsn> {
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
        trace!(
            "Start waiting for LSN {}, valid LSN is {}",
            lsn,
            self.last_valid_lsn.load()
        );
        self.last_valid_lsn
            .wait_for_timeout(lsn, TIMEOUT)
            .with_context(|| {
                format!(
                    "Timed out while waiting for WAL record at LSN {} to arrive. valid LSN in {}",
                    lsn,
                    self.last_valid_lsn.load(),
                )
            })?;
        //trace!("Stop waiting for LSN {}, valid LSN is {}", lsn,  self.last_valid_lsn.load());

        Ok(lsn)
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

        let lsn = self.wait_lsn(req_lsn)?;

        // Look up cache entry. If it's a page image, return that. If it's a WAL record,
        // ask the WAL redo service to reconstruct the page image from the WAL records.
        let key = CacheKey { tag, lsn };

        let mut iter = self.db.raw_iterator();
        iter.seek_for_prev(key.ser()?);

        if iter.valid() {
            let key = CacheKey::des(iter.key().unwrap())?;
            if key.tag == tag {
                let content = CacheEntryContent::from_slice(iter.value().unwrap());
                let page_img: Bytes;
                if let CacheEntryContent::PageImage(img) = content {
                    page_img = img;
                } else if let CacheEntryContent::WALRecord(_rec) = content {
                    // Request the WAL redo manager to apply the WAL records for us.
                    let (base_img, records) = self.collect_records_for_apply(tag, lsn);
                    page_img = self.walredo_mgr.request_redo(tag, lsn, base_img, records)?;

                    self.put_page_image(tag, lsn, page_img.clone());
                } else {
                    // No base image, and no WAL record. Huh?
                    bail!("no page image or WAL record for requested page");
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
        static ZERO_PAGE: [u8; 8192] = [0u8; 8192];
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

        let key = CacheKey {
            tag: BufferTag {
                rel,
                blknum: u32::MAX,
            },
            lsn,
        };
        let mut iter = self.db.raw_iterator();
        iter.seek_for_prev(key.ser()?);
        if iter.valid() {
            let key = CacheKey::des(iter.key().unwrap())?;
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
    fn put_wal_record(&self, tag: BufferTag, rec: WALRecord) {
        let lsn = rec.lsn;
        let key = CacheKey { tag, lsn };

        let content = CacheEntryContent::WALRecord(rec);

        let serialized_key = key.ser().expect("serialize CacheKey should always succeed");
        let _res = self.db.put(serialized_key, content.to_bytes());
        trace!(
            "put_wal_record rel {} blk {} at {}",
            tag.rel,
            tag.blknum,
            lsn
        );

        self.num_entries.fetch_add(1, Ordering::Relaxed);
        self.num_wal_records.fetch_add(1, Ordering::Relaxed);
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
            let key = CacheKey {
                tag: BufferTag { rel, blknum },
                lsn,
            };
            trace!("put_wal_record lsn: {}", key.lsn);
            let _res = self.db.put(key.ser()?, content.to_bytes());
        }
        let n = (old_rel_size - nblocks) as u64;
        self.num_entries.fetch_add(n, Ordering::Relaxed);
        self.num_wal_records.fetch_add(n, Ordering::Relaxed);
        Ok(())
    }

    ///
    /// Memorize a full image of a page version
    ///
    fn put_page_image(&self, tag: BufferTag, lsn: Lsn, img: Bytes) {
        let img_len = img.len();
        let key = CacheKey { tag, lsn };
        let content = CacheEntryContent::PageImage(img);

        let mut val_buf = content.to_bytes();

        // Zero size of page image indicates that page can be removed
        if img_len == 0 {
            if (val_buf[0] & UNUSED_VERSION_FLAG) != 0 {
                // records already marked for deletion
                return;
            } else {
                // delete truncated multixact page
                val_buf[0] |= UNUSED_VERSION_FLAG;
            }
        }

        trace!("put_wal_record lsn: {}", key.lsn);
        let serialized_key = key.ser().expect("serialize CacheKey should always succeed");
        let _res = self.db.put(serialized_key, content.to_bytes());

        trace!(
            "put_page_image rel {} blk {} at {}",
            tag.rel,
            tag.blknum,
            lsn
        );
        self.num_page_images.fetch_add(1, Ordering::Relaxed);
    }

    fn put_create_database(
        &self,
        lsn: Lsn,
        db_id: Oid,
        tablespace_id: Oid,
        src_db_id: Oid,
        src_tablespace_id: Oid,
    ) -> Result<()> {
        let key = CacheKey {
            tag: BufferTag {
                rel: RelTag {
                    spcnode: src_tablespace_id,
                    dbnode: src_db_id,
                    relnode: 0,
                    forknum: 0u8,
                },
                blknum: 0,
            },
            lsn: Lsn(0),
        };
        let mut iter = self.db.raw_iterator();
        iter.seek(key.ser()?);
        let mut n = 0;
        while iter.valid() {
            let mut key = CacheKey::des(iter.key().unwrap())?;
            if key.tag.rel.spcnode != src_tablespace_id || key.tag.rel.dbnode != src_db_id {
                break;
            }

            key.tag.rel.spcnode = tablespace_id;
            key.tag.rel.dbnode = db_id;
            key.lsn = lsn;

            let v = iter.value().unwrap();
            self.db.put(key.ser()?, v)?;
            n += 1;
            iter.next();
        }
        info!(
            "Create database {}/{}, copy {} entries",
            tablespace_id, db_id, n
        );
        Ok(())
    }

    /// Remember that WAL has been received and added to the timeline up to the given LSN
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

    ///
    /// Remember the (end of) last valid WAL record remembered for the timeline.
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

    fn init_valid_lsn(&self, lsn: Lsn) {
        let old = self.last_valid_lsn.advance(lsn);
        assert!(old == Lsn(0));
        let old = self.last_record_lsn.fetch_max(lsn);
        assert!(old == Lsn(0));
    }

    fn get_last_valid_lsn(&self) -> Lsn {
        self.last_valid_lsn.load()
    }

    // Flush all the changes written so far with PUT functions to disk.
    // RocksDB writes out things as we go (?), so we don't need to do much here. We just
    // write out the last valid and record LSNs.
    fn checkpoint(&self) -> Result<()> {
        let last_valid_lsn = self.last_valid_lsn.load();
        self.db
            .put(LAST_VALID_LSN_KEY.ser()?, last_valid_lsn.to_string())?;
        self.db.put(
            LAST_VALID_RECORD_LSN_KEY.ser()?,
            self.last_record_lsn.load().to_string(),
        )?;

        trace!("checkpoint at {}", last_valid_lsn);

        Ok(())
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
