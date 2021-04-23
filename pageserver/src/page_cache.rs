//
// Page Cache holds all the different page versions and WAL records
//
// The Page Cache is currenusing RocksDB for storing wal records and full page images, keyed by the RelFileNode, blocknumber, and the LSN.
//

use crate::restore_local_repo::restore_timeline;
use crate::waldecoder::Oid;
use crate::ZTimelineId;
use crate::{walredo, zenith_repo_dir, PageServerConf};
use anyhow::{bail, Context};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crossbeam_channel::unbounded;
use crossbeam_channel::{Receiver, Sender};
use lazy_static::lazy_static;
use log::*;
use rocksdb;
use std::cmp::min;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;
use std::{convert::TryInto, ops::AddAssign};
use zenith_utils::seqwait::SeqWait;

// Timeout when waiting or WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(60);

pub struct PageCache {
    shared: Mutex<PageCacheShared>,

    // RocksDB handle
    db: rocksdb::DB,

    // Channel for communicating with the WAL redo process here.
    pub walredo_sender: Sender<Arc<CacheEntry>>,
    pub walredo_receiver: Receiver<Arc<CacheEntry>>,

    // Allows .await on the arrival of a particular LSN.
    seqwait_lsn: SeqWait,

    // Counters, for metrics collection.
    pub num_entries: AtomicU64,
    pub num_page_images: AtomicU64,
    pub num_wal_records: AtomicU64,
    pub num_getpage_requests: AtomicU64,

    // copies of shared.first/last_valid_lsn fields (copied here so
    // that they can be read without acquiring the mutex).
    pub first_valid_lsn: AtomicU64,
    pub last_valid_lsn: AtomicU64,
    pub last_record_lsn: AtomicU64,
}

#[derive(Clone)]
pub struct PageCacheStats {
    pub num_entries: u64,
    pub num_page_images: u64,
    pub num_wal_records: u64,
    pub num_getpage_requests: u64,
    pub first_valid_lsn: u64,
    pub last_valid_lsn: u64,
    pub last_record_lsn: u64,
}

impl AddAssign for PageCacheStats {
    fn add_assign(&mut self, other: Self) {
        *self = Self {
            num_entries: self.num_entries + other.num_entries,
            num_page_images: self.num_page_images + other.num_page_images,
            num_wal_records: self.num_wal_records + other.num_wal_records,
            num_getpage_requests: self.num_getpage_requests + other.num_getpage_requests,
            first_valid_lsn: self.first_valid_lsn + other.first_valid_lsn,
            last_valid_lsn: self.last_valid_lsn + other.last_valid_lsn,
            last_record_lsn: self.last_record_lsn + other.last_record_lsn,
        }
    }
}

//
// Shared data structure, holding page cache and related auxiliary information
//
struct PageCacheShared {
    // What page versions do we hold in the cache? If we get GetPage with
    // LSN < first_valid_lsn, that's an error because we (no longer) hold that
    // page version. If we get a request > last_valid_lsn, we need to wait until
    // we receive all the WAL up to the request.
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
    first_valid_lsn: u64,
    last_valid_lsn: u64,
    last_record_lsn: u64,
}

lazy_static! {
    pub static ref PAGECACHES: Mutex<HashMap<ZTimelineId, Arc<PageCache>>> =
        Mutex::new(HashMap::new());
}

// Get Page Cache for given timeline. It is assumed to already exist.
pub fn get_pagecache(_conf: &PageServerConf, timelineid: ZTimelineId) -> Option<Arc<PageCache>> {
    let pcaches = PAGECACHES.lock().unwrap();

    match pcaches.get(&timelineid) {
        Some(pcache) => Some(pcache.clone()),
        None => None,
    }
}

pub fn get_or_restore_pagecache(
    conf: &PageServerConf,
    timelineid: ZTimelineId,
) -> anyhow::Result<Arc<PageCache>> {
    let mut pcaches = PAGECACHES.lock().unwrap();

    match pcaches.get(&timelineid) {
        Some(pcache) => Ok(pcache.clone()),
        None => {
            let pcache = init_page_cache(conf, timelineid);

            restore_timeline(conf, &pcache, timelineid)?;

            let result = Arc::new(pcache);

            pcaches.insert(timelineid, result.clone());

            // Initialize the WAL redo thread
            //
            // Now join_handle is not saved any where and we won'try restart tharead
            // if it is dead. We may later stop that treads after some inactivity period
            // and restart them on demand.
            let conf_copy = conf.clone();
            let _walredo_thread = thread::Builder::new()
                .name("WAL redo thread".into())
                .spawn(move || {
                    walredo::wal_redo_main(&conf_copy, timelineid);
                })
                .unwrap();
            if conf.gc_horizon != 0 {
                let conf_copy = conf.clone();
                let _gc_thread = thread::Builder::new()
                    .name("Garbage collection thread".into())
                    .spawn(move || {
                        gc_thread_main(&conf_copy, timelineid);
                    })
                    .unwrap();
            }
            Ok(result)
        }
    }
}

fn gc_thread_main(conf: &PageServerConf, timelineid: ZTimelineId) {
    info!("Garbage collection thread started {}", timelineid);
    let pcache = get_pagecache(conf, timelineid).unwrap();
    pcache.do_gc(conf).unwrap();
}

fn open_rocksdb(_conf: &PageServerConf, timelineid: ZTimelineId) -> rocksdb::DB {
    let path = zenith_repo_dir().join(timelineid.to_string());
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.set_use_fsync(true);
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.create_missing_column_families(true);
    rocksdb::DB::open_cf(&opts, &path, &[rocksdb::DEFAULT_COLUMN_FAMILY_NAME]).unwrap()
}

fn init_page_cache(conf: &PageServerConf, timelineid: ZTimelineId) -> PageCache {
    // Initialize the channel between the page cache and the WAL applicator
    let (s, r) = unbounded();

    PageCache {
        db: open_rocksdb(&conf, timelineid),
        shared: Mutex::new(PageCacheShared {
            first_valid_lsn: 0,
            last_valid_lsn: 0,
            last_record_lsn: 0,
        }),
        seqwait_lsn: SeqWait::new(0),

        walredo_sender: s,
        walredo_receiver: r,

        num_entries: AtomicU64::new(0),
        num_page_images: AtomicU64::new(0),
        num_wal_records: AtomicU64::new(0),
        num_getpage_requests: AtomicU64::new(0),

        first_valid_lsn: AtomicU64::new(0),
        last_valid_lsn: AtomicU64::new(0),
        last_record_lsn: AtomicU64::new(0),
    }
}

//
// We store two kinds of entries in the page cache:
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
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct CacheKey {
    pub tag: BufferTag,
    pub lsn: u64,
}

impl CacheKey {
    pub fn pack(&self, buf: &mut BytesMut) {
        self.tag.pack(buf);
        buf.put_u64(self.lsn);
    }
    pub fn unpack(buf: &mut BytesMut) -> CacheKey {
        CacheKey {
            tag: BufferTag::unpack(buf),
            lsn: buf.get_u64(),
        }
    }
}

pub struct CacheEntry {
    pub key: CacheKey,

    pub content: Mutex<CacheEntryContent>,

    // Condition variable used by the WAL redo service, to wake up
    // requester.
    //
    // FIXME: this takes quite a lot of space. Consider using parking_lot::Condvar
    // or something else.
    pub walredo_condvar: Condvar,
}

pub struct CacheEntryContent {
    pub page_image: Option<Bytes>,
    pub wal_record: Option<WALRecord>,
    pub apply_pending: bool,
}

impl CacheEntryContent {
    pub fn pack(&self, buf: &mut BytesMut) {
        if let Some(image) = &self.page_image {
            buf.put_u8(1);
            buf.put_u16(image.len() as u16);
            buf.put_slice(&image[..]);
        } else if let Some(rec) = &self.wal_record {
            buf.put_u8(0);
            rec.pack(buf);
        }
    }
    pub fn unpack(buf: &mut BytesMut) -> CacheEntryContent {
        if buf.get_u8() == 1 {
            let mut dst = vec![0u8; buf.get_u16() as usize];
            buf.copy_to_slice(&mut dst);
            CacheEntryContent {
                page_image: Some(Bytes::from(dst)),
                wal_record: None,
                apply_pending: false,
            }
        } else {
            CacheEntryContent {
                page_image: None,
                wal_record: Some(WALRecord::unpack(buf)),
                apply_pending: false,
            }
        }
    }
}

impl CacheEntry {
    fn new(key: CacheKey, content: CacheEntryContent) -> CacheEntry {
        CacheEntry {
            key,
            content: Mutex::new(content),
            walredo_condvar: Condvar::new(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Hash, Ord, Clone, Copy)]
pub struct RelTag {
    pub spcnode: u32,
    pub dbnode: u32,
    pub relnode: u32,
    pub forknum: u8,
}

impl RelTag {
    pub fn pack(&self, buf: &mut BytesMut) {
        buf.put_u32(self.spcnode);
        buf.put_u32(self.dbnode);
        buf.put_u32(self.relnode);
        buf.put_u32(self.forknum as u32); // encode forknum as u32 to provide compatibility with wal_redo_postgres
    }
    pub fn unpack(buf: &mut BytesMut) -> RelTag {
        RelTag {
            spcnode: buf.get_u32(),
            dbnode: buf.get_u32(),
            relnode: buf.get_u32(),
            forknum: buf.get_u32() as u8,
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct BufferTag {
    pub rel: RelTag,
    pub blknum: u32,
}

impl BufferTag {
    pub fn pack(&self, buf: &mut BytesMut) {
        self.rel.pack(buf);
        buf.put_u32(self.blknum);
    }
    pub fn unpack(buf: &mut BytesMut) -> BufferTag {
        BufferTag {
            rel: RelTag::unpack(buf),
            blknum: buf.get_u32(),
        }
    }
}

#[derive(Clone)]
pub struct WALRecord {
    pub lsn: u64, // LSN at the *end* of the record
    pub will_init: bool,
    pub truncate: bool,
    pub rec: Bytes,
    // Remember the offset of main_data in rec,
    // so that we don't have to parse the record again.
    // If record has no main_data, this offset equals rec.len().
    pub main_data_offset: u32,
}

impl WALRecord {
    pub fn pack(&self, buf: &mut BytesMut) {
        buf.put_u64(self.lsn);
        buf.put_u8(self.will_init as u8);
        buf.put_u8(self.truncate as u8);
        buf.put_u32(self.main_data_offset);
        buf.put_u32(self.rec.len() as u32);
        buf.put_slice(&self.rec[..]);
    }
    pub fn unpack(buf: &mut BytesMut) -> WALRecord {
        let lsn = buf.get_u64();
        let will_init = buf.get_u8() != 0;
        let truncate = buf.get_u8() != 0;
        let main_data_offset = buf.get_u32();
        let mut dst = vec![0u8; buf.get_u32() as usize];
        buf.copy_to_slice(&mut dst);
        WALRecord {
            lsn,
            will_init,
            truncate,
            rec: Bytes::from(dst),
            main_data_offset,
        }
    }
}

// Public interface functions

impl PageCache {
    fn do_gc(&self, conf: &PageServerConf) -> anyhow::Result<Bytes> {
        let mut minbuf = BytesMut::new();
        let mut maxbuf = BytesMut::new();
        let cf = self
            .db
            .cf_handle(rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
            .unwrap();
        loop {
            thread::sleep(conf.gc_period);
            let last_lsn = self.get_last_valid_lsn();
            if last_lsn > conf.gc_horizon {
                let horizon = last_lsn - conf.gc_horizon;
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
                    lsn: u64::MAX,
                };
                loop {
                    maxbuf.clear();
                    maxkey.pack(&mut maxbuf);
                    let mut iter = self.db.iterator(rocksdb::IteratorMode::From(
                        &maxbuf[..],
                        rocksdb::Direction::Reverse,
                    ));
                    if let Some((k, v)) = iter.next() {
                        minbuf.clear();
                        minbuf.extend_from_slice(&v);
                        let content = CacheEntryContent::unpack(&mut minbuf);
                        minbuf.clear();
                        minbuf.extend_from_slice(&k);
                        let key = CacheKey::unpack(&mut minbuf);

                        // Construct boundaries for old records cleanup
                        maxkey.tag = key.tag;
                        let last_lsn = key.lsn;
                        maxkey.lsn = min(horizon, last_lsn); // do not remove last version

                        let mut minkey = maxkey.clone();
                        minkey.lsn = 0;

                        // reconstruct most recent page version
                        if content.wal_record.is_some() {
                            trace!("Reconstruct most recent page {:?}", key);
                            // force reconstruction of most recent page version
                            self.reconstruct_page(key, content)?;
                        }

                        maxbuf.clear();
                        maxkey.pack(&mut maxbuf);

                        if last_lsn > horizon {
                            // locate most recent record before horizon
                            let mut iter = self.db.iterator(rocksdb::IteratorMode::From(
                                &maxbuf[..],
                                rocksdb::Direction::Reverse,
                            ));
                            if let Some((k, v)) = iter.next() {
                                minbuf.clear();
                                minbuf.extend_from_slice(&v);
                                let content = CacheEntryContent::unpack(&mut minbuf);
                                if content.wal_record.is_some() {
                                    minbuf.clear();
                                    minbuf.extend_from_slice(&k);
                                    let key = CacheKey::unpack(&mut minbuf);
                                    trace!("Reconstruct horizon page {:?}", key);
                                    self.reconstruct_page(key, content)?;
                                }
                            }
                        }
                        // remove records prior to horizon
                        minbuf.clear();
                        minkey.pack(&mut minbuf);
                        trace!("Delete records in range {:?}..{:?}", minkey, maxkey);
                        self.db.delete_range_cf(cf, &minbuf[..], &maxbuf[..])?;

                        maxkey = minkey;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    fn reconstruct_page(&self, key: CacheKey, content: CacheEntryContent) -> anyhow::Result<Bytes> {
        let entry_rc = Arc::new(CacheEntry::new(key.clone(), content));

        let mut entry_content = entry_rc.content.lock().unwrap();
        entry_content.apply_pending = true;

        let s = &self.walredo_sender;
        s.send(entry_rc.clone())?;

        while entry_content.apply_pending {
            entry_content = entry_rc.walredo_condvar.wait(entry_content).unwrap();
        }
        // We should now have a page image. If we don't, it means that WAL redo
        // failed to reconstruct it. WAL redo should've logged that error already.
        let page_img = match &entry_content.page_image {
            Some(p) => p.clone(),
            None => {
                error!("could not apply WAL to reconstruct page image for GetPage@LSN request");
                bail!("could not apply WAL to reconstruct page image");
            }
        };
        self.put_page_image(key.tag, key.lsn, page_img.clone());
        Ok(page_img)
    }

    async fn wait_lsn(&self, req_lsn: u64) -> anyhow::Result<u64> {
        let mut lsn = req_lsn;
        //When invalid LSN is requested, it means "don't wait, return latest version of the page"
        //This is necessary for bootstrap.
        if lsn == 0 {
            lsn = self.last_valid_lsn.load(Ordering::Acquire);
            trace!(
                "walreceiver doesn't work yet last_valid_lsn {}, requested {}",
                self.last_valid_lsn.load(Ordering::Acquire),
                lsn
            );
        }
        self.seqwait_lsn
            .wait_for_timeout(lsn, TIMEOUT)
            .await
            .with_context(|| {
                format!(
                    "Timed out while waiting for WAL record at LSN {:X}/{:X} to arrive",
                    lsn >> 32,
                    lsn & 0xffff_ffff
                )
            })?;

        Ok(lsn)
    }

    //
    // GetPage@LSN
    //
    // Returns an 8k page image
    //
    pub async fn get_page_at_lsn(&self, tag: BufferTag, req_lsn: u64) -> anyhow::Result<Bytes> {
        self.num_getpage_requests.fetch_add(1, Ordering::Relaxed);

        let lsn = self.wait_lsn(req_lsn).await?;

        // Look up cache entry. If it's a page image, return that. If it's a WAL record,
        // ask the WAL redo service to reconstruct the page image from the WAL records.
        let minkey = CacheKey { tag, lsn: 0 };
        let maxkey = CacheKey { tag, lsn };

        let mut buf = BytesMut::new();
        minkey.pack(&mut buf);

        let mut readopts = rocksdb::ReadOptions::default();
        readopts.set_iterate_lower_bound(buf.to_vec());

        buf.clear();
        maxkey.pack(&mut buf);
        let mut iter = self.db.iterator_opt(
            rocksdb::IteratorMode::From(&buf[..], rocksdb::Direction::Reverse),
            readopts,
        );
        let entry_opt = iter.next();

        if entry_opt.is_none() {
            static ZERO_PAGE: [u8; 8192] = [0u8; 8192];
            debug!("Page {:?} at {}({}) not found", tag, req_lsn, lsn);
            return Ok(Bytes::from_static(&ZERO_PAGE));
            /* return Err("could not find page image")?; */
        }
        let (k, v) = entry_opt.unwrap();
        buf.clear();
        buf.extend_from_slice(&v);
        let content = CacheEntryContent::unpack(&mut buf);
        let page_img: Bytes;
        if let Some(img) = &content.page_image {
            page_img = img.clone();
        } else if content.wal_record.is_some() {
            buf.clear();
            buf.extend_from_slice(&k);
            let key = CacheKey::unpack(&mut buf);
            page_img = self.reconstruct_page(key, content)?;
        } else {
            // No base image, and no WAL record. Huh?
            bail!("no page image or WAL record for requested page");
        }

        // FIXME: assumes little-endian. Only used for the debugging log though
        let page_lsn_hi = u32::from_le_bytes(page_img.get(0..4).unwrap().try_into().unwrap());
        let page_lsn_lo = u32::from_le_bytes(page_img.get(4..8).unwrap().try_into().unwrap());
        debug!(
            "Returning page with LSN {:X}/{:X} for {}/{}/{}.{} blk {}",
            page_lsn_hi,
            page_lsn_lo,
            tag.rel.spcnode,
            tag.rel.dbnode,
            tag.rel.relnode,
            tag.rel.forknum,
            tag.blknum
        );

        Ok(page_img)
    }

    //
    // Collect all the WAL records that are needed to reconstruct a page
    // image for the given cache entry.
    //
    // Returns an old page image (if any), and a vector of WAL records to apply
    // over it.
    //
    pub fn collect_records_for_apply(&self, entry: &CacheEntry) -> (Option<Bytes>, Vec<WALRecord>) {
        let minkey = CacheKey {
            tag: BufferTag {
                rel: entry.key.tag.rel,
                blknum: 0,
            },
            lsn: 0,
        };

        let mut buf = BytesMut::new();
        minkey.pack(&mut buf);

        let mut readopts = rocksdb::ReadOptions::default();
        readopts.set_iterate_lower_bound(buf.to_vec());

        buf.clear();
        entry.key.pack(&mut buf);
        let iter = self.db.iterator_opt(
            rocksdb::IteratorMode::From(&buf[..], rocksdb::Direction::Reverse),
            readopts,
        );

        let mut base_img: Option<Bytes> = None;
        let mut records: Vec<WALRecord> = Vec::new();

        // Scan backwards, collecting the WAL records, until we hit an
        // old page image.
        for (_k, v) in iter {
            buf.clear();
            buf.extend_from_slice(&v);
            let content = CacheEntryContent::unpack(&mut buf);
            if let Some(img) = &content.page_image {
                // We have a base image. No need to dig deeper into the list of
                // records
                base_img = Some(img.clone());
                break;
            } else if let Some(rec) = &content.wal_record {
                records.push(rec.clone());

                // If this WAL record initializes the page, no need to dig deeper.
                if rec.will_init {
                    break;
                }
            } else {
                panic!("no base image and no WAL record on cache entry");
            }
        }

        records.reverse();
        (base_img, records)
    }

    //
    // Adds a WAL record to the page cache
    //
    pub fn put_wal_record(&self, tag: BufferTag, rec: WALRecord) {
        let lsn = rec.lsn;
        let key = CacheKey { tag, lsn };

        let content = CacheEntryContent {
            page_image: None,
            wal_record: Some(rec),
            apply_pending: false,
        };

        let mut key_buf = BytesMut::new();
        key.pack(&mut key_buf);
        let mut val_buf = BytesMut::new();
        content.pack(&mut val_buf);

        let _res = self.db.put(&key_buf[..], &val_buf[..]);
        //trace!("put_wal_record lsn: {}", lsn);

        self.num_entries.fetch_add(1, Ordering::Relaxed);
        self.num_wal_records.fetch_add(1, Ordering::Relaxed);
    }

    //
    // Adds a relation-wide WAL record (like truncate) to the page cache,
    // associating it with all pages started with specified block number
    //
    pub async fn put_rel_wal_record(&self, tag: BufferTag, rec: WALRecord) -> anyhow::Result<()> {
        let mut key = CacheKey { tag, lsn: rec.lsn };
        let old_rel_size = self.relsize_get(&tag.rel, u64::MAX).await?;
        let content = CacheEntryContent {
            page_image: None,
            wal_record: Some(rec),
            apply_pending: false,
        };
        // set new relation size
        trace!("Truncate relation {:?}", tag);
        let mut key_buf = BytesMut::new();
        let mut val_buf = BytesMut::new();
        content.pack(&mut val_buf);

        for blknum in tag.blknum..old_rel_size {
            key_buf.clear();
            key.tag.blknum = blknum;
            key.pack(&mut key_buf);
            trace!("put_wal_record lsn: {}", key.lsn);
            let _res = self.db.put(&key_buf[..], &val_buf[..]);
        }
        let n = (old_rel_size - tag.blknum) as u64;
        self.num_entries.fetch_add(n, Ordering::Relaxed);
        self.num_wal_records.fetch_add(n, Ordering::Relaxed);
        Ok(())
    }

    //
    // Memorize a full image of a page version
    //
    pub fn put_page_image(&self, tag: BufferTag, lsn: u64, img: Bytes) {
        let key = CacheKey { tag, lsn };
        let content = CacheEntryContent {
            page_image: Some(img),
            wal_record: None,
            apply_pending: false,
        };

        let mut key_buf = BytesMut::new();
        key.pack(&mut key_buf);
        let mut val_buf = BytesMut::new();
        content.pack(&mut val_buf);

        trace!("put_wal_record lsn: {}", key.lsn);
        let _res = self.db.put(&key_buf[..], &val_buf[..]);

        //debug!("inserted page image for {}/{}/{}_{} blk {} at {}",
        //        tag.spcnode, tag.dbnode, tag.relnode, tag.forknum, tag.blknum, lsn);
        self.num_page_images.fetch_add(1, Ordering::Relaxed);
    }

    //
    pub fn advance_last_valid_lsn(&self, lsn: u64) {
        let mut shared = self.shared.lock().unwrap();

        // Can't move backwards.
        let oldlsn = shared.last_valid_lsn;
        if lsn >= oldlsn {
            shared.last_valid_lsn = lsn;
            self.seqwait_lsn.advance(lsn);

            self.last_valid_lsn.store(lsn, Ordering::Relaxed);
        } else {
            warn!(
                "attempted to move last valid LSN backwards (was {:X}/{:X}, new {:X}/{:X})",
                oldlsn >> 32,
                oldlsn & 0xffffffff,
                lsn >> 32,
                lsn & 0xffffffff
            );
        }
    }

    //
    // NOTE: this updates last_valid_lsn as well.
    //
    pub fn advance_last_record_lsn(&self, lsn: u64) {
        let mut shared = self.shared.lock().unwrap();

        // Can't move backwards.
        assert!(lsn >= shared.last_valid_lsn);
        assert!(lsn >= shared.last_record_lsn);

        shared.last_valid_lsn = lsn;
        shared.last_record_lsn = lsn;
        self.seqwait_lsn.advance(lsn);

        self.last_valid_lsn.store(lsn, Ordering::Relaxed);
        self.last_record_lsn.store(lsn, Ordering::Relaxed);
    }

    //
    pub fn _advance_first_valid_lsn(&self, lsn: u64) {
        let mut shared = self.shared.lock().unwrap();

        // Can't move backwards.
        assert!(lsn >= shared.first_valid_lsn);

        // Can't overtake last_valid_lsn (except when we're
        // initializing the system and last_valid_lsn hasn't been set yet.
        assert!(shared.last_valid_lsn == 0 || lsn < shared.last_valid_lsn);

        shared.first_valid_lsn = lsn;
        self.first_valid_lsn.store(lsn, Ordering::Relaxed);
    }

    pub fn init_valid_lsn(&self, lsn: u64) {
        let mut shared = self.shared.lock().unwrap();

        assert!(shared.first_valid_lsn == 0);
        assert!(shared.last_valid_lsn == 0);
        assert!(shared.last_record_lsn == 0);

        shared.first_valid_lsn = lsn;
        shared.last_valid_lsn = lsn;
        shared.last_record_lsn = lsn;

        self.first_valid_lsn.store(lsn, Ordering::Relaxed);
        self.last_valid_lsn.store(lsn, Ordering::Relaxed);
        self.last_record_lsn.store(lsn, Ordering::Relaxed);
    }

    pub fn get_last_valid_lsn(&self) -> u64 {
        let shared = self.shared.lock().unwrap();

        shared.last_record_lsn
    }

    pub async fn relsize_get(&self, rel: &RelTag, req_lsn: u64) -> anyhow::Result<u32> {
        let mut lsn = req_lsn;
        if lsn != u64::MAX {
            lsn = self.wait_lsn(lsn).await?;
        }

        let mut key = CacheKey {
            tag: BufferTag {
                rel: *rel,
                blknum: u32::MAX,
            },
            lsn,
        };
        let mut buf = BytesMut::new();

        loop {
            buf.clear();
            key.pack(&mut buf);
            let mut iter = self.db.iterator(rocksdb::IteratorMode::From(
                &buf[..],
                rocksdb::Direction::Reverse,
            ));
            if let Some((k, v)) = iter.next() {
                buf.clear();
                buf.extend_from_slice(&k);
                let tag = BufferTag::unpack(&mut buf);
                if tag.rel == *rel {
                    buf.clear();
                    buf.extend_from_slice(&v);
                    let content = CacheEntryContent::unpack(&mut buf);
                    if let Some(rec) = &content.wal_record {
                        if rec.truncate {
                            if tag.blknum > 0 {
                                key.tag.blknum = tag.blknum - 1;
                                continue;
                            }
                            break;
                        }
                    }
                    let relsize = tag.blknum + 1;
                    debug!("Size of relation {:?} at {} is {}", rel, lsn, relsize);
                    return Ok(relsize);
                }
            }
            break;
        }
        debug!("Size of relation {:?} at {} is zero", rel, lsn);
        Ok(0)
    }

    pub async fn relsize_exist(&self, rel: &RelTag, req_lsn: u64) -> anyhow::Result<bool> {
        let lsn = self.wait_lsn(req_lsn).await?;

        let key = CacheKey {
            tag: BufferTag {
                rel: *rel,
                blknum: u32::MAX,
            },
            lsn,
        };
        let mut buf = BytesMut::new();
        key.pack(&mut buf);
        let mut iter = self.db.iterator(rocksdb::IteratorMode::From(
            &buf[..],
            rocksdb::Direction::Reverse,
        ));
        if let Some((k, _v)) = iter.next() {
            buf.clear();
            buf.extend_from_slice(&k);
            let tag = BufferTag::unpack(&mut buf);
            if tag.rel == *rel {
                debug!("Relation {:?} exists at {}", rel, lsn);
                return Ok(true);
            }
        }
        debug!("Relation {:?} doesn't exist at {}", rel, lsn);
        Ok(false)
    }

    pub fn get_stats(&self) -> PageCacheStats {
        PageCacheStats {
            num_entries: self.num_entries.load(Ordering::Relaxed),
            num_page_images: self.num_page_images.load(Ordering::Relaxed),
            num_wal_records: self.num_wal_records.load(Ordering::Relaxed),
            num_getpage_requests: self.num_getpage_requests.load(Ordering::Relaxed),
            first_valid_lsn: self.first_valid_lsn.load(Ordering::Relaxed),
            last_valid_lsn: self.last_valid_lsn.load(Ordering::Relaxed),
            last_record_lsn: self.last_record_lsn.load(Ordering::Relaxed),
        }
    }

    pub fn create_database(
        &self,
        lsn: u64,
        db_id: Oid,
        tablespace_id: Oid,
        src_db_id: Oid,
        src_tablespace_id: Oid,
    ) -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
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
            lsn: 0,
        };
        key.pack(&mut buf);
        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            &buf[..],
            rocksdb::Direction::Forward,
        ));
        let mut n = 0;
        for (k, v) in iter {
            buf.clear();
            buf.extend_from_slice(&k);
            let mut key = CacheKey::unpack(&mut buf);
            if key.tag.rel.spcnode != src_tablespace_id || key.tag.rel.dbnode != src_db_id {
                break;
            }
            key.tag.rel.spcnode = tablespace_id;
            key.tag.rel.dbnode = db_id;
            key.lsn = lsn;
            buf.clear();
            key.pack(&mut buf);

            self.db.put(&buf[..], v)?;
            n += 1;
        }
        info!(
            "Create database {}/{}, copy {} entries",
            tablespace_id, db_id, n
        );
        Ok(())
    }
}

pub fn get_stats() -> PageCacheStats {
    let pcaches = PAGECACHES.lock().unwrap();

    let mut stats = PageCacheStats {
        num_entries: 0,
        num_page_images: 0,
        num_wal_records: 0,
        num_getpage_requests: 0,
        first_valid_lsn: 0,
        last_valid_lsn: 0,
        last_record_lsn: 0,
    };

    pcaches.iter().for_each(|(_sys_id, pcache)| {
        stats += pcache.get_stats();
    });
    stats
}
