//
// Page Cache holds all the different page versions and WAL records
//
//
//

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::sync::Mutex;
use bytes::Bytes;
use lazy_static::lazy_static;
use rand::Rng;
use log::*;

use crate::walredo;

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub struct RelTag {
    pub spcnode: u32,
    pub dbnode: u32,
    pub relnode: u32,
    pub forknum: u8,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct BufferTag {
    pub spcnode: u32,
    pub dbnode: u32,
    pub relnode: u32,
    pub forknum: u8,
    pub blknum: u32,
}

#[derive(Clone)]
pub struct WALRecord {
    pub lsn: u64,
    pub will_init: bool,
    pub rec: Bytes
}

//
// Shared data structure, holding page cache and related auxiliary information
//
struct PageCacheShared {

    // The actual page cache
    pagecache: BTreeMap<CacheKey, CacheEntry>,

    // Relation n_blocks cache
    //
    // This hashtable should be updated together with the pagecache. Now it is
    // accessed unreasonably often through the smgr_nblocks(). It is better to just
    // cache it in postgres smgr and ask only on restart.
    relsize_cache: HashMap<RelTag, u32>,

    // What page versions do we hold in the cache? If we get GetPage with
    // LSN < first_valid_lsn, that's an error because we (no longer) hold that
    // page version. If we get a request > last_valid_lsn, we need to wait until
    // we receive all the WAL up to the request.
    //
    first_valid_lsn: u64,
    last_valid_lsn: u64
}

lazy_static! {
    static ref PAGECACHE: Mutex<PageCacheShared> = Mutex::new(
        PageCacheShared {
            pagecache: BTreeMap::new(),
            relsize_cache: HashMap::new(),
            first_valid_lsn: 0,
            last_valid_lsn: 0,
        });
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
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct CacheKey {
    pub tag: BufferTag,
    pub lsn: u64
}

#[derive(Clone)]
enum CacheEntry {
    PageImage(Bytes),

    WALRecord(WALRecord)
}

// Public interface functions

//
// GetPage@LSN
//
// Returns an 8k page image
//
pub fn get_page_at_lsn(tag: BufferTag, lsn: u64) -> Result<Bytes, Box<dyn Error>>
{
    // TODO:
    //
    // Look up cache entry
    // If it's a page image, return that. If it's a WAL record, walk backwards
    // to the latest page image. Then apply all the WAL records up until the
    // given LSN.
    //
    let minkey = CacheKey {
        tag: tag,
        lsn: 0
    };
    let maxkey = CacheKey {
        tag: tag,
        lsn: lsn + 1
    };

    let shared = PAGECACHE.lock().unwrap();

    if lsn > shared.last_valid_lsn {
        // TODO: Wait for the WAL receiver to catch up
    }
    if lsn < shared.first_valid_lsn {
        return Err(format!("LSN {} has already been removed", lsn))?;
    }

    let pagecache = &shared.pagecache;
    let entries = pagecache.range(&minkey .. &maxkey);

    let mut records: Vec<WALRecord> = Vec::new();

    let mut base_img: Option<Bytes> = None;
    
    for (_key, e) in entries.rev() {
        match e {
            CacheEntry::PageImage(img) => {
                // We have a base image. No need to dig deeper into the list of
                // records
                base_img = Some(img.clone());
                break;
            }
            CacheEntry::WALRecord(rec) => {
                records.push(rec.clone());

                if rec.will_init {
                    debug!("WAL record at LSN {} initializes the page", rec.lsn);
                }
            }
        }
    }

    let page_img: Bytes;

    if !records.is_empty() {
        records.reverse();

        page_img = walredo::apply_wal_records(tag, base_img, &records)?;

        debug!("applied {} WAL records to produce page image at LSN {}", records.len(), lsn);

        // Here, we could put the new page image back to the page cache, to save effort if the
        // same (or later) page version is requested again. It's a tradeoff though, as each
        // page image consumes some memory
    } else if base_img.is_some() {
        page_img = base_img.unwrap();
    } else {
        let zero_page = vec![0 as u8; 8192];
        page_img = Bytes::from(zero_page);
        /* return Err("could not find page image")?; */
    }

    return Ok(page_img);
}

//
// Adds a WAL record to the page cache
//
pub fn put_wal_record(tag: BufferTag, rec: WALRecord)
{
    let key = CacheKey {
        tag: tag,
        lsn: rec.lsn
    };

    let entry = CacheEntry::WALRecord(rec);

    let mut shared = PAGECACHE.lock().unwrap();
    // let pagecache = &mut shared.pagecache;

    let rel_tag = RelTag {
        spcnode: tag.spcnode,
        dbnode: tag.dbnode,
        relnode: tag.relnode,
        forknum: tag.forknum,
    };
    let rel_entry = shared.relsize_cache.entry(rel_tag).or_insert(0);
    if tag.blknum >= *rel_entry {
        *rel_entry = tag.blknum + 1;
    }

    let oldentry = shared.pagecache.insert(key, entry);
    assert!(oldentry.is_none());
}

//
//
//
pub fn put_page_image(tag: BufferTag, lsn: u64, img: Bytes)
{
    let key = CacheKey {
        tag: tag,
        lsn: lsn
    };

    let entry = CacheEntry::PageImage(img);

    let mut shared = PAGECACHE.lock().unwrap();
    let pagecache = &mut shared.pagecache;

    let oldentry = pagecache.insert(key, entry);
    assert!(oldentry.is_none());

    debug!("inserted page image for {}/{}/{}_{} blk {} at {}",
            tag.spcnode, tag.dbnode, tag.relnode, tag.forknum, tag.blknum, lsn);
}

//
pub fn advance_last_valid_lsn(lsn: u64)
{
    let mut shared = PAGECACHE.lock().unwrap();

    // Can't move backwards.
    assert!(lsn >= shared.last_valid_lsn);

    shared.last_valid_lsn = lsn;
}

//
pub fn _advance_first_valid_lsn(lsn: u64)
{
    let mut shared = PAGECACHE.lock().unwrap();

    // Can't move backwards.
    assert!(lsn >= shared.first_valid_lsn);

    // Can't overtake last_valid_lsn (except when we're
    // initializing the system and last_valid_lsn hasn't been set yet.
    assert!(shared.last_valid_lsn == 0 || lsn < shared.last_valid_lsn);

    shared.first_valid_lsn = lsn;
}

pub fn init_valid_lsn(lsn: u64)
{
    let mut shared = PAGECACHE.lock().unwrap();

    assert!(shared.first_valid_lsn == 0);
    assert!(shared.last_valid_lsn == 0);

    shared.first_valid_lsn = lsn;
    shared.last_valid_lsn = lsn;
}

pub fn get_last_valid_lsn() -> u64
{
    let shared = PAGECACHE.lock().unwrap();

    return shared.last_valid_lsn;
}


//
// Simple test function for the WAL redo code:
//
// 1. Pick a page from the page cache at random.
// 2. Request that page with GetPage@LSN, using Max LSN (i.e. get the latest page version)
//
//
pub fn _test_get_page_at_lsn()
{
    // for quick testing of the get_page_at_lsn() funcion.
    //
    // Get a random page from the page cache. Apply all its WAL, by requesting
    // that page at the highest lsn.

    let mut tag: Option<BufferTag> = None;

    {
        let shared = PAGECACHE.lock().unwrap();
        let pagecache = &shared.pagecache;

        if pagecache.is_empty() {
            info!("page cache is empty");
            return;
        }

        // Find nth entry in the map, where n is picked at random
        let n = rand::thread_rng().gen_range(0..pagecache.len());
        let mut i = 0;
        for (key, _e) in pagecache.iter() {
            if i == n {
                tag = Some(key.tag);
                break;
            }
            i += 1;
        }
    }

    info!("testing GetPage@LSN for block {}", tag.unwrap().blknum);
    match get_page_at_lsn(tag.unwrap(), 0xffff_ffff_ffff_eeee) {
        Ok(_img) => {
            // This prints out the whole page image.
            //println!("{:X?}", img);
        },
        Err(error) => {
            error!("GetPage@LSN failed: {}", error);
        }
    }
}


// FIXME: Shouldn't relation size also be tracked with an LSN?
// If a replica is lagging behind, it needs to get the size as it was on
// the replica's current replay LSN.
pub fn relsize_inc(rel: &RelTag, to: Option<u32>)
{
    let mut shared = PAGECACHE.lock().unwrap();
    let entry = shared.relsize_cache.entry(*rel).or_insert(0);

    if let Some(to) = to {
        if to >= *entry {
            *entry = to + 1;
        }
    }
}

pub fn relsize_get(rel: &RelTag) -> u32
{
    let mut shared = PAGECACHE.lock().unwrap();
    let entry = shared.relsize_cache.entry(*rel).or_insert(0);
    *entry
}

pub fn relsize_exist(rel: &RelTag) -> bool
{
    let shared = PAGECACHE.lock().unwrap();
    let relsize_cache = &shared.relsize_cache;
    relsize_cache.contains_key(rel)
}
