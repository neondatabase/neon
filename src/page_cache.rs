use std::collections::BTreeMap;
use std::sync::Mutex;
use bytes::Bytes;
use lazy_static::lazy_static;
use rand::Rng;

use crate::walredo;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct BufferTag {
    pub spcnode: u32,
    pub dbnode: u32,
    pub relnode: u32,
    pub forknum: u32,
    pub blknum: u32,
}

#[derive(Clone)]
pub struct WALRecord {
    pub lsn: u64,
    pub will_init: bool,
    pub rec: Bytes
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

lazy_static! {
    static ref PAGECACHE: Mutex<BTreeMap<CacheKey, CacheEntry>> = Mutex::new(BTreeMap::new());
}



// Public interface functions


//
// Simple test function for the WAL redo code:
//
// 1. Pick a page from the page cache at random.
// 2. Request that page with GetPage@LSN, using Max LSN (i.e. get the latest page version)
//
//
pub fn test_get_page_at_lsn()
{
    // for quick testing of the get_page_at_lsn() funcion.
    //
    // Get a random page from the page cache. Apply all its WAL, by requesting
    // that page at the highest lsn.

    let mut tag: Option<BufferTag> = None;

    {
        let pagecache = PAGECACHE.lock().unwrap();

        if pagecache.is_empty() {
            println!("page cache is empty");
            return;
        }

        // Find nth entry in the map, where
        let n = rand::thread_rng().gen_range(0..pagecache.len());
        let mut i = 0;
        for (key, _e) in pagecache.iter() {
            if i == n {
                tag = Some(key.tag);
                break;
            }
            i +=1;
        }
    }

    println!("testing GetPage@LSN: {}", tag.unwrap().blknum);
    
    get_page_at_lsn(tag.unwrap(), 0xffff_ffff_ffff_eeee);
    
}


//
// GetPage@LSN
//
#[allow(dead_code)]
#[allow(unused_variables)]
pub fn get_page_at_lsn(tag: BufferTag, lsn: u64)
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

    let pagecache = PAGECACHE.lock().unwrap();

    let entries = pagecache.range(&minkey .. &maxkey);

    let mut records: Vec<WALRecord> = Vec::new();

    let mut base_img: Option<Bytes> = None;
    
    for (key, e) in entries.rev() {
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
                    println!("WAL record at LSN {} initializes the page", rec.lsn);
                }
            }
        }
    }

    if !records.is_empty() {
        records.reverse();

        walredo::apply_wal_records(tag, base_img, &records).expect("could not apply WAL records");

        println!("applied {} WAL records to produce page image at LSN {}", records.len(), lsn);
    }
}

//
// Add WAL record
//
#[allow(dead_code)]
#[allow(unused_variables)]
pub fn put_wal_record(tag: BufferTag, rec: WALRecord)
{
    let key = CacheKey {
        tag: tag,
        lsn: rec.lsn
    };

    let entry = CacheEntry::WALRecord(rec);

    let mut pagecache = PAGECACHE.lock().unwrap();

    let oldentry = pagecache.insert(key, entry);
    assert!(oldentry.is_none());
}
