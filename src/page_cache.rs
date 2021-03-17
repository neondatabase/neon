use std::collections::BTreeMap;
use bytes::Bytes;
use lazy_static::lazy_static;

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct BufferTag {
    pub spcnode: u32,
    pub dbnode: u32,
    pub relnode: u32,
    pub forknum: u32,
    pub blknum: u32,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct CacheKey {
    pub tag: BufferTag,
    pub lsn: u64
}

pub struct WALRecord {
    pub lsn: u64,
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
enum CacheEntry {

    PageImage {
        img: Bytes
    },

    WALRecord {
        will_init: bool,
        rec: Bytes
    },
}

lazy_static! {
    static ref PAGECACHE: BTreeMap<CacheKey, CacheEntry> = BTreeMap::new();
}



// Public interface functions

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


    // PAGECACHE.get(&tag);

}



//
// Add WAL record
//
#[allow(dead_code)]
#[allow(unused_variables)]
pub fn put_wal_record(tag: BufferTag, lsn: u64, rec: Bytes)
{

}
