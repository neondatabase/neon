
use bytes::Bytes;

#[derive(PartialEq, Eq, Hash)]
pub struct BufferTag {
    pub spcnode: u32,
    pub dbnode: u32,
    pub relnode: u32,
    pub forknum: u32,
    pub blknum: u32,
}



pub struct WALRecord {
    pub lsn: u64,
    pub rec: Bytes
}

// FIXME: dead code, but I left this here to remind how lazy_static works.
/*
struct CacheEntry {

    _records: VecDeque<WALRecord>,

    // Oldest base image of the page, and its LSN
    _lsn: u64,
    _base: [u8; 8192]   // the first 8 bytes of the page are actually the lsn, too

}

lazy_static! {
    static ref PAGECACHE: CHashMap<BufferTag, CacheEntry> = CHashMap::new();
}


#[allow(dead_code)]
pub fn lookup_page(key: BufferTag, _lsn: u64) {

    PAGECACHE.get(&key);
}

pub fn _append_record(_key: BufferTag, _lsn: u64) { // and wal record in some form


}
*/
