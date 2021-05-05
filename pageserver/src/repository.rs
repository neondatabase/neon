pub mod rocksdb;

use crate::waldecoder::Oid;
use crate::ZTimelineId;
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::sync::Arc;
use zenith_utils::lsn::Lsn;

///
/// A repository corresponds to one .zenith directory. One repository holds multiple
/// timelines, forked off from the same initial call to 'initdb'.
///
pub trait Repository {
    // FIXME: I wish these would return an abstract `&dyn Timeline` instead
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<rocksdb::RocksTimeline>>;
    fn get_or_restore_timeline(
        &self,
        timelineid: ZTimelineId,
    ) -> Result<Arc<rocksdb::RocksTimeline>>;

    //fn get_stats(&self) -> RepositoryStats;
}

pub trait Timeline {

    /// Look up given page in the cache.
    fn get_page_at_lsn(&self, tag: BufferTag, lsn: Lsn) -> Result<Bytes>;

    /// Get size of relation
    fn get_relsize(&self, tag: RelTag, lsn: Lsn) -> Result<u32>;

    /// Does relation exist?
    fn get_relsize_exists(&self, tag: RelTag, lsn: Lsn) -> Result<bool>;

    // Functions used by WAL receiver

    fn put_wal_record(&self, tag: BufferTag, rec: WALRecord);
    fn put_rel_wal_record(&self, tag: BufferTag, rec: WALRecord) -> anyhow::Result<()>;
    fn put_page_image(&self, tag: BufferTag, lsn: Lsn, img: Bytes);
    fn create_database(
        &self,
        lsn: Lsn,
        db_id: Oid,
        tablespace_id: Oid,
        src_db_id: Oid,
        src_tablespace_id: Oid,
    ) -> Result<()>;

    fn advance_last_record_lsn(&self, lsn: Lsn);
    fn advance_last_valid_lsn(&self, lsn: Lsn);
    fn init_valid_lsn(&self, lsn: Lsn);
    fn get_last_valid_lsn(&self) -> Lsn;
}

#[derive(Clone)]
pub struct RepositoryStats {
    pub num_entries: Lsn,
    pub num_page_images: Lsn,
    pub num_wal_records: Lsn,
    pub num_getpage_requests: Lsn,
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

#[derive(Debug, Clone)]
pub struct WALRecord {
    pub lsn: Lsn, // LSN at the *end* of the record
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
        buf.put_u64(self.lsn.0);
        buf.put_u8(self.will_init as u8);
        buf.put_u8(self.truncate as u8);
        buf.put_u32(self.main_data_offset);
        buf.put_u32(self.rec.len() as u32);
        buf.put_slice(&self.rec[..]);
    }
    pub fn unpack(buf: &mut BytesMut) -> WALRecord {
        let lsn = Lsn::from(buf.get_u64());
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
