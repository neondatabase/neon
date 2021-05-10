pub mod rocksdb;

use crate::waldecoder::{DecodedWALRecord, Oid};
use crate::ZTimelineId;
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;
use std::sync::Arc;
use postgres_ffi::relfile_utils::forknumber_to_name;
use zenith_utils::lsn::Lsn;

///
/// A repository corresponds to one .zenith directory. One repository holds multiple
/// timelines, forked off from the same initial call to 'initdb'.
pub trait Repository {
    /// Get Timeline handle for given zenith timeline ID.
    ///
    /// The Timeline is expected to be already "open", i.e. `get_or_restore_timeline`
    /// should've been called on it earlier already.
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>>;

    /// Get Timeline handle for given zenith timeline ID.
    ///
    /// Creates a new Timeline object if it's not "open" already.
    fn get_or_restore_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>>;

    //fn get_stats(&self) -> RepositoryStats;
}

pub trait Timeline {
    //------------------------------------------------------------------------------
    // Public GET functions
    //------------------------------------------------------------------------------

    /// Look up given page in the cache.
    fn get_page_at_lsn(&self, tag: BufferTag, lsn: Lsn) -> Result<Bytes>;

    /// Get size of relation
    fn get_relsize(&self, tag: RelTag, lsn: Lsn) -> Result<u32>;

    /// Does relation exist?
    fn get_relsize_exists(&self, tag: RelTag, lsn: Lsn) -> Result<bool>;

    //------------------------------------------------------------------------------
    // Public PUT functions, to update the repository with new page versions.
    //
    // These are called by the WAL receiver to digest WAL records.
    //------------------------------------------------------------------------------

    /// Put a new page version that can be constructed from a WAL record
    ///
    /// This will implicitly extend the relation, if the page is beyond the
    /// current end-of-file.
    fn put_wal_record(&self, tag: BufferTag, rec: WALRecord);

    /// Like put_wal_record, but with ready-made image of the page.
    fn put_page_image(&self, tag: BufferTag, lsn: Lsn, img: Bytes);

    /// Truncate relation
    fn put_truncation(&self, rel: RelTag, lsn: Lsn, nblocks: u32) -> anyhow::Result<()>;

    /// Create a new database from a template database
    ///
    /// In PostgreSQL, CREATE DATABASE works by scanning the data directory and
    /// copying all relation files from the template database. This is the equivalent
    /// of that.
    fn put_create_database(
        &self,
        lsn: Lsn,
        db_id: Oid,
        tablespace_id: Oid,
        src_db_id: Oid,
        src_tablespace_id: Oid,
    ) -> Result<()>;

    ///
    /// Helper function to parse a WAL record and call the above functions for all the
    /// relations/pages that the record affects.
    ///
    fn save_decoded_record(
        &self,
        decoded: DecodedWALRecord,
        recdata: Bytes,
        lsn: Lsn,
    ) -> anyhow::Result<()>;

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

/// Display RelTag in the same format that's used in most PostgreSQL debug messages:
///
/// <spcnode>/<dbnode>/<relnode>[_fsm|_vm|_init]
///
impl fmt::Display for RelTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(forkname) = forknumber_to_name(self.forknum) {
            write!(f, "{}/{}/{}_{}", self.spcnode, self.dbnode, self.relnode, forkname)
        } else {
            write!(f, "{}/{}/{}", self.spcnode, self.dbnode, self.relnode)
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
    pub fn unpack(buf: &mut BytesMut) -> WALRecord {
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
