//! This module houses types which represent decoded PG WAL records
//! ready for the pageserver to interpret. They are higher level
//! than their counterparts in [`postgres_ffi::record`].

use bytes::Bytes;
use pageserver_api::reltag::{RelTag, SlruKind};
use postgres_ffi::record::{
    XlMultiXactCreate, XlMultiXactTruncate, XlRelmapUpdate, XlReploriginDrop, XlReploriginSet,
    XlSmgrTruncate, XlXactParsedRecord,
};
use postgres_ffi::{Oid, TransactionId};
use utils::lsn::Lsn;

pub enum HeapamRecord {
    ClearVmBits(ClearVmBits),
}

pub struct ClearVmBits {
    pub new_heap_blkno: Option<u32>,
    pub old_heap_blkno: Option<u32>,
    pub vm_rel: RelTag,
    pub flags: u8,
}

pub enum NeonrmgrRecord {
    ClearVmBits(ClearVmBits),
}

pub enum SmgrRecord {
    Create(SmgrCreate),
    Truncate(XlSmgrTruncate),
}

pub struct SmgrCreate {
    pub rel: RelTag,
}

pub enum DbaseRecord {
    Create(DbaseCreate),
    Drop(DbaseDrop),
}

pub struct DbaseCreate {
    pub db_id: Oid,
    pub tablespace_id: Oid,
    pub src_db_id: Oid,
    pub src_tablespace_id: Oid,
}

pub struct DbaseDrop {
    pub db_id: Oid,
    pub tablespace_ids: Vec<Oid>,
}

pub enum ClogRecord {
    ZeroPage(ClogZeroPage),
    Truncate(ClogTruncate),
}

pub struct ClogZeroPage {
    pub segno: u32,
    pub rpageno: u32,
}

pub struct ClogTruncate {
    pub pageno: u32,
    pub oldest_xid: TransactionId,
    pub oldest_xid_db: Oid,
}

pub enum XactRecord {
    Commit(XactCommon),
    Abort(XactCommon),
    CommitPrepared(XactCommon),
    AbortPrepared(XactCommon),
    Prepare(XactPrepare),
}

pub struct XactCommon {
    pub parsed: XlXactParsedRecord,
    pub origin_id: u16,
    // Fields below are only used for logging
    pub xl_xid: TransactionId,
    pub lsn: Lsn,
}

pub struct XactPrepare {
    pub xl_xid: TransactionId,
    pub data: Bytes,
}

pub enum MultiXactRecord {
    ZeroPage(MultiXactZeroPage),
    Create(XlMultiXactCreate),
    Truncate(XlMultiXactTruncate),
}

pub struct MultiXactZeroPage {
    pub slru_kind: SlruKind,
    pub segno: u32,
    pub rpageno: u32,
}

pub enum RelmapRecord {
    Update(RelmapUpdate),
}

pub struct RelmapUpdate {
    pub update: XlRelmapUpdate,
    pub buf: Bytes,
}

pub enum XlogRecord {
    Raw(RawXlogRecord),
}

pub struct RawXlogRecord {
    pub info: u8,
    pub lsn: Lsn,
    pub buf: Bytes,
}

pub enum LogicalMessageRecord {
    Put(PutLogicalMessage),
    #[cfg(feature = "testing")]
    Failpoint,
}

pub struct PutLogicalMessage {
    pub path: String,
    pub buf: Bytes,
}

pub enum StandbyRecord {
    RunningXacts(StandbyRunningXacts),
}

pub struct StandbyRunningXacts {
    pub oldest_running_xid: TransactionId,
}

pub enum ReploriginRecord {
    Set(XlReploriginSet),
    Drop(XlReploriginDrop),
}
