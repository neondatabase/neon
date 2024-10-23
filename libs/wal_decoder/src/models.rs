//! This module houses types which represent decoded PG WAL records
//! ready for the pageserver to interpret.

use bytes::Bytes;
use pageserver_api::key::CompactKey;
use pageserver_api::reltag::{BlockNumber, RelTag, SlruKind};
use pageserver_api::value::Value;
use postgres_ffi::record::{
    XlMultiXactCreate, XlMultiXactTruncate, XlRelmapUpdate, XlReploriginDrop, XlReploriginSet,
    XlXactParsedRecord,
};
use postgres_ffi::{Oid, TransactionId};
use utils::lsn::Lsn;

pub enum FlushUncommittedRecords {
    Yes,
    No,
}

pub struct InterpretedWalRecord {
    pub metadata_record: Option<MetadataRecord>,
    pub blocks: Vec<(CompactKey, Option<Value>)>,
    pub lsn: Lsn,
    pub flush_uncommitted: FlushUncommittedRecords,
    pub xid: TransactionId,
}

pub enum MetadataRecord {
    Heapam(HeapamRecord),
    Neonrmgr(NeonrmgrRecord),
    Smgr(SmgrRecord),
    Dbase(DbaseRecord),
    Clog(ClogRecord),
    Xact(XactRecord),
    MultiXact(MultiXactRecord),
    Relmap(RelmapRecord),
    Xlog(XlogRecord),
    LogicalMessage(LogicalMessageRecord),
    Standby(StandbyRecord),
    Replorigin(ReploriginRecord),
}

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
    Truncate(SmgrTruncate),
}

pub struct SmgrCreate {
    pub rel: RelTag,
}

pub struct SmgrTruncate {
    pub rel: RelTag,
    pub to: BlockNumber,
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
}

pub struct PutLogicalMessage {
    pub buf: Bytes,
    pub prefix_size: usize,
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
