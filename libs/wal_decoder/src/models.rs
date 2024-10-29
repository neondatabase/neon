//! This module houses types which represent decoded PG WAL records
//! ready for the pageserver to interpret. They are derived from the original
//! WAL records, so that each struct corresponds closely to one WAL record of
//! a specific kind. They contain the same information as the original WAL records,
//! just decoded into structs and fields for easier access.
//!
//! The ingestion code uses these structs to help with parsing the WAL records,
//! and it splits them into a stream of modifications to the key-value pairs that
//! are ultimately stored in delta layers.  See also the split-out counterparts in
//! [`postgres_ffi::walrecord`].
//!
//! The pipeline which processes WAL records is not super obvious, so let's follow
//! the flow of an example XACT_COMMIT Postgres record:
//!
//! (Postgres XACT_COMMIT record)
//! |
//! |--> pageserver::walingest::WalIngest::decode_xact_record
//!      |
//!      |--> ([`XactRecord::Commit`])
//!           |
//!           |--> pageserver::walingest::WalIngest::ingest_xact_record
//!                |
//!                |--> (NeonWalRecord::ClogSetCommitted)
//!                     |
//!                     |--> write to KV store within the pageserver

use bytes::Bytes;
use pageserver_api::reltag::{RelTag, SlruKind};
use postgres_ffi::walrecord::{
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
