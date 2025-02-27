//! This module houses types which represent decoded PG WAL records
//! ready for the pageserver to interpret. They are derived from the original
//! WAL records, so that each struct corresponds closely to one WAL record of
//! a specific kind. They contain the same information as the original WAL records,
//! but the values are already serialized in a [`SerializedValueBatch`], which
//! is the format that the pageserver is expecting them in.
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
use serde::{Deserialize, Serialize};
use utils::lsn::Lsn;

use crate::serialized_batch::SerializedValueBatch;

// Code generated by protobuf.
pub mod proto {
    // Tonic does derives as `#[derive(Clone, PartialEq, ::prost::Message)]`
    // we don't use these types for anything but broker data transmission,
    // so it's ok to ignore this one.
    #![allow(clippy::derive_partial_eq_without_eq)]
    // The generated ValueMeta has a `len` method generate for its `len` field.
    #![allow(clippy::len_without_is_empty)]
    include!(concat!(env!("OUT_DIR"), concat!("/interpreted_wal.rs")));
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum FlushUncommittedRecords {
    Yes,
    No,
}

/// A batch of interpreted WAL records
#[derive(Serialize, Deserialize)]
pub struct InterpretedWalRecords {
    pub records: Vec<InterpretedWalRecord>,
    // Start LSN of the next record after the batch.
    // Note that said record may not belong to the current shard.
    pub next_record_lsn: Lsn,
    // Inclusive start LSN of the PG WAL from which the interpreted
    // WAL records were extracted. Note that this is not necessarily the
    // start LSN of the first interpreted record in the batch.
    pub raw_wal_start_lsn: Option<Lsn>,
}

/// An interpreted Postgres WAL record, ready to be handled by the pageserver
#[derive(Serialize, Deserialize, Clone)]
pub struct InterpretedWalRecord {
    /// Optional metadata record - may cause writes to metadata keys
    /// in the storage engine
    pub metadata_record: Option<MetadataRecord>,
    /// A pre-serialized batch along with the required metadata for ingestion
    /// by the pageserver
    pub batch: SerializedValueBatch,
    /// Byte offset within WAL for the start of the next PG WAL record.
    /// Usually this is the end LSN of the current record, but in case of
    /// XLOG SWITCH records it will be within the next segment.
    pub next_record_lsn: Lsn,
    /// Whether to flush all uncommitted modifications to the storage engine
    /// before ingesting this record. This is currently only used for legacy PG
    /// database creations which read pages from a template database. Such WAL
    /// records require reading data blocks while ingesting, hence the need to flush.
    pub flush_uncommitted: FlushUncommittedRecords,
    /// Transaction id of the original PG WAL record
    pub xid: TransactionId,
}

impl InterpretedWalRecord {
    /// Checks if the WAL record is empty
    ///
    /// An empty interpreted WAL record has no data or metadata and does not have to be sent to the
    /// pageserver.
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
            && self.metadata_record.is_none()
            && matches!(self.flush_uncommitted, FlushUncommittedRecords::No)
    }

    /// Checks if the WAL record is observed (i.e. contains only metadata
    /// for observed values)
    pub fn is_observed(&self) -> bool {
        self.batch.is_observed()
            && self.metadata_record.is_none()
            && matches!(self.flush_uncommitted, FlushUncommittedRecords::No)
    }
}

/// The interpreted part of the Postgres WAL record which requires metadata
/// writes to the underlying storage engine.
#[derive(Clone, Serialize, Deserialize)]
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

#[derive(Clone, Serialize, Deserialize)]
pub enum HeapamRecord {
    ClearVmBits(ClearVmBits),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ClearVmBits {
    pub new_heap_blkno: Option<u32>,
    pub old_heap_blkno: Option<u32>,
    pub vm_rel: RelTag,
    pub flags: u8,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum NeonrmgrRecord {
    ClearVmBits(ClearVmBits),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum SmgrRecord {
    Create(SmgrCreate),
    Truncate(XlSmgrTruncate),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SmgrCreate {
    pub rel: RelTag,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum DbaseRecord {
    Create(DbaseCreate),
    Drop(DbaseDrop),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DbaseCreate {
    pub db_id: Oid,
    pub tablespace_id: Oid,
    pub src_db_id: Oid,
    pub src_tablespace_id: Oid,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DbaseDrop {
    pub db_id: Oid,
    pub tablespace_ids: Vec<Oid>,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ClogRecord {
    ZeroPage(ClogZeroPage),
    Truncate(ClogTruncate),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ClogZeroPage {
    pub segno: u32,
    pub rpageno: u32,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ClogTruncate {
    pub pageno: u32,
    pub oldest_xid: TransactionId,
    pub oldest_xid_db: Oid,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum XactRecord {
    Commit(XactCommon),
    Abort(XactCommon),
    CommitPrepared(XactCommon),
    AbortPrepared(XactCommon),
    Prepare(XactPrepare),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct XactCommon {
    pub parsed: XlXactParsedRecord,
    pub origin_id: u16,
    // Fields below are only used for logging
    pub xl_xid: TransactionId,
    pub lsn: Lsn,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct XactPrepare {
    pub xl_xid: TransactionId,
    pub data: Bytes,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum MultiXactRecord {
    ZeroPage(MultiXactZeroPage),
    Create(XlMultiXactCreate),
    Truncate(XlMultiXactTruncate),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MultiXactZeroPage {
    pub slru_kind: SlruKind,
    pub segno: u32,
    pub rpageno: u32,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum RelmapRecord {
    Update(RelmapUpdate),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RelmapUpdate {
    pub update: XlRelmapUpdate,
    pub buf: Bytes,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum XlogRecord {
    Raw(RawXlogRecord),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RawXlogRecord {
    pub info: u8,
    pub lsn: Lsn,
    pub buf: Bytes,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum LogicalMessageRecord {
    Put(PutLogicalMessage),
    #[cfg(feature = "testing")]
    Failpoint,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PutLogicalMessage {
    pub path: String,
    pub buf: Bytes,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum StandbyRecord {
    RunningXacts(StandbyRunningXacts),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct StandbyRunningXacts {
    pub oldest_running_xid: TransactionId,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ReploriginRecord {
    Set(XlReploriginSet),
    Drop(XlReploriginDrop),
}
