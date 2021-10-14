//!
//! WAL decoder. For each WAL record, it decodes the record to figure out which data blocks
//! the record affects, so that they can be stored in repository.
//!
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32c::*;
use log::*;
use postgres_ffi::pg_constants;
use postgres_ffi::xlog_utils::*;
use postgres_ffi::XLogLongPageHeaderData;
use postgres_ffi::XLogPageHeaderData;
use postgres_ffi::XLogRecord;
use postgres_ffi::{BlockNumber, OffsetNumber};
use postgres_ffi::{MultiXactId, MultiXactOffset, MultiXactStatus, Oid, TransactionId};
use std::cmp::min;
use thiserror::Error;
use zenith_utils::lsn::Lsn;

#[allow(dead_code)]
pub struct WalStreamDecoder {
    lsn: Lsn,

    startlsn: Lsn, // LSN where this record starts
    contlen: u32,
    padlen: u32,

    inputbuf: BytesMut,

    recordbuf: BytesMut,
}

#[derive(Error, Debug, Clone)]
#[error("{msg} at {lsn}")]
pub struct WalDecodeError {
    msg: String,
    lsn: Lsn,
}

//
// WalRecordStream is a Stream that returns a stream of WAL records
// FIXME: This isn't a proper rust stream
//
impl WalStreamDecoder {
    pub fn new(lsn: Lsn) -> WalStreamDecoder {
        WalStreamDecoder {
            lsn,

            startlsn: Lsn(0),
            contlen: 0,
            padlen: 0,

            inputbuf: BytesMut::new(),
            recordbuf: BytesMut::new(),
        }
    }

    // The latest LSN position fed to the decoder.
    pub fn available(&self) -> Lsn {
        self.lsn + self.inputbuf.remaining() as u64
    }

    pub fn feed_bytes(&mut self, buf: &[u8]) {
        self.inputbuf.extend_from_slice(buf);
    }

    /// Attempt to decode another WAL record from the input that has been fed to the
    /// decoder so far.
    ///
    /// Returns one of the following:
    ///     Ok((Lsn, Bytes)): a tuple containing the LSN of next record, and the record itself
    ///     Ok(None): there is not enough data in the input buffer. Feed more by calling the `feed_bytes` function
    ///     Err(WalDecodeError): an error occured while decoding, meaning the input was invalid.
    ///
    pub fn poll_decode(&mut self) -> Result<Option<(Lsn, Bytes)>, WalDecodeError> {
        let recordbuf;

        // Run state machine that validates page headers, and reassembles records
        // that cross page boundaries.
        loop {
            // parse and verify page boundaries as we go
            if self.lsn.segment_offset(pg_constants::WAL_SEGMENT_SIZE) == 0 {
                // parse long header

                if self.inputbuf.remaining() < XLOG_SIZE_OF_XLOG_LONG_PHD {
                    return Ok(None);
                }

                let hdr = XLogLongPageHeaderData::from_bytes(&mut self.inputbuf);

                if hdr.std.xlp_pageaddr != self.lsn.0 {
                    return Err(WalDecodeError {
                        msg: "invalid xlog segment header".into(),
                        lsn: self.lsn,
                    });
                }
                // TODO: verify the remaining fields in the header

                self.lsn += XLOG_SIZE_OF_XLOG_LONG_PHD as u64;
                continue;
            } else if self.lsn.block_offset() == 0 {
                if self.inputbuf.remaining() < XLOG_SIZE_OF_XLOG_SHORT_PHD {
                    return Ok(None);
                }

                let hdr = XLogPageHeaderData::from_bytes(&mut self.inputbuf);

                if hdr.xlp_pageaddr != self.lsn.0 {
                    return Err(WalDecodeError {
                        msg: "invalid xlog page header".into(),
                        lsn: self.lsn,
                    });
                }
                // TODO: verify the remaining fields in the header

                self.lsn += XLOG_SIZE_OF_XLOG_SHORT_PHD as u64;
                continue;
            } else if self.padlen > 0 {
                if self.inputbuf.remaining() < self.padlen as usize {
                    return Ok(None);
                }

                // skip padding
                self.inputbuf.advance(self.padlen as usize);
                self.lsn += self.padlen as u64;
                self.padlen = 0;
            } else if self.contlen == 0 {
                assert!(self.recordbuf.is_empty());

                // need to have at least the xl_tot_len field
                if self.inputbuf.remaining() < 4 {
                    return Ok(None);
                }

                // peek xl_tot_len at the beginning of the record.
                // FIXME: assumes little-endian
                self.startlsn = self.lsn;
                let xl_tot_len = (&self.inputbuf[0..4]).get_u32_le();
                if (xl_tot_len as usize) < XLOG_SIZE_OF_XLOG_RECORD {
                    return Err(WalDecodeError {
                        msg: format!("invalid xl_tot_len {}", xl_tot_len),
                        lsn: self.lsn,
                    });
                }

                // Fast path for the common case that the whole record fits on the page.
                let pageleft = self.lsn.remaining_in_block() as u32;
                if self.inputbuf.remaining() >= xl_tot_len as usize && xl_tot_len <= pageleft {
                    // Take the record from the 'inputbuf', and validate it.
                    recordbuf = self.inputbuf.copy_to_bytes(xl_tot_len as usize);
                    self.lsn += xl_tot_len as u64;
                    break;
                } else {
                    // Need to assemble the record from pieces. Remember the size of the
                    // record, and loop back. On next iteration, we will reach the 'else'
                    // branch below, and copy the part of the record that was on this page
                    // to 'recordbuf'.  Subsequent iterations will skip page headers, and
                    // append the continuations from the next pages to 'recordbuf'.
                    self.recordbuf.reserve(xl_tot_len as usize);
                    self.contlen = xl_tot_len;
                    continue;
                }
            } else {
                // we're continuing a record, possibly from previous page.
                let pageleft = self.lsn.remaining_in_block() as u32;

                // read the rest of the record, or as much as fits on this page.
                let n = min(self.contlen, pageleft) as usize;

                if self.inputbuf.remaining() < n {
                    return Ok(None);
                }

                self.recordbuf.put(self.inputbuf.split_to(n));
                self.lsn += n as u64;
                self.contlen -= n as u32;

                if self.contlen == 0 {
                    // The record is now complete.
                    recordbuf = std::mem::replace(&mut self.recordbuf, BytesMut::new()).freeze();
                    break;
                }
                continue;
            }
        }

        // We now have a record in the 'recordbuf' local variable.
        let xlogrec = XLogRecord::from_slice(&recordbuf[0..XLOG_SIZE_OF_XLOG_RECORD]);

        let mut crc = 0;
        crc = crc32c_append(crc, &recordbuf[XLOG_RECORD_CRC_OFFS + 4..]);
        crc = crc32c_append(crc, &recordbuf[0..XLOG_RECORD_CRC_OFFS]);
        if crc != xlogrec.xl_crc {
            return Err(WalDecodeError {
                msg: "WAL record crc mismatch".into(),
                lsn: self.lsn,
            });
        }

        // XLOG_SWITCH records are special. If we see one, we need to skip
        // to the next WAL segment.
        if xlogrec.is_xlog_switch_record() {
            trace!("saw xlog switch record at {}", self.lsn);
            self.padlen = self.lsn.calc_padding(pg_constants::WAL_SEGMENT_SIZE as u64) as u32;
        } else {
            // Pad to an 8-byte boundary
            self.padlen = self.lsn.calc_padding(8u32) as u32;
        }

        // Always align resulting LSN on 0x8 boundary -- that is important for getPage()
        // and WalReceiver integration. Since this code is used both for WalReceiver and
        // initial WAL import let's force alignment right here.
        let result = (self.lsn.align(), recordbuf);
        Ok(Some(result))
    }
}

#[allow(dead_code)]
#[derive(Default)]
pub struct DecodedBkpBlock {
    /* Is this block ref in use? */
    //in_use: bool,

    /* Identify the block this refers to */
    pub rnode_spcnode: u32,
    pub rnode_dbnode: u32,
    pub rnode_relnode: u32,
    // Note that we have a few special forknum values for non-rel files.
    pub forknum: u8,
    pub blkno: u32,

    /* copy of the fork_flags field from the XLogRecordBlockHeader */
    flags: u8,

    /* Information on full-page image, if any */
    has_image: bool,       /* has image, even for consistency checking */
    pub apply_image: bool, /* has image that should be restored */
    pub will_init: bool,   /* record doesn't need previous page version to apply */
    //char	   *bkp_image;
    hole_offset: u16,
    hole_length: u16,
    bimg_len: u16,
    bimg_info: u8,

    /* Buffer holding the rmgr-specific data associated with this block */
    has_data: bool,
    data_len: u16,
}

impl DecodedBkpBlock {
    pub fn new() -> DecodedBkpBlock {
        Default::default()
    }
}

pub struct DecodedWALRecord {
    pub xl_xid: TransactionId,
    pub xl_info: u8,
    pub xl_rmid: u8,
    pub record: Bytes, // raw XLogRecord

    pub blocks: Vec<DecodedBkpBlock>,
    pub main_data_offset: usize,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RelFileNode {
    pub spcnode: Oid, /* tablespace */
    pub dbnode: Oid,  /* database */
    pub relnode: Oid, /* relation */
}

#[repr(C)]
#[derive(Debug)]
pub struct XlRelmapUpdate {
    pub dbid: Oid,   /* database ID, or 0 for shared map */
    pub tsid: Oid,   /* database's tablespace, or pg_global */
    pub nbytes: i32, /* size of relmap data */
}

impl XlRelmapUpdate {
    pub fn decode(buf: &mut Bytes) -> XlRelmapUpdate {
        XlRelmapUpdate {
            dbid: buf.get_u32_le(),
            tsid: buf.get_u32_le(),
            nbytes: buf.get_i32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlSmgrTruncate {
    pub blkno: BlockNumber,
    pub rnode: RelFileNode,
    pub flags: u32,
}

impl XlSmgrTruncate {
    pub fn decode(buf: &mut Bytes) -> XlSmgrTruncate {
        XlSmgrTruncate {
            blkno: buf.get_u32_le(),
            rnode: RelFileNode {
                spcnode: buf.get_u32_le(), /* tablespace */
                dbnode: buf.get_u32_le(),  /* database */
                relnode: buf.get_u32_le(), /* relation */
            },
            flags: buf.get_u32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlCreateDatabase {
    pub db_id: Oid,
    pub tablespace_id: Oid,
    pub src_db_id: Oid,
    pub src_tablespace_id: Oid,
}

impl XlCreateDatabase {
    pub fn decode(buf: &mut Bytes) -> XlCreateDatabase {
        XlCreateDatabase {
            db_id: buf.get_u32_le(),
            tablespace_id: buf.get_u32_le(),
            src_db_id: buf.get_u32_le(),
            src_tablespace_id: buf.get_u32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlDropDatabase {
    pub db_id: Oid,
    pub n_tablespaces: Oid, /* number of tablespace IDs */
    pub tablespace_ids: Vec<Oid>,
}

impl XlDropDatabase {
    pub fn decode(buf: &mut Bytes) -> XlDropDatabase {
        let mut rec = XlDropDatabase {
            db_id: buf.get_u32_le(),
            n_tablespaces: buf.get_u32_le(),
            tablespace_ids: Vec::<Oid>::new(),
        };

        for _i in 0..rec.n_tablespaces {
            let id = buf.get_u32_le();
            rec.tablespace_ids.push(id);
        }

        rec
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlHeapInsert {
    pub offnum: OffsetNumber,
    pub flags: u8,
}

impl XlHeapInsert {
    pub fn decode(buf: &mut Bytes) -> XlHeapInsert {
        XlHeapInsert {
            offnum: buf.get_u16_le(),
            flags: buf.get_u8(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlHeapMultiInsert {
    pub flags: u8,
    pub _padding: u8,
    pub ntuples: u16,
}

impl XlHeapMultiInsert {
    pub fn decode(buf: &mut Bytes) -> XlHeapMultiInsert {
        XlHeapMultiInsert {
            flags: buf.get_u8(),
            _padding: buf.get_u8(),
            ntuples: buf.get_u16_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlHeapDelete {
    pub xmax: TransactionId,
    pub offnum: OffsetNumber,
    pub _padding: u16,
    pub t_cid: u32,
    pub infobits_set: u8,
    pub flags: u8,
}

impl XlHeapDelete {
    pub fn decode(buf: &mut Bytes) -> XlHeapDelete {
        XlHeapDelete {
            xmax: buf.get_u32_le(),
            offnum: buf.get_u16_le(),
            _padding: buf.get_u16_le(),
            t_cid: buf.get_u32_le(),
            infobits_set: buf.get_u8(),
            flags: buf.get_u8(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlHeapUpdate {
    pub old_xmax: TransactionId,
    pub old_offnum: OffsetNumber,
    pub old_infobits_set: u8,
    pub flags: u8,
    pub t_cid: u32,
    pub new_xmax: TransactionId,
    pub new_offnum: OffsetNumber,
}

impl XlHeapUpdate {
    pub fn decode(buf: &mut Bytes) -> XlHeapUpdate {
        XlHeapUpdate {
            old_xmax: buf.get_u32_le(),
            old_offnum: buf.get_u16_le(),
            old_infobits_set: buf.get_u8(),
            flags: buf.get_u8(),
            t_cid: buf.get_u32(),
            new_xmax: buf.get_u32_le(),
            new_offnum: buf.get_u16_le(),
        }
    }
}

///
/// Note: Parsing some fields is missing, because they're not needed.
///
/// This is similar to the xl_xact_parsed_commit and
/// xl_xact_parsed_abort structs in PostgreSQL, but we use the same
/// struct for commits and aborts.
///
#[derive(Debug)]
pub struct XlXactParsedRecord {
    pub xid: TransactionId,
    pub info: u8,
    pub xact_time: TimestampTz,
    pub xinfo: u32,

    pub db_id: Oid, /* MyDatabaseId */
    pub ts_id: Oid, /* MyDatabaseTableSpace */

    pub subxacts: Vec<TransactionId>,

    pub xnodes: Vec<RelFileNode>,
}

impl XlXactParsedRecord {
    /// Decode a XLOG_XACT_COMMIT/ABORT/COMMIT_PREPARED/ABORT_PREPARED
    /// record. This should agree with the ParseCommitRecord and ParseAbortRecord
    /// functions in PostgreSQL (in src/backend/access/rmgr/xactdesc.c)
    pub fn decode(buf: &mut Bytes, mut xid: TransactionId, xl_info: u8) -> XlXactParsedRecord {
        let info = xl_info & pg_constants::XLOG_XACT_OPMASK;
        // The record starts with time of commit/abort
        let xact_time = buf.get_i64_le();
        let xinfo;
        if xl_info & pg_constants::XLOG_XACT_HAS_INFO != 0 {
            xinfo = buf.get_u32_le();
        } else {
            xinfo = 0;
        }
        let db_id;
        let ts_id;
        if xinfo & pg_constants::XACT_XINFO_HAS_DBINFO != 0 {
            db_id = buf.get_u32_le();
            ts_id = buf.get_u32_le();
        } else {
            db_id = 0;
            ts_id = 0;
        }
        let mut subxacts = Vec::<TransactionId>::new();
        if xinfo & pg_constants::XACT_XINFO_HAS_SUBXACTS != 0 {
            let nsubxacts = buf.get_i32_le();
            for _i in 0..nsubxacts {
                let subxact = buf.get_u32_le();
                subxacts.push(subxact);
            }
        }
        let mut xnodes = Vec::<RelFileNode>::new();
        if xinfo & pg_constants::XACT_XINFO_HAS_RELFILENODES != 0 {
            let nrels = buf.get_i32_le();
            for _i in 0..nrels {
                let spcnode = buf.get_u32_le();
                let dbnode = buf.get_u32_le();
                let relnode = buf.get_u32_le();
                trace!(
                    "XLOG_XACT_COMMIT relfilenode {}/{}/{}",
                    spcnode,
                    dbnode,
                    relnode
                );
                xnodes.push(RelFileNode {
                    spcnode,
                    dbnode,
                    relnode,
                });
            }
        }
        if xinfo & pg_constants::XACT_XINFO_HAS_INVALS != 0 {
            let nmsgs = buf.get_i32_le();
            for _i in 0..nmsgs {
                let sizeof_shared_invalidation_message = 0;
                buf.advance(sizeof_shared_invalidation_message);
            }
        }
        if xinfo & pg_constants::XACT_XINFO_HAS_TWOPHASE != 0 {
            xid = buf.get_u32_le();
            trace!("XLOG_XACT_COMMIT-XACT_XINFO_HAS_TWOPHASE");
        }
        XlXactParsedRecord {
            xid,
            info,
            xact_time,
            xinfo,
            db_id,
            ts_id,
            subxacts,
            xnodes,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlClogTruncate {
    pub pageno: u32,
    pub oldest_xid: TransactionId,
    pub oldest_xid_db: Oid,
}

impl XlClogTruncate {
    pub fn decode(buf: &mut Bytes) -> XlClogTruncate {
        XlClogTruncate {
            pageno: buf.get_u32_le(),
            oldest_xid: buf.get_u32_le(),
            oldest_xid_db: buf.get_u32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct MultiXactMember {
    pub xid: TransactionId,
    pub status: MultiXactStatus,
}

impl MultiXactMember {
    pub fn decode(buf: &mut Bytes) -> MultiXactMember {
        MultiXactMember {
            xid: buf.get_u32_le(),
            status: buf.get_u32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlMultiXactCreate {
    pub mid: MultiXactId,      /* new MultiXact's ID */
    pub moff: MultiXactOffset, /* its starting offset in members file */
    pub nmembers: u32,         /* number of member XIDs */
    pub members: Vec<MultiXactMember>,
}

impl XlMultiXactCreate {
    pub fn decode(buf: &mut Bytes) -> XlMultiXactCreate {
        let mid = buf.get_u32_le();
        let moff = buf.get_u32_le();
        let nmembers = buf.get_u32_le();
        let mut members = Vec::new();
        for _ in 0..nmembers {
            members.push(MultiXactMember::decode(buf));
        }
        XlMultiXactCreate {
            mid,
            moff,
            nmembers,
            members,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlMultiXactTruncate {
    pub oldest_multi_db: Oid,
    /* to-be-truncated range of multixact offsets */
    pub start_trunc_off: MultiXactId, /* just for completeness' sake */
    pub end_trunc_off: MultiXactId,

    /* to-be-truncated range of multixact members */
    pub start_trunc_memb: MultiXactOffset,
    pub end_trunc_memb: MultiXactOffset,
}

impl XlMultiXactTruncate {
    pub fn decode(buf: &mut Bytes) -> XlMultiXactTruncate {
        XlMultiXactTruncate {
            oldest_multi_db: buf.get_u32_le(),
            start_trunc_off: buf.get_u32_le(),
            end_trunc_off: buf.get_u32_le(),
            start_trunc_memb: buf.get_u32_le(),
            end_trunc_memb: buf.get_u32_le(),
        }
    }
}

/// Main routine to decode a WAL record and figure out which blocks are modified
//
// See xlogrecord.h for details
// The overall layout of an XLOG record is:
//		Fixed-size header (XLogRecord struct)
//      XLogRecordBlockHeader struct
//          If pg_constants::BKPBLOCK_HAS_IMAGE, an XLogRecordBlockImageHeader struct follows
//	           If pg_constants::BKPIMAGE_HAS_HOLE and pg_constants::BKPIMAGE_IS_COMPRESSED, an
//	           XLogRecordBlockCompressHeader struct follows.
//          If pg_constants::BKPBLOCK_SAME_REL is not set, a RelFileNode follows
//          BlockNumber follows
//      XLogRecordBlockHeader struct
//      ...
//      XLogRecordDataHeader[Short|Long] struct
//      block data
//      block data
//      ...
//      main data
pub fn decode_wal_record(record: Bytes) -> DecodedWALRecord {
    let mut rnode_spcnode: u32 = 0;
    let mut rnode_dbnode: u32 = 0;
    let mut rnode_relnode: u32 = 0;
    let mut got_rnode = false;

    let mut buf = record.clone();

    // 1. Parse XLogRecord struct

    // FIXME: assume little-endian here
    let xlogrec = XLogRecord::from_bytes(&mut buf);

    trace!(
        "decode_wal_record xl_rmid = {} xl_info = {}",
        xlogrec.xl_rmid,
        xlogrec.xl_info
    );

    let remaining: usize = xlogrec.xl_tot_len as usize - XLOG_SIZE_OF_XLOG_RECORD;

    if buf.remaining() != remaining {
        //TODO error
    }

    let mut max_block_id = 0;
    let mut blocks_total_len: u32 = 0;
    let mut main_data_len = 0;
    let mut datatotal: u32 = 0;
    let mut blocks: Vec<DecodedBkpBlock> = Vec::new();

    // 2. Decode the headers.
    // XLogRecordBlockHeaders if any,
    // XLogRecordDataHeader[Short|Long]
    while buf.remaining() > datatotal as usize {
        let block_id = buf.get_u8();

        match block_id {
            pg_constants::XLR_BLOCK_ID_DATA_SHORT => {
                /* XLogRecordDataHeaderShort */
                main_data_len = buf.get_u8() as u32;
                datatotal += main_data_len;
            }

            pg_constants::XLR_BLOCK_ID_DATA_LONG => {
                /* XLogRecordDataHeaderLong */
                main_data_len = buf.get_u32_le();
                datatotal += main_data_len;
            }

            pg_constants::XLR_BLOCK_ID_ORIGIN => {
                // RepOriginId is uint16
                buf.advance(2);
            }

            pg_constants::XLR_BLOCK_ID_TOPLEVEL_XID => {
                // TransactionId is uint32
                buf.advance(4);
            }

            0..=pg_constants::XLR_MAX_BLOCK_ID => {
                /* XLogRecordBlockHeader */
                let mut blk = DecodedBkpBlock::new();
                let fork_flags: u8;

                if block_id <= max_block_id {
                    // TODO
                    //report_invalid_record(state,
                    //			  "out-of-order block_id %u at %X/%X",
                    //			  block_id,
                    //			  (uint32) (state->ReadRecPtr >> 32),
                    //			  (uint32) state->ReadRecPtr);
                    //    goto err;
                }
                max_block_id = block_id;

                fork_flags = buf.get_u8();
                blk.forknum = fork_flags & pg_constants::BKPBLOCK_FORK_MASK;
                blk.flags = fork_flags;
                blk.has_image = (fork_flags & pg_constants::BKPBLOCK_HAS_IMAGE) != 0;
                blk.has_data = (fork_flags & pg_constants::BKPBLOCK_HAS_DATA) != 0;
                blk.will_init = (fork_flags & pg_constants::BKPBLOCK_WILL_INIT) != 0;
                blk.data_len = buf.get_u16_le();

                /* TODO cross-check that the HAS_DATA flag is set iff data_length > 0 */

                datatotal += blk.data_len as u32;
                blocks_total_len += blk.data_len as u32;

                if blk.has_image {
                    blk.bimg_len = buf.get_u16_le();
                    blk.hole_offset = buf.get_u16_le();
                    blk.bimg_info = buf.get_u8();

                    blk.apply_image = (blk.bimg_info & pg_constants::BKPIMAGE_APPLY) != 0;

                    if blk.bimg_info & pg_constants::BKPIMAGE_IS_COMPRESSED != 0 {
                        if blk.bimg_info & pg_constants::BKPIMAGE_HAS_HOLE != 0 {
                            blk.hole_length = buf.get_u16_le();
                        } else {
                            blk.hole_length = 0;
                        }
                    } else {
                        blk.hole_length = pg_constants::BLCKSZ - blk.bimg_len;
                    }
                    datatotal += blk.bimg_len as u32;
                    blocks_total_len += blk.bimg_len as u32;

                    /*
                     * cross-check that hole_offset > 0, hole_length > 0 and
                     * bimg_len < BLCKSZ if the HAS_HOLE flag is set.
                     */
                    if blk.bimg_info & pg_constants::BKPIMAGE_HAS_HOLE != 0
                        && (blk.hole_offset == 0
                            || blk.hole_length == 0
                            || blk.bimg_len == pg_constants::BLCKSZ)
                    {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "pg_constants::BKPIMAGE_HAS_HOLE set, but hole offset %u length %u block image length %u at %X/%X",
                                      (unsigned int) blk->hole_offset,
                                      (unsigned int) blk->hole_length,
                                      (unsigned int) blk->bimg_len,
                                      (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                        goto err;
                                     */
                    }

                    /*
                     * cross-check that hole_offset == 0 and hole_length == 0 if
                     * the HAS_HOLE flag is not set.
                     */
                    if blk.bimg_info & pg_constants::BKPIMAGE_HAS_HOLE == 0
                        && (blk.hole_offset != 0 || blk.hole_length != 0)
                    {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "pg_constants::BKPIMAGE_HAS_HOLE not set, but hole offset %u length %u at %X/%X",
                                      (unsigned int) blk->hole_offset,
                                      (unsigned int) blk->hole_length,
                                      (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                        goto err;
                                     */
                    }

                    /*
                     * cross-check that bimg_len < BLCKSZ if the IS_COMPRESSED
                     * flag is set.
                     */
                    if (blk.bimg_info & pg_constants::BKPIMAGE_IS_COMPRESSED == 0)
                        && blk.bimg_len == pg_constants::BLCKSZ
                    {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "pg_constants::BKPIMAGE_IS_COMPRESSED set, but block image length %u at %X/%X",
                                      (unsigned int) blk->bimg_len,
                                      (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                        goto err;
                                     */
                    }

                    /*
                     * cross-check that bimg_len = BLCKSZ if neither HAS_HOLE nor
                     * IS_COMPRESSED flag is set.
                     */
                    if blk.bimg_info & pg_constants::BKPIMAGE_HAS_HOLE == 0
                        && blk.bimg_info & pg_constants::BKPIMAGE_IS_COMPRESSED == 0
                        && blk.bimg_len != pg_constants::BLCKSZ
                    {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "neither pg_constants::BKPIMAGE_HAS_HOLE nor pg_constants::BKPIMAGE_IS_COMPRESSED set, but block image length is %u at %X/%X",
                                      (unsigned int) blk->data_len,
                                      (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                        goto err;
                                     */
                    }
                }
                if fork_flags & pg_constants::BKPBLOCK_SAME_REL == 0 {
                    rnode_spcnode = buf.get_u32_le();
                    rnode_dbnode = buf.get_u32_le();
                    rnode_relnode = buf.get_u32_le();
                    got_rnode = true;
                } else if !got_rnode {
                    // TODO
                    /*
                    report_invalid_record(state,
                                    "pg_constants::BKPBLOCK_SAME_REL set but no previous rel at %X/%X",
                                    (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                    goto err;           */
                }

                blk.rnode_spcnode = rnode_spcnode;
                blk.rnode_dbnode = rnode_dbnode;
                blk.rnode_relnode = rnode_relnode;

                blk.blkno = buf.get_u32_le();
                trace!(
                    "this record affects {}/{}/{} blk {}",
                    rnode_spcnode,
                    rnode_dbnode,
                    rnode_relnode,
                    blk.blkno
                );

                blocks.push(blk);
            }

            _ => {
                // TODO: invalid block_id
            }
        }
    }

    // 3. Decode blocks.
    // We don't need them, so just skip blocks_total_len bytes
    buf.advance(blocks_total_len as usize);

    let main_data_offset = (xlogrec.xl_tot_len - main_data_len) as usize;

    // 4. Decode main_data
    if main_data_len > 0 {
        assert_eq!(buf.remaining(), main_data_len as usize);
    }

    // 5. Handle a few special record types that modify blocks without registering
    // them with the standard mechanism.
    if xlogrec.xl_rmid == pg_constants::RM_HEAP_ID {
        let info = xlogrec.xl_info & pg_constants::XLOG_HEAP_OPMASK;
        let blkno = blocks[0].blkno / pg_constants::HEAPBLOCKS_PER_PAGE as u32;
        if info == pg_constants::XLOG_HEAP_INSERT {
            let xlrec = XlHeapInsert::decode(&mut buf);
            assert_eq!(0, buf.remaining());
            if (xlrec.flags
                & (pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED
                    | pg_constants::XLH_INSERT_ALL_FROZEN_SET))
                != 0
            {
                let mut blk = DecodedBkpBlock::new();
                blk.forknum = pg_constants::VISIBILITYMAP_FORKNUM;
                blk.blkno = blkno;
                blk.rnode_spcnode = blocks[0].rnode_spcnode;
                blk.rnode_dbnode = blocks[0].rnode_dbnode;
                blk.rnode_relnode = blocks[0].rnode_relnode;
                blocks.push(blk);
            }
        } else if info == pg_constants::XLOG_HEAP_DELETE {
            let xlrec = XlHeapDelete::decode(&mut buf);
            assert_eq!(0, buf.remaining());
            if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                let mut blk = DecodedBkpBlock::new();
                blk.forknum = pg_constants::VISIBILITYMAP_FORKNUM;
                blk.blkno = blkno;
                blk.rnode_spcnode = blocks[0].rnode_spcnode;
                blk.rnode_dbnode = blocks[0].rnode_dbnode;
                blk.rnode_relnode = blocks[0].rnode_relnode;
                blocks.push(blk);
            }
        } else if info == pg_constants::XLOG_HEAP_UPDATE
            || info == pg_constants::XLOG_HEAP_HOT_UPDATE
        {
            let xlrec = XlHeapUpdate::decode(&mut buf);
            // the size of tuple data is inferred from the size of the record.
            // we can't validate the remaining number of bytes without parsing
            // the tuple data.
            if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                let mut blk = DecodedBkpBlock::new();
                blk.forknum = pg_constants::VISIBILITYMAP_FORKNUM;
                blk.blkno = blkno;
                blk.rnode_spcnode = blocks[0].rnode_spcnode;
                blk.rnode_dbnode = blocks[0].rnode_dbnode;
                blk.rnode_relnode = blocks[0].rnode_relnode;
                blocks.push(blk);
            }
            if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0
                && blocks.len() > 1
            {
                let mut blk = DecodedBkpBlock::new();
                blk.forknum = pg_constants::VISIBILITYMAP_FORKNUM;
                blk.blkno = blocks[1].blkno / pg_constants::HEAPBLOCKS_PER_PAGE as u32;
                blk.rnode_spcnode = blocks[1].rnode_spcnode;
                blk.rnode_dbnode = blocks[1].rnode_dbnode;
                blk.rnode_relnode = blocks[1].rnode_relnode;
                blocks.push(blk);
            }
        }
    } else if xlogrec.xl_rmid == pg_constants::RM_HEAP2_ID {
        let info = xlogrec.xl_info & pg_constants::XLOG_HEAP_OPMASK;
        if info == pg_constants::XLOG_HEAP2_MULTI_INSERT {
            let xlrec = XlHeapMultiInsert::decode(&mut buf);

            let offset_array_len = if xlogrec.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                0
            } else {
                std::mem::size_of::<u16>() * xlrec.ntuples as usize
            };
            assert_eq!(offset_array_len, buf.remaining());

            if (xlrec.flags
                & (pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED
                    | pg_constants::XLH_INSERT_ALL_FROZEN_SET))
                != 0
            {
                let mut blk = DecodedBkpBlock::new();
                let blkno = blocks[0].blkno / pg_constants::HEAPBLOCKS_PER_PAGE as u32;
                blk.forknum = pg_constants::VISIBILITYMAP_FORKNUM;
                blk.blkno = blkno;
                blk.rnode_spcnode = blocks[0].rnode_spcnode;
                blk.rnode_dbnode = blocks[0].rnode_dbnode;
                blk.rnode_relnode = blocks[0].rnode_relnode;
                blocks.push(blk);
            }
        }
    }

    DecodedWALRecord {
        xl_xid: xlogrec.xl_xid,
        xl_info: xlogrec.xl_info,
        xl_rmid: xlogrec.xl_rmid,
        record,
        blocks,
        main_data_offset,
    }
}

///
/// Build a human-readable string to describe a WAL record
///
/// For debugging purposes
pub fn describe_wal_record(record: &Bytes) -> String {
    // TODO: It would be nice to use the PostgreSQL rmgrdesc infrastructure for this.
    // Maybe use the postgres wal redo process, the same used for replaying WAL records?
    // Or could we compile the rmgrdesc routines into the dump_layer_file() binary directly,
    // without worrying about security?
    //
    // But for now, we have a hand-written code for a few common WAL record types here.

    let mut buf = record.clone();

    // 1. Parse XLogRecord struct

    // FIXME: assume little-endian here
    let xlogrec = XLogRecord::from_bytes(&mut buf);

    let unknown_str: String;

    let result: &str = match xlogrec.xl_rmid {
        pg_constants::RM_HEAP2_ID => {
            let info = xlogrec.xl_info & pg_constants::XLOG_HEAP_OPMASK;
            match info {
                pg_constants::XLOG_HEAP2_MULTI_INSERT => "HEAP2 MULTI_INSERT",
                pg_constants::XLOG_HEAP2_VISIBLE => "HEAP2 VISIBLE",
                _ => {
                    unknown_str = format!("HEAP2 UNKNOWN_0x{:02x}", info);
                    &unknown_str
                }
            }
        }
        pg_constants::RM_HEAP_ID => {
            let info = xlogrec.xl_info & pg_constants::XLOG_HEAP_OPMASK;
            match info {
                pg_constants::XLOG_HEAP_INSERT => "HEAP INSERT",
                pg_constants::XLOG_HEAP_DELETE => "HEAP DELETE",
                pg_constants::XLOG_HEAP_UPDATE => "HEAP UPDATE",
                pg_constants::XLOG_HEAP_HOT_UPDATE => "HEAP HOT_UPDATE",
                _ => {
                    unknown_str = format!("HEAP2 UNKNOWN_0x{:02x}", info);
                    &unknown_str
                }
            }
        }
        pg_constants::RM_XLOG_ID => {
            let info = xlogrec.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
            match info {
                pg_constants::XLOG_FPI => "XLOG FPI",
                pg_constants::XLOG_FPI_FOR_HINT => "XLOG FPI_FOR_HINT",
                _ => {
                    unknown_str = format!("XLOG UNKNOWN_0x{:02x}", info);
                    &unknown_str
                }
            }
        }
        rmid => {
            let info = xlogrec.xl_info & pg_constants::XLR_RMGR_INFO_MASK;

            unknown_str = format!("UNKNOWN_RM_{} INFO_0x{:02x}", rmid, info);
            &unknown_str
        }
    };

    String::from(result)
}
