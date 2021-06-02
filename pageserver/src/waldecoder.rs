//!
//! WAL decoder. For each WAL record, it decodes the record to figure out which data blocks
//! the record affects, to add the records to the page cache.
//!
use crate::repository::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::*;
use postgres_ffi::pg_constants;
use postgres_ffi::xlog_utils::*;
use postgres_ffi::XLogLongPageHeaderData;
use postgres_ffi::XLogPageHeaderData;
use postgres_ffi::XLogRecord;
use postgres_ffi::CheckPoint;

use std::cmp::min;
use std::str;
use thiserror::Error;
use zenith_utils::lsn::Lsn;

pub type Oid = u32;
pub type TransactionId = u32;
pub type BlockNumber = u32;
pub type OffsetNumber = u16;
pub type MultiXactId = TransactionId;
pub type MultiXactOffset = u32;
pub type MultiXactStatus = u32;
pub type TimeLineID = u32;
pub type PgTime = i64;

const MAX_MBR_BLKNO: u32 =
    pg_constants::MAX_MULTIXACT_ID / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;

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

    pub fn set_position(&mut self, lsn: Lsn) {
        self.lsn = lsn;
        self.contlen = 0;
        self.padlen = 0;
        self.inputbuf.clear();
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
                // need to have at least the xl_tot_len field

                if self.inputbuf.remaining() < 4 {
                    return Ok(None);
                }

                // read xl_tot_len FIXME: assumes little-endian
                self.startlsn = self.lsn;
                let xl_tot_len = self.inputbuf.get_u32_le();
                if (xl_tot_len as usize) < XLOG_SIZE_OF_XLOG_RECORD {
                    return Err(WalDecodeError {
                        msg: format!("invalid xl_tot_len {}", xl_tot_len),
                        lsn: self.lsn,
                    });
                }
                self.lsn += 4;

                self.recordbuf.clear();
                self.recordbuf.reserve(xl_tot_len as usize);
                self.recordbuf.put_u32_le(xl_tot_len);

                self.contlen = xl_tot_len - 4;
                continue;
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
                    let recordbuf = std::mem::replace(&mut self.recordbuf, BytesMut::new());

                    let recordbuf = recordbuf.freeze();
                    let mut buf = recordbuf.clone();

                    // XLOG_SWITCH records are special. If we see one, we need to skip
                    // to the next WAL segment.
                    let xlogrec = XLogRecord::from_bytes(&mut buf);
                    if xlogrec.is_xlog_switch_record() {
                        trace!("saw xlog switch record at {}", self.lsn);
                        self.padlen =
                            self.lsn.calc_padding(pg_constants::WAL_SEGMENT_SIZE as u64) as u32;
                    } else {
                        // Pad to an 8-byte boundary
                        self.padlen = self.lsn.calc_padding(8u32) as u32;
                    }

                    let result = (self.lsn, recordbuf);
                    return Ok(Some(result));
                }
                continue;
            }
        }
        // check record boundaries

        // deal with continuation records

        // deal with xlog_switch records
    }
}

#[allow(dead_code)]
pub struct DecodedBkpBlock {
    /* Is this block ref in use? */
    //in_use: bool,

    /* Identify the block this refers to */
    pub tag: ObjectTag,

    /* copy of the fork_flags field from the XLogRecordBlockHeader */
    flags: u8,

    /* Information on full-page image, if any */
    has_image: bool,       /* has image, even for consistency checking */
    pub apply_image: bool, /* has image that should be restored */
    pub will_init: bool,   /* record doesn't need previous page version to apply */
    pub will_drop: bool,   /* record drops relation */
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
        DecodedBkpBlock {
            tag: ObjectTag::FirstTag,

            flags: 0,
            has_image: false,
            apply_image: false,
            will_init: false,
            will_drop: false,
            hole_offset: 0,
            hole_length: 0,
            bimg_len: 0,
            bimg_info: 0,

            has_data: false,
            data_len: 0,
        }
    }
}

pub struct DecodedWALRecord {
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
    pub fn decode(decoded: &DecodedWALRecord) -> XlSmgrTruncate {
        let mut buf = decoded.record.clone();
        buf.advance((XLOG_SIZE_OF_XLOG_RECORD + 2) as usize);
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
    pub fn decode(decoded: &DecodedWALRecord) -> XlCreateDatabase {
        let mut buf = decoded.record.clone();
        buf.advance((XLOG_SIZE_OF_XLOG_RECORD + 2) as usize);
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
    pub ntuples: u16,
}

impl XlHeapMultiInsert {
    pub fn decode(buf: &mut Bytes) -> XlHeapMultiInsert {
        XlHeapMultiInsert {
            flags: buf.get_u8(),
            ntuples: buf.get_u16_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlHeapDelete {
    pub xmax: TransactionId,
    pub offnum: OffsetNumber,
    pub infobits_set: u8,
    pub flags: u8,
}

impl XlHeapDelete {
    pub fn decode(buf: &mut Bytes) -> XlHeapDelete {
        XlHeapDelete {
            xmax: buf.get_u32_le(),
            offnum: buf.get_u16_le(),
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
            new_xmax: buf.get_u32_le(),
            new_offnum: buf.get_u16_le(),
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
    oldest_multi_db: Oid,
    /* to-be-truncated range of multixact offsets */
    start_trunc_off: MultiXactId, /* just for completeness' sake */
    end_trunc_off: MultiXactId,

    /* to-be-truncated range of multixact members */
    start_trunc_memb: MultiXactOffset,
    end_trunc_memb: MultiXactOffset,
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
pub fn decode_wal_record(checkpoint: &mut CheckPoint, record: Bytes) -> DecodedWALRecord {
    let mut spcnode: u32 = 0;
    let mut dbnode: u32 = 0;
    let mut relnode: u32 = 0;
    let mut forknum: u8;
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

    checkpoint.update_next_xid(xlogrec.xl_xid);
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
                forknum = fork_flags & pg_constants::BKPBLOCK_FORK_MASK;
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
                    spcnode = buf.get_u32_le();
                    dbnode = buf.get_u32_le();
                    relnode = buf.get_u32_le();
                    got_rnode = true;
                } else if !got_rnode {
                    // TODO
                    /*
                    report_invalid_record(state,
                                    "pg_constants::BKPBLOCK_SAME_REL set but no previous rel at %X/%X",
                                    (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                    goto err;           */
                }

                blk.tag = ObjectTag::RelationBuffer(BufferTag {
                    rel: RelTag {
                        forknum,
                        spcnode,
                        dbnode,
                        relnode,
                    },
                    blknum: buf.get_u32_le(),
                });
                trace!("this record affects {:?}", blk.tag);

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

    //5. Handle special CLOG and XACT records
    if xlogrec.xl_rmid == pg_constants::RM_CLOG_ID {
        let mut blk = DecodedBkpBlock::new();
        let blknum = buf.get_i32_le() as u32;
        blk.tag = ObjectTag::Clog(SlruBufferTag { blknum });
        let info = xlogrec.xl_info & !pg_constants::XLR_INFO_MASK;
        if info == pg_constants::CLOG_ZEROPAGE {
            blk.will_init = true;
        } else {
            assert!(info == pg_constants::CLOG_TRUNCATE);
            blk.will_drop = true;
            checkpoint.oldestXid = buf.get_u32_le();
            checkpoint.oldestXidDB = buf.get_u32_le();
            trace!(
                "RM_CLOG_ID truncate blkno {} oldestXid {} oldestXidDB {}",
                blknum,
                checkpoint.oldestXid,
                checkpoint.oldestXidDB
            );
        }
        trace!("RM_CLOG_ID updates block {}", blknum);
        blocks.push(blk);
    } else if xlogrec.xl_rmid == pg_constants::RM_XACT_ID {
        let info = xlogrec.xl_info & pg_constants::XLOG_XACT_OPMASK;
        if info == pg_constants::XLOG_XACT_COMMIT || info == pg_constants::XLOG_XACT_COMMIT_PREPARED
        {
            if info == pg_constants::XLOG_XACT_COMMIT {
                let mut blk = DecodedBkpBlock::new();
                let blknum = xlogrec.xl_xid / pg_constants::CLOG_XACTS_PER_PAGE;
                blk.tag = ObjectTag::Clog(SlruBufferTag { blknum });
                trace!(
					"XLOG_XACT_COMMIT xl_info {} xl_prev {:X}/{:X}  xid {} updates block {} main_data_len {}",
					xlogrec.xl_info, (xlogrec.xl_prev >> 32),
					xlogrec.xl_prev & 0xffffffff,
					xlogrec.xl_xid,
					blknum,
					main_data_len
				);
                blocks.push(blk);
            }
            //parse commit record to extract subtrans entries
            // xl_xact_commit starts with time of commit
            let _xact_time = buf.get_i64_le();

            let mut xinfo = 0;
            if xlogrec.xl_info & pg_constants::XLOG_XACT_HAS_INFO != 0 {
                xinfo = buf.get_u32_le();
            }
            if xinfo & pg_constants::XACT_XINFO_HAS_DBINFO != 0 {
                let _dbid = buf.get_u32_le();
                let _tsid = buf.get_u32_le();
            }
            if xinfo & pg_constants::XACT_XINFO_HAS_SUBXACTS != 0 {
                let nsubxacts = buf.get_i32_le();
                let mut prev_blknum = u32::MAX;
                for _i in 0..nsubxacts {
                    let subxact = buf.get_u32_le();
                    let blknum = subxact / pg_constants::CLOG_XACTS_PER_PAGE;
                    if prev_blknum != blknum {
                        prev_blknum = blknum;
                        let mut blk = DecodedBkpBlock::new();
                        blk.tag = ObjectTag::Clog(SlruBufferTag { blknum });
                        blocks.push(blk);
                    }
                }
            }
            if xinfo & pg_constants::XACT_XINFO_HAS_RELFILENODES != 0 {
                let nrels = buf.get_i32_le();
                for _i in 0..nrels {
                    let spcnode = buf.get_u32_le();
                    let dbnode = buf.get_u32_le();
                    let relnode = buf.get_u32_le();
                    let mut blk = DecodedBkpBlock::new();
                    blk.tag = ObjectTag::RelationMetadata(RelTag {
                        forknum: pg_constants::MAIN_FORKNUM,
                        spcnode,
                        dbnode,
                        relnode,
                    });
                    blk.will_drop = true;
                    blocks.push(blk);
                    trace!(
                        "XLOG_XACT_COMMIT relfilenode {}/{}/{}",
                        spcnode,
                        dbnode,
                        relnode
                    );
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
                let xid = buf.get_u32_le();
                let mut blk = DecodedBkpBlock::new();
                let blknum = xid / pg_constants::CLOG_XACTS_PER_PAGE;
                blk.tag = ObjectTag::Clog(SlruBufferTag { blknum });
                blocks.push(blk);
                trace!("XLOG_XACT_COMMIT-XACT_XINFO_HAS_TWOPHASE");
                //TODO handle this to be able to restore pg_twophase on node start
            }
        } else if info == pg_constants::XLOG_XACT_ABORT
            || info == pg_constants::XLOG_XACT_ABORT_PREPARED
        {
            if info == pg_constants::XLOG_XACT_ABORT {
                let mut blk = DecodedBkpBlock::new();
                let blknum = xlogrec.xl_xid / pg_constants::CLOG_XACTS_PER_PAGE;
                blk.tag = ObjectTag::Clog(SlruBufferTag { blknum });
                trace!(
					"XLOG_XACT_ABORT xl_info {} xl_prev {:X}/{:X} xid {} updates block {} main_data_len {}",
					xlogrec.xl_info, (xlogrec.xl_prev >> 32),
					xlogrec.xl_prev & 0xffffffff,
					xlogrec.xl_xid,
					blknum,
					main_data_len
				);
                blocks.push(blk);
            }
            //parse abort record to extract subtrans entries
            // xl_xact_abort starts with time of commit
            let _xact_time = buf.get_i64_le();

            let mut xinfo = 0;
            if xlogrec.xl_info & pg_constants::XLOG_XACT_HAS_INFO != 0 {
                xinfo = buf.get_u32_le();
            }
            if xinfo & pg_constants::XACT_XINFO_HAS_DBINFO != 0 {
                let _dbid = buf.get_u32_le();
                let _tsid = buf.get_u32_le();
            }
            if xinfo & pg_constants::XACT_XINFO_HAS_SUBXACTS != 0 {
                let nsubxacts = buf.get_i32_le();
                let mut prev_blknum = u32::MAX;
                for _i in 0..nsubxacts {
                    let subxact = buf.get_u32_le();
                    let blknum = subxact / pg_constants::CLOG_XACTS_PER_PAGE;
                    if prev_blknum != blknum {
                        prev_blknum = blknum;
                        let mut blk = DecodedBkpBlock::new();
                        blk.tag = ObjectTag::Clog(SlruBufferTag { blknum });
                        blocks.push(blk);
                    }
                }
            }
            if xinfo & pg_constants::XACT_XINFO_HAS_RELFILENODES != 0 {
                let nrels = buf.get_i32_le();
                for _i in 0..nrels {
                    let spcnode = buf.get_u32_le();
                    let dbnode = buf.get_u32_le();
                    let relnode = buf.get_u32_le();
                    let mut blk = DecodedBkpBlock::new();
                    blk.tag = ObjectTag::RelationMetadata(RelTag {
                        forknum: pg_constants::MAIN_FORKNUM,
                        spcnode,
                        dbnode,
                        relnode,
                    });
                    blk.will_drop = true;
                    blocks.push(blk);
                    trace!(
                        "XLOG_XACT_ABORT relfilenode {}/{}/{}",
                        spcnode,
                        dbnode,
                        relnode
                    );
                }
            }
            if xinfo & pg_constants::XACT_XINFO_HAS_TWOPHASE != 0 {
                let xid = buf.get_u32_le();
                let mut blk = DecodedBkpBlock::new();
                let blknum = xid / pg_constants::CLOG_XACTS_PER_PAGE;
                blk.tag = ObjectTag::Clog(SlruBufferTag { blknum });
                blocks.push(blk);
                trace!("XLOG_XACT_ABORT-XACT_XINFO_HAS_TWOPHASE");
            }
        } else if info == pg_constants::XLOG_XACT_PREPARE {
            let mut blk = DecodedBkpBlock::new();
            blk.tag = ObjectTag::TwoPhase(PrepareTag {
                xid: xlogrec.xl_xid,
            });
            blk.will_init = true;
            blocks.push(blk);
            debug!("Prepare transaction {}", xlogrec.xl_xid);
        }
    } else if xlogrec.xl_rmid == pg_constants::RM_DBASE_ID {
        let info = xlogrec.xl_info & !pg_constants::XLR_INFO_MASK;
        if info == pg_constants::XLOG_DBASE_CREATE {
            //buf points to main_data
            let db_id = buf.get_u32_le();
            let tablespace_id = buf.get_u32_le();
            let src_db_id = buf.get_u32_le();
            let src_tablespace_id = buf.get_u32_le();
            trace!(
                "XLOG_DBASE_CREATE tablespace_id/db_id {}/{} src_db_id {}/{}",
                tablespace_id,
                db_id,
                src_tablespace_id,
                src_db_id
            );
            // in postgres it is implemented as copydir
            // we need to copy all pages in page_cache
        } else {
            trace!("XLOG_DBASE_DROP is not handled yet");
        }
    } else if xlogrec.xl_rmid == pg_constants::RM_TBLSPC_ID {
        let info = xlogrec.xl_info & !pg_constants::XLR_INFO_MASK;
        if info == pg_constants::XLOG_TBLSPC_CREATE {
            //buf points to main_data
            let ts_id = buf.get_u32_le();
            let ts_path = str::from_utf8(&buf).unwrap();
            trace!("XLOG_TBLSPC_CREATE ts_id {} ts_path {}", ts_id, ts_path);
        } else {
            trace!("XLOG_TBLSPC_DROP is not handled yet");
        }
    } else if xlogrec.xl_rmid == pg_constants::RM_HEAP_ID {
        let info = xlogrec.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        if info == pg_constants::XLOG_HEAP_INSERT {
            let xlrec = XlHeapInsert::decode(&mut buf);
            if (xlrec.flags
                & (pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED
                    | pg_constants::XLH_INSERT_ALL_FROZEN_SET))
                != 0
            {
                if let ObjectTag::RelationBuffer(tag0) = blocks[0].tag {
                    let mut blk = DecodedBkpBlock::new();
                    blk.tag = ObjectTag::RelationBuffer(BufferTag {
                        rel: RelTag {
                            forknum: pg_constants::VISIBILITYMAP_FORKNUM,
                            spcnode: tag0.rel.spcnode,
                            dbnode: tag0.rel.dbnode,
                            relnode: tag0.rel.relnode,
                        },
                        blknum: tag0.blknum / pg_constants::HEAPBLOCKS_PER_PAGE as u32,
                    });
                    blocks.push(blk);
                } else {
                    panic!(
                        "Block 0 is expected to be relation buffer tag but it is {:?}",
                        blocks[0].tag
                    );
                }
            }
        } else if info == pg_constants::XLOG_HEAP_DELETE {
            let xlrec = XlHeapDelete::decode(&mut buf);
            if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                if let ObjectTag::RelationBuffer(tag0) = blocks[0].tag {
                    let mut blk = DecodedBkpBlock::new();
                    blk.tag = ObjectTag::RelationBuffer(BufferTag {
                        rel: RelTag {
                            forknum: pg_constants::VISIBILITYMAP_FORKNUM,
                            spcnode: tag0.rel.spcnode,
                            dbnode: tag0.rel.dbnode,
                            relnode: tag0.rel.relnode,
                        },
                        blknum: tag0.blknum / pg_constants::HEAPBLOCKS_PER_PAGE as u32,
                    });
                    blocks.push(blk);
                } else {
                    panic!(
                        "Block 0 is expected to be relation buffer tag but it is {:?}",
                        blocks[0].tag
                    );
                }
            }
        } else if info == pg_constants::XLOG_HEAP_UPDATE
            || info == pg_constants::XLOG_HEAP_HOT_UPDATE
        {
            let xlrec = XlHeapUpdate::decode(&mut buf);
            if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                if let ObjectTag::RelationBuffer(tag0) = blocks[0].tag {
                    let mut blk = DecodedBkpBlock::new();
                    blk.tag = ObjectTag::RelationBuffer(BufferTag {
                        rel: RelTag {
                            forknum: pg_constants::VISIBILITYMAP_FORKNUM,
                            spcnode: tag0.rel.spcnode,
                            dbnode: tag0.rel.dbnode,
                            relnode: tag0.rel.relnode,
                        },
                        blknum: tag0.blknum / pg_constants::HEAPBLOCKS_PER_PAGE as u32,
                    });
                    blocks.push(blk);
                } else {
                    panic!(
                        "Block 0 is expected to be relation buffer tag but it is {:?}",
                        blocks[0].tag
                    );
                }
            }
            if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0
                && blocks.len() > 1
            {
                if let ObjectTag::RelationBuffer(tag1) = blocks[1].tag {
                    let mut blk = DecodedBkpBlock::new();
                    blk.tag = ObjectTag::RelationBuffer(BufferTag {
                        rel: RelTag {
                            forknum: pg_constants::VISIBILITYMAP_FORKNUM,
                            spcnode: tag1.rel.spcnode,
                            dbnode: tag1.rel.dbnode,
                            relnode: tag1.rel.relnode,
                        },
                        blknum: tag1.blknum / pg_constants::HEAPBLOCKS_PER_PAGE as u32,
                    });
                    blocks.push(blk);
                } else {
                    panic!(
                        "Block 1 is expected to be relation buffer tag but it is {:?}",
                        blocks[1].tag
                    );
                }
            }
        }
    } else if xlogrec.xl_rmid == pg_constants::RM_HEAP2_ID {
        let info = xlogrec.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        if info == pg_constants::XLOG_HEAP2_MULTI_INSERT {
            let xlrec = XlHeapMultiInsert::decode(&mut buf);
            if (xlrec.flags
                & (pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED
                    | pg_constants::XLH_INSERT_ALL_FROZEN_SET))
                != 0
            {
                if let ObjectTag::RelationBuffer(tag0) = blocks[0].tag {
                    let mut blk = DecodedBkpBlock::new();
                    blk.tag = ObjectTag::RelationBuffer(BufferTag {
                        rel: RelTag {
                            forknum: pg_constants::VISIBILITYMAP_FORKNUM,
                            spcnode: tag0.rel.spcnode,
                            dbnode: tag0.rel.dbnode,
                            relnode: tag0.rel.relnode,
                        },
                        blknum: tag0.blknum / pg_constants::HEAPBLOCKS_PER_PAGE as u32,
                    });
                    blocks.push(blk);
                } else {
                    panic!(
                        "Block 0 is expected to be relation buffer tag but it is {:?}",
                        blocks[0].tag
                    );
                }
            }
        }
    } else if xlogrec.xl_rmid == pg_constants::RM_MULTIXACT_ID {
        let info = xlogrec.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        if info == pg_constants::XLOG_MULTIXACT_ZERO_OFF_PAGE {
            let mut blk = DecodedBkpBlock::new();
            blk.tag = ObjectTag::MultiXactOffsets(SlruBufferTag {
                blknum: buf.get_u32_le(),
            });
            blk.will_init = true;
            blocks.push(blk);
        } else if info == pg_constants::XLOG_MULTIXACT_ZERO_MEM_PAGE {
            let mut blk = DecodedBkpBlock::new();
            blk.tag = ObjectTag::MultiXactMembers(SlruBufferTag {
                blknum: buf.get_u32_le(),
            });
            blk.will_init = true;
            blocks.push(blk);
        } else if info == pg_constants::XLOG_MULTIXACT_CREATE_ID {
            let xlrec = XlMultiXactCreate::decode(&mut buf);
            // Update offset page
            let mut blk = DecodedBkpBlock::new();
            let blknum = xlrec.mid / pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32;
            blk.tag = ObjectTag::MultiXactOffsets(SlruBufferTag { blknum });
            blocks.push(blk);
            let first_mbr_blkno = xlrec.moff / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;
            let last_mbr_blkno =
                (xlrec.moff + xlrec.nmembers - 1) / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;
            // The members SLRU can, in contrast to the offsets one, be filled to almost
            // the full range at once. So we need to handle wraparound.
            let mut blknum = first_mbr_blkno;
            loop {
                // Update members page
                let mut blk = DecodedBkpBlock::new();
                blk.tag = ObjectTag::MultiXactMembers(SlruBufferTag { blknum });
                blocks.push(blk);

                if blknum == last_mbr_blkno {
                    // last block inclusive
                    break;
                }

                // handle wraparound
                if blknum == MAX_MBR_BLKNO {
                    blknum = 0;
                } else {
                    blknum += 1;
                }
            }
            if xlrec.mid >= checkpoint.nextMulti {
                checkpoint.nextMulti = xlrec.mid + 1;
            }
            if xlrec.moff + xlrec.nmembers > checkpoint.nextMultiOffset {
                checkpoint.nextMultiOffset = xlrec.moff + xlrec.nmembers;
            }
            let max_mbr_xid =
                xlrec.members.iter().fold(
                    0u32,
                    |acc, mbr| {
                        if mbr.xid > acc {
                            mbr.xid
                        } else {
                            acc
                        }
                    },
                );
            checkpoint.update_next_xid(max_mbr_xid);
        } else if info == pg_constants::XLOG_MULTIXACT_TRUNCATE_ID {
            let xlrec = XlMultiXactTruncate::decode(&mut buf);
            checkpoint.oldestMulti = xlrec.end_trunc_off;
            checkpoint.oldestMultiDB = xlrec.oldest_multi_db;
            let first_off_blkno =
                xlrec.start_trunc_off / pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32;
            let last_off_blkno =
                xlrec.end_trunc_off / pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32;
            // Delete all the segments but the last one. The last segment can still
            // contain, possibly partially, valid data.
            for blknum in first_off_blkno..last_off_blkno {
                let mut blk = DecodedBkpBlock::new();
                blk.tag = ObjectTag::MultiXactOffsets(SlruBufferTag { blknum });
                blk.will_drop = true;
                blocks.push(blk);
            }
            let first_mbr_blkno =
                xlrec.start_trunc_memb / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;
            let last_mbr_blkno =
                xlrec.end_trunc_memb / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;
            // The members SLRU can, in contrast to the offsets one, be filled to almost
            // the full range at once. So we need to handle wraparound.
            let mut blknum = first_mbr_blkno;
            // Delete all the segments but the last one. The last segment can still
            // contain, possibly partially, valid data.
            while blknum != last_mbr_blkno {
                let mut blk = DecodedBkpBlock::new();
                blk.tag = ObjectTag::MultiXactMembers(SlruBufferTag { blknum });
                blk.will_drop = true;
                blocks.push(blk);
                // handle wraparound
                if blknum == MAX_MBR_BLKNO {
                    blknum = 0;
                } else {
                    blknum += 1;
                }
            }
        } else {
            panic!()
        }
    } else if xlogrec.xl_rmid == pg_constants::RM_RELMAP_ID {
        let xlrec = XlRelmapUpdate::decode(&mut buf);
        let mut blk = DecodedBkpBlock::new();
        blk.tag = ObjectTag::FileNodeMap(DatabaseTag {
            spcnode: xlrec.tsid,
            dbnode: xlrec.dbid,
        });
        blk.will_init = true;
        blocks.push(blk);
    } else if xlogrec.xl_rmid == pg_constants::RM_XLOG_ID {
        let info = xlogrec.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        if info == pg_constants::XLOG_NEXTOID {
            let next_oid = buf.get_u32_le();
            if next_oid > checkpoint.nextOid {
                checkpoint.nextOid = next_oid;
            }
        }
    }

    DecodedWALRecord {
        xl_info: xlogrec.xl_info,
        xl_rmid: xlogrec.xl_rmid,
        record,
        blocks,
        main_data_offset,
    }
}
