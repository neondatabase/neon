//#![allow(non_upper_case_globals)]
//#![allow(non_camel_case_types)]
//#![allow(non_snake_case)]
//#![allow(dead_code)]
//include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use crate::pg_constants;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use std::cmp::min;

use log::*;

const XLOG_BLCKSZ: u32 = 8192;

// FIXME: this is configurable in PostgreSQL, 16 MB is the default
const WAL_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;

// From PostgreSQL headers

#[repr(C)]
#[derive(Debug)]
struct XLogPageHeaderData {
    xlp_magic: u16,    /* magic value for correctness checks */
    xlp_info: u16,     /* flag bits, see below */
    xlp_tli: u32,      /* TimeLineID of first record on page */
    xlp_pageaddr: u64, /* XLOG address of this page */
    xlp_rem_len: u32,  /* total len of remaining data for record */
}

// FIXME: this assumes MAXIMUM_ALIGNOF 8. There are 4 padding bytes at end
#[allow(non_upper_case_globals)]
const SizeOfXLogShortPHD: usize = 2 + 2 + 4 + 8 + 4 + 4;

#[repr(C)]
#[derive(Debug)]
struct XLogLongPageHeaderData {
    std: XLogPageHeaderData, /* standard header fields */
    xlp_sysid: u64,          /* system identifier from pg_control */
    xlp_seg_size: u32,       /* just as a cross-check */
    xlp_xlog_blcksz: u32,    /* just as a cross-check */
}

// FIXME: this assumes MAXIMUM_ALIGNOF 8.
#[allow(non_upper_case_globals)]
const SizeOfXLogLongPHD: usize = (2 + 2 + 4 + 8 + 4) + 4 + 8 + 4 + 4;

pub struct WalStreamDecoder {
    lsn: u64,

    startlsn: u64, // LSN where this record starts
    contlen: u32,
    padlen: u32,

    inputbuf: BytesMut,

    recordbuf: BytesMut,
}

//
// WalRecordStream is a Stream that returns a stream of WAL records
// FIXME: This isn't a proper rust stream
//
impl WalStreamDecoder {
    pub fn new(lsn: u64) -> WalStreamDecoder {
        WalStreamDecoder {
            lsn: lsn,

            startlsn: 0,
            contlen: 0,
            padlen: 0,

            inputbuf: BytesMut::new(),
            recordbuf: BytesMut::new(),
        }
    }

    pub fn feed_bytes(&mut self, buf: &[u8]) {
        self.inputbuf.extend_from_slice(buf);
    }

    // Returns a tuple:
    // (end LSN, record)
    pub fn poll_decode(&mut self) -> Option<(u64, Bytes)> {
        loop {
            // parse and verify page boundaries as we go
            if self.lsn % WAL_SEGMENT_SIZE == 0 {
                // parse long header

                if self.inputbuf.remaining() < SizeOfXLogLongPHD {
                    return None;
                }

                self.decode_XLogLongPageHeaderData();
                self.lsn += SizeOfXLogLongPHD as u64;

                // TODO: verify the fields in the header

                continue;
            } else if self.lsn % (XLOG_BLCKSZ as u64) == 0 {
                // parse page header

                if self.inputbuf.remaining() < SizeOfXLogShortPHD {
                    return None;
                }

                self.decode_XLogPageHeaderData();
                self.lsn += SizeOfXLogShortPHD as u64;

                // TODO: verify the fields in the header

                continue;
            } else if self.padlen > 0 {
                if self.inputbuf.remaining() < self.padlen as usize {
                    return None;
                }

                // skip padding
                self.inputbuf.advance(self.padlen as usize);
                self.lsn += self.padlen as u64;
                self.padlen = 0;
            } else if self.contlen == 0 {
                // need to have at least the xl_tot_len field

                if self.inputbuf.remaining() < 4 {
                    return None;
                }

                // read xl_tot_len FIXME: assumes little-endian
                self.startlsn = self.lsn;
                let xl_tot_len = self.inputbuf.get_u32_le();
                if xl_tot_len < SizeOfXLogRecord {
                    error!(
                        "invalid xl_tot_len {} at {:X}/{:X}",
                        xl_tot_len,
                        self.lsn >> 32,
                        self.lsn & 0xffffffff
                    );
                    panic!();
                }
                self.lsn += 4;

                self.recordbuf.clear();
                self.recordbuf.reserve(xl_tot_len as usize);
                self.recordbuf.put_u32_le(xl_tot_len);

                self.contlen = xl_tot_len - 4;
                continue;
            } else {
                // we're continuing a record, possibly from previous page.
                let pageleft: u32 = XLOG_BLCKSZ - (self.lsn % (XLOG_BLCKSZ as u64)) as u32;

                // read the rest of the record, or as much as fits on this page.
                let n = min(self.contlen, pageleft) as usize;

                if self.inputbuf.remaining() < n {
                    return None;
                }

                self.recordbuf.put(self.inputbuf.split_to(n));
                self.lsn += n as u64;
                self.contlen -= n as u32;

                if self.contlen == 0 {
                    let recordbuf = std::mem::replace(&mut self.recordbuf, BytesMut::new());

                    let recordbuf = recordbuf.freeze();

                    // XLOG_SWITCH records are special. If we see one, we need to skip
                    // to the next WAL segment.
                    if is_xlog_switch_record(&recordbuf) {
                        trace!(
                            "saw xlog switch record at {:X}/{:X}",
                            (self.lsn >> 32),
                            self.lsn & 0xffffffff
                        );
                        self.padlen = (WAL_SEGMENT_SIZE - (self.lsn % WAL_SEGMENT_SIZE)) as u32;
                    }

                    if self.lsn % 8 != 0 {
                        self.padlen = 8 - (self.lsn % 8) as u32;
                    }

                    let result = (self.lsn, recordbuf);
                    return Some(result);
                }
                continue;
            }
        }
        // check record boundaries

        // deal with continuation records

        // deal with xlog_switch records
    }

    #[allow(non_snake_case)]
    fn decode_XLogPageHeaderData(&mut self) -> XLogPageHeaderData {
        let buf = &mut self.inputbuf;

        // FIXME: Assume little-endian

        let hdr: XLogPageHeaderData = XLogPageHeaderData {
            xlp_magic: buf.get_u16_le(),
            xlp_info: buf.get_u16_le(),
            xlp_tli: buf.get_u32_le(),
            xlp_pageaddr: buf.get_u64_le(),
            xlp_rem_len: buf.get_u32_le(),
        };
        // 4 bytes of padding, on 64-bit systems
        buf.advance(4);

        // FIXME: check that hdr.xlp_rem_len matches self.contlen
        //println!("next xlog page (xlp_rem_len: {})", hdr.xlp_rem_len);

        return hdr;
    }

    #[allow(non_snake_case)]
    fn decode_XLogLongPageHeaderData(&mut self) -> XLogLongPageHeaderData {
        let hdr: XLogLongPageHeaderData = XLogLongPageHeaderData {
            std: self.decode_XLogPageHeaderData(),
            xlp_sysid: self.inputbuf.get_u64_le(),
            xlp_seg_size: self.inputbuf.get_u32_le(),
            xlp_xlog_blcksz: self.inputbuf.get_u32_le(),
        };

        return hdr;
    }
}

// FIXME:
const BLCKSZ: u16 = 8192;

//
// Constants from xlogrecord.h
//
const XLR_INFO_MASK: u8 = 0x0F;

const XLR_MAX_BLOCK_ID:u8 = 32;

const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
const XLR_BLOCK_ID_ORIGIN: u8 = 253;
const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;

const BKPBLOCK_FORK_MASK: u8 = 0x0F;
const _BKPBLOCK_FLAG_MASK: u8 = 0xF0;
const BKPBLOCK_HAS_IMAGE: u8 = 0x10; /* block data is an XLogRecordBlockImage */
const BKPBLOCK_HAS_DATA: u8 = 0x20;
const BKPBLOCK_WILL_INIT: u8 = 0x40; /* redo will re-init the page */
const BKPBLOCK_SAME_REL: u8 = 0x80; /* RelFileNode omitted, same as previous */

/* Information stored in bimg_info */
const BKPIMAGE_HAS_HOLE: u8 = 0x01; /* page image has "hole" */
const BKPIMAGE_IS_COMPRESSED: u8 = 0x02; /* page image is compressed */
const BKPIMAGE_APPLY: u8 = 0x04; /* page image should be restored during replay */

//
// constants from clog.h
//
const CLOG_XACTS_PER_BYTE:u32 = 4;
const CLOG_XACTS_PER_PAGE:u32 = 8192 * CLOG_XACTS_PER_BYTE;

pub struct DecodedBkpBlock {
    /* Is this block ref in use? */
    //in_use: bool,

    /* Identify the block this refers to */
    pub rnode_spcnode: u32,
    pub rnode_dbnode: u32,
    pub rnode_relnode: u32,
    pub forknum: u8, // Note that we have a few special forknum values for non-rel files. Handle them too
    pub blkno: u32,

    /* copy of the fork_flags field from the XLogRecordBlockHeader */
    flags: u8,

    /* Information on full-page image, if any */
    has_image: bool,       /* has image, even for consistency checking */
    pub apply_image: bool, /* has image that should be restored */
    pub will_init: bool,
    //char	   *bkp_image;
    hole_offset: u16,
    hole_length: u16,
    bimg_len: u16,
    bimg_info: u8,

    /* Buffer holding the rmgr-specific data associated with this block */
    has_data: bool,
    //char	   *data;
    data_len: u16,
}

#[allow(non_upper_case_globals)]
const SizeOfXLogRecord: u32 = 24;

pub struct DecodedWALRecord {
    pub lsn: u64,      // LSN at the *end* of the record
    pub record: Bytes, // raw XLogRecord

    pub blocks: Vec<DecodedBkpBlock>,
}

// From pg_control.h and rmgrlist.h
const XLOG_SWITCH: u8 = 0x40;
const RM_XLOG_ID: u8 = 0;

const RM_XACT_ID:u8 = 1;
// const RM_CLOG_ID:u8 = 3;
//const RM_MULTIXACT_ID:u8 = 6;

// from xact.h
const XLOG_XACT_COMMIT: u8 = 0x00;
// const XLOG_XACT_PREPARE: u8 = 0x10;
// const XLOG_XACT_ABORT: u8 = 0x20;
const XLOG_XACT_COMMIT_PREPARED: u8 = 0x30;
// const XLOG_XACT_ABORT_PREPARED: u8 = 0x40;
// const XLOG_XACT_ASSIGNMENT: u8 = 0x50;
// const XLOG_XACT_INVALIDATIONS: u8 = 0x60;
/* free opcode 0x70 */

/* mask for filtering opcodes out of xl_info */
const XLOG_XACT_OPMASK: u8 = 0x70;

/* does this record have a 'xinfo' field or not */
// const XLOG_XACT_HAS_INFO: u8 = 0x80;


// Is this record an XLOG_SWITCH record? They need some special processing,
// so we need to check for that before the rest of the parsing.
//
// FIXME: refactor this and decode_wal_record() below to avoid the duplication.
fn is_xlog_switch_record(rec: &Bytes) -> bool {
    let mut buf = rec.clone();

    // FIXME: assume little-endian here
    let _xl_tot_len = buf.get_u32_le();
    let _xl_xid = buf.get_u32_le();
    let _xl_prev = buf.get_u64_le();
    let xl_info = buf.get_u8();
    let xl_rmid = buf.get_u8();
    buf.advance(2); // 2 bytes of padding
    let _xl_crc = buf.get_u32_le();

    return xl_info == XLOG_SWITCH && xl_rmid == RM_XLOG_ID;
}

//
// Routines to decode a WAL record and figure out which blocks are modified
//
pub fn decode_wal_record(lsn: u64, rec: Bytes) -> DecodedWALRecord {
    trace!(
        "decoding record with LSN {:08X}/{:08X} ({} bytes)",
        lsn >> 32,
        lsn & 0xffff_ffff,
        rec.remaining()
    );

    let mut buf = rec.clone();

    // FIXME: assume little-endian here
    let xl_tot_len = buf.get_u32_le();
    let xl_xid = buf.get_u32_le();
    let _xl_prev = buf.get_u64_le();
    let xl_info = buf.get_u8();
    let xl_rmid = buf.get_u8();
    buf.advance(2); // 2 bytes of padding
    let _xl_crc = buf.get_u32_le();

    info!("decode_wal_record xl_rmid = {}" , xl_rmid);

	let rminfo: u8 = xl_info & !XLR_INFO_MASK;

    let remaining = xl_tot_len - SizeOfXLogRecord;

    if buf.remaining() != remaining as usize {
        //TODO error
    }

    let mut rnode_spcnode: u32 = 0;
    let mut rnode_dbnode: u32 = 0;
    let mut rnode_relnode: u32 = 0;
    let mut got_rnode = false;

    if xl_rmid == RM_XACT_ID &&
       ((rminfo & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT ||
        (rminfo & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT_PREPARED)
    {
        info!("decode_wal_record RM_XACT_ID - XLOG_XACT_COMMIT");

        let mut blocks: Vec<DecodedBkpBlock> = Vec::new();

        let blkno = xl_xid/CLOG_XACTS_PER_PAGE;

        let mut blk = DecodedBkpBlock {
            rnode_spcnode: 0,
            rnode_dbnode: 0,
            rnode_relnode: 0,
            forknum: pg_constants::PG_XACT_FORKNUM as u8,
            blkno: blkno,

            flags: 0,
            has_image: false,
            apply_image: false,
            will_init: false,
            hole_offset: 0,
            hole_length: 0,
            bimg_len: 0,
            bimg_info: 0,

            has_data: true,
            data_len: 0
        };

        let fork_flags = buf.get_u8();
        blk.has_data = (fork_flags & BKPBLOCK_HAS_DATA) != 0;
        blk.data_len = buf.get_u16_le();

        info!("decode_wal_record RM_XACT_ID blk has data with data_len {}", blk.data_len);

        blocks.push(blk);
        return DecodedWALRecord {
            lsn: lsn,
            record: rec,
            blocks: blocks
        }
    }

    // Decode the headers

    let mut max_block_id = 0;
    let mut datatotal: u32 = 0;
    let mut blocks: Vec<DecodedBkpBlock> = Vec::new();
    while buf.remaining() > datatotal as usize {
        let block_id = buf.get_u8();

        match block_id {
            XLR_BLOCK_ID_DATA_SHORT => {
                /* XLogRecordDataHeaderShort */
                let main_data_len = buf.get_u8() as u32;

                datatotal += main_data_len;
            }

            XLR_BLOCK_ID_DATA_LONG => {
                /* XLogRecordDataHeaderShort */
                let main_data_len = buf.get_u32();

                datatotal += main_data_len;
            }

            XLR_BLOCK_ID_ORIGIN => {
                // RepOriginId is uint16
                buf.advance(2);
            }

            XLR_BLOCK_ID_TOPLEVEL_XID => {
                // TransactionId is uint32
                buf.advance(4);
            }

            0..=XLR_MAX_BLOCK_ID => {
                /* XLogRecordBlockHeader */
                let mut blk = DecodedBkpBlock {
                    rnode_spcnode: 0,
                    rnode_dbnode: 0,
                    rnode_relnode: 0,
                    forknum: 0,
                    blkno: 0,

                    flags: 0,
                    has_image: false,
                    apply_image: false,
                    will_init: false,
                    hole_offset: 0,
                    hole_length: 0,
                    bimg_len: 0,
                    bimg_info: 0,

                    has_data: false,
                    data_len: 0,
                };
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
                blk.forknum = fork_flags & BKPBLOCK_FORK_MASK;
                blk.flags = fork_flags;
                blk.has_image = (fork_flags & BKPBLOCK_HAS_IMAGE) != 0;
                blk.has_data = (fork_flags & BKPBLOCK_HAS_DATA) != 0;
                blk.will_init = (fork_flags & BKPBLOCK_WILL_INIT) != 0;

                blk.data_len = buf.get_u16_le();
                /* cross-check that the HAS_DATA flag is set iff data_length > 0 */
                // TODO
                /*
                if (blk->has_data && blk->data_len == 0)
                {
                    report_invalid_record(state,
                              "BKPBLOCK_HAS_DATA set, but no data included at %X/%X",
                              (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                    goto err;
                }
                if (!blk->has_data && blk->data_len != 0)
                {
                    report_invalid_record(state,
                              "BKPBLOCK_HAS_DATA not set, but data length is %u at %X/%X",
                              (unsigned int) blk->data_len,
                              (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                    goto err;
                }
                         */
                datatotal += blk.data_len as u32;

                if blk.has_image {
                    blk.bimg_len = buf.get_u16_le();
                    blk.hole_offset = buf.get_u16_le();
                    blk.bimg_info = buf.get_u8();

                    blk.apply_image = (blk.bimg_info & BKPIMAGE_APPLY) != 0;

                    if blk.bimg_info & BKPIMAGE_IS_COMPRESSED != 0 {
                        if blk.bimg_info & BKPIMAGE_HAS_HOLE != 0 {
                            blk.hole_length = buf.get_u16_le();
                        } else {
                            blk.hole_length = 0;
                        }
                    } else {
                        blk.hole_length = BLCKSZ - blk.bimg_len;
                    }
                    datatotal += blk.bimg_len as u32;

                    /*
                     * cross-check that hole_offset > 0, hole_length > 0 and
                     * bimg_len < BLCKSZ if the HAS_HOLE flag is set.
                     */
                    if blk.bimg_info & BKPIMAGE_HAS_HOLE != 0
                        && (blk.hole_offset == 0 || blk.hole_length == 0 || blk.bimg_len == BLCKSZ)
                    {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "BKPIMAGE_HAS_HOLE set, but hole offset %u length %u block image length %u at %X/%X",
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
                    if blk.bimg_info & BKPIMAGE_HAS_HOLE == 0
                        && (blk.hole_offset != 0 || blk.hole_length != 0)
                    {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "BKPIMAGE_HAS_HOLE not set, but hole offset %u length %u at %X/%X",
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
                    if (blk.bimg_info & BKPIMAGE_IS_COMPRESSED == 0) && blk.bimg_len == BLCKSZ {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "BKPIMAGE_IS_COMPRESSED set, but block image length %u at %X/%X",
                                      (unsigned int) blk->bimg_len,
                                      (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                        goto err;
                                     */
                    }

                    /*
                     * cross-check that bimg_len = BLCKSZ if neither HAS_HOLE nor
                     * IS_COMPRESSED flag is set.
                     */
                    if blk.bimg_info & BKPIMAGE_HAS_HOLE == 0
                        && blk.bimg_info & BKPIMAGE_IS_COMPRESSED == 0
                        && blk.bimg_len != BLCKSZ
                    {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "neither BKPIMAGE_HAS_HOLE nor BKPIMAGE_IS_COMPRESSED set, but block image length is %u at %X/%X",
                                      (unsigned int) blk->data_len,
                                      (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                        goto err;
                                     */
                    }
                }
                if fork_flags & BKPBLOCK_SAME_REL == 0 {
                    rnode_spcnode = buf.get_u32_le();
                    rnode_dbnode = buf.get_u32_le();
                    rnode_relnode = buf.get_u32_le();
                    //rnode = &blk->rnode;
                    got_rnode = true;
                } else {
                    if !got_rnode {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "BKPBLOCK_SAME_REL set but no previous rel at %X/%X",
                                      (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                        goto err;
                                     */
                    }

                    //blk->rnode = *rnode;
                }

                blk.rnode_spcnode = rnode_spcnode;
                blk.rnode_dbnode = rnode_dbnode;
                blk.rnode_relnode = rnode_relnode;

                blk.blkno = buf.get_u32_le();

                //println!("this record affects {}/{}/{} blk {}",rnode_spcnode, rnode_dbnode, rnode_relnode, blk.blkno);

                blocks.push(blk);
            }

            _ => {
                // TODO: invalid block_id
            }
        }
    }

    /*
     * Ok, we've parsed the fragment headers, and verified that the total
     * length of the payload in the fragments is equal to the amount of data
     * left. Copy the data of each fragment to a separate buffer.
     *
     * We could just set up pointers into readRecordBuf, but we want to align
     * the data for the convenience of the callers. Backup images are not
     * copied, however; they don't need alignment.
     */

    // Since we don't care about the data payloads here, we're done.

    return DecodedWALRecord {
        lsn: lsn,
        record: rec,
        blocks: blocks,
    };
}
