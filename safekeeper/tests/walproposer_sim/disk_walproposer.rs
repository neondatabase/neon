use std::{ffi::{CStr, CString}, sync::Arc};

use byteorder::{WriteBytesExt, BigEndian, LittleEndian};
use crc32c::crc32c_append;
use parking_lot::{Mutex, MutexGuard};
use utils::lsn::Lsn;
use postgres_ffi::{v16::{xlog_utils::{XlLogicalMessage, XLOG_SIZE_OF_XLOG_RECORD, XLOG_RECORD_CRC_OFFS, XLOG_SIZE_OF_XLOG_SHORT_PHD, XLogSegNoOffsetToRecPtr, XLOG_SIZE_OF_XLOG_LONG_PHD, XLP_FIRST_IS_CONTRECORD}, XLogRecord, wal_craft_test_export::{XLogPageHeaderData, XLOG_PAGE_MAGIC, XLogLongPageHeaderData}}, pg_constants::{RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE, XLR_BLOCK_ID_DATA_LONG, XLR_BLOCK_ID_DATA_SHORT, XLP_LONG_HEADER}, XLOG_BLCKSZ, WAL_SEGMENT_SIZE};

use super::disk::BlockStorage;

pub struct DiskWalProposer {
    state: Mutex<State>,
}

impl DiskWalProposer {
    pub fn new() -> Arc<DiskWalProposer> {
        Arc::new(DiskWalProposer {
            state: Mutex::new(State {
                internal_available_lsn: Lsn(0),
                prev_lsn: Lsn(0),
                disk: BlockStorage::new(),
            }),
        })
    }

    pub fn lock(&self) -> MutexGuard<State> {
        self.state.lock()
    }
}

pub struct State {
    internal_available_lsn: Lsn,
    // needed for WAL generation
    prev_lsn: Lsn,
    disk: BlockStorage,
}

impl State {
    pub fn read(&self, pos: u64, buf: &mut [u8]) {
        self.disk.read(pos, buf);
        // TODO: fail on reading uninitialized data
    }

    fn write(&mut self, pos: u64, buf: &[u8]) {
        self.disk.write(pos, buf);
    }

    /// Update the internal available LSN to the given value.
    pub fn reset_to(&mut self, lsn: Lsn) {
        self.internal_available_lsn = lsn;
    }

    /// Get current LSN.
    pub fn flush_rec_ptr(&self) -> Lsn {
        self.internal_available_lsn
    }

    pub fn insert_logical_message(&mut self, prefix: &str, msg: &[u8]) -> anyhow::Result<()> {
        let prefix_cstr = CString::new(prefix)?;
        let prefix_bytes = prefix_cstr.as_bytes_with_nul();

        let lm = XlLogicalMessage {
            db_id: 0,
            transactional: 0,
            prefix_size: prefix_bytes.len() as ::std::os::raw::c_ulong,
            message_size: msg.len() as ::std::os::raw::c_ulong,
        };

        let record_bytes = lm.encode();

        let mut rdatas: Vec<&[u8]> = Vec::new();
        rdatas.push(&record_bytes);
        rdatas.push(prefix_bytes);
        rdatas.push(msg);

        const XLOG_INCLUDE_ORIGIN: u8 = 0x01;
        insert_wal_record(self, rdatas, RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE);

        Ok(())
    }
}


fn insert_wal_record(state: &mut State, rdatas: Vec<&[u8]>, rmid: u8, info: u8) -> anyhow::Result<()> {
    // bytes right after the header, in the same rdata block
    let mut scratch = Vec::new();
    let mainrdata_len: usize = rdatas.iter().map(|rdata| rdata.len()).sum();

    if mainrdata_len > 0 {
        if mainrdata_len > 255 {
            scratch.push(XLR_BLOCK_ID_DATA_LONG);
            // TODO: verify endiness
            let _ = scratch.write_u32::<LittleEndian>(mainrdata_len as u32);
        } else {
            scratch.push(XLR_BLOCK_ID_DATA_SHORT);
            scratch.push(mainrdata_len as u8);
        }
    }

    let total_len: u32 = (XLOG_SIZE_OF_XLOG_RECORD + scratch.len() + mainrdata_len) as u32;
    let size = maxalign(total_len);
    assert!(size as usize > XLOG_SIZE_OF_XLOG_RECORD);

    let start_bytepos = recptr_to_bytepos(state.internal_available_lsn);
    let end_bytepos = start_bytepos + size as u64;

    let start_recptr = bytepos_to_recptr(start_bytepos);
    let end_recptr = bytepos_to_recptr(end_bytepos);

    assert!(recptr_to_bytepos(start_recptr) == start_bytepos);
    assert!(recptr_to_bytepos(end_recptr) == end_bytepos);

    let mut crc = crc32c_append(0, &scratch);
    for rdata in &rdatas {
        crc = crc32c_append(crc, rdata);
    }

    let mut header = XLogRecord {
        xl_tot_len: total_len,
        xl_xid: 0,
        xl_prev: state.prev_lsn.0,
        xl_info: info,
        xl_rmid: rmid,
        __bindgen_padding_0: [0u8; 2usize],
        xl_crc: crc,
    };

    // now we have the header and can finish the crc
    let header_bytes = header.encode()?;
    let crc = crc32c_append(crc, &header_bytes[0..XLOG_RECORD_CRC_OFFS]);
    header.xl_crc = crc;

    let mut header_bytes = header.encode()?.to_vec();
    assert!(header_bytes.len() == XLOG_SIZE_OF_XLOG_RECORD);

    header_bytes.extend_from_slice(&scratch);

    // finish rdatas
    let mut rdatas = rdatas;
    rdatas.insert(0, &header_bytes);

    write_walrecord_to_disk(state, total_len as u64, rdatas, start_recptr, end_recptr)?;

    state.internal_available_lsn = end_recptr;
    state.prev_lsn = start_recptr;
    Ok(())
}

fn write_walrecord_to_disk(state: &mut State, total_len: u64, rdatas: Vec<&[u8]>, start: Lsn, end: Lsn) -> anyhow::Result<()> {
    let mut curr_ptr = start;
    let mut freespace = insert_freespace(curr_ptr);
    let mut written: usize = 0;

    assert!(freespace >= std::mem::size_of::<u32>());

    for mut rdata in rdatas {
        while rdata.len() >= freespace {
            assert!(curr_ptr.segment_offset(WAL_SEGMENT_SIZE) >= XLOG_SIZE_OF_XLOG_SHORT_PHD || freespace == 0);
            
            state.write(curr_ptr.0, &rdata[..freespace]);
            rdata = &rdata[freespace..];
            written += freespace;
            curr_ptr = Lsn(curr_ptr.0 + freespace as u64);

            let mut new_page = XLogPageHeaderData {
                xlp_magic: XLOG_PAGE_MAGIC as u16,
                xlp_info: XLP_BKP_REMOVABLE,
                xlp_tli: 1,
                xlp_pageaddr: curr_ptr.0,
                xlp_rem_len: (total_len - written as u64) as u32,
                ..Default::default() // Put 0 in padding fields.
            };
            if new_page.xlp_rem_len > 0 {
                new_page.xlp_info |= XLP_FIRST_IS_CONTRECORD;
            }

            if curr_ptr.segment_offset(WAL_SEGMENT_SIZE) == 0 {
                new_page.xlp_info |= XLP_LONG_HEADER;
                let long_page = XLogLongPageHeaderData {
                    std: new_page,
                    xlp_sysid: 0,
                    xlp_seg_size: WAL_SEGMENT_SIZE as u32,
                    xlp_xlog_blcksz: XLOG_BLCKSZ as u32,
                };
                let header_bytes = long_page.encode()?;
                assert!(header_bytes.len() == XLOG_SIZE_OF_XLOG_LONG_PHD);
                state.write(curr_ptr.0, &header_bytes);
                curr_ptr = Lsn(curr_ptr.0 + header_bytes.len() as u64);
            } else {
                let header_bytes = new_page.encode()?;
                assert!(header_bytes.len() == XLOG_SIZE_OF_XLOG_SHORT_PHD);
                state.write(curr_ptr.0, &header_bytes);
                curr_ptr = Lsn(curr_ptr.0 + header_bytes.len() as u64);
            }
            freespace = insert_freespace(curr_ptr);
        }

        assert!(curr_ptr.segment_offset(WAL_SEGMENT_SIZE) >= XLOG_SIZE_OF_XLOG_SHORT_PHD || rdata.len() == 0);
        state.write(curr_ptr.0, rdata);
        curr_ptr = Lsn(curr_ptr.0 + rdata.len() as u64);
        written += rdata.len();
        freespace -= rdata.len();
    }

    // Assert(written == write_len);
	// CurrPos = MAXALIGN64(CurrPos);
	// Assert(CurrPos == EndPos);
    assert!(written == total_len as usize);
    curr_ptr.0 = maxalign(curr_ptr.0);
    assert!(curr_ptr == end);
    Ok(())
}

fn maxalign<T>(size: T) -> T
where
    T: std::ops::BitAnd<Output = T> + std::ops::Add<Output = T> + std::ops::Not<Output = T> + From<u8>,
{
    (size + T::from(7)) & !T::from(7)
}

fn insert_freespace(ptr: Lsn) -> usize {
    if ptr.block_offset() == 0 {
        0
    } else {
        (XLOG_BLCKSZ as u64 - ptr.block_offset()) as usize
    }
}

const XLP_BKP_REMOVABLE: u16 = 0x0004;
const USABLE_BYTES_IN_PAGE: u64 = (XLOG_BLCKSZ - XLOG_SIZE_OF_XLOG_SHORT_PHD) as u64;
const USABLE_BYTES_IN_SEGMENT: u64 = ((WAL_SEGMENT_SIZE / XLOG_BLCKSZ) as u64 * USABLE_BYTES_IN_PAGE) - (XLOG_SIZE_OF_XLOG_RECORD - XLOG_SIZE_OF_XLOG_SHORT_PHD) as u64;

fn bytepos_to_recptr(bytepos: u64) -> Lsn {
    let fullsegs = bytepos / USABLE_BYTES_IN_SEGMENT;
    let mut bytesleft = bytepos % USABLE_BYTES_IN_SEGMENT;

    let seg_offset = if bytesleft < (XLOG_BLCKSZ - XLOG_SIZE_OF_XLOG_SHORT_PHD) as u64 {
        // fits on first page of segment
        bytesleft + XLOG_SIZE_OF_XLOG_SHORT_PHD as u64
    } else {
        // account for the first page on segment with long header
        bytesleft -= (XLOG_BLCKSZ - XLOG_SIZE_OF_XLOG_SHORT_PHD) as u64;
        let fullpages = bytesleft / USABLE_BYTES_IN_PAGE;
        bytesleft = bytesleft % USABLE_BYTES_IN_PAGE;

        XLOG_BLCKSZ as u64 + fullpages * XLOG_BLCKSZ as u64 + bytesleft + XLOG_SIZE_OF_XLOG_SHORT_PHD as u64
    };

    Lsn(XLogSegNoOffsetToRecPtr(fullsegs, seg_offset as u32, WAL_SEGMENT_SIZE))
}

fn recptr_to_bytepos(ptr: Lsn) -> u64 {
    let fullsegs = ptr.segment_number(WAL_SEGMENT_SIZE);
    let offset = ptr.segment_offset(WAL_SEGMENT_SIZE) as u64;

    let fullpages = offset / XLOG_BLCKSZ as u64;
    let offset = offset % XLOG_BLCKSZ as u64;

    if fullpages == 0 {
        fullsegs * USABLE_BYTES_IN_SEGMENT + if offset > 0 {
            assert!(offset >= XLOG_SIZE_OF_XLOG_SHORT_PHD as u64);
            offset - XLOG_SIZE_OF_XLOG_SHORT_PHD as u64
        } else {
            0
        }
    } else {
        fullsegs * USABLE_BYTES_IN_SEGMENT + (XLOG_BLCKSZ - XLOG_SIZE_OF_XLOG_SHORT_PHD) as u64 + (fullpages - 1) * USABLE_BYTES_IN_PAGE + if offset > 0 {
            assert!(offset >= XLOG_SIZE_OF_XLOG_SHORT_PHD as u64);
            offset - XLOG_SIZE_OF_XLOG_SHORT_PHD as u64
        } else {
            0
        }
    }
}
