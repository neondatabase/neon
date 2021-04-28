//
// This file contains common utilities for dealing with PostgreSQL WAL files and
// LSNs.
//
// Many of these functions have been copied from PostgreSQL, and rewritten in
// Rust. That's why they don't follow the usual Rust naming conventions, they
// have been named the same as the corresponding PostgreSQL functions instead.
//

use crate::pg_constants;
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes};
use crc32c::*;
use log::*;
use std::cmp::min;
use std::fs::{self, File};
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

pub const XLOG_FNAME_LEN: usize = 24;
pub const XLOG_BLCKSZ: usize = 8192;
pub const XLP_FIRST_IS_CONTRECORD: u16 = 0x0001;
pub const XLOG_PAGE_MAGIC: u16 = 0xD109;
pub const XLP_REM_LEN_OFFS: usize = 2 + 2 + 4 + 8;
pub const XLOG_SIZE_OF_XLOG_SHORT_PHD: usize = XLP_REM_LEN_OFFS + 4 + 4;
pub const XLOG_SIZE_OF_XLOG_LONG_PHD: usize = XLOG_SIZE_OF_XLOG_SHORT_PHD + 8 + 4 + 4;
pub const XLOG_RECORD_CRC_OFFS: usize = 4 + 4 + 8 + 1 + 1 + 2;
pub const XLOG_SIZE_OF_XLOG_RECORD: usize = XLOG_RECORD_CRC_OFFS + 4;
pub type XLogRecPtr = u64;
pub type TimeLineID = u32;
pub type TimestampTz = u64;
pub type XLogSegNo = u64;

#[allow(non_snake_case)]
pub fn XLogSegmentOffset(xlogptr: XLogRecPtr, wal_segsz_bytes: usize) -> u32 {
    (xlogptr as u32) & (wal_segsz_bytes as u32 - 1)
}

#[allow(non_snake_case)]
pub fn XLogSegmentsPerXLogId(wal_segsz_bytes: usize) -> XLogSegNo {
    (0x100000000u64 / wal_segsz_bytes as u64) as XLogSegNo
}

#[allow(non_snake_case)]
pub fn XLByteToSeg(xlogptr: XLogRecPtr, wal_segsz_bytes: usize) -> XLogSegNo {
    xlogptr / wal_segsz_bytes as u64
}

#[allow(non_snake_case)]
pub fn XLogSegNoOffsetToRecPtr(
    segno: XLogSegNo,
    offset: u32,
    wal_segsz_bytes: usize,
) -> XLogRecPtr {
    segno * (wal_segsz_bytes as u64) + (offset as u64)
}

#[allow(non_snake_case)]
pub fn XLogFileName(tli: TimeLineID, logSegNo: XLogSegNo, wal_segsz_bytes: usize) -> String {
    return format!(
        "{:>08X}{:>08X}{:>08X}",
        tli,
        logSegNo / XLogSegmentsPerXLogId(wal_segsz_bytes),
        logSegNo % XLogSegmentsPerXLogId(wal_segsz_bytes)
    );
}

#[allow(non_snake_case)]
pub fn XLogFromFileName(fname: &str, wal_seg_size: usize) -> (XLogSegNo, TimeLineID) {
    let tli = u32::from_str_radix(&fname[0..8], 16).unwrap();
    let log = u32::from_str_radix(&fname[8..16], 16).unwrap() as XLogSegNo;
    let seg = u32::from_str_radix(&fname[16..24], 16).unwrap() as XLogSegNo;
    (log * XLogSegmentsPerXLogId(wal_seg_size) + seg, tli)
}

#[allow(non_snake_case)]
pub fn IsXLogFileName(fname: &str) -> bool {
    return fname.len() == XLOG_FNAME_LEN && fname.chars().all(|c| c.is_ascii_hexdigit());
}

#[allow(non_snake_case)]
pub fn IsPartialXLogFileName(fname: &str) -> bool {
    fname.ends_with(".partial") && IsXLogFileName(&fname[0..fname.len() - 8])
}

pub fn get_current_timestamp() -> TimestampTz {
    const UNIX_EPOCH_JDATE: u64 = 2440588; /* == date2j(1970, 1, 1) */
    const POSTGRES_EPOCH_JDATE: u64 = 2451545; /* == date2j(2000, 1, 1) */
    const SECS_PER_DAY: u64 = 86400;
    const USECS_PER_SEC: u64 = 1000000;
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => {
            (n.as_secs() - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY))
                * USECS_PER_SEC
                + n.subsec_micros() as u64
        }
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

fn find_end_of_wal_segment(
    data_dir: &Path,
    segno: XLogSegNo,
    tli: TimeLineID,
    wal_seg_size: usize,
) -> u32 {
    let mut offs: usize = 0;
    let mut contlen: usize = 0;
    let mut wal_crc: u32 = 0;
    let mut crc: u32 = 0;
    let mut rec_offs: usize = 0;
    let mut buf = [0u8; XLOG_BLCKSZ];
    let file_name = XLogFileName(tli, segno, wal_seg_size);
    let mut last_valid_rec_pos: usize = 0;
    let mut file = File::open(data_dir.join(file_name.clone() + ".partial")).unwrap();
    let mut rec_hdr = [0u8; XLOG_RECORD_CRC_OFFS];

    while offs < wal_seg_size {
        if offs % XLOG_BLCKSZ == 0 {
            if let Ok(bytes_read) = file.read(&mut buf) {
                if bytes_read != buf.len() {
                    break;
                }
            } else {
                break;
            }
            let xlp_magic = LittleEndian::read_u16(&buf[0..2]);
            let xlp_info = LittleEndian::read_u16(&buf[2..4]);
            let xlp_rem_len = LittleEndian::read_u32(&buf[XLP_REM_LEN_OFFS..XLP_REM_LEN_OFFS + 4]);
            if xlp_magic != XLOG_PAGE_MAGIC {
                info!("Invalid WAL file {}.partial magic {}", file_name, xlp_magic);
                break;
            }
            if offs == 0 {
                offs = XLOG_SIZE_OF_XLOG_LONG_PHD;
                if (xlp_info & XLP_FIRST_IS_CONTRECORD) != 0 {
                    offs += ((xlp_rem_len + 7) & !7) as usize;
                }
            } else {
                offs += XLOG_SIZE_OF_XLOG_SHORT_PHD;
            }
        } else if contlen == 0 {
            let page_offs = offs % XLOG_BLCKSZ;
            let xl_tot_len = LittleEndian::read_u32(&buf[page_offs..page_offs + 4]) as usize;
            if xl_tot_len == 0 {
                break;
            }
            last_valid_rec_pos = offs;
            offs += 4;
            rec_offs = 4;
            contlen = xl_tot_len - 4;
            rec_hdr[0..4].copy_from_slice(&buf[page_offs..page_offs + 4]);
        } else {
            let page_offs = offs % XLOG_BLCKSZ;
            // we're continuing a record, possibly from previous page.
            let pageleft = XLOG_BLCKSZ - page_offs;

            // read the rest of the record, or as much as fits on this page.
            let n = min(contlen, pageleft);
            if rec_offs < XLOG_RECORD_CRC_OFFS {
                let len = min(XLOG_RECORD_CRC_OFFS - rec_offs, n);
                rec_hdr[rec_offs..rec_offs + len].copy_from_slice(&buf[page_offs..page_offs + len]);
            }
            if rec_offs <= XLOG_RECORD_CRC_OFFS && rec_offs + n >= XLOG_SIZE_OF_XLOG_RECORD {
                let crc_offs = page_offs - rec_offs + XLOG_RECORD_CRC_OFFS;
                wal_crc = LittleEndian::read_u32(&buf[crc_offs..crc_offs + 4]);
                crc = crc32c_append(0, &buf[crc_offs + 4..page_offs + n]);
                crc = !crc;
            } else {
                crc ^= 0xFFFFFFFFu32;
                crc = crc32c_append(crc, &buf[page_offs..page_offs + n]);
                crc = !crc;
            }
            rec_offs += n;
            offs += n;
            contlen -= n;

            if contlen == 0 {
                crc = !crc;
                crc = crc32c_append(crc, &rec_hdr);
                offs = (offs + 7) & !7; // pad on 8 bytes boundary */
                if crc == wal_crc {
                    last_valid_rec_pos = offs;
                } else {
                    info!(
                        "CRC mismatch {} vs {} at {}",
                        crc, wal_crc, last_valid_rec_pos
                    );
                    break;
                }
            }
        }
    }
    last_valid_rec_pos as u32
}

///
/// Scan a directory that contains PostgreSQL WAL files, for the end of WAL.
///
pub fn find_end_of_wal(
    data_dir: &Path,
    wal_seg_size: usize,
    precise: bool,
) -> (XLogRecPtr, TimeLineID) {
    let mut high_segno: XLogSegNo = 0;
    let mut high_tli: TimeLineID = 0;
    let mut high_ispartial = false;

    for entry in fs::read_dir(data_dir).unwrap() {
        if let Ok(entry) = entry {
            let ispartial: bool;
            let entry_name = entry.file_name();
            let fname = entry_name.to_str().unwrap();
            /*
             * Check if the filename looks like an xlog file, or a .partial file.
             */
            if IsXLogFileName(fname) {
                ispartial = false;
            } else if IsPartialXLogFileName(fname) {
                ispartial = true;
            } else {
                continue;
            }
            let (segno, tli) = XLogFromFileName(fname, wal_seg_size);
            if !ispartial && entry.metadata().unwrap().len() != wal_seg_size as u64 {
                continue;
            }
            if segno > high_segno
                || (segno == high_segno && tli > high_tli)
                || (segno == high_segno && tli == high_tli && high_ispartial && !ispartial)
            {
                high_segno = segno;
                high_tli = tli;
                high_ispartial = ispartial;
            }
        }
    }
    if high_segno > 0 {
        let mut high_offs = 0;
        /*
         * Move the starting pointer to the start of the next segment, if the
         * highest one we saw was completed.
         */
        if !high_ispartial {
            high_segno += 1;
        } else if precise {
            /* otherwise locate last record in last partial segment */
            high_offs = find_end_of_wal_segment(data_dir, high_segno, high_tli, wal_seg_size);
        }
        let high_ptr = XLogSegNoOffsetToRecPtr(high_segno, high_offs, wal_seg_size);
        return (high_ptr, high_tli);
    }
    (0, 0)
}

pub fn main() {
    let mut data_dir = PathBuf::new();
    data_dir.push(".");
    let wal_seg_size = 16 * 1024 * 1024;
    let (wal_end, tli) = find_end_of_wal(&data_dir, wal_seg_size, true);
    println!(
        "wal_end={:>08X}{:>08X}, tli={}",
        (wal_end >> 32) as u32,
        wal_end as u32,
        tli
    );
}

//
// Xlog record parsing routines
// TODO move here other related code from waldecoder.rs
//
#[repr(C)]
#[derive(Debug)]
pub struct XLogRecord {
    pub xl_tot_len: u32,
    pub xl_xid: u32,
    pub xl_prev: u64,
    pub xl_info: u8,
    pub xl_rmid: u8,
    pub xl_crc: u32,
}

impl XLogRecord {
    pub fn from_bytes(buf: &mut Bytes) -> XLogRecord {
        XLogRecord {
            xl_tot_len: buf.get_u32_le(),
            xl_xid: buf.get_u32_le(),
            xl_prev: buf.get_u64_le(),
            xl_info: buf.get_u8(),
            xl_rmid: buf.get_u8(),
            xl_crc: {
                buf.advance(2);
                buf.get_u32_le()
            },
        }
    }

    // Is this record an XLOG_SWITCH record? They need some special processing,
    pub fn is_xlog_switch_record(&self) -> bool {

    self.xl_info == pg_constants::XLOG_SWITCH && self.xl_rmid == pg_constants::RM_XLOG_ID
    }
}
