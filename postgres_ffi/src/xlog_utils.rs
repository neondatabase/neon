//
// This file contains common utilities for dealing with PostgreSQL WAL files and
// LSNs.
//
// Many of these functions have been copied from PostgreSQL, and rewritten in
// Rust. That's why they don't follow the usual Rust naming conventions, they
// have been named the same as the corresponding PostgreSQL functions instead.
//

use crate::pg_constants;
use crate::CheckPoint;
use crate::ControlFileData;
use crate::FullTransactionId;
use crate::XLogLongPageHeaderData;
use crate::XLogPageHeaderData;
use crate::XLogRecord;
use crate::XLOG_PAGE_MAGIC;

use anyhow::{bail, Result};
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes};
use bytes::{BufMut, BytesMut};
use crc32c::*;
use log::*;
use std::cmp::max;
use std::cmp::min;
use std::fs::{self, File};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use zenith_utils::lsn::Lsn;

pub const XLOG_FNAME_LEN: usize = 24;
pub const XLOG_BLCKSZ: usize = 8192;
pub const XLP_FIRST_IS_CONTRECORD: u16 = 0x0001;
pub const XLP_REM_LEN_OFFS: usize = 2 + 2 + 4 + 8;
pub const XLOG_RECORD_CRC_OFFS: usize = 4 + 4 + 8 + 1 + 1 + 2;
pub const MAX_SEND_SIZE: usize = XLOG_BLCKSZ * 16;

pub const XLOG_SIZE_OF_XLOG_SHORT_PHD: usize = std::mem::size_of::<XLogPageHeaderData>();
pub const XLOG_SIZE_OF_XLOG_LONG_PHD: usize = std::mem::size_of::<XLogLongPageHeaderData>();
pub const XLOG_SIZE_OF_XLOG_RECORD: usize = std::mem::size_of::<XLogRecord>();
#[allow(clippy::identity_op)]
pub const SIZE_OF_XLOG_RECORD_DATA_HEADER_SHORT: usize = 1 * 2;

pub type XLogRecPtr = u64;
pub type TimeLineID = u32;
pub type TimestampTz = i64;
pub type XLogSegNo = u64;

const XID_CHECKPOINT_INTERVAL: u32 = 1024;

#[allow(non_snake_case)]
pub fn XLogSegmentsPerXLogId(wal_segsz_bytes: usize) -> XLogSegNo {
    (0x100000000u64 / wal_segsz_bytes as u64) as XLogSegNo
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

/// If LSN points to the beginning of the page, then shift it to first record,
/// otherwise align on 8-bytes boundary (required for WAL records)
pub fn normalize_lsn(lsn: Lsn, seg_sz: usize) -> Lsn {
    if lsn.0 % XLOG_BLCKSZ as u64 == 0 {
        let hdr_size = if lsn.0 % seg_sz as u64 == 0 {
            XLOG_SIZE_OF_XLOG_LONG_PHD
        } else {
            XLOG_SIZE_OF_XLOG_SHORT_PHD
        };
        lsn + hdr_size as u64
    } else {
        lsn.align()
    }
}

pub fn get_current_timestamp() -> TimestampTz {
    const UNIX_EPOCH_JDATE: u64 = 2440588; /* == date2j(1970, 1, 1) */
    const POSTGRES_EPOCH_JDATE: u64 = 2451545; /* == date2j(2000, 1, 1) */
    const SECS_PER_DAY: u64 = 86400;
    const USECS_PER_SEC: u64 = 1000000;
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => {
            ((n.as_secs() - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY))
                * USECS_PER_SEC
                + n.subsec_micros() as u64) as i64
        }
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

fn find_end_of_wal_segment(
    data_dir: &Path,
    segno: XLogSegNo,
    tli: TimeLineID,
    wal_seg_size: usize,
    start_offset: usize, // start reading at this point
) -> Result<u32> {
    // step back to the beginning of the page to read it in...
    let mut offs: usize = start_offset - start_offset % XLOG_BLCKSZ;
    let mut contlen: usize = 0;
    let mut wal_crc: u32 = 0;
    let mut crc: u32 = 0;
    let mut rec_offs: usize = 0;
    let mut buf = [0u8; XLOG_BLCKSZ];
    let file_name = XLogFileName(tli, segno, wal_seg_size);
    let mut last_valid_rec_pos: usize = 0;
    let mut file = File::open(data_dir.join(file_name.clone() + ".partial")).unwrap();
    file.seek(SeekFrom::Start(offs as u64))?;
    let mut rec_hdr = [0u8; XLOG_RECORD_CRC_OFFS];

    while offs < wal_seg_size {
        // we are at the beginning of the page; read it in
        if offs % XLOG_BLCKSZ == 0 {
            let bytes_read = file.read(&mut buf)?;
            if bytes_read != buf.len() {
                bail!(
                    "failed to read {} bytes from {} at {}",
                    XLOG_BLCKSZ,
                    file_name,
                    offs
                );
            }

            let xlp_magic = LittleEndian::read_u16(&buf[0..2]);
            let xlp_info = LittleEndian::read_u16(&buf[2..4]);
            let xlp_rem_len = LittleEndian::read_u32(&buf[XLP_REM_LEN_OFFS..XLP_REM_LEN_OFFS + 4]);
            // this is expected in current usage when valid WAL starts after page header
            if xlp_magic != XLOG_PAGE_MAGIC as u16 {
                trace!(
                    "invalid WAL file {}.partial magic {} at {:?}",
                    file_name,
                    xlp_magic,
                    Lsn(XLogSegNoOffsetToRecPtr(segno, offs as u32, wal_seg_size)),
                );
            }
            if offs == 0 {
                offs = XLOG_SIZE_OF_XLOG_LONG_PHD;
                if (xlp_info & XLP_FIRST_IS_CONTRECORD) != 0 {
                    offs += ((xlp_rem_len + 7) & !7) as usize;
                }
            } else {
                offs += XLOG_SIZE_OF_XLOG_SHORT_PHD;
            }
            // ... and step forward again if asked
            offs = max(offs, start_offset);

        // beginning of the next record
        } else if contlen == 0 {
            let page_offs = offs % XLOG_BLCKSZ;
            let xl_tot_len = LittleEndian::read_u32(&buf[page_offs..page_offs + 4]) as usize;
            if xl_tot_len == 0 {
                info!(
                    "find_end_of_wal_segment reached zeros at {:?}",
                    Lsn(XLogSegNoOffsetToRecPtr(segno, offs as u32, wal_seg_size))
                );
                break; // zeros, reached the end
            }
            last_valid_rec_pos = offs;
            offs += 4;
            rec_offs = 4;
            contlen = xl_tot_len - 4;
            rec_hdr[0..4].copy_from_slice(&buf[page_offs..page_offs + 4]);
        } else {
            // we're continuing a record, possibly from previous page.
            let page_offs = offs % XLOG_BLCKSZ;
            let pageleft = XLOG_BLCKSZ - page_offs;

            // read the rest of the record, or as much as fits on this page.
            let n = min(contlen, pageleft);
            // fill rec_hdr (header up to (but not including) xl_crc field)
            if rec_offs < XLOG_RECORD_CRC_OFFS {
                let len = min(XLOG_RECORD_CRC_OFFS - rec_offs, n);
                rec_hdr[rec_offs..rec_offs + len].copy_from_slice(&buf[page_offs..page_offs + len]);
            }
            if rec_offs <= XLOG_RECORD_CRC_OFFS && rec_offs + n >= XLOG_SIZE_OF_XLOG_RECORD {
                let crc_offs = page_offs - rec_offs + XLOG_RECORD_CRC_OFFS;
                wal_crc = LittleEndian::read_u32(&buf[crc_offs..crc_offs + 4]);
                crc = crc32c_append(0, &buf[crc_offs + 4..page_offs + n]);
            } else {
                crc ^= 0xFFFFFFFFu32;
                crc = crc32c_append(crc, &buf[page_offs..page_offs + n]);
            }
            crc = !crc;
            rec_offs += n;
            offs += n;
            contlen -= n;

            if contlen == 0 {
                crc = !crc;
                crc = crc32c_append(crc, &rec_hdr);
                offs = (offs + 7) & !7; // pad on 8 bytes boundary */
                if crc == wal_crc {
                    // record is valid, advance the result to its end (with
                    // alignment to the next record taken into account)
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
    Ok(last_valid_rec_pos as u32)
}

///
/// Scan a directory that contains PostgreSQL WAL files, for the end of WAL.
/// If precise, returns end LSN (next insertion point, basically);
/// otherwise, start of the last segment.
/// Returns (0, 0) if there is no WAL.
///
pub fn find_end_of_wal(
    data_dir: &Path,
    wal_seg_size: usize,
    precise: bool,
    start_lsn: Lsn, // start reading WAL at this point or later
) -> Result<(XLogRecPtr, TimeLineID)> {
    let mut high_segno: XLogSegNo = 0;
    let mut high_tli: TimeLineID = 0;
    let mut high_ispartial = false;

    for entry in fs::read_dir(data_dir).unwrap().flatten() {
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
            if start_lsn.segment_number(wal_seg_size) > high_segno {
                bail!(
                    "provided start_lsn {:?} is beyond highest segno {:?} available",
                    start_lsn,
                    high_segno,
                );
            }
            high_offs = find_end_of_wal_segment(
                data_dir,
                high_segno,
                high_tli,
                wal_seg_size,
                start_lsn.segment_offset(wal_seg_size),
            )?;
        }
        let high_ptr = XLogSegNoOffsetToRecPtr(high_segno, high_offs, wal_seg_size);
        return Ok((high_ptr, high_tli));
    }
    Ok((0, 0))
}

pub fn main() {
    let mut data_dir = PathBuf::new();
    data_dir.push(".");
    let wal_seg_size = 16 * 1024 * 1024;
    let (wal_end, tli) = find_end_of_wal(&data_dir, wal_seg_size, true, Lsn(0)).unwrap();
    println!(
        "wal_end={:>08X}{:>08X}, tli={}",
        (wal_end >> 32) as u32,
        wal_end as u32,
        tli
    );
}

impl XLogRecord {
    pub fn from_bytes(buf: &mut Bytes) -> XLogRecord {
        use zenith_utils::bin_ser::LeSer;
        XLogRecord::des_from(&mut buf.reader()).unwrap()
    }

    pub fn encode(&self) -> Bytes {
        use zenith_utils::bin_ser::LeSer;
        self.ser().unwrap().into()
    }

    // Is this record an XLOG_SWITCH record? They need some special processing,
    pub fn is_xlog_switch_record(&self) -> bool {
        self.xl_info == pg_constants::XLOG_SWITCH && self.xl_rmid == pg_constants::RM_XLOG_ID
    }
}

impl XLogPageHeaderData {
    pub fn from_bytes<B: Buf>(buf: &mut B) -> XLogPageHeaderData {
        use zenith_utils::bin_ser::LeSer;
        XLogPageHeaderData::des_from(&mut buf.reader()).unwrap()
    }
}

impl XLogLongPageHeaderData {
    pub fn from_bytes<B: Buf>(buf: &mut B) -> XLogLongPageHeaderData {
        use zenith_utils::bin_ser::LeSer;
        XLogLongPageHeaderData::des_from(&mut buf.reader()).unwrap()
    }

    pub fn encode(&self) -> Bytes {
        use zenith_utils::bin_ser::LeSer;
        self.ser().unwrap().into()
    }
}

pub const SIZEOF_CHECKPOINT: usize = std::mem::size_of::<CheckPoint>();

impl CheckPoint {
    pub fn encode(&self) -> Bytes {
        use zenith_utils::bin_ser::LeSer;
        self.ser().unwrap().into()
    }

    pub fn decode(buf: &[u8]) -> Result<CheckPoint, anyhow::Error> {
        use zenith_utils::bin_ser::LeSer;
        Ok(CheckPoint::des(buf)?)
    }

    // Update next XID based on provided new_xid and stored epoch.
    // Next XID should be greater than new_xid.
    // Also take in account 32-bit wrap-around.
    pub fn update_next_xid(&mut self, xid: u32) {
        let xid = xid.wrapping_add(XID_CHECKPOINT_INTERVAL - 1) & !(XID_CHECKPOINT_INTERVAL - 1);
        let full_xid = self.nextXid.value;
        let new_xid = std::cmp::max(xid + 1, pg_constants::FIRST_NORMAL_TRANSACTION_ID);
        let old_xid = full_xid as u32;
        if new_xid.wrapping_sub(old_xid) as i32 > 0 {
            let mut epoch = full_xid >> 32;
            if new_xid < old_xid {
                // wrap-around
                epoch += 1;
            }
            self.nextXid = FullTransactionId {
                value: (epoch << 32) | new_xid as u64,
            };
        }
    }
}

//
// Generate new WAL segment with single XLOG_CHECKPOINT_SHUTDOWN record.
// We need this segment to start compute node.
// In order to minimize changes in Postgres core, we prefer to
// provide WAL segment from which is can extract checkpoint record in standard way,
// rather then implement some alternative mechanism.
//
pub fn generate_wal_segment(pg_control: &ControlFileData) -> Bytes {
    let mut seg_buf = BytesMut::with_capacity(pg_constants::WAL_SEGMENT_SIZE as usize);

    let hdr = XLogLongPageHeaderData {
        std: {
            XLogPageHeaderData {
                xlp_magic: XLOG_PAGE_MAGIC as u16,
                xlp_info: pg_constants::XLP_LONG_HEADER,
                xlp_tli: 1, // FIXME: always use Postgres timeline 1
                xlp_pageaddr: pg_control.checkPoint - XLOG_SIZE_OF_XLOG_LONG_PHD as u64,
                xlp_rem_len: 0,
                ..Default::default() // Put 0 in padding fields.
            }
        },
        xlp_sysid: pg_control.system_identifier,
        xlp_seg_size: pg_constants::WAL_SEGMENT_SIZE as u32,
        xlp_xlog_blcksz: XLOG_BLCKSZ as u32,
    };

    let hdr_bytes = hdr.encode();
    seg_buf.extend_from_slice(&hdr_bytes);

    let rec_hdr = XLogRecord {
        xl_tot_len: (XLOG_SIZE_OF_XLOG_RECORD
            + SIZE_OF_XLOG_RECORD_DATA_HEADER_SHORT
            + SIZEOF_CHECKPOINT) as u32,
        xl_xid: 0, //0 is for InvalidTransactionId
        xl_prev: 0,
        xl_info: pg_constants::XLOG_CHECKPOINT_SHUTDOWN,
        xl_rmid: pg_constants::RM_XLOG_ID,
        xl_crc: 0,
        ..Default::default() // Put 0 in padding fields.
    };

    let mut rec_shord_hdr_bytes = BytesMut::new();
    rec_shord_hdr_bytes.put_u8(pg_constants::XLR_BLOCK_ID_DATA_SHORT);
    rec_shord_hdr_bytes.put_u8(SIZEOF_CHECKPOINT as u8);

    let rec_bytes = rec_hdr.encode();
    let checkpoint_bytes = pg_control.checkPointCopy.encode();

    //calculate record checksum
    let mut crc = 0;
    crc = crc32c_append(crc, &rec_shord_hdr_bytes[..]);
    crc = crc32c_append(crc, &checkpoint_bytes[..]);
    crc = crc32c_append(crc, &rec_bytes[0..XLOG_RECORD_CRC_OFFS]);

    seg_buf.extend_from_slice(&rec_bytes[0..XLOG_RECORD_CRC_OFFS]);
    seg_buf.put_u32_le(crc);
    seg_buf.extend_from_slice(&rec_shord_hdr_bytes);
    seg_buf.extend_from_slice(&checkpoint_bytes);

    //zero out the rest of the file
    seg_buf.resize(pg_constants::WAL_SEGMENT_SIZE, 0);
    seg_buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use regex::Regex;
    use std::{env, process::Command, str::FromStr};

    // Run find_end_of_wal against file in test_wal dir
    // Ensure that it finds last record correctly
    #[test]
    pub fn test_find_end_of_wal() {
        // 1. Run initdb to generate some WAL
        let top_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
        let data_dir = top_path.join("test_output/test_find_end_of_wal");
        let initdb_path = top_path.join("tmp_install/bin/initdb");
        let lib_path = top_path.join("tmp_install/lib");
        if data_dir.exists() {
            fs::remove_dir_all(&data_dir).unwrap();
        }
        println!("Using initdb from '{}'", initdb_path.display());
        println!("Data directory '{}'", data_dir.display());
        let initdb_output = Command::new(initdb_path)
            .args(&["-D", data_dir.to_str().unwrap()])
            .arg("--no-instructions")
            .arg("--no-sync")
            .env_clear()
            .env("LD_LIBRARY_PATH", &lib_path)
            .env("DYLD_LIBRARY_PATH", &lib_path)
            .output()
            .unwrap();
        assert!(initdb_output.status.success());

        // 2. Pick WAL generated by initdb
        let wal_dir = data_dir.join("pg_wal");
        let wal_seg_size = 16 * 1024 * 1024;

        // 3. Check end_of_wal on non-partial WAL segment (we treat it as fully populated)
        let (wal_end, tli) = find_end_of_wal(&wal_dir, wal_seg_size, true, Lsn(0)).unwrap();
        let wal_end = Lsn(wal_end);
        println!("wal_end={}, tli={}", wal_end, tli);
        assert_eq!(wal_end, "0/2000000".parse::<Lsn>().unwrap());

        // 4. Get the actual end of WAL by pg_waldump
        let waldump_path = top_path.join("tmp_install/bin/pg_waldump");
        let waldump_output = Command::new(waldump_path)
            .arg(wal_dir.join("000000010000000000000001"))
            .env_clear()
            .env("LD_LIBRARY_PATH", &lib_path)
            .env("DYLD_LIBRARY_PATH", &lib_path)
            .output()
            .unwrap();
        let waldump_output = std::str::from_utf8(&waldump_output.stderr).unwrap();
        println!("waldump_output = '{}'", &waldump_output);
        let re = Regex::new(r"invalid record length at (.+):").unwrap();
        let caps = re.captures(waldump_output).unwrap();
        let waldump_wal_end = Lsn::from_str(caps.get(1).unwrap().as_str()).unwrap();

        // 5. Rename file to partial to actually find last valid lsn
        fs::rename(
            wal_dir.join("000000010000000000000001"),
            wal_dir.join("000000010000000000000001.partial"),
        )
        .unwrap();
        let (wal_end, tli) = find_end_of_wal(&wal_dir, wal_seg_size, true, Lsn(0)).unwrap();
        let wal_end = Lsn(wal_end);
        println!("wal_end={}, tli={}", wal_end, tli);
        assert_eq!(wal_end, waldump_wal_end);
    }
}
