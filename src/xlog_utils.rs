use std::fs::{self,File};
use std::time::SystemTime;
use std::path::PathBuf;
use std::cmp::min;
use std::io::prelude::*;
use byteorder::{LittleEndian, ByteOrder};

pub const XLOG_FNAME_LEN : usize = 24;
pub const XLOG_BLCKSZ : usize =  8192;
pub const XLOG_SIZE_OF_XLOG_SHORT_PHD : usize = 2+2+4+8+4 + 4;
pub const XLOG_SIZE_OF_XLOG_LONG_PHD : usize = (2+2+4+8+4) + 4 + 8 + 4 + 4;

pub type XLogRecPtr = u64;
pub type TimeLineID = u32;
pub type TimestampTz = u64;
pub type XLogSegNo = u32;

#[allow(non_snake_case)]
pub fn XLogSegmentOffset(xlogptr : XLogRecPtr, wal_segsz_bytes : usize) -> u32 {
	return (xlogptr as u32) & (wal_segsz_bytes as u32 - 1);
}

#[allow(non_snake_case)]
pub fn XLogSegmentsPerXLogId(wal_segsz_bytes : usize) -> XLogSegNo {
	return (0x100000000u64 / wal_segsz_bytes as u64) as XLogSegNo;
}

#[allow(non_snake_case)]
pub fn XLByteToSeg(xlogptr : XLogRecPtr, wal_segsz_bytes : usize) -> XLogSegNo {
	return xlogptr as u32 / wal_segsz_bytes as u32;
}

#[allow(non_snake_case)]
pub fn XLogSegNoOffsetToRecPtr(segno: XLogSegNo, offset:u32, wal_segsz_bytes: usize) -> XLogRecPtr {
	return (segno as u64) * (wal_segsz_bytes as u64) + (offset as u64);
}

#[allow(non_snake_case)]
pub fn XLogFileName(tli : TimeLineID, logSegNo : XLogSegNo, wal_segsz_bytes : usize) -> String {
	return format!("{:>08X}{:>08X}{:>08X}",
				   tli,
				   logSegNo / XLogSegmentsPerXLogId(wal_segsz_bytes),
				   logSegNo % XLogSegmentsPerXLogId(wal_segsz_bytes));
}

#[allow(non_snake_case)]
pub fn XLogFromFileName(fname:&str, wal_seg_size: usize) -> (XLogSegNo,TimeLineID) {
	let tli = u32::from_str_radix(&fname[0..8], 16).unwrap();
	let log = u32::from_str_radix(&fname[8..16], 16).unwrap();
	let seg = u32::from_str_radix(&fname[16..24], 16).unwrap();
	return (log * XLogSegmentsPerXLogId(wal_seg_size) + seg, tli);
}

#[allow(non_snake_case)]
pub fn IsXLogFileName(fname:&str) -> bool {
	return fname.len() == XLOG_FNAME_LEN
		&& fname.chars().all(|c| c.is_ascii_hexdigit());
}

#[allow(non_snake_case)]
pub fn IsPartialXLogFileName(fname:&str) -> bool {
	return fname.ends_with(".partial")
		&& IsXLogFileName(&fname[0..fname.len()-8]);
}

pub fn get_current_timestamp() -> TimestampTz
{
	const UNIX_EPOCH_JDATE : u64 = 2440588; /* == date2j(1970, 1, 1) */
	const POSTGRES_EPOCH_JDATE : u64 = 2451545; /* == date2j(2000, 1, 1) */
	const SECS_PER_DAY : u64 = 86400;
	const USECS_PER_SEC : u64 = 1000000;
	match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
		Ok(n) => (n.as_secs() - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY)) * USECS_PER_SEC + n.subsec_micros() as u64,
		Err(_) => panic!("SystemTime before UNIX EPOCH!"),
	}
}

fn find_end_of_wal_segment(data_dir: &PathBuf, segno: XLogSegNo, tli: TimeLineID, wal_seg_size: usize) -> u32 {
	let mut offs = XLOG_SIZE_OF_XLOG_LONG_PHD;
	let mut padlen : usize = 0;
	let mut contlen : usize = 0;
	let mut buf = [0u8;XLOG_BLCKSZ];
	let file_name = XLogFileName(tli, segno, wal_seg_size);
	let mut file = File::open(data_dir.join(file_name)).unwrap();

	while offs < wal_seg_size {
		if offs % XLOG_BLCKSZ == 0 {
			if let Ok(bytes_read) = file.read(&mut buf) {
				if bytes_read != buf.len() {
					break;
				}
			} else {
				break;
			}
            offs += XLOG_SIZE_OF_XLOG_SHORT_PHD;
        } else if padlen > 0 {
            offs += padlen;
            padlen = 0;
        } else if contlen == 0 {
            let xl_tot_len = LittleEndian::read_u32(&buf[offs..offs+4]) as usize;
			if xl_tot_len == 0 {
				break;
			}
			offs += 4;
            contlen = xl_tot_len - 4;
        } else {
            // we're continuing a record, possibly from previous page.
            let pageleft = XLOG_BLCKSZ - (offs % XLOG_BLCKSZ);

            // read the rest of the record, or as much as fits on this page.
            let n = min(contlen, pageleft);

            offs += n;
            contlen -= n;

            if contlen == 0 {
                if offs % 8 != 0 {
                    padlen = 8 - (offs % 8);
                }
            }
        }
	}
	return offs as u32;
}

pub fn find_end_of_wal(data_dir: &PathBuf, wal_seg_size:usize, precise:bool) -> (XLogRecPtr,TimeLineID) {
	let mut high_segno : XLogSegNo = 0;
	let mut high_tli : TimeLineID = 0;
	let mut high_ispartial = false;

	for entry in fs::read_dir(data_dir).unwrap() {
		if let Ok(entry) = entry {
			let ispartial : bool;
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
			let (segno,tli) = XLogFromFileName(fname, wal_seg_size);
			if !ispartial && entry.metadata().unwrap().len() != wal_seg_size as u64 {
				continue;
			}
			if segno > high_segno ||
				(segno == high_segno && tli > high_tli) ||
				(segno == high_segno && tli == high_tli && high_ispartial && !ispartial)
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
		} else if precise {  /* otherwise locate last record in last partial segment */
			high_offs = find_end_of_wal_segment(data_dir, high_segno, high_tli, wal_seg_size);
		}
		let high_ptr = XLogSegNoOffsetToRecPtr(high_segno, high_offs, wal_seg_size);
		return (high_ptr,high_tli);
	}
	return (0,0);
}
