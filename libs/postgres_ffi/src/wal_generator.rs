use std::ffi::CStr;

use bytes::{Bytes, BytesMut};
use crc32c::crc32c_append;
use utils::lsn::Lsn;

use super::bindings::{XLogLongPageHeaderData, XLogPageHeaderData, XLOG_PAGE_MAGIC};
use super::xlog_utils::{
    XlLogicalMessage, XLOG_RECORD_CRC_OFFS, XLOG_SIZE_OF_XLOG_RECORD, XLP_BKP_REMOVABLE,
    XLP_FIRST_IS_CONTRECORD,
};
use super::XLogRecord;
use crate::pg_constants::{
    RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE, XLP_LONG_HEADER, XLR_BLOCK_ID_DATA_LONG,
    XLR_BLOCK_ID_DATA_SHORT,
};
use crate::{WAL_SEGMENT_SIZE, XLOG_BLCKSZ};

/// Generates binary WAL records for use in tests and benchmarks. Currently only generates logical
/// messages (effectively noops) with a fixed payload. It is used as an iterator which yields
/// encoded bytes for a single WAL record, including internal page headers if it spans pages.
/// Concatenating the bytes will yield a complete, well-formed WAL, which can be chunked at segment
/// boundaries if desired. Not optimized for performance.
///
/// The WAL format is version-dependant (see e.g. `XLOG_PAGE_MAGIC`), so make sure to import this
/// for the appropriate Postgres version (e.g. `postgres_ffi::v17::wal_generator::WalGenerator`).
///
/// A WAL is split into 16 MB segments. Each segment is split into 8 KB pages, with headers.
/// Records are arbitrary length, 8-byte aligned, and may span pages. The layout is e.g.:
///
/// |        Segment 1         |        Segment 2         |        Segment 3         |
/// | Page 1 | Page 2 | Page 3 | Page 4 | Page 5 | Page 6 | Page 7 | Page 8 | Page 9 |
/// | R1 |   R2  |R3|  R4  | R5  |  R6  |                 R7            | R8  |
///
/// TODO: support generating actual tables and rows.
#[derive(Default)]
pub struct WalGenerator {
    /// Current LSN to append the next record at.
    ///
    /// Callers can modify this (and prev_lsn) to restart generation at a different LSN, but should
    /// ensure that the LSN is on a valid record boundary (i.e. we can't start appending in the
    /// middle on an existing record or header, or beyond the end of the existing WAL).
    pub lsn: Lsn,
    /// The starting LSN of the previous record. Used in WAL record headers. The Safekeeper doesn't
    /// care about this, unlike Postgres, but we include it for completeness.
    pub prev_lsn: Lsn,
}

impl WalGenerator {
    // For now, hardcode the message payload.
    // TODO: support specifying the payload size.
    const PREFIX: &CStr = c"prefix";
    const MESSAGE: &[u8] = b"message";

    // Hardcode the sys, timeline, and DB IDs. We can make them configurable if we care about them.
    const SYS_ID: u64 = 0;
    const TIMELINE_ID: u32 = 1;
    const DB_ID: u32 = 0;

    /// Creates a new WAL generator, which emits logical message records (noops).
    pub fn new() -> Self {
        Self::default()
    }

    /// Encodes a logical message (basically a noop), with the given prefix and message.
    pub(crate) fn encode_logical_message(prefix: &CStr, message: &[u8]) -> Bytes {
        let prefix = prefix.to_bytes_with_nul();
        let header = XlLogicalMessage {
            db_id: Self::DB_ID,
            transactional: 0,
            prefix_size: prefix.len() as u64,
            message_size: message.len() as u64,
        };
        [&header.encode(), prefix, message].concat().into()
    }

    /// Encode a WAL record with the given payload data (e.g. a logical message).
    pub(crate) fn encode_record(data: Bytes, rmid: u8, info: u8, prev_lsn: Lsn) -> Bytes {
        // Prefix data with block ID and length.
        let data_header = Bytes::from(match data.len() {
            0 => vec![],
            1..=255 => vec![XLR_BLOCK_ID_DATA_SHORT, data.len() as u8],
            256.. => {
                let len_bytes = (data.len() as u32).to_le_bytes();
                [&[XLR_BLOCK_ID_DATA_LONG], len_bytes.as_slice()].concat()
            }
        });

        // Construct the WAL record header.
        let mut header = XLogRecord {
            xl_tot_len: (XLOG_SIZE_OF_XLOG_RECORD + data_header.len() + data.len()) as u32,
            xl_xid: 0,
            xl_prev: prev_lsn.into(),
            xl_info: info,
            xl_rmid: rmid,
            __bindgen_padding_0: [0; 2],
            xl_crc: 0, // see below
        };

        // Compute the CRC checksum for the data, and the header up to the CRC field.
        let mut crc = 0;
        crc = crc32c_append(crc, &data_header);
        crc = crc32c_append(crc, &data);
        crc = crc32c_append(crc, &header.encode().unwrap()[0..XLOG_RECORD_CRC_OFFS]);
        header.xl_crc = crc;

        // Encode the final header and record.
        let header = header.encode().unwrap();

        [header, data_header, data].concat().into()
    }

    /// Injects page headers on 8KB page boundaries. Takes the current LSN position where the record
    /// is to be appended.
    fn encode_pages(record: Bytes, mut lsn: Lsn) -> Bytes {
        // Fast path: record fits in current page, and the page already has a header.
        if lsn.remaining_in_block() as usize >= record.len() && lsn.block_offset() > 0 {
            return record;
        }

        let mut pages = BytesMut::new();
        let mut remaining = record.clone(); // Bytes::clone() is cheap
        while !remaining.is_empty() {
            // At new page boundary, inject page header.
            if lsn.block_offset() == 0 {
                let mut page_header = XLogPageHeaderData {
                    xlp_magic: XLOG_PAGE_MAGIC as u16,
                    xlp_info: XLP_BKP_REMOVABLE,
                    xlp_tli: Self::TIMELINE_ID,
                    xlp_pageaddr: lsn.0,
                    xlp_rem_len: 0,
                    __bindgen_padding_0: [0; 4],
                };
                // If the record was split across page boundaries, mark as continuation.
                if remaining.len() < record.len() {
                    page_header.xlp_rem_len = remaining.len() as u32;
                    page_header.xlp_info |= XLP_FIRST_IS_CONTRECORD;
                }
                // At start of segment, use a long page header.
                let page_header = if lsn.segment_offset(WAL_SEGMENT_SIZE) == 0 {
                    page_header.xlp_info |= XLP_LONG_HEADER;
                    XLogLongPageHeaderData {
                        std: page_header,
                        xlp_sysid: Self::SYS_ID,
                        xlp_seg_size: WAL_SEGMENT_SIZE as u32,
                        xlp_xlog_blcksz: XLOG_BLCKSZ as u32,
                    }
                    .encode()
                    .unwrap()
                } else {
                    page_header.encode().unwrap()
                };
                pages.extend_from_slice(&page_header);
                lsn += page_header.len() as u64;
            }

            // Append the record up to the next page boundary, if any.
            let page_free = lsn.remaining_in_block() as usize;
            let chunk = remaining.split_to(std::cmp::min(page_free, remaining.len()));
            pages.extend_from_slice(&chunk);
            lsn += chunk.len() as u64;
        }
        pages.freeze()
    }

    /// Records must be 8-byte aligned. Take an encoded record (including any injected page
    /// boundaries), starting at the given LSN, and add any necessary padding at the end.
    fn pad_record(record: Bytes, mut lsn: Lsn) -> Bytes {
        lsn += record.len() as u64;
        let padding = lsn.calc_padding(8u64) as usize;
        if padding == 0 {
            return record;
        }
        [record, Bytes::from(vec![0; padding])].concat().into()
    }

    /// Generates a record with an arbitrary payload at the current LSN, then increments the LSN.
    pub fn generate_record(&mut self, data: Bytes, rmid: u8, info: u8) -> Bytes {
        let record = Self::encode_record(data, rmid, info, self.prev_lsn);
        let record = Self::encode_pages(record, self.lsn);
        let record = Self::pad_record(record, self.lsn);
        self.prev_lsn = self.lsn;
        self.lsn += record.len() as u64;
        record
    }

    /// Generates a logical message at the current LSN. Can be used to construct arbitrary messages.
    pub fn generate_logical_message(&mut self, prefix: &CStr, message: &[u8]) -> Bytes {
        let data = Self::encode_logical_message(prefix, message);
        self.generate_record(data, RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE)
    }
}

/// Generate WAL records as an iterator.
impl Iterator for WalGenerator {
    type Item = (Lsn, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        let lsn = self.lsn;
        let record = self.generate_logical_message(Self::PREFIX, Self::MESSAGE);
        Some((lsn, record))
    }
}
