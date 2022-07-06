//!
//! Basic WAL stream decoding.
//!
//! This understands the WAL page and record format, enough to figure out where the WAL record
//! boundaries are, and to reassemble WAL records that cross page boundaries.
//!
//! This functionality is needed by both the pageserver and the safekeepers. The pageserver needs
//! to look deeper into the WAL records to also understand which blocks they modify, the code
//! for that is in pageserver/src/walrecord.rs
//!
use super::pg_constants;
use super::xlog_utils::*;
use super::XLogLongPageHeaderData;
use super::XLogPageHeaderData;
use super::XLogRecord;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32c::*;
use log::*;
use std::cmp::min;
use thiserror::Error;
use utils::lsn::Lsn;

pub struct WalStreamDecoder {
    lsn: Lsn,

    startlsn: Lsn, // LSN where this record starts
    contlen: u32,
    padlen: u32,

    inputbuf: BytesMut,

    /// buffer used to reassemble records that cross page boundaries.
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
    ///     Err(WalDecodeError): an error occurred while decoding, meaning the input was invalid.
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

                let hdr = XLogLongPageHeaderData::from_bytes(&mut self.inputbuf).map_err(|e| {
                    WalDecodeError {
                        msg: format!("long header deserialization failed {}", e),
                        lsn: self.lsn,
                    }
                })?;

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

                let hdr = XLogPageHeaderData::from_bytes(&mut self.inputbuf).map_err(|e| {
                    WalDecodeError {
                        msg: format!("header deserialization failed {}", e),
                        lsn: self.lsn,
                    }
                })?;

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
        let xlogrec =
            XLogRecord::from_slice(&recordbuf[0..XLOG_SIZE_OF_XLOG_RECORD]).map_err(|e| {
                WalDecodeError {
                    msg: format!("xlog record deserialization failed {}", e),
                    lsn: self.lsn,
                }
            })?;

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
