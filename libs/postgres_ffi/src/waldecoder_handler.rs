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
use super::super::waldecoder::{State, WalDecodeError, WalStreamDecoder};
use super::bindings::{XLogLongPageHeaderData, XLogPageHeaderData, XLogRecord, XLOG_PAGE_MAGIC};
use super::xlog_utils::*;
use crate::WAL_SEGMENT_SIZE;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32c::*;
use log::*;
use std::cmp::min;
use std::num::NonZeroU32;
use utils::lsn::Lsn;

pub trait WalStreamDecoderHandler {
    fn validate_page_header(&self, hdr: &XLogPageHeaderData) -> Result<(), WalDecodeError>;
    fn poll_decode_internal(&mut self) -> Result<Option<(Lsn, Bytes)>, WalDecodeError>;
    fn complete_record(&mut self, recordbuf: Bytes) -> Result<(Lsn, Bytes), WalDecodeError>;
}

//
// This is a trick to support several postgres versions simultaneously.
//
// Page decoding code depends on postgres bindings, so it is compiled for each version.
// Thus WalStreamDecoder implements several WalStreamDecoderHandler traits.
// WalStreamDecoder poll_decode() method dispatches to the right handler based on the postgres version.
// Other methods are internal and are not dispatched.
//
// It is similar to having several impl blocks for the same struct,
// but the impls here are in different modules, so need to use a trait.
//
impl WalStreamDecoderHandler for WalStreamDecoder {
    fn validate_page_header(&self, hdr: &XLogPageHeaderData) -> Result<(), WalDecodeError> {
        let validate_impl = || {
            if hdr.xlp_magic != XLOG_PAGE_MAGIC as u16 {
                return Err(format!(
                    "invalid xlog page header: xlp_magic={}, expected {}",
                    hdr.xlp_magic, XLOG_PAGE_MAGIC
                ));
            }
            if hdr.xlp_pageaddr != self.lsn.0 {
                return Err(format!(
                    "invalid xlog page header: xlp_pageaddr={}, expected {}",
                    hdr.xlp_pageaddr, self.lsn
                ));
            }
            match self.state {
                State::WaitingForRecord => {
                    if hdr.xlp_info & XLP_FIRST_IS_CONTRECORD != 0 {
                        return Err(
                            "invalid xlog page header: unexpected XLP_FIRST_IS_CONTRECORD".into(),
                        );
                    }
                    if hdr.xlp_rem_len != 0 {
                        return Err(format!(
                            "invalid xlog page header: xlp_rem_len={}, but it's not a contrecord",
                            hdr.xlp_rem_len
                        ));
                    }
                }
                State::ReassemblingRecord { contlen, .. } => {
                    if hdr.xlp_info & XLP_FIRST_IS_CONTRECORD == 0 {
                        return Err(
                            "invalid xlog page header: XLP_FIRST_IS_CONTRECORD expected, not found"
                                .into(),
                        );
                    }
                    if hdr.xlp_rem_len != contlen.get() {
                        return Err(format!(
                            "invalid xlog page header: xlp_rem_len={}, expected {}",
                            hdr.xlp_rem_len,
                            contlen.get()
                        ));
                    }
                }
                State::SkippingEverything { .. } => {
                    panic!("Should not be validating page header in the SkippingEverything state");
                }
            };
            Ok(())
        };
        validate_impl().map_err(|msg| WalDecodeError { msg, lsn: self.lsn })
    }

    /// Attempt to decode another WAL record from the input that has been fed to the
    /// decoder so far.
    ///
    /// Returns one of the following:
    ///     Ok((Lsn, Bytes)): a tuple containing the LSN of next record, and the record itself
    ///     Ok(None): there is not enough data in the input buffer. Feed more by calling the `feed_bytes` function
    ///     Err(WalDecodeError): an error occurred while decoding, meaning the input was invalid.
    ///
    fn poll_decode_internal(&mut self) -> Result<Option<(Lsn, Bytes)>, WalDecodeError> {
        // Run state machine that validates page headers, and reassembles records
        // that cross page boundaries.
        loop {
            // parse and verify page boundaries as we go
            // However, we may have to skip some page headers if we're processing the XLOG_SWITCH record or skipping padding for whatever reason.
            match self.state {
                State::WaitingForRecord | State::ReassemblingRecord { .. } => {
                    if self.lsn.segment_offset(WAL_SEGMENT_SIZE) == 0 {
                        // parse long header

                        if self.inputbuf.remaining() < XLOG_SIZE_OF_XLOG_LONG_PHD {
                            return Ok(None);
                        }

                        let hdr = XLogLongPageHeaderData::from_bytes(&mut self.inputbuf).map_err(
                            |e| WalDecodeError {
                                msg: format!("long header deserialization failed {}", e),
                                lsn: self.lsn,
                            },
                        )?;

                        self.validate_page_header(&hdr.std)?;

                        self.lsn += XLOG_SIZE_OF_XLOG_LONG_PHD as u64;
                    } else if self.lsn.block_offset() == 0 {
                        if self.inputbuf.remaining() < XLOG_SIZE_OF_XLOG_SHORT_PHD {
                            return Ok(None);
                        }

                        let hdr =
                            XLogPageHeaderData::from_bytes(&mut self.inputbuf).map_err(|e| {
                                WalDecodeError {
                                    msg: format!("header deserialization failed {}", e),
                                    lsn: self.lsn,
                                }
                            })?;

                        self.validate_page_header(&hdr)?;

                        self.lsn += XLOG_SIZE_OF_XLOG_SHORT_PHD as u64;
                    }
                }
                State::SkippingEverything { .. } => {}
            }
            // now read page contents
            match &mut self.state {
                State::WaitingForRecord => {
                    // need to have at least the xl_tot_len field
                    if self.inputbuf.remaining() < 4 {
                        return Ok(None);
                    }

                    // peek xl_tot_len at the beginning of the record.
                    // FIXME: assumes little-endian
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
                        self.lsn += xl_tot_len as u64;
                        let recordbuf = self.inputbuf.copy_to_bytes(xl_tot_len as usize);
                        return Ok(Some(self.complete_record(recordbuf)?));
                    } else {
                        // Need to assemble the record from pieces. Remember the size of the
                        // record, and loop back. On next iterations, we will reach the branch
                        // below, and copy the part of the record that was on this or next page(s)
                        // to 'recordbuf'.  Subsequent iterations will skip page headers, and
                        // append the continuations from the next pages to 'recordbuf'.
                        self.state = State::ReassemblingRecord {
                            recordbuf: BytesMut::with_capacity(xl_tot_len as usize),
                            contlen: NonZeroU32::new(xl_tot_len).unwrap(),
                        }
                    }
                }
                State::ReassemblingRecord { recordbuf, contlen } => {
                    // we're continuing a record, possibly from previous page.
                    let pageleft = self.lsn.remaining_in_block() as u32;

                    // read the rest of the record, or as much as fits on this page.
                    let n = min(contlen.get(), pageleft) as usize;

                    if self.inputbuf.remaining() < n {
                        return Ok(None);
                    }

                    recordbuf.put(self.inputbuf.split_to(n));
                    self.lsn += n as u64;
                    *contlen = match NonZeroU32::new(contlen.get() - n as u32) {
                        Some(x) => x,
                        None => {
                            // The record is now complete.
                            let recordbuf = std::mem::replace(recordbuf, BytesMut::new()).freeze();
                            return Ok(Some(self.complete_record(recordbuf)?));
                        }
                    }
                }
                State::SkippingEverything { skip_until_lsn } => {
                    assert!(*skip_until_lsn >= self.lsn);
                    let n = skip_until_lsn.0 - self.lsn.0;
                    if self.inputbuf.remaining() < n as usize {
                        return Ok(None);
                    }
                    self.inputbuf.advance(n as usize);
                    self.lsn += n;
                    self.state = State::WaitingForRecord;
                }
            }
        }
    }

    fn complete_record(&mut self, recordbuf: Bytes) -> Result<(Lsn, Bytes), WalDecodeError> {
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
        let next_lsn = if xlogrec.is_xlog_switch_record() {
            trace!("saw xlog switch record at {}", self.lsn);
            self.lsn + self.lsn.calc_padding(WAL_SEGMENT_SIZE as u64)
        } else {
            // Pad to an 8-byte boundary
            self.lsn.align()
        };
        self.state = State::SkippingEverything {
            skip_until_lsn: next_lsn,
        };

        // We should return LSN of the next record, not the last byte of this record or
        // the byte immediately after. Note that this handles both XLOG_SWITCH and usual
        // records, the former "spans" until the next WAL segment (see test_xlog_switch).
        Ok((next_lsn, recordbuf))
    }
}
