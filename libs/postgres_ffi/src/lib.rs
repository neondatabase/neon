#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
// bindgen creates some unsafe code with no doc comments.
#![allow(clippy::missing_safety_doc)]
// noted at 1.63 that in many cases there's a u32 -> u32 transmutes in bindgen code.
#![allow(clippy::useless_transmute)]
// modules included with the postgres_ffi macro depend on the types of the specific version's
// types, and trigger a too eager lint.
#![allow(clippy::duplicate_mod)]

use bytes::Bytes;
use utils::bin_ser::SerializeError;
use utils::lsn::Lsn;

macro_rules! postgres_ffi {
    ($version:ident) => {
        #[path = "."]
        pub mod $version {
            pub mod bindings {
                // bindgen generates bindings for a lot of stuff we don't need
                #![allow(dead_code)]

                use serde::{Deserialize, Serialize};
                include!(concat!(
                    env!("OUT_DIR"),
                    "/bindings_",
                    stringify!($version),
                    ".rs"
                ));

                include!(concat!("pg_constants_", stringify!($version), ".rs"));
            }
            pub mod controlfile_utils;
            pub mod nonrelfile_utils;
            pub mod waldecoder_handler;
            pub mod xlog_utils;

            pub const PG_MAJORVERSION: &str = stringify!($version);

            // Re-export some symbols from bindings
            pub use bindings::DBState_DB_SHUTDOWNED;
            pub use bindings::{CheckPoint, ControlFileData, XLogRecord};
        }
    };
}

postgres_ffi!(v14);
postgres_ffi!(v15);

pub mod pg_constants;
pub mod relfile_utils;

// Export some widely used datatypes that are unlikely to change across Postgres versions
pub use v14::bindings::{uint32, uint64, Oid};
pub use v14::bindings::{BlockNumber, OffsetNumber};
pub use v14::bindings::{MultiXactId, TransactionId};
pub use v14::bindings::{TimeLineID, TimestampTz, XLogRecPtr, XLogSegNo};

// Likewise for these, although the assumption that these don't change is a little more iffy.
pub use v14::bindings::{MultiXactOffset, MultiXactStatus};
pub use v14::bindings::{PageHeaderData, XLogRecord};
pub use v14::xlog_utils::{XLOG_SIZE_OF_XLOG_RECORD, XLOG_SIZE_OF_XLOG_SHORT_PHD};

pub use v14::bindings::{CheckPoint, ControlFileData};

// from pg_config.h. These can be changed with configure options --with-blocksize=BLOCKSIZE and
// --with-segsize=SEGSIZE, but assume the defaults for now.
pub const BLCKSZ: u16 = 8192;
pub const RELSEG_SIZE: u32 = 1024 * 1024 * 1024 / (BLCKSZ as u32);
pub const XLOG_BLCKSZ: usize = 8192;
pub const WAL_SEGMENT_SIZE: usize = 16 * 1024 * 1024;

pub const MAX_SEND_SIZE: usize = XLOG_BLCKSZ * 16;

// Export some version independent functions that are used outside of this mod
pub use v14::xlog_utils::encode_logical_message;
pub use v14::xlog_utils::get_current_timestamp;
pub use v14::xlog_utils::to_pg_timestamp;
pub use v14::xlog_utils::XLogFileName;

pub use v14::bindings::DBState_DB_SHUTDOWNED;

pub fn bkpimage_is_compressed(bimg_info: u8, version: u32) -> anyhow::Result<bool> {
    match version {
        14 => Ok(bimg_info & v14::bindings::BKPIMAGE_IS_COMPRESSED != 0),
        15 => Ok(bimg_info & v15::bindings::BKPIMAGE_COMPRESS_PGLZ != 0
            || bimg_info & v15::bindings::BKPIMAGE_COMPRESS_LZ4 != 0
            || bimg_info & v15::bindings::BKPIMAGE_COMPRESS_ZSTD != 0),
        _ => anyhow::bail!("Unknown version {}", version),
    }
}

pub fn generate_wal_segment(
    segno: u64,
    system_id: u64,
    pg_version: u32,
) -> Result<Bytes, SerializeError> {
    match pg_version {
        14 => v14::xlog_utils::generate_wal_segment(segno, system_id),
        15 => v15::xlog_utils::generate_wal_segment(segno, system_id),
        _ => Err(SerializeError::BadInput),
    }
}

pub fn generate_pg_control(
    pg_control_bytes: &[u8],
    checkpoint_bytes: &[u8],
    lsn: Lsn,
    pg_version: u32,
) -> anyhow::Result<(Bytes, u64)> {
    match pg_version {
        14 => v14::xlog_utils::generate_pg_control(pg_control_bytes, checkpoint_bytes, lsn),
        15 => v15::xlog_utils::generate_pg_control(pg_control_bytes, checkpoint_bytes, lsn),
        _ => anyhow::bail!("Unknown version {}", pg_version),
    }
}

// PG timeline is always 1, changing it doesn't have any useful meaning in Neon.
//
// NOTE: this is not to be confused with Neon timelines; different concept!
//
// It's a shaky assumption, that it's always 1. We might import a
// PostgreSQL data directory that has gone through timeline bumps,
// for example. FIXME later.
pub const PG_TLI: u32 = 1;

//  See TransactionIdIsNormal in transam.h
pub const fn transaction_id_is_normal(id: TransactionId) -> bool {
    id > pg_constants::FIRST_NORMAL_TRANSACTION_ID
}

// See TransactionIdPrecedes in transam.c
pub const fn transaction_id_precedes(id1: TransactionId, id2: TransactionId) -> bool {
    /*
     * If either ID is a permanent XID then we can just do unsigned
     * comparison.  If both are normal, do a modulo-2^32 comparison.
     */

    if !(transaction_id_is_normal(id1)) || !transaction_id_is_normal(id2) {
        return id1 < id2;
    }

    let diff = id1.wrapping_sub(id2) as i32;
    diff < 0
}

// Check if page is not yet initialized (port of Postgres PageIsInit() macro)
pub fn page_is_new(pg: &[u8]) -> bool {
    pg[14] == 0 && pg[15] == 0 // pg_upper == 0
}

// ExtractLSN from page header
pub fn page_get_lsn(pg: &[u8]) -> Lsn {
    Lsn(
        ((u32::from_le_bytes(pg[0..4].try_into().unwrap()) as u64) << 32)
            | u32::from_le_bytes(pg[4..8].try_into().unwrap()) as u64,
    )
}

pub fn page_set_lsn(pg: &mut [u8], lsn: Lsn) {
    pg[0..4].copy_from_slice(&((lsn.0 >> 32) as u32).to_le_bytes());
    pg[4..8].copy_from_slice(&(lsn.0 as u32).to_le_bytes());
}

pub fn fsm_logical_to_physical(addr: BlockNumber) -> BlockNumber {
    let mut leafno = addr;
    const FSM_TREE_DEPTH: u32 = if pg_constants::SLOTS_PER_FSM_PAGE >= 1626 {
        3
    } else {
        4
    };

    /* Count upper level nodes required to address the leaf page */
    let mut pages: BlockNumber = 0;
    for _l in 0..FSM_TREE_DEPTH {
        pages += leafno + 1;
        leafno /= pg_constants::SLOTS_PER_FSM_PAGE;
    }
    /* Turn the page count into 0-based block number */
    pages - 1;
}

pub mod waldecoder {

    use crate::{v14, v15};
    use bytes::{Buf, Bytes, BytesMut};
    use std::num::NonZeroU32;
    use thiserror::Error;
    use utils::lsn::Lsn;

    pub enum State {
        WaitingForRecord,
        ReassemblingRecord {
            recordbuf: BytesMut,
            contlen: NonZeroU32,
        },
        SkippingEverything {
            skip_until_lsn: Lsn,
        },
    }

    pub struct WalStreamDecoder {
        pub lsn: Lsn,
        pub pg_version: u32,
        pub inputbuf: BytesMut,
        pub state: State,
    }

    #[derive(Error, Debug, Clone)]
    #[error("{msg} at {lsn}")]
    pub struct WalDecodeError {
        pub msg: String,
        pub lsn: Lsn,
    }

    impl WalStreamDecoder {
        pub fn new(lsn: Lsn, pg_version: u32) -> WalStreamDecoder {
            WalStreamDecoder {
                lsn,
                pg_version,
                inputbuf: BytesMut::new(),
                state: State::WaitingForRecord,
            }
        }

        // The latest LSN position fed to the decoder.
        pub fn available(&self) -> Lsn {
            self.lsn + self.inputbuf.remaining() as u64
        }

        pub fn feed_bytes(&mut self, buf: &[u8]) {
            self.inputbuf.extend_from_slice(buf);
        }

        pub fn poll_decode(&mut self) -> Result<Option<(Lsn, Bytes)>, WalDecodeError> {
            match self.pg_version {
                // This is a trick to support both versions simultaneously.
                // See WalStreamDecoderHandler comments.
                14 => {
                    use self::v14::waldecoder_handler::WalStreamDecoderHandler;
                    self.poll_decode_internal()
                }
                15 => {
                    use self::v15::waldecoder_handler::WalStreamDecoderHandler;
                    self.poll_decode_internal()
                }
                _ => Err(WalDecodeError {
                    msg: format!("Unknown version {}", self.pg_version),
                    lsn: self.lsn,
                }),
            }
        }
    }
}
