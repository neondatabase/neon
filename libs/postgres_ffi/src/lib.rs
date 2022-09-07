#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
// bindgen creates some unsafe code with no doc comments.
#![allow(clippy::missing_safety_doc)]
// suppress warnings on rust 1.53 due to bindgen unit tests.
// https://github.com/rust-lang/rust-bindgen/issues/1651
#![allow(deref_nullptr)]

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
            }
            pub mod controlfile_utils;
            pub mod nonrelfile_utils;
            pub mod pg_constants;
            pub mod relfile_utils;
            pub mod waldecoder;
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

// Export some widely used datatypes that are unlikely to change across Postgres versions
pub use v14::bindings::{uint32, uint64, Oid};
pub use v14::bindings::{BlockNumber, OffsetNumber};
pub use v14::bindings::{MultiXactId, TransactionId};
pub use v14::bindings::{TimeLineID, TimestampTz, XLogRecPtr, XLogSegNo};

// Likewise for these, although the assumption that these don't change is a little more iffy.
pub use v14::bindings::{MultiXactOffset, MultiXactStatus};
pub use v14::xlog_utils::{XLOG_SIZE_OF_XLOG_RECORD, XLOG_SIZE_OF_XLOG_SHORT_PHD};

// from pg_config.h. These can be changed with configure options --with-blocksize=BLOCKSIZE and
// --with-segsize=SEGSIZE, but assume the defaults for now.
pub const BLCKSZ: u16 = 8192;
pub const RELSEG_SIZE: u32 = 1024 * 1024 * 1024 / (BLCKSZ as u32);
pub const XLOG_BLCKSZ: usize = 8192;
pub const WAL_SEGMENT_SIZE: usize = 16 * 1024 * 1024;

pub const MAX_SEND_SIZE: usize = XLOG_BLCKSZ * 16;

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
    id > v14::pg_constants::FIRST_NORMAL_TRANSACTION_ID
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
