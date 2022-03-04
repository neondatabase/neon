#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
// bindgen creates some unsafe code with no doc comments.
#![allow(clippy::missing_safety_doc)]
// suppress warnings on rust 1.53 due to bindgen unit tests.
// https://github.com/rust-lang/rust-bindgen/issues/1651
#![allow(deref_nullptr)]

use serde::{Deserialize, Serialize};

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

pub mod controlfile_utils;
pub mod nonrelfile_utils;
pub mod pg_constants;
pub mod pg_page;
pub mod relfile_utils;
pub mod waldecoder;
pub mod xlog_utils;

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
