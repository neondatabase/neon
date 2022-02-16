#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
// bindgen creates some unsafe code with no doc comments.
#![allow(clippy::missing_safety_doc)]
// suppress warnings on rust 1.53 due to bindgen unit tests.
// https://github.com/rust-lang/rust-bindgen/issues/1651
#![allow(deref_nullptr)]

use serde::{Deserialize, Serialize};
use utils::lsn::Lsn;
use utils::pg_checksum_page::pg_checksum_page;

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

pub mod controlfile_utils;
pub mod nonrelfile_utils;
pub mod pg_constants;
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

/// Calculate page checksum and stamp it onto the page.
/// NB: this will zero out and ignore any existing checksum.
pub fn page_set_checksum(page: &mut [u8], blkno: u32) {
    page[8..10].copy_from_slice(&[0u8; 2]);
    let checksum = pg_checksum_page(page, blkno);
    page[8..10].copy_from_slice(&checksum.to_le_bytes());
}
