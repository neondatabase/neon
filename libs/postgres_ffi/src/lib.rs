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
/// # Safety
/// See safety notes for `pg_checksum_page`
pub unsafe fn page_set_checksum(page: &mut [u8], blkno: u32) {
    let checksum = pg_checksum_page(page, blkno);
    page[8..10].copy_from_slice(&checksum.to_le_bytes());
}

/// Check if page checksum is valid.
/// # Safety
/// See safety notes for `pg_checksum_page`
pub unsafe fn page_verify_checksum(page: &[u8], blkno: u32) -> bool {
    let checksum = pg_checksum_page(page, blkno);
    checksum == u16::from_le_bytes(page[8..10].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use crate::pg_constants::BLCKSZ;
    use crate::{page_set_checksum, page_verify_checksum};
    use utils::pg_checksum_page::pg_checksum_page;

    #[test]
    fn set_and_verify_checksum() {
        // Create a page with some content and without a correct checksum.
        let mut page: [u8; BLCKSZ as usize] = [0; BLCKSZ as usize];
        for (i, byte) in page.iter_mut().enumerate().take(BLCKSZ as usize) {
            *byte = i as u8;
        }

        // Calculate the checksum.
        let checksum = unsafe { pg_checksum_page(&page[..], 0) };

        // Sanity check: random bytes in the checksum attribute should not be
        // a valid checksum.
        assert_ne!(
            checksum,
            u16::from_le_bytes(page[8..10].try_into().unwrap())
        );

        // Set the actual checksum.
        unsafe { page_set_checksum(&mut page, 0) };

        // Verify the checksum.
        assert!(unsafe { page_verify_checksum(&page[..], 0) });

        // Checksum is not valid with another block number.
        assert!(!unsafe { page_verify_checksum(&page[..], 1) });
    }
}
