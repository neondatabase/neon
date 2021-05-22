//!
//! Common utilities for dealing with PostgreSQL non-relation files.
//!
use crate::pg_constants;
use bytes::BytesMut;
use log::*;

pub fn transaction_id_set_status(xid: u32, status: u8, page: &mut BytesMut) {
    trace!(
        "handle_apply_request for RM_XACT_ID-{} (1-commit, 2-abort, 3-sub_commit)",
        status
    );

    let byteno: usize = ((xid as u32 % pg_constants::CLOG_XACTS_PER_PAGE as u32)
        / pg_constants::CLOG_XACTS_PER_BYTE) as usize;

    let bshift: u8 =
        ((xid % pg_constants::CLOG_XACTS_PER_BYTE) * pg_constants::CLOG_BITS_PER_XACT as u32) as u8;

    page[byteno] =
        (page[byteno] & !(pg_constants::CLOG_XACT_BITMASK << bshift)) | (status << bshift);
}

pub fn transaction_id_get_status(xid: u32, page: &[u8]) -> u8 {
    let byteno: usize = ((xid as u32 % pg_constants::CLOG_XACTS_PER_PAGE as u32)
        / pg_constants::CLOG_XACTS_PER_BYTE) as usize;

    let bshift: u8 =
        ((xid % pg_constants::CLOG_XACTS_PER_BYTE) * pg_constants::CLOG_BITS_PER_XACT as u32) as u8;

    ((page[byteno] >> bshift) & pg_constants::CLOG_XACT_BITMASK) as u8
}
