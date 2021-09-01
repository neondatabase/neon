//!
//! Common utilities for dealing with PostgreSQL non-relation files.
//!
use crate::pg_constants;
use bytes::BytesMut;
use log::*;

use crate::MultiXactId;

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

// Multixact utils

pub fn mx_offset_to_flags_offset(xid: MultiXactId) -> usize {
    ((xid / pg_constants::MULTIXACT_MEMBERS_PER_MEMBERGROUP as u32) as u16
        % pg_constants::MULTIXACT_MEMBERGROUPS_PER_PAGE
        * pg_constants::MULTIXACT_MEMBERGROUP_SIZE) as usize
}

pub fn mx_offset_to_flags_bitshift(xid: MultiXactId) -> u16 {
    (xid as u16) % pg_constants::MULTIXACT_MEMBERS_PER_MEMBERGROUP
        * pg_constants::MXACT_MEMBER_BITS_PER_XACT
}

/* Location (byte offset within page) of TransactionId of given member */
pub fn mx_offset_to_member_offset(xid: MultiXactId) -> usize {
    mx_offset_to_flags_offset(xid)
        + (pg_constants::MULTIXACT_FLAGBYTES_PER_GROUP
            + (xid as u16 % pg_constants::MULTIXACT_MEMBERS_PER_MEMBERGROUP) * 4) as usize
}

fn mx_offset_to_member_page(xid: u32) -> u32 {
    xid / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32
}

pub fn mx_offset_to_member_segment(xid: u32) -> i32 {
    (mx_offset_to_member_page(xid) / pg_constants::SLRU_PAGES_PER_SEGMENT) as i32
}
