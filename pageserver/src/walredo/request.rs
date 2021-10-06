///! Functions for constructing messages to send to the postgres WAL redo
///! process. See vendor/postgres/src/backend/tcop/zenith_wal_redo.c for
///! explanation of the protocol.
use bytes::{BufMut, Bytes, BytesMut};
use zenith_utils::{bin_ser::BeSer, lsn::Lsn};

use crate::repository::WALRecord;

use super::BufferTag;

pub fn serialize_request(
    tag: BufferTag,
    base_img: &Option<Bytes>,
    records: &[WALRecord],
) -> BytesMut {
    let mut capacity = 1 + BEGIN_REDO_MSG_LEN;
    if base_img.is_some() {
        capacity += 1 + PUSH_PAGE_MSG_LEN;
    }
    capacity += (1 + APPLY_MSG_HEADER_LEN) * records.len();
    capacity += records.iter().map(|rec| rec.rec.len()).sum::<usize>();
    capacity += 1 + GET_PAGE_MSG_LEN;

    let mut buf = BytesMut::with_capacity(capacity);

    build_begin_redo_for_block_msg(&mut buf, tag);

    if let Some(base_img) = base_img.as_ref() {
        build_push_page_msg(&mut buf, tag, base_img);
    }

    for record in records {
        build_apply_record_msg(&mut buf, record.lsn, &record.rec);
    }

    build_get_page_msg(&mut buf, tag);

    debug_assert_eq!(capacity, buf.len());

    buf
}

const TAG_LEN: usize = 4 * 4;
const PAGE_SIZE: usize = 8192;
const BEGIN_REDO_MSG_LEN: usize = 4 + 1 + TAG_LEN;
const PUSH_PAGE_MSG_LEN: usize = 4 + 1 + TAG_LEN + PAGE_SIZE;
const APPLY_MSG_HEADER_LEN: usize = 4 + 8;
const GET_PAGE_MSG_LEN: usize = 4 + 1 + TAG_LEN;

fn build_begin_redo_for_block_msg(buf: &mut BytesMut, tag: BufferTag) {
    buf.put_u8(b'B');
    buf.put_u32(BEGIN_REDO_MSG_LEN as u32);

    // TODO tag is serialized multiple times
    // let's try to serialize it just once
    // or make the protocol less repetitive
    tag.ser_into(&mut buf.writer())
        .expect("serialize BufferTag should always succeed");
}

fn build_push_page_msg(buf: &mut BytesMut, tag: BufferTag, base_img: &Bytes) {
    debug_assert_eq!(base_img.len(), PAGE_SIZE);

    buf.put_u8(b'P');
    buf.put_u32(PUSH_PAGE_MSG_LEN as u32);
    tag.ser_into(&mut buf.writer())
        .expect("serialize BufferTag should always succeed");
    buf.extend(base_img);
}

fn build_apply_record_msg(buf: &mut BytesMut, endlsn: Lsn, rec: &Bytes) {
    buf.put_u8(b'A');

    let len = APPLY_MSG_HEADER_LEN + rec.len();
    buf.put_u32(len as u32);

    buf.put_u64(endlsn.0);
    buf.extend(rec);
}

fn build_get_page_msg(buf: &mut BytesMut, tag: BufferTag) {
    buf.put_u8(b'G');
    buf.put_u32(GET_PAGE_MSG_LEN as u32);
    tag.ser_into(&mut buf.writer())
        .expect("serialize BufferTag should always succeed");
}
