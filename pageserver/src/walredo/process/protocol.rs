use bytes::BufMut;
use pageserver_api::reltag::RelTag;
use serde::Serialize;
use utils::bin_ser::BeSer;
use utils::lsn::Lsn;

///
/// `RelTag` + block number (`blknum`) gives us a unique id of the page in the cluster.
///
/// In Postgres `BufferTag` structure is used for exactly the same purpose.
/// [See more related comments here](https://github.com/postgres/postgres/blob/99c5852e20a0987eca1c38ba0c09329d4076b6a0/src/include/storage/buf_internals.h#L91).
///
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize)]
pub(crate) struct BufferTag {
    pub rel: RelTag,
    pub blknum: u32,
}

pub(crate) fn build_begin_redo_for_block_msg(tag: BufferTag, buf: &mut Vec<u8>) {
    let len = 4 + 1 + 4 * 4;

    buf.put_u8(b'B');
    buf.put_u32(len as u32);

    tag.ser_into(buf)
        .expect("serialize BufferTag should always succeed");
}

pub(crate) fn build_push_page_msg(tag: BufferTag, base_img: &[u8], buf: &mut Vec<u8>) {
    assert!(base_img.len() == 8192);

    let len = 4 + 1 + 4 * 4 + base_img.len();

    buf.put_u8(b'P');
    buf.put_u32(len as u32);
    tag.ser_into(buf)
        .expect("serialize BufferTag should always succeed");
    buf.put(base_img);
}

pub(crate) fn build_apply_record_msg(endlsn: Lsn, rec: &[u8], buf: &mut Vec<u8>) {
    let len = 4 + 8 + rec.len();

    buf.put_u8(b'A');
    buf.put_u32(len as u32);
    buf.put_u64(endlsn.0);
    buf.put(rec);
}

pub(crate) fn build_get_page_msg(tag: BufferTag, buf: &mut Vec<u8>) {
    let len = 4 + 1 + 4 * 4;

    buf.put_u8(b'G');
    buf.put_u32(len as u32);
    tag.ser_into(buf)
        .expect("serialize BufferTag should always succeed");
}

pub(crate) fn build_ping_msg(buf: &mut Vec<u8>) {
    buf.put_u8(b'H');
    buf.put_u32(4);
}
