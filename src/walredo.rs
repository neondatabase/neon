//
// WAL redo
//
// We rely on Postgres to perform WAL redo for us. We launch a
// postgres process in special "wal redo" mode that's similar to
// single-user mode. We then pass the the previous page image, if any,
// and all the WAL records we want to apply, to the postgress
// process. Then we get the page image back. Communication with the
// postgres process happens via stdin/stdout
//
// See src/backend/tcop/zenith_wal_redo.c for the other side of
// this communication.
//
// TODO: Even though the postgres code runs in a separate process,
// it's not a secure sandbox.
//
use std::process::{Command, Stdio};
use std::io::{Read, Write, Error};
use std::assert;

use bytes::{Bytes, BytesMut, BufMut};

use crate::page_cache::BufferTag;
use crate::page_cache::WALRecord;


//
// Apply given WAL records ('records') over an old page image. Returns
// new page image.
//
pub fn apply_wal_records(tag: BufferTag, base_img: Option<Bytes>, records: &Vec<WALRecord>) -> Result<Bytes, Error>
{
    //
    // Start postgres binary in special WAL redo mode.
    //
    let mut child =
        Command::new("postgres")
        .arg("--wal-redo")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("postgres --wal-redo command failed to start");

    let stdin = child.stdin.as_mut().expect("Failed to open stdin");

    // Send base image, if any. (If the record initializes the page, previous page
    // version is not needed.)
    stdin.write(&build_begin_redo_for_block_msg(tag))?;
    if base_img.is_some() {
        stdin.write(&build_push_page_msg(tag, base_img.unwrap()))?;
    }

    // Send WAL records.
    for rec in records.iter() {
        let r = rec.clone();

        stdin.write(&build_apply_record_msg(r.lsn, r.rec))?;
    }

    // Read back new page image
    stdin.write(&build_get_page_msg(tag))?;
    let mut buf = vec![0u8; 8192];
    child.stdout.unwrap().read_exact(&mut buf)?;

    // Kill the process. This closes its stdin, which should signal the process
    // to terminate. TODO: SIGKILL if needed
    //child.wait();

    return Ok(Bytes::from(buf));
}

fn build_begin_redo_for_block_msg(tag: BufferTag) -> Bytes
{
    let mut buf = BytesMut::new();

    buf.put_u8('B' as u8);
    buf.put_u32(4 + 5*4);
    buf.put_u32(tag.spcnode);
    buf.put_u32(tag.dbnode);
    buf.put_u32(tag.relnode);
    buf.put_u32(tag.forknum);
    buf.put_u32(tag.blknum);

    return buf.freeze();
}

fn build_push_page_msg(tag: BufferTag, base_img: Bytes) -> Bytes
{
    assert!(base_img.len() == 8192);

    let mut buf = BytesMut::new();

    buf.put_u8('P' as u8);
    buf.put_u32(4 + 5*4 + base_img.len() as u32);
    buf.put_u32(tag.spcnode);
    buf.put_u32(tag.dbnode);
    buf.put_u32(tag.relnode);
    buf.put_u32(tag.forknum);
    buf.put_u32(tag.blknum);
    buf.put(base_img);

    return buf.freeze();
}

fn build_apply_record_msg(lsn: u64, rec: Bytes) -> Bytes {

    let mut buf = BytesMut::new();

    buf.put_u8('A' as u8);
    buf.put_u32(4 + 8 + rec.len() as u32);
    buf.put_u64(lsn);
    buf.put(rec);

    return buf.freeze();
}

fn build_get_page_msg(tag: BufferTag, ) -> Bytes {

    let mut buf = BytesMut::new();

    buf.put_u8('G' as u8);
    buf.put_u32(4 + 5*4);
    buf.put_u32(tag.spcnode);
    buf.put_u32(tag.dbnode);
    buf.put_u32(tag.relnode);
    buf.put_u32(tag.forknum);
    buf.put_u32(tag.blknum);

    return buf.freeze();
}
