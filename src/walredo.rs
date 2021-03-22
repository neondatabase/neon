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
use tokio::runtime::Runtime;
use tokio::process::{Command};
use std::process::Stdio;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::AsyncBufReadExt;
use std::io::Error;
use std::assert;
use std::sync::{Arc};
use log::*;

use bytes::{Bytes, BytesMut, BufMut};

use crate::page_cache::BufferTag;
use crate::page_cache::CacheEntry;
use crate::page_cache::WALRecord;
use crate::page_cache;

//
// Main entry point for the WAL applicator thread.
//
pub fn wal_applicator_main()
{
    // We block on waiting for requests on the walredo request channel, but
    // use async I/O to communicate with the child process. Initialize the
    // runtime for the async part.
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    // Loop forever, handling requests as they come.
    let walredo_channel_receiver = &page_cache::PAGECACHE.walredo_receiver;
    loop {
        let request = walredo_channel_receiver.recv().unwrap();

        handle_apply_request(&runtime, request);
    }
}

fn handle_apply_request(runtime: &Runtime, entry_rc: Arc<CacheEntry>)
{
    let tag = entry_rc.key.tag;
    let (base_img, records) = page_cache::collect_records_for_apply(entry_rc.as_ref());

    let mut entry = entry_rc.content.lock().unwrap();
    entry.apply_pending = false;

    let apply_result = runtime.block_on(apply_wal_records(tag, base_img, records));

    if let Err(e) = apply_result {
        error!("could not apply WAL records: {}", e);
    } else {
        entry.page_image = Some(apply_result.unwrap());
    }

    // Wake up the requester, whether the operation succeeded or not.
    entry_rc.walredo_condvar.notify_all();
}

//
// Apply given WAL records ('records') over an old page image. Returns
// new page image.
//
async fn apply_wal_records(tag: BufferTag, base_img: Option<Bytes>, records: Vec<WALRecord>) -> Result<Bytes, Error>
{
    //
    // Start postgres binary in special WAL redo mode.
    //
    let child =
        Command::new("postgres")
        .arg("--wal-redo")
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("postgres --wal-redo command failed to start");

    let mut stdin = child.stdin.expect("failed to open child's stdin");
    let stderr = child.stderr.expect("failed to open childs' stderr");
    let mut stdout = child.stdout.expect("failed to open childs' stdout");

    //
    // This async block sends all the commands to the process.
    //
    // For reasons I don't understand, this needs to be a "move" block;
    // otherwise the stdin pipe doesn't get closed, despite the shutdown()
    // call.
    //
    let f_stdin = async move {
        // Send base image, if any. (If the record initializes the page, previous page
        // version is not needed.)
        stdin.write(&build_begin_redo_for_block_msg(tag)).await?;
        if base_img.is_some() {
            stdin.write(&build_push_page_msg(tag, base_img.unwrap())).await?;
        }

        // Send WAL records.
        for rec in records.iter() {
            let r = rec.clone();

            stdin.write(&build_apply_record_msg(r.lsn, r.rec)).await?;
        }
        debug!("sent {} WAL records to wal redo postgres process", records.len());

        // Send GetPage command to get the result back
        stdin.write(&build_get_page_msg(tag)).await?;
        debug!("sent GetPage");
        stdin.flush().await?;
        stdin.shutdown().await?;
        debug!("stdin finished");
        Ok::<(), Error>(())
    };

    // This async block reads the child's stderr, and forwards it to the logger
    let f_stderr = async move {
        let mut stderr_buffered = tokio::io::BufReader::new(stderr);

        let mut line = String::new();
        loop {
            let res = stderr_buffered.read_line(&mut line).await;
            if res.is_err() {
                debug!("could not convert line to utf-8");
                continue;
            }
            if res.unwrap() == 0 {
                break;
            }
            debug!("{}", line.trim());
            line.clear();
        }
        Ok::<(), Error>(())
    };

    // Read back new page image
    let f_stdout = async move {
        let mut buf = [0u8; 8192];

        stdout.read_exact(&mut buf).await?;
        debug!("got response");
        Ok::<[u8;8192], Error>(buf)
    };

    // Kill the process. This closes its stdin, which should signal the process
    // to terminate. TODO: SIGKILL if needed
    //child.wait();

    let res = futures::try_join!(f_stdout, f_stdin, f_stderr)?;

    let buf = res.0;
    
    Ok::<Bytes, Error>(Bytes::from(std::vec::Vec::from(buf)))
}

fn build_begin_redo_for_block_msg(tag: BufferTag) -> Bytes
{
    let mut buf = BytesMut::new();

    buf.put_u8('B' as u8);
    buf.put_u32(4 + 5*4);
    buf.put_u32(tag.spcnode);
    buf.put_u32(tag.dbnode);
    buf.put_u32(tag.relnode);
    buf.put_u32(tag.forknum as u32);
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
    buf.put_u32(tag.forknum as u32);
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
    buf.put_u32(tag.forknum as u32);
    buf.put_u32(tag.blknum);

    return buf.freeze();
}
