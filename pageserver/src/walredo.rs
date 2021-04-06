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
use log::*;
use std::assert;
use std::cell::RefCell;
use std::fs;
use std::io::Error;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::{path::PathBuf, process::Stdio};
use tokio::io::AsyncBufReadExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::runtime::Runtime;
use tokio::time::timeout;

use bytes::{BufMut, Bytes, BytesMut};

use crate::page_cache;
use crate::page_cache::CacheEntry;
use crate::page_cache::WALRecord;
use crate::{page_cache::BufferTag, PageServerConf};

static TIMEOUT: Duration = Duration::from_secs(20);

//
// Main entry point for the WAL applicator thread.
//
pub fn wal_redo_main(conf: PageServerConf, sys_id: u64) {
    info!("WAL redo thread started {}", sys_id);

    // We block on waiting for requests on the walredo request channel, but
    // use async I/O to communicate with the child process. Initialize the
    // runtime for the async part.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let pcache = page_cache::get_pagecahe(conf.clone(), sys_id);

    // Loop forever, handling requests as they come.
    let walredo_channel_receiver = &pcache.walredo_receiver;
    loop {
        let mut process: WalRedoProcess;
        let datadir = conf.data_dir.join(format!("wal-redo/{}", sys_id));

        info!("launching WAL redo postgres process {}", sys_id);
        {
            let _guard = runtime.enter();
            process = WalRedoProcess::launch(&datadir, &runtime).unwrap();
        }

        // Pretty arbitrarily, reuse the same Postgres process for 100 requests.
        // After that, kill it and start a new one. This is mostly to avoid
        // using up all shared buffers in Postgres's shared buffer cache; we don't
        // want to write any pages to disk in the WAL redo process.
        for _i in 1..100 {
            let request = walredo_channel_receiver.recv().unwrap();

            let result = handle_apply_request(&pcache, &process, &runtime, request);
            if result.is_err() {
                // On error, kill the process.
                break;
            }
        }

        info!("killing WAL redo postgres process");
        let _ = runtime.block_on(process.stdin.get_mut().shutdown());
        let mut child = process.child;
        drop(process.stdin);
        let _ = runtime.block_on(child.wait());
    }
}

fn handle_apply_request(
    pcache: &page_cache::PageCache,
    process: &WalRedoProcess,
    runtime: &Runtime,
    entry_rc: Arc<CacheEntry>,
) -> Result<(), Error> {
    let tag = entry_rc.key.tag;
    let lsn = entry_rc.key.lsn;
    let (base_img, records) = pcache.collect_records_for_apply(entry_rc.as_ref());

    let mut entry = entry_rc.content.lock().unwrap();
    entry.apply_pending = false;

    let nrecords = records.len();

    let start = Instant::now();
    let apply_result = process.apply_wal_records(runtime, tag, base_img, records);
    let duration = start.elapsed();

    let result;

    debug!(
        "applied {} WAL records in {} ms to reconstruct page image at LSN {:X}/{:X}",
        nrecords,
        duration.as_millis(),
        lsn >> 32,
        lsn & 0xffff_ffff
    );

    if let Err(e) = apply_result {
        error!("could not apply WAL records: {}", e);
        result = Err(e);
    } else {
        entry.page_image = Some(apply_result.unwrap());
        pcache
            .num_page_images
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        result = Ok(());
    }

    // Wake up the requester, whether the operation succeeded or not.
    entry_rc.walredo_condvar.notify_all();

    return result;
}

struct WalRedoProcess {
    child: Child,
    stdin: RefCell<ChildStdin>,
    stdout: RefCell<ChildStdout>,
}

impl WalRedoProcess {
    //
    // Start postgres binary in special WAL redo mode.
    //
    // Tests who run pageserver binary are setting proper PG_BIN_DIR
    // and PG_LIB_DIR so that WalRedo would start right postgres. We may later
    // switch to setting same things in pageserver config file.
    fn launch(datadir: &PathBuf, runtime: &Runtime) -> Result<WalRedoProcess, Error> {
        // Create empty data directory for wal-redo postgres deleting old one.
        fs::remove_dir_all(datadir.to_str().unwrap()).ok();
        let initdb = runtime
            .block_on(
                Command::new("initdb")
                    .args(&["-D", datadir.to_str().unwrap()])
                    .arg("-N")
                    .status(),
            )
            .expect("failed to execute initdb");

        if !initdb.success() {
            panic!("initdb failed");
        }

        // Start postgres itself
        let mut child = Command::new("postgres")
            .arg("--wal-redo")
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .env("PGDATA", datadir.to_str().unwrap())
            .spawn()
            .expect("postgres --wal-redo command failed to start");

        info!(
            "launched WAL redo postgres process on {}",
            datadir.to_str().unwrap()
        );

        let stdin = child.stdin.take().expect("failed to open child's stdin");
        let stderr = child.stderr.take().expect("failed to open child's stderr");
        let stdout = child.stdout.take().expect("failed to open child's stdout");

        // This async block reads the child's stderr, and forwards it to the logger
        let f_stderr = async {
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
                debug!("wal-redo-postgres: {}", line.trim());
                line.clear();
            }
            Ok::<(), Error>(())
        };
        tokio::spawn(f_stderr);

        Ok(WalRedoProcess {
            child: child,
            stdin: RefCell::new(stdin),
            stdout: RefCell::new(stdout),
        })
    }

    //
    // Apply given WAL records ('records') over an old page image. Returns
    // new page image.
    //
    fn apply_wal_records(
        &self,
        runtime: &Runtime,
        tag: BufferTag,
        base_img: Option<Bytes>,
        records: Vec<WALRecord>,
    ) -> Result<Bytes, Error> {
        let mut stdin = self.stdin.borrow_mut();
        let mut stdout = self.stdout.borrow_mut();
        return runtime.block_on(async {
            //
            // This async block sends all the commands to the process.
            //
            // For reasons I don't understand, this needs to be a "move" block;
            // otherwise the stdin pipe doesn't get closed, despite the shutdown()
            // call.
            //
            let f_stdin = async {
                // Send base image, if any. (If the record initializes the page, previous page
                // version is not needed.)
                timeout(
                    TIMEOUT,
                    stdin.write_all(&build_begin_redo_for_block_msg(tag)),
                )
                .await??;
                if base_img.is_some() {
                    timeout(
                        TIMEOUT,
                        stdin.write_all(&build_push_page_msg(tag, base_img.unwrap())),
                    )
                    .await??;
                }

                // Send WAL records.
                for rec in records.iter() {
                    let r = rec.clone();

                    stdin
                        .write_all(&build_apply_record_msg(r.lsn, r.rec))
                        .await?;

                    //debug!("sent WAL record to wal redo postgres process ({:X}/{:X}",
                    //       r.lsn >> 32, r.lsn & 0xffff_ffff);
                }
                //debug!("sent {} WAL records to wal redo postgres process ({:X}/{:X}",
                //       records.len(), lsn >> 32, lsn & 0xffff_ffff);

                // Send GetPage command to get the result back
                timeout(TIMEOUT, stdin.write_all(&build_get_page_msg(tag))).await??;
                timeout(TIMEOUT, stdin.flush()).await??;
                //debug!("sent GetPage for {}", tag.blknum);
                Ok::<(), Error>(())
            };

            // Read back new page image
            let f_stdout = async {
                let mut buf = [0u8; 8192];

                timeout(TIMEOUT, stdout.read_exact(&mut buf)).await??;
                //debug!("got response for {}", tag.blknum);
                Ok::<[u8; 8192], Error>(buf)
            };

            // Kill the process. This closes its stdin, which should signal the process
            // to terminate. TODO: SIGKILL if needed
            //child.wait();

            let res = futures::try_join!(f_stdout, f_stdin)?;

            let buf = res.0;

            Ok::<Bytes, Error>(Bytes::from(std::vec::Vec::from(buf)))
        });
    }
}

fn build_begin_redo_for_block_msg(tag: BufferTag) -> Bytes {
    let len = 4 + 5 * 4;
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8('B' as u8);
    buf.put_u32(len as u32);
    buf.put_u32(tag.spcnode);
    buf.put_u32(tag.dbnode);
    buf.put_u32(tag.relnode);
    buf.put_u32(tag.forknum as u32);
    buf.put_u32(tag.blknum);

    assert!(buf.len() == 1 + len);

    return buf.freeze();
}

fn build_push_page_msg(tag: BufferTag, base_img: Bytes) -> Bytes {
    assert!(base_img.len() == 8192);

    let len = 4 + 5 * 4 + base_img.len();
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8('P' as u8);
    buf.put_u32(len as u32);
    buf.put_u32(tag.spcnode);
    buf.put_u32(tag.dbnode);
    buf.put_u32(tag.relnode);
    buf.put_u32(tag.forknum as u32);
    buf.put_u32(tag.blknum);
    buf.put(base_img);

    assert!(buf.len() == 1 + len);

    return buf.freeze();
}

fn build_apply_record_msg(endlsn: u64, rec: Bytes) -> Bytes {
    let len = 4 + 8 + rec.len();
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8('A' as u8);
    buf.put_u32(len as u32);
    buf.put_u64(endlsn);
    buf.put(rec);

    assert!(buf.len() == 1 + len);

    return buf.freeze();
}

fn build_get_page_msg(tag: BufferTag) -> Bytes {
    let len = 4 + 5 * 4;
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8('G' as u8);
    buf.put_u32(len as u32);
    buf.put_u32(tag.spcnode);
    buf.put_u32(tag.dbnode);
    buf.put_u32(tag.relnode);
    buf.put_u32(tag.forknum as u32);
    buf.put_u32(tag.blknum);

    assert!(buf.len() == 1 + len);

    return buf.freeze();
}
