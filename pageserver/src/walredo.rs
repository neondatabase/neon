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
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::Error;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::io::AsyncBufReadExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::runtime::Runtime;
use tokio::time::timeout;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::page_cache;
use crate::page_cache::CacheEntry;
use crate::page_cache::WALRecord;
use crate::ZTimelineId;
use crate::{page_cache::BufferTag, pg_constants, PageServerConf};

static TIMEOUT: Duration = Duration::from_secs(20);

//
// Main entry point for the WAL applicator thread.
//
pub fn wal_redo_main(conf: &PageServerConf, timelineid: ZTimelineId) {
    info!("WAL redo thread started {}", timelineid);

    // We block on waiting for requests on the walredo request channel, but
    // use async I/O to communicate with the child process. Initialize the
    // runtime for the async part.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let pcache = page_cache::get_pagecache(conf, timelineid).unwrap();

    // Loop forever, handling requests as they come.
    let walredo_channel_receiver = &pcache.walredo_receiver;
    loop {
        let mut process: WalRedoProcess;
        let datadir = format!("wal-redo/{}", timelineid);

        info!("launching WAL redo postgres process {}", timelineid);
        {
            let _guard = runtime.enter();
            process = WalRedoProcess::launch(&datadir, &runtime).unwrap();
        }
        info!("WAL redo postgres started");

        // Pretty arbitrarily, reuse the same Postgres process for 100 requests.
        // After that, kill it and start a new one. This is mostly to avoid
        // using up all shared buffers in Postgres's shared buffer cache; we don't
        // want to write any pages to disk in the WAL redo process.
        for _i in 1..100000 {
            let request = walredo_channel_receiver.recv().unwrap();

            let result = handle_apply_request(&pcache, &process, &runtime, request);
            if result.is_err() {
                // Something went wrong with handling the request. It's not clear
                // if the request was faulty, and the next request would succeed
                // again, or if the 'postgres' process went haywire. To be safe,
                // kill the 'postgres' process so that we will start from a clean
                // slate, with a new process, for the next request.
                break;
            }
        }

        // Time to kill the 'postgres' process. A new one will be launched on next
        // iteration of the loop.
        info!("killing WAL redo postgres process");
        let _ = runtime.block_on(process.stdin.get_mut().shutdown());
        let mut child = process.child;
        drop(process.stdin);
        let _ = runtime.block_on(child.wait());
    }
}

fn transaction_id_set_status_bit(
    xl_info: u8,
    xl_rmid: u8,
    xl_xid: u32,
    record: WALRecord,
    page: &mut BytesMut,
) {
    let info = xl_info & pg_constants::XLOG_XACT_OPMASK;
    let mut status = 0;
    if info == pg_constants::XLOG_XACT_COMMIT {
        status = pg_constants::TRANSACTION_STATUS_COMMITTED;
    } else if info == pg_constants::XLOG_XACT_ABORT {
        status = pg_constants::TRANSACTION_STATUS_ABORTED;
    } else {
        trace!("handle_apply_request for RM_XACT_ID-{} NOT SUPPORTED YET. RETURN. lsn {:X}/{:X} main_data_offset {}, rec.len {}",
        status,
        record.lsn >> 32,
        record.lsn & 0xffffffff,
        record.main_data_offset, record.rec.len());
        return;
    }

    trace!("handle_apply_request for RM_XACT_ID-{} (1-commit, 2-abort) lsn {:X}/{:X} main_data_offset {}, rec.len {}",
    status,
    record.lsn >> 32,
    record.lsn & 0xffffffff,
    record.main_data_offset, record.rec.len());

    let byteno: usize = ((xl_rmid as u32 % pg_constants::CLOG_XACTS_PER_PAGE as u32)
        / pg_constants::CLOG_XACTS_PER_BYTE) as usize;

    let byteptr = &mut page[byteno..byteno + 1];
    let bshift: u8 = ((xl_xid % pg_constants::CLOG_XACTS_PER_BYTE)
        * pg_constants::CLOG_BITS_PER_XACT as u32) as u8;

    let mut curval = byteptr[0];
    curval = (curval >> bshift) & pg_constants::CLOG_XACT_BITMASK;

    let mut byteval = [0];
    byteval[0] = curval;
    byteval[0] &= !(((1 << pg_constants::CLOG_BITS_PER_XACT as u8) - 1) << bshift);
    byteval[0] |= status << bshift;

    byteptr.copy_from_slice(&byteval);
    trace!(
        "xl_xid {} byteno {} curval {} byteval {}",
        xl_xid,
        byteno,
        curval,
        byteval[0]
    );
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
    assert!(entry.apply_pending);
    entry.apply_pending = false;

    let nrecords = records.len();

    let start = Instant::now();

    let apply_result: Result<Bytes, Error>;
    if tag.rel.forknum == pg_constants::PG_XACT_FORKNUM as u8 {
        //TODO use base image if any
        static ZERO_PAGE: [u8; 8192] = [0u8; 8192];
        let zero_page_bytes: &[u8] = &ZERO_PAGE;
        let mut page = BytesMut::from(zero_page_bytes);

        for record in records {
            let mut buf = record.rec.clone();

            // 1. Parse XLogRecord struct
            // FIXME: refactor to avoid code duplication.
            let _xl_tot_len = buf.get_u32_le();
            let xl_xid = buf.get_u32_le();
            let _xl_prev = buf.get_u64_le();
            let xl_info = buf.get_u8();
            let xl_rmid = buf.get_u8();
            buf.advance(2); // 2 bytes of padding
            let _xl_crc = buf.get_u32_le();

            if xl_rmid == pg_constants::RM_CLOG_ID {
                let info = xl_info & !pg_constants::XLR_INFO_MASK;
                if info == pg_constants::CLOG_ZEROPAGE {
                    page.clone_from_slice(zero_page_bytes);
                    trace!("handle_apply_request for RM_CLOG_ID-CLOG_ZEROPAGE lsn {:X}/{:X} main_data_offset {}, rec.len {}",
                    record.lsn >> 32,
                    record.lsn & 0xffffffff,
                    record.main_data_offset, record.rec.len());
                }
            } else if xl_rmid == pg_constants::RM_XACT_ID {
                transaction_id_set_status_bit(xl_info, xl_rmid, xl_xid, record, &mut page);
            }
        }

        apply_result = Ok::<Bytes, Error>(page.freeze());
    } else {
        apply_result = process.apply_wal_records(runtime, tag, base_img, records);
    }

    let duration = start.elapsed();

    let result;

    trace!(
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
        result = Ok(());
    }

    // Wake up the requester, whether the operation succeeded or not.
    entry_rc.walredo_condvar.notify_all();

    result
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
    fn launch(datadir: &str, runtime: &Runtime) -> Result<WalRedoProcess, Error> {
        // Create empty data directory for wal-redo postgres deleting old one.
        fs::remove_dir_all(datadir).ok();
        let initdb = runtime
            .block_on(
                Command::new("initdb")
                    .args(&["-D", datadir])
                    .arg("-N")
                    .output(),
            )
            .expect("failed to execute initdb");

        if !initdb.status.success() {
            panic!(
                "initdb failed: {}\nstderr:\n{}",
                std::str::from_utf8(&initdb.stdout).unwrap(),
                std::str::from_utf8(&initdb.stderr).unwrap()
            );
        } else {
            // Limit shared cache for wal-redo-postres
            let mut config = OpenOptions::new()
                .append(true)
                .open(PathBuf::from(&datadir).join("postgresql.conf"))?;
            config.write(b"shared_buffers=128kB\n")?;
            config.write(b"fsync=off\n")?;
        }
        // Start postgres itself
        let mut child = Command::new("postgres")
            .arg("--wal-redo")
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .env("PGDATA", datadir)
            .spawn()
            .expect("postgres --wal-redo command failed to start");

        info!("launched WAL redo postgres process on {}", datadir);

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
                error!("wal-redo-postgres: {}", line.trim());
                line.clear();
            }
            Ok::<(), Error>(())
        };
        tokio::spawn(f_stderr);

        Ok(WalRedoProcess {
            child,
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
        runtime.block_on(async {
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
        })
    }
}

fn build_begin_redo_for_block_msg(tag: BufferTag) -> Bytes {
    let len = 4 + 5 * 4;
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8(b'B');
    buf.put_u32(len as u32);
    tag.pack(&mut buf);

    assert!(buf.len() == 1 + len);

    buf.freeze()
}

fn build_push_page_msg(tag: BufferTag, base_img: Bytes) -> Bytes {
    assert!(base_img.len() == 8192);

    let len = 4 + 5 * 4 + base_img.len();
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8(b'P');
    buf.put_u32(len as u32);
    tag.pack(&mut buf);
    buf.put(base_img);

    assert!(buf.len() == 1 + len);

    buf.freeze()
}

fn build_apply_record_msg(endlsn: u64, rec: Bytes) -> Bytes {
    let len = 4 + 8 + rec.len();
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8(b'A');
    buf.put_u32(len as u32);
    buf.put_u64(endlsn);
    buf.put(rec);

    assert!(buf.len() == 1 + len);

    buf.freeze()
}

fn build_get_page_msg(tag: BufferTag) -> Bytes {
    let len = 4 + 5 * 4;
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8(b'G');
    buf.put_u32(len as u32);
    tag.pack(&mut buf);

    assert!(buf.len() == 1 + len);

    buf.freeze()
}
