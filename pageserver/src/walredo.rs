//!
//! WAL redo
//!
//! We rely on Postgres to perform WAL redo for us. We launch a
//! postgres process in special "wal redo" mode that's similar to
//! single-user mode. We then pass the the previous page image, if any,
//! and all the WAL records we want to apply, to the postgres
//! process. Then we get the page image back. Communication with the
//! postgres process happens via stdin/stdout
//!
//! See src/backend/tcop/zenith_wal_redo.c for the other side of
//! this communication.
//!
//! TODO: Even though the postgres code runs in a separate process,
//! it's not a secure sandbox.
//!
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::*;
use std::assert;
use std::cell::RefCell;
use std::fs;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::mpsc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use tokio::io::AsyncBufReadExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::time::timeout;
use zenith_utils::lsn::Lsn;

use crate::repository::BufferTag;
use crate::repository::WALRecord;
use crate::waldecoder::{MultiXactId, XlMultiXactCreate};
use crate::PageServerConf;
use postgres_ffi::nonrelfile_utils::transaction_id_set_status;
use postgres_ffi::pg_constants;
use postgres_ffi::xlog_utils::XLogRecord;

///
/// WAL Redo Manager is responsible for replaying WAL records.
///
/// Callers use the WAL redo manager through this abstract interface,
/// which makes it easy to mock it in tests.
pub trait WalRedoManager: Send + Sync {
    /// Apply some WAL records.
    ///
    /// The caller passes an old page image, and WAL records that should be
    /// applied over it. The return value is a new page image, after applying
    /// the reords.
    fn request_redo(
        &self,
        tag: BufferTag,
        lsn: Lsn,
        base_img: Option<Bytes>,
        records: Vec<WALRecord>,
    ) -> Result<Bytes, WalRedoError>;
}

static TIMEOUT: Duration = Duration::from_secs(20);

///
/// The implementation consists of two parts: PostgresRedoManager, and
/// PostgresRedoManagerInternal. PostgresRedoManager is the public struct
/// that can be used to send redo requests to the manager.
/// PostgresRedoManagerInternal is used by the manager thread itself.
///
pub struct PostgresRedoManager {
    request_tx: Mutex<mpsc::Sender<WalRedoRequest>>,
}

struct PostgresRedoManagerInternal {
    conf: &'static PageServerConf,

    request_rx: mpsc::Receiver<WalRedoRequest>,
}

#[derive(Debug)]
struct WalRedoRequest {
    tag: BufferTag,
    lsn: Lsn,

    base_img: Option<Bytes>,
    records: Vec<WALRecord>,

    response_channel: mpsc::Sender<Result<Bytes, WalRedoError>>,
}

/// An error happened in WAL redo
#[derive(Debug, thiserror::Error)]
pub enum WalRedoError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

///
/// Public interface of WAL redo manager
///
impl PostgresRedoManager {
    ///
    /// Create a new PostgresRedoManager.
    ///
    /// This launches a new thread to handle the requests.
    pub fn new(conf: &'static PageServerConf) -> PostgresRedoManager {
        let (tx, rx) = mpsc::channel();

        //
        // Launch the WAL redo thread
        //
        // Get mutable references to the values that we need to pass to the
        // thread.
        let request_rx = rx;

        // Currently, the join handle is not saved anywhere and we
        // won't try restart the thread if it dies.
        let _walredo_thread = std::thread::Builder::new()
            .name("WAL redo thread".into())
            .spawn(move || {
                let mut internal = PostgresRedoManagerInternal { conf, request_rx };
                internal.wal_redo_main();
            })
            .unwrap();

        PostgresRedoManager {
            request_tx: Mutex::new(tx),
        }
    }
}

impl WalRedoManager for PostgresRedoManager {
    ///
    /// Request the WAL redo manager to apply some WAL records
    ///
    /// The WAL redo is handled by a separate thread, so this just sends a request
    /// to the thread and waits for response.
    ///
    fn request_redo(
        &self,
        tag: BufferTag,
        lsn: Lsn,
        base_img: Option<Bytes>,
        records: Vec<WALRecord>,
    ) -> Result<Bytes, WalRedoError> {
        // Create a channel where to receive the response
        let (tx, rx) = mpsc::channel::<Result<Bytes, WalRedoError>>();

        let request = WalRedoRequest {
            tag,
            lsn,
            base_img,
            records,
            response_channel: tx,
        };

        self.request_tx
            .lock()
            .unwrap()
            .send(request)
            .expect("could not send WAL redo request");

        rx.recv()
            .expect("could not receive response to WAL redo request")
    }
}

fn mx_offset_to_flags_offset(xid: MultiXactId) -> usize {
    return ((xid / pg_constants::MULTIXACT_MEMBERS_PER_MEMBERGROUP as u32) as u16
        % pg_constants::MULTIXACT_MEMBERGROUPS_PER_PAGE
        * pg_constants::MULTIXACT_MEMBERGROUP_SIZE) as usize;
}

fn mx_offset_to_flags_bitshift(xid: MultiXactId) -> u16 {
    return (xid as u16) % pg_constants::MULTIXACT_MEMBERS_PER_MEMBERGROUP
        * pg_constants::MXACT_MEMBER_BITS_PER_XACT;
}

/* Location (byte offset within page) of TransactionId of given member */
fn mx_offset_to_member_offset(xid: MultiXactId) -> usize {
    return mx_offset_to_flags_offset(xid)
        + (pg_constants::MULTIXACT_FLAGBYTES_PER_GROUP
            + (xid as u16 % pg_constants::MULTIXACT_MEMBERS_PER_MEMBERGROUP) * 4)
            as usize;
}

///
/// WAL redo thread
///
impl PostgresRedoManagerInternal {
    //
    // Main entry point for the WAL applicator thread.
    //
    fn wal_redo_main(&mut self) {
        info!("WAL redo thread started");

        // We block on waiting for requests on the walredo request channel, but
        // use async I/O to communicate with the child process. Initialize the
        // runtime for the async part.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let process: PostgresRedoProcess;

        // FIXME: We need a dummy Postgres cluster to run the process in. Currently, we
        // just create one with constant name. That fails if you try to launch more than
        // one WAL redo manager concurrently.
        let datadir = self.conf.workdir.join("wal-redo-datadir");

        info!("launching WAL redo postgres process");

        process = runtime
            .block_on(PostgresRedoProcess::launch(&datadir))
            .unwrap();

        // Loop forever, handling requests as they come.
        loop {
            let request = self
                .request_rx
                .recv()
                .expect("WAL redo request channel was closed");

            let result = runtime.block_on(self.handle_apply_request(&process, &request));
            let result_ok = result.is_ok();

            // Send the result to the requester
            let _ = request.response_channel.send(result);

            if !result_ok {
                error!("wal-redo-postgres failed to apply request {:?}", request);
            }
        }
    }

    ///
    /// Process one request for WAL redo.
    ///
    async fn handle_apply_request(
        &self,
        process: &PostgresRedoProcess,
        request: &WalRedoRequest,
    ) -> Result<Bytes, WalRedoError> {
        let tag = request.tag;
        let lsn = request.lsn;
        let base_img = request.base_img.clone();
        let records = &request.records;

        let nrecords = records.len();

        let start = Instant::now();

        let apply_result: Result<Bytes, Error>;
        if tag.rel.forknum > pg_constants::INIT_FORKNUM {
            const ZERO_PAGE: [u8; 8192] = [0u8; 8192];
            let mut page = BytesMut::new();
            if let Some(fpi) = base_img {
                page.extend_from_slice(&fpi[..]);
            } else {
                page.extend_from_slice(&ZERO_PAGE);
            }
            for record in records {
                let mut buf = record.rec.clone();

                // 1. Parse XLogRecord struct
                // FIXME: refactor to avoid code duplication.
                let xlogrec = XLogRecord::from_bytes(&mut buf);

                //move to main data
                // TODO probably, we should store some records in our special format
                // to avoid this weird parsing on replay
                let skip = (record.main_data_offset - pg_constants::SIZEOF_XLOGRECORD) as usize;
                if buf.remaining() > skip {
                    buf.advance(skip);
                }

                if xlogrec.xl_rmid == pg_constants::RM_CLOG_ID {
                    let info = xlogrec.xl_info & !pg_constants::XLR_INFO_MASK;
                    if info == pg_constants::CLOG_ZEROPAGE {
                        page.copy_from_slice(&ZERO_PAGE);
                    }
                } else if xlogrec.xl_rmid == pg_constants::RM_XACT_ID {
                    let info = xlogrec.xl_info & pg_constants::XLOG_XACT_OPMASK;
                    let mut status = 0;
                    if info == pg_constants::XLOG_XACT_COMMIT || info == pg_constants::XLOG_XACT_COMMIT_PREPARED {
                        status = pg_constants::TRANSACTION_STATUS_COMMITTED;
						if info == pg_constants::XLOG_XACT_COMMIT {
							transaction_id_set_status(xlogrec.xl_xid, status, &mut page);
						}
                        //handle subtrans
                        let _xact_time = buf.get_i64_le();
                        let mut xinfo = 0;
                        if xlogrec.xl_info & pg_constants::XLOG_XACT_HAS_INFO != 0 {
                            xinfo = buf.get_u32_le();
                            if xinfo & pg_constants::XACT_XINFO_HAS_DBINFO != 0 {
                                let _dbid = buf.get_u32_le();
                                let _tsid = buf.get_u32_le();
                            }
                        }

                        if xinfo & pg_constants::XACT_XINFO_HAS_SUBXACTS != 0 {
                            let nsubxacts = buf.get_i32_le();
                            for _i in 0..nsubxacts {
                                let subxact = buf.get_u32_le();
                                let blkno = subxact as u32 / pg_constants::CLOG_XACTS_PER_PAGE;
                                // only update xids on the requested page
                                if tag.blknum == blkno {
                                    status = pg_constants::TRANSACTION_STATUS_SUB_COMMITTED;
                                    transaction_id_set_status(subxact, status, &mut page);
                                }
                            }
                        }
						if info == pg_constants::XLOG_XACT_COMMIT_PREPARED {
							if xinfo & pg_constants::XACT_XINFO_HAS_RELFILENODES != 0 {
								let nrels = buf.get_i32_le();
								for _i in 0..nrels {
									let spcnode = buf.get_u32_le();
									let dbnode = buf.get_u32_le();
									let relnode = buf.get_u32_le();
									//TODO handle this too?
									trace!(
										"XLOG_XACT_COMMIT relfilenode {}/{}/{}",
										spcnode,
										dbnode,
										relnode
									);
								}
							}
							if xinfo & pg_constants::XACT_XINFO_HAS_INVALS != 0 {
								let nmsgs = buf.get_i32_le();
								for _i in 0..nmsgs {
									let sizeof_shared_invalidation_message = 0;
									buf.advance(sizeof_shared_invalidation_message);
								}
							}
							assert!((xinfo & pg_constants::XACT_XINFO_HAS_TWOPHASE) != 0);
							let xid = buf.get_u32_le();
							transaction_id_set_status(xid, status, &mut page);
						}
                    } else if info == pg_constants::XLOG_XACT_ABORT || info == pg_constants::XLOG_XACT_ABORT_PREPARED {
                        status = pg_constants::TRANSACTION_STATUS_ABORTED;
						if info == pg_constants::XLOG_XACT_ABORT {
							transaction_id_set_status(xlogrec.xl_xid, status, &mut page);
						}
                        //handle subtrans
                        let _xact_time = buf.get_i64_le();
                        let mut xinfo = 0;
                        if xlogrec.xl_info & pg_constants::XLOG_XACT_HAS_INFO != 0 {
                            xinfo = buf.get_u32_le();
                            if xinfo & pg_constants::XACT_XINFO_HAS_DBINFO != 0 {
                                let _dbid = buf.get_u32_le();
                                let _tsid = buf.get_u32_le();
                            }
                        }

                        if xinfo & pg_constants::XACT_XINFO_HAS_SUBXACTS != 0 {
                            let nsubxacts = buf.get_i32_le();
                            for _i in 0..nsubxacts {
                                let subxact = buf.get_u32_le();
                                let blkno = subxact as u32 / pg_constants::CLOG_XACTS_PER_PAGE;
                                // only update xids on the requested page
                                if tag.blknum == blkno {
                                    status = pg_constants::TRANSACTION_STATUS_ABORTED;
                                    transaction_id_set_status(subxact, status, &mut page);
                                }
                            }
                        }
						if info == pg_constants::XLOG_XACT_ABORT_PREPARED {
							if xinfo & pg_constants::XACT_XINFO_HAS_RELFILENODES != 0 {
								let nrels = buf.get_i32_le();
								for _i in 0..nrels {
									let spcnode = buf.get_u32_le();
									let dbnode = buf.get_u32_le();
									let relnode = buf.get_u32_le();
									//TODO handle this too?
									trace!(
										"XLOG_XACT_COMMIT relfilenode {}/{}/{}",
										spcnode,
										dbnode,
										relnode
									);
								}
							}
							if xinfo & pg_constants::XACT_XINFO_HAS_INVALS != 0 {
								let nmsgs = buf.get_i32_le();
								for _i in 0..nmsgs {
									let sizeof_shared_invalidation_message = 0;
									buf.advance(sizeof_shared_invalidation_message);
								}
							}
							assert!((xinfo & pg_constants::XACT_XINFO_HAS_TWOPHASE) != 0);
							let xid = buf.get_u32_le();
							transaction_id_set_status(xid, status, &mut page);
						}
                    } else if info == pg_constants::XLOG_XACT_PREPARE {
						info!("Apply prepare {} record", xlogrec.xl_xid);
						page.clear();
						page.extend_from_slice(&buf[..]);
					} else {
                        error!("handle_apply_request for RM_XACT_ID-{} NOT SUPPORTED YET. RETURN. lsn {} main_data_offset {}, rec.len {}",
                               status,
                               record.lsn,
                               record.main_data_offset, record.rec.len());
                    }
                } else if xlogrec.xl_rmid == pg_constants::RM_MULTIXACT_ID {
                    let info = xlogrec.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
                    if info == pg_constants::XLOG_MULTIXACT_ZERO_OFF_PAGE {
                        page.copy_from_slice(&ZERO_PAGE);
                    } else if info == pg_constants::XLOG_MULTIXACT_ZERO_MEM_PAGE {
                        page.copy_from_slice(&ZERO_PAGE);
                    } else if info == pg_constants::XLOG_MULTIXACT_CREATE_ID {
                        let xlrec = XlMultiXactCreate::decode(&mut buf);
                        if tag.rel.forknum == pg_constants::PG_MXACT_OFFSETS_FORKNUM {
                            let offs = (xlrec.mid % pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32
                                * 4) as usize;
                            LittleEndian::write_u32(&mut page[offs..offs + 4], xlrec.moff);
                        } else {
                            assert!(tag.rel.forknum == pg_constants::PG_MXACT_MEMBERS_FORKNUM);
                            for i in 0..xlrec.nmembers {
                                let blkno = i / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;
                                if blkno == tag.blknum {
                                    // update only target block
                                    let offset = xlrec.moff + i;
                                    let memberoff = mx_offset_to_member_offset(offset);
                                    let flagsoff = mx_offset_to_flags_offset(offset);
                                    let bshift = mx_offset_to_flags_bitshift(offset);
                                    let mut flagsval =
                                        LittleEndian::read_u32(&page[flagsoff..flagsoff + 4]);
                                    flagsval &=
                                        !(((1 << pg_constants::MXACT_MEMBER_BITS_PER_XACT) - 1)
                                            << bshift);
                                    flagsval |= xlrec.members[i as usize].status << bshift;
                                    LittleEndian::write_u32(
                                        &mut page[flagsoff..flagsoff + 4],
                                        flagsval,
                                    );
                                    LittleEndian::write_u32(
                                        &mut page[memberoff..memberoff + 4],
                                        xlrec.members[i as usize].xid,
                                    );
                                }
                            }
                        }
                    } else if info == pg_constants::XLOG_MULTIXACT_TRUNCATE_ID {
                        // empty page image indicates that this SLRU page is truncated and can be removed by GC
                        page.clear();
                    } else {
                        assert!(false);
                    }
                } else if xlogrec.xl_rmid == pg_constants::RM_RELMAP_ID {
                    page.clear();
                    page.extend_from_slice(&buf[12..]); // skip xl_relmap_update
                    assert!(page.len() == 512); // size of pg_filenode.map
                }
            }

            apply_result = Ok::<Bytes, Error>(page.freeze());
        } else {
            apply_result = process.apply_wal_records(tag, base_img, records).await;
        }

        let duration = start.elapsed();

        let result: Result<Bytes, WalRedoError>;

        trace!(
            "applied {} WAL records in {} ms to reconstruct page image at LSN {}",
            nrecords,
            duration.as_millis(),
            lsn
        );

        if let Err(e) = apply_result {
            error!("could not apply WAL records: {}", e);
            result = Err(WalRedoError::IoError(e));
        } else {
            let img = apply_result.unwrap();

            result = Ok(img);
        }

        // The caller is responsible for sending the response
        result
    }
}

struct PostgresRedoProcess {
    stdin: RefCell<ChildStdin>,
    stdout: RefCell<ChildStdout>,
}

impl PostgresRedoProcess {
    //
    // Start postgres binary in special WAL redo mode.
    //
    // Tests who run pageserver binary are setting proper PG_BIN_DIR
    // and PG_LIB_DIR so that WalRedo would start right postgres.

    // do that: We may later
    // switch to setting same things in pageserver config file.
    async fn launch(datadir: &Path) -> Result<PostgresRedoProcess, Error> {
        // Create empty data directory for wal-redo postgres, deleting old one first.
        if datadir.exists() {
            info!("directory {:?} exists, removing", &datadir);
            if let Err(e) = fs::remove_dir_all(&datadir) {
                error!("could not remove old wal-redo-datadir: {:?}", e);
            }
        }
        info!("running initdb in {:?}", datadir.display());
        let initdb = Command::new("initdb")
            .args(&["-D", datadir.to_str().unwrap()])
            .arg("-N")
            .output()
            .await
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
            config.write_all(b"shared_buffers=128kB\n")?;
            config.write_all(b"fsync=off\n")?;
            config.write_all(b"shared_preload_libraries=zenith\n")?;
            config.write_all(b"zenith.wal_redo=on\n")?;
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

        info!(
            "launched WAL redo postgres process on {:?}",
            datadir.display()
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
                error!("wal-redo-postgres: {}", line.trim());
                line.clear();
            }
            Ok::<(), Error>(())
        };
        tokio::spawn(f_stderr);

        Ok(PostgresRedoProcess {
            stdin: RefCell::new(stdin),
            stdout: RefCell::new(stdout),
        })
    }

    //
    // Apply given WAL records ('records') over an old page image. Returns
    // new page image.
    //
    async fn apply_wal_records(
        &self,
        tag: BufferTag,
        base_img: Option<Bytes>,
        records: &[WALRecord],
    ) -> Result<Bytes, std::io::Error> {
        let mut stdin = self.stdin.borrow_mut();
        let mut stdout = self.stdout.borrow_mut();

        // We do three things simultaneously: send the old base image and WAL records to
        // the child process's stdin, read the result from child's stdout, and forward any logging
        // information that the child writes to its stderr to the page server's log.
        //
        // 'f_stdin' handles writing the base image and WAL records to the child process.
        // 'f_stdout' below reads the result back. And 'f_stderr', which was spawned into the
        // tokio runtime in the 'launch' function already, forwards the logging.
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

        let res = tokio::try_join!(f_stdout, f_stdin)?;

        let buf = res.0;

        Ok::<Bytes, Error>(Bytes::from(std::vec::Vec::from(buf)))
    }
}

// Functions for constructing messages to send to the postgres WAL redo
// process. See vendor/postgres/src/backend/tcop/zenith_wal_redo.c for
// explanation of the protocol.

fn build_begin_redo_for_block_msg(tag: BufferTag) -> Bytes {
    let len = 4 + 1 + 4 * 4;
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8(b'B');
    buf.put_u32(len as u32);
    tag.pack(&mut buf);

    assert!(buf.len() == 1 + len);

    buf.freeze()
}

fn build_push_page_msg(tag: BufferTag, base_img: Bytes) -> Bytes {
    assert!(base_img.len() == 8192);

    let len = 4 + 1 + 4 * 4 + base_img.len();
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8(b'P');
    buf.put_u32(len as u32);
    tag.pack(&mut buf);
    buf.put(base_img);

    assert!(buf.len() == 1 + len);

    buf.freeze()
}

fn build_apply_record_msg(endlsn: Lsn, rec: Bytes) -> Bytes {
    let len = 4 + 8 + rec.len();
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8(b'A');
    buf.put_u32(len as u32);
    buf.put_u64(endlsn.0);
    buf.put(rec);

    assert!(buf.len() == 1 + len);

    buf.freeze()
}

fn build_get_page_msg(tag: BufferTag) -> Bytes {
    let len = 4 + 1 + 4 * 4;
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8(b'G');
    buf.put_u32(len as u32);
    tag.pack(&mut buf);

    assert!(buf.len() == 1 + len);

    buf.freeze()
}
