//!
//! WAL redo. This service runs PostgreSQL in a special wal_redo mode
//! to apply given WAL records over an old page image and return new
//! page image.
//!
//! We rely on Postgres to perform WAL redo for us. We launch a
//! postgres process in special "wal redo" mode that's similar to
//! single-user mode. We then pass the previous page image, if any,
//! and all the WAL records we want to apply, to the postgres
//! process. Then we get the page image back. Communication with the
//! postgres process happens via stdin/stdout
//!
//! See src/backend/tcop/zenith_wal_redo.c for the other side of
//! this communication.
//!
//! The Postgres process is assumed to be secure against malicious WAL
//! records. It achieves it by dropping privileges before replaying
//! any WAL records, so that even if an attacker hijacks the Postgres
//! process, he cannot escape out of it.
//!
use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::{Error, ErrorKind};
use std::ops::{Deref, DerefMut};
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::CommandExt;
use std::path::PathBuf;
use std::process::Stdio;
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender, SyncSender};
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use std::{fs, io};
use tracing::*;
use utils::crashsafe::path_with_suffix_extension;
use utils::{bin_ser::BeSer, id::TenantId, lsn::Lsn};

use crate::metrics::{
    WAL_REDO_BYTES_HISTOGRAM, WAL_REDO_RECORDS_HISTOGRAM, WAL_REDO_RECORD_COUNTER, WAL_REDO_TIME,
    WAL_REDO_WAIT_TIME,
};
use crate::pgdatadir_mapping::{key_to_rel_block, key_to_slru_block};
use crate::repository::Key;
use crate::task_mgr::BACKGROUND_RUNTIME;
use crate::walrecord::NeonWalRecord;
use crate::{config::PageServerConf, TEMP_FILE_SUFFIX};
use pageserver_api::reltag::{RelTag, SlruKind};
use postgres_ffi::pg_constants;
use postgres_ffi::relfile_utils::VISIBILITYMAP_FORKNUM;
use postgres_ffi::v14::nonrelfile_utils::{
    mx_offset_to_flags_bitshift, mx_offset_to_flags_offset, mx_offset_to_member_offset,
    transaction_id_set_status,
};
use postgres_ffi::v14::PG_MAJORVERSION;
use postgres_ffi::BLCKSZ;

const N_CHANNELS: usize = 16;
const CHANNEL_SIZE: usize = 1024 * 1024;
type ChannelId = usize;

///
/// `RelTag` + block number (`blknum`) gives us a unique id of the page in the cluster.
///
/// In Postgres `BufferTag` structure is used for exactly the same purpose.
/// [See more related comments here](https://github.com/postgres/postgres/blob/99c5852e20a0987eca1c38ba0c09329d4076b6a0/src/include/storage/buf_internals.h#L91).
///
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize)]
pub struct BufferTag {
    pub rel: RelTag,
    pub blknum: u32,
}

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
        key: Key,
        lsn: Lsn,
        base_img: Option<Bytes>,
        records: Vec<(Lsn, NeonWalRecord)>,
        pg_version: u32,
    ) -> Result<Bytes, WalRedoError>;
}

///
/// This is the real implementation that uses a Postgres process to
/// perform WAL replay. It multiplexes requests from multiple threads
/// using `sender` channel and send them to the postgres wal-redo process
/// pipe by separate thread. Responses are returned through set of `receivers`
/// channels, used in round robin manner.  Receiver thread is protected by mutex
/// to prevent it's usage by more than one thread
/// In the future, we might want to launch a pool of processes to allow concurrent
/// replay of multiple records.
///
pub struct PostgresRedoManager {
    // mutiplexor pipe: use sync_channel to allow sharing sender by multiple threads
    // and limit size of buffer
    sender: SyncSender<(ChannelId, Vec<u8>)>,
    // set of receiver channels
    receivers: Vec<Mutex<Receiver<Bytes>>>,
    // atomicly incremented counter for choosing receiver
    round_robin: AtomicUsize,
}

/// Can this request be served by neon redo functions
/// or we need to pass it to wal-redo postgres process?
fn can_apply_in_neon(rec: &NeonWalRecord) -> bool {
    // Currently, we don't have bespoken Rust code to replay any
    // Postgres WAL records. But everything else is handled in neon.
    #[allow(clippy::match_like_matches_macro)]
    match rec {
        NeonWalRecord::Postgres {
            will_init: _,
            rec: _,
        } => false,
        _ => true,
    }
}

/// An error happened in WAL redo
#[derive(Debug, thiserror::Error)]
pub enum WalRedoError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("cannot perform WAL redo now")]
    InvalidState,
    #[error("cannot perform WAL redo for this request")]
    InvalidRequest,
    #[error("cannot perform WAL redo for this record")]
    InvalidRecord,
}

///
/// Public interface of WAL redo manager
///
impl WalRedoManager for PostgresRedoManager {
    ///
    /// Request the WAL redo manager to apply some WAL records
    ///
    /// The WAL redo is handled by a separate thread, so this just sends a request
    /// to the thread and waits for response.
    ///
    fn request_redo(
        &self,
        key: Key,
        lsn: Lsn,
        base_img: Option<Bytes>,
        records: Vec<(Lsn, NeonWalRecord)>,
        pg_version: u32,
    ) -> Result<Bytes, WalRedoError> {
        if records.is_empty() {
            error!("invalid WAL redo request with no records");
            return Err(WalRedoError::InvalidRequest);
        }

        let mut img: Option<Bytes> = base_img;
        let mut batch_neon = can_apply_in_neon(&records[0].1);
        let mut batch_start = 0;
        for i in 1..records.len() {
            let rec_neon = can_apply_in_neon(&records[i].1);

            if rec_neon != batch_neon {
                let result = if batch_neon {
                    self.apply_batch_neon(key, lsn, img, &records[batch_start..i])
                } else {
                    self.apply_batch_postgres(key, lsn, img, &records[batch_start..i])
                };
                img = Some(result?);

                batch_neon = rec_neon;
                batch_start = i;
            }
        }
        // last batch
        if batch_neon {
            self.apply_batch_neon(key, lsn, img, &records[batch_start..])
        } else {
            self.apply_batch_postgres(key, lsn, img, &records[batch_start..])
        }
    }
}

impl PostgresRedoManager {
    ///
    /// Create a new PostgresRedoManager.
    ///
    pub fn new(conf: &'static PageServerConf, tenant_id: TenantId) -> PostgresRedoManager {
        let (tx, rx): (
            SyncSender<(ChannelId, Vec<u8>)>,
            Receiver<(ChannelId, Vec<u8>)>,
        ) = mpsc::sync_channel(CHANNEL_SIZE);
        let mut senders: Vec<Sender<Bytes>> = Vec::with_capacity(N_CHANNELS);
        let mut receivers: Vec<Mutex<Receiver<Bytes>>> = Vec::with_capacity(N_CHANNELS);
        for _ in 0..N_CHANNELS {
            let (tx, rx) = mpsc::channel();
            senders.push(tx);
            receivers.push(Mutex::new(rx));
        }
        if let Ok(mut proc) = PostgresRedoProcess::launch(conf, &tenant_id) {
            let _proxy = std::thread::spawn(move || loop {
                let mut requests = Vec::new();
                let (id, data) = rx.recv().unwrap();
                proc.stdin.write_all(&data).unwrap();
                requests.push(id);
                while let Ok((id, data)) = rx.try_recv() {
                    proc.stdin.write_all(&data).unwrap();
                    requests.push(id);
                }
                for id in requests {
                    let mut page = vec![0; BLCKSZ as usize];
                    proc.stdout.read_exact(&mut page).unwrap();
                    senders[id as usize].send(Bytes::from(page)).unwrap();
                }
            });
            PostgresRedoManager {
                sender: tx,
                receivers,
                round_robin: AtomicUsize::new(0),
            }
        } else {
            panic!("Failed to launch wal-redo postgres");
        }
    }

    #[instrument(skip_all, fields(tenant_id=%self.tenant_id, pid=%self.child.id()))]
    fn apply_wal_records(
        &self,
        tag: BufferTag,
        base_img: Option<Bytes>,
        records: &[(Lsn, NeonWalRecord)],
    ) -> Result<Bytes, WalRedoError> {
        // Serialize all the messages to send the WAL redo process first.
        //
        // This could be problematic if there are millions of records to replay,
        // but in practice the number of records is usually so small that it doesn't
        // matter, and it's better to keep this code simple.
        let mut writebuf: Vec<u8> = Vec::new();
        build_begin_redo_for_block_msg(tag, &mut writebuf);
        if let Some(img) = base_img {
            build_push_page_msg(tag, &img, &mut writebuf);
        }
        for (lsn, rec) in records.iter() {
            if let NeonWalRecord::Postgres {
                will_init: _,
                rec: postgres_rec,
            } = rec
            {
                build_apply_record_msg(*lsn, postgres_rec, &mut writebuf);
            } else {
                return Err(WalRedoError::InvalidRecord);
            }
        }
        build_get_page_msg(tag, &mut writebuf);
        WAL_REDO_RECORD_COUNTER.inc_by(records.len() as u64);

        let id = self.round_robin.fetch_add(1, Ordering::Relaxed) % N_CHANNELS;
        let rx = self.receivers[id].lock().unwrap();
        self.sender.send((id, writebuf)).unwrap();
        Ok(rx.recv().unwrap())
    }

    ///
    // Apply given WAL records ('records') over an old page image. Returns
    // new page image.
    ///
    fn apply_batch_postgres(
        &self,
        key: Key,
        _lsn: Lsn,
        base_img: Option<Bytes>,
        records: &[(Lsn, NeonWalRecord)],
    ) -> Result<Bytes, WalRedoError> {
        let (rel, blknum) = key_to_rel_block(key).or(Err(WalRedoError::InvalidRecord))?;

        let start_time = Instant::now();
        let buf_tag = BufferTag { rel, blknum };
        let result = self.apply_wal_records(buf_tag, base_img, records);
        let end_time = Instant::now();
        WAL_REDO_TIME.observe(end_time.duration_since(start_time).as_secs_f64());
        result
    }

    ///
    /// Process a batch of WAL records using bespoken Neon code.
    ///
    fn apply_batch_neon(
        &self,
        key: Key,
        lsn: Lsn,
        base_img: Option<Bytes>,
        records: &[(Lsn, NeonWalRecord)],
    ) -> Result<Bytes, WalRedoError> {
        let start_time = Instant::now();

        let mut page = BytesMut::new();
        if let Some(fpi) = base_img {
            // If full-page image is provided, then use it...
            page.extend_from_slice(&fpi[..]);
        } else {
            // All the current WAL record types that we can handle require a base image.
            error!("invalid neon WAL redo request with no base image");
            return Err(WalRedoError::InvalidRequest);
        }

        // Apply all the WAL records in the batch
        for (record_lsn, record) in records.iter() {
            self.apply_record_neon(key, &mut page, *record_lsn, record)?;
        }
        // Success!
        let end_time = Instant::now();
        let duration = end_time.duration_since(start_time);
        WAL_REDO_TIME.observe(duration.as_secs_f64());

        debug!(
            "neon applied {} WAL records in {} ms to reconstruct page image at LSN {}",
            records.len(),
            duration.as_micros(),
            lsn
        );

        Ok(page.freeze())
    }

    fn apply_record_neon(
        &self,
        key: Key,
        page: &mut BytesMut,
        _record_lsn: Lsn,
        record: &NeonWalRecord,
    ) -> Result<(), WalRedoError> {
        match record {
            NeonWalRecord::Postgres {
                will_init: _,
                rec: _,
            } => {
                error!("tried to pass postgres wal record to neon WAL redo");
                return Err(WalRedoError::InvalidRequest);
            }
            NeonWalRecord::ClearVisibilityMapFlags {
                new_heap_blkno,
                old_heap_blkno,
                flags,
            } => {
                // sanity check that this is modifying the correct relation
                let (rel, blknum) = key_to_rel_block(key).or(Err(WalRedoError::InvalidRecord))?;
                assert!(
                    rel.forknum == VISIBILITYMAP_FORKNUM,
                    "ClearVisibilityMapFlags record on unexpected rel {}",
                    rel
                );
                if let Some(heap_blkno) = *new_heap_blkno {
                    // Calculate the VM block and offset that corresponds to the heap block.
                    let map_block = pg_constants::HEAPBLK_TO_MAPBLOCK(heap_blkno);
                    let map_byte = pg_constants::HEAPBLK_TO_MAPBYTE(heap_blkno);
                    let map_offset = pg_constants::HEAPBLK_TO_OFFSET(heap_blkno);

                    // Check that we're modifying the correct VM block.
                    assert!(map_block == blknum);

                    // equivalent to PageGetContents(page)
                    let map = &mut page[pg_constants::MAXALIGN_SIZE_OF_PAGE_HEADER_DATA..];

                    map[map_byte as usize] &= !(flags << map_offset);
                }

                // Repeat for 'old_heap_blkno', if any
                if let Some(heap_blkno) = *old_heap_blkno {
                    let map_block = pg_constants::HEAPBLK_TO_MAPBLOCK(heap_blkno);
                    let map_byte = pg_constants::HEAPBLK_TO_MAPBYTE(heap_blkno);
                    let map_offset = pg_constants::HEAPBLK_TO_OFFSET(heap_blkno);

                    assert!(map_block == blknum);

                    let map = &mut page[pg_constants::MAXALIGN_SIZE_OF_PAGE_HEADER_DATA..];

                    map[map_byte as usize] &= !(flags << map_offset);
                }
            }
            // Non-relational WAL records are handled here, with custom code that has the
            // same effects as the corresponding Postgres WAL redo function.
            NeonWalRecord::ClogSetCommitted { xids, timestamp } => {
                let (slru_kind, segno, blknum) =
                    key_to_slru_block(key).or(Err(WalRedoError::InvalidRecord))?;
                assert_eq!(
                    slru_kind,
                    SlruKind::Clog,
                    "ClogSetCommitted record with unexpected key {}",
                    key
                );
                for &xid in xids {
                    let pageno = xid as u32 / pg_constants::CLOG_XACTS_PER_PAGE;
                    let expected_segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                    let expected_blknum = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;

                    // Check that we're modifying the correct CLOG block.
                    assert!(
                        segno == expected_segno,
                        "ClogSetCommitted record for XID {} with unexpected key {}",
                        xid,
                        key
                    );
                    assert!(
                        blknum == expected_blknum,
                        "ClogSetCommitted record for XID {} with unexpected key {}",
                        xid,
                        key
                    );

                    transaction_id_set_status(
                        xid,
                        pg_constants::TRANSACTION_STATUS_COMMITTED,
                        page,
                    );
                }

                // Append the timestamp
                if page.len() == BLCKSZ as usize + 8 {
                    page.truncate(BLCKSZ as usize);
                }
                if page.len() == BLCKSZ as usize {
                    page.extend_from_slice(&timestamp.to_be_bytes());
                } else {
                    warn!(
                        "CLOG blk {} in seg {} has invalid size {}",
                        blknum,
                        segno,
                        page.len()
                    );
                }
            }
            NeonWalRecord::ClogSetAborted { xids } => {
                let (slru_kind, segno, blknum) =
                    key_to_slru_block(key).or(Err(WalRedoError::InvalidRecord))?;
                assert_eq!(
                    slru_kind,
                    SlruKind::Clog,
                    "ClogSetAborted record with unexpected key {}",
                    key
                );
                for &xid in xids {
                    let pageno = xid as u32 / pg_constants::CLOG_XACTS_PER_PAGE;
                    let expected_segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                    let expected_blknum = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;

                    // Check that we're modifying the correct CLOG block.
                    assert!(
                        segno == expected_segno,
                        "ClogSetAborted record for XID {} with unexpected key {}",
                        xid,
                        key
                    );
                    assert!(
                        blknum == expected_blknum,
                        "ClogSetAborted record for XID {} with unexpected key {}",
                        xid,
                        key
                    );

                    transaction_id_set_status(xid, pg_constants::TRANSACTION_STATUS_ABORTED, page);
                }
            }
            NeonWalRecord::MultixactOffsetCreate { mid, moff } => {
                let (slru_kind, segno, blknum) =
                    key_to_slru_block(key).or(Err(WalRedoError::InvalidRecord))?;
                assert_eq!(
                    slru_kind,
                    SlruKind::MultiXactOffsets,
                    "MultixactOffsetCreate record with unexpected key {}",
                    key
                );
                // Compute the block and offset to modify.
                // See RecordNewMultiXact in PostgreSQL sources.
                let pageno = mid / pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32;
                let entryno = mid % pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32;
                let offset = (entryno * 4) as usize;

                // Check that we're modifying the correct multixact-offsets block.
                let expected_segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                let expected_blknum = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                assert!(
                    segno == expected_segno,
                    "MultiXactOffsetsCreate record for multi-xid {} with unexpected key {}",
                    mid,
                    key
                );
                assert!(
                    blknum == expected_blknum,
                    "MultiXactOffsetsCreate record for multi-xid {} with unexpected key {}",
                    mid,
                    key
                );

                LittleEndian::write_u32(&mut page[offset..offset + 4], *moff);
            }
            NeonWalRecord::MultixactMembersCreate { moff, members } => {
                let (slru_kind, segno, blknum) =
                    key_to_slru_block(key).or(Err(WalRedoError::InvalidRecord))?;
                assert_eq!(
                    slru_kind,
                    SlruKind::MultiXactMembers,
                    "MultixactMembersCreate record with unexpected key {}",
                    key
                );
                for (i, member) in members.iter().enumerate() {
                    let offset = moff + i as u32;

                    // Compute the block and offset to modify.
                    // See RecordNewMultiXact in PostgreSQL sources.
                    let pageno = offset / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;
                    let memberoff = mx_offset_to_member_offset(offset);
                    let flagsoff = mx_offset_to_flags_offset(offset);
                    let bshift = mx_offset_to_flags_bitshift(offset);

                    // Check that we're modifying the correct multixact-members block.
                    let expected_segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                    let expected_blknum = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                    assert!(
                        segno == expected_segno,
                        "MultiXactMembersCreate record for offset {} with unexpected key {}",
                        moff,
                        key
                    );
                    assert!(
                        blknum == expected_blknum,
                        "MultiXactMembersCreate record for offset {} with unexpected key {}",
                        moff,
                        key
                    );

                    let mut flagsval = LittleEndian::read_u32(&page[flagsoff..flagsoff + 4]);
                    flagsval &= !(((1 << pg_constants::MXACT_MEMBER_BITS_PER_XACT) - 1) << bshift);
                    flagsval |= member.status << bshift;
                    LittleEndian::write_u32(&mut page[flagsoff..flagsoff + 4], flagsval);
                    LittleEndian::write_u32(&mut page[memberoff..memberoff + 4], member.xid);
                }
            }
        }

        Ok(())
    }
}

///
/// Command with ability not to give all file descriptors to child process
///
trait CloseFileDescriptors: CommandExt {
    ///
    /// Close file descriptors (other than stdin, stdout, stderr) in child process
    ///
    fn close_fds(&mut self) -> &mut Command;
}

impl<C: CommandExt> CloseFileDescriptors for C {
    fn close_fds(&mut self) -> &mut Command {
        unsafe {
            self.pre_exec(move || {
                // SAFETY: Code executed inside pre_exec should have async-signal-safety,
                // which means it should be safe to execute inside a signal handler.
                // The precise meaning depends on platform. See `man signal-safety`
                // for the linux definition.
                //
                // The set_fds_cloexec_threadsafe function is documented to be
                // async-signal-safe.
                //
                // Aside from this function, the rest of the code is re-entrant and
                // doesn't make any syscalls. We're just passing constants.
                //
                // NOTE: It's easy to indirectly cause a malloc or lock a mutex,
                // which is not async-signal-safe. Be careful.
                close_fds::set_fds_cloexec_threadsafe(3, &[]);
                Ok(())
            })
        }
    }
}

///
/// Handle to the Postgres WAL redo process
///
struct PostgresRedoProcess {
    tenant_id: TenantId,
    child: NoLeakChild,
    stdin: ChildStdin,
    stdout: ChildStdout,
    stderr: ChildStderr,
    wal_redo_timeout: Duration,
}

impl PostgresRedoProcess {
    //
    // Start postgres binary in special WAL redo mode.
    //
    #[instrument(skip_all,fields(tenant_id=%tenant_id, pg_version=pg_version))]
    fn launch(conf: &PageServerConf, tenant_id: TenantId) -> Result<PostgresRedoProcess, Error> {
        // FIXME: We need a dummy Postgres cluster to run the process in. Currently, we
        // just create one with constant name. That fails if you try to launch more than
        // one WAL redo manager concurrently.
        let datadir = path_with_suffix_extension(
            conf.tenant_path(&tenant_id).join("wal-redo-datadir"),
            TEMP_FILE_SUFFIX,
        );
        let pg_version = PG_MAJORVERSION[1..3].parse::<u32>().unwrap();
        // Create empty data directory for wal-redo postgres, deleting old one first.
        if datadir.exists() {
            info!(
                "old temporary datadir {} exists, removing",
                datadir.display()
            );
            fs::remove_dir_all(&datadir)?;
        }
        let pg_bin_dir_path = conf.pg_bin_dir(pg_version).map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("incorrect pg_bin_dir path: {}", e),
            )
        })?;
        let pg_lib_dir_path = conf.pg_lib_dir(pg_version).map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("incorrect pg_lib_dir path: {}", e),
            )
        })?;

        info!("running initdb in {}", datadir.display());
        let initdb = Command::new(pg_bin_dir_path.join("initdb"))
            .args(&["-D", &datadir.to_string_lossy()])
            .arg("-N")
            .env_clear()
            .env("LD_LIBRARY_PATH", &pg_lib_dir_path)
            .env("DYLD_LIBRARY_PATH", &pg_lib_dir_path) // macOS
            .close_fds()
            .output()
            .map_err(|e| Error::new(e.kind(), format!("failed to execute initdb: {e}")))?;

        if !initdb.status.success() {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "initdb failed\nstdout: {}\nstderr:\n{}",
                    String::from_utf8_lossy(&initdb.stdout),
                    String::from_utf8_lossy(&initdb.stderr)
                ),
            ));
        } else {
            // Limit shared cache for wal-redo-postres
            let mut config = OpenOptions::new()
                .append(true)
                .open(PathBuf::from(&datadir).join("postgresql.conf"))?;
            config.write_all(b"shared_buffers=128kB\n")?;
            config.write_all(b"fsync=off\n")?;
            config.write_all(b"shared_preload_libraries=neon\n")?;
            config.write_all(b"neon.wal_redo=on\n")?;
        }

        // Start postgres itself
        let child = Command::new(pg_bin_dir_path.join("postgres"))
            .arg("--wal-redo")
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .env_clear()
            .env("LD_LIBRARY_PATH", &pg_lib_dir_path)
            .env("DYLD_LIBRARY_PATH", &pg_lib_dir_path)
            .env("PGDATA", &datadir)
            // The redo process is not trusted, so it runs in seccomp mode
            // (see seccomp in zenith_wal_redo.c). We have to make sure it doesn't
            // inherit any file descriptors from the pageserver that would allow
            // an attacker to do bad things.
            //
            // The Rust standard library makes sure to mark any file descriptors with
            // as close-on-exec by default, but that's not enough, since we use
            // libraries that directly call libc open without setting that flag.
            //
            // One example is the pidfile of the daemonize library, which doesn't
            // currently mark file descriptors as close-on-exec. Either way, we
            // want to be on the safe side and prevent accidental regression.
            .close_fds()
            .spawn_no_leak_child()
            .map_err(|e| {
                Error::new(
                    e.kind(),
                    format!("postgres --wal-redo command failed to start: {}", e),
                )
            })?;

        let mut child = scopeguard::guard(child, |child| {
            error!("killing wal-redo-postgres process due to a problem during launch");
            child.kill_and_wait();
        });

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        macro_rules! set_nonblock_or_log_err {
            ($file:ident) => {{
                let res = set_nonblock($file.as_raw_fd());
                if let Err(e) = &res {
                    error!(error = %e, file = stringify!($file), pid = child.id(), "set_nonblock failed");
                }
                res
            }};
        }
        set_nonblock_or_log_err!(stdin)?;
        set_nonblock_or_log_err!(stdout)?;
        set_nonblock_or_log_err!(stderr)?;

        // all fallible operations post-spawn are complete, so get rid of the guard
        let child = scopeguard::ScopeGuard::into_inner(child);

        Ok(PostgresRedoProcess {
            tenant_id,
            child,
            stdin,
            stdout,
            stderr,
            wal_redo_timeout: conf.wal_redo_timeout,
        })
    }
}

/// Wrapper type around `std::process::Child` which guarantees that the child
/// will be killed and waited-for by this process before being dropped.
struct NoLeakChild {
    child: Option<Child>,
}

impl Deref for NoLeakChild {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        self.child.as_ref().expect("must not use from drop")
    }
}

impl DerefMut for NoLeakChild {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.child.as_mut().expect("must not use from drop")
    }
}

impl NoLeakChild {
    fn spawn(command: &mut Command) -> io::Result<Self> {
        let child = command.spawn()?;
        Ok(NoLeakChild { child: Some(child) })
    }

    fn kill_and_wait(mut self) {
        let child = match self.child.take() {
            Some(child) => child,
            None => return,
        };
        Self::kill_and_wait_impl(child);
    }

    #[instrument(skip_all, fields(pid=child.id()))]
    fn kill_and_wait_impl(mut child: Child) {
        let res = child.kill();
        if let Err(e) = res {
            // This branch is very unlikely because:
            // - We (= pageserver) spawned this process successfully, so, we're allowed to kill it.
            // - This is the only place that calls .kill()
            // - We consume `self`, so, .kill() can't be called twice.
            // - If the process exited by itself or was killed by someone else,
            //   .kill() will still succeed because we haven't wait()'ed yet.
            //
            // So, if we arrive here, we have really no idea what happened,
            // whether the PID stored in self.child is still valid, etc.
            // If this function were fallible, we'd return an error, but
            // since it isn't, all we can do is log an error and proceed
            // with the wait().
            error!(error = %e, "failed to SIGKILL; subsequent wait() might fail or wait for wrong process");
        }

        match child.wait() {
            Ok(exit_status) => {
                // log at error level since .kill() is something we only do on errors ATM
                error!(exit_status = %exit_status, "wait successful");
            }
            Err(e) => {
                error!(error = %e, "wait error; might leak the child process; it will show as zombie (defunct)");
            }
        }
    }
}

impl Drop for NoLeakChild {
    fn drop(&mut self) {
        let child = match self.child.take() {
            Some(child) => child,
            None => return,
        };
        // Offload the kill+wait of the child process into the background.
        // If someone stops the runtime, we'll leak the child process.
        // We can ignore that case because we only stop the runtime on pageserver exit.
        BACKGROUND_RUNTIME.spawn(async move {
            tokio::task::spawn_blocking(move || {
                Self::kill_and_wait_impl(child);
            })
            .await
        });
    }
}

trait NoLeakChildCommandExt {
    fn spawn_no_leak_child(&mut self) -> io::Result<NoLeakChild>;
}

impl NoLeakChildCommandExt for Command {
    fn spawn_no_leak_child(&mut self) -> io::Result<NoLeakChild> {
        NoLeakChild::spawn(self)
    }
}

// Functions for constructing messages to send to the postgres WAL redo
// process. See vendor/postgres/src/backend/tcop/zenith_wal_redo.c for
// explanation of the protocol.

fn build_begin_redo_for_block_msg(tag: BufferTag, buf: &mut Vec<u8>) {
    let len = 4 + 1 + 4 * 4;

    buf.put_u8(b'B');
    buf.put_u32(len as u32);

    tag.ser_into(buf)
        .expect("serialize BufferTag should always succeed");
}

fn build_push_page_msg(tag: BufferTag, base_img: &[u8], buf: &mut Vec<u8>) {
    assert!(base_img.len() == 8192);

    let len = 4 + 1 + 4 * 4 + base_img.len();

    buf.put_u8(b'P');
    buf.put_u32(len as u32);
    tag.ser_into(buf)
        .expect("serialize BufferTag should always succeed");
    buf.put(base_img);
}

fn build_apply_record_msg(endlsn: Lsn, rec: &[u8], buf: &mut Vec<u8>) {
    let len = 4 + 8 + rec.len();

    buf.put_u8(b'A');
    buf.put_u32(len as u32);
    buf.put_u64(endlsn.0);
    buf.put(rec);
}

fn build_get_page_msg(tag: BufferTag, buf: &mut Vec<u8>) {
    let len = 4 + 1 + 4 * 4;

    buf.put_u8(b'G');
    buf.put_u32(len as u32);
    tag.ser_into(buf)
        .expect("serialize BufferTag should always succeed");
}
