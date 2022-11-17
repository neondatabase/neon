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
//! See pgxn/neon_walredo/walredoproc.c for the other side of
//! this communication.
//!
//! The Postgres process is assumed to be secure against malicious WAL
//! records. It achieves it by dropping privileges before replaying
//! any WAL records, so that even if an attacker hijacks the Postgres
//! process, he cannot escape out of it.
//!
use anyhow::Context;
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::Serialize;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tracing::*;
use utils::crashsafe::path_with_suffix_extension;
use utils::{bin_ser::BeSer, id::TenantId, lsn::Lsn};

use crate::metrics::{
    WAL_REDO_BYTES_HISTOGRAM, WAL_REDO_RECORDS_HISTOGRAM, WAL_REDO_RECORD_COUNTER, WAL_REDO_TIME,
};
use crate::pgdatadir_mapping::{key_to_rel_block, key_to_slru_block};
use crate::repository::Key;
use crate::task_mgr::WALREDO_RUNTIME;
use crate::walrecord::NeonWalRecord;
use crate::{config::PageServerConf, TEMP_FILE_SUFFIX};
use pageserver_api::reltag::{RelTag, SlruKind};
use postgres_ffi::pg_constants;
use postgres_ffi::relfile_utils::VISIBILITYMAP_FORKNUM;
use postgres_ffi::v14::nonrelfile_utils::{
    mx_offset_to_flags_bitshift, mx_offset_to_flags_offset, mx_offset_to_member_offset,
    transaction_id_set_status,
};
use postgres_ffi::BLCKSZ;

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

impl BufferTag {
    /// Serialized length
    pub const LEN: u32 = RelTag::LEN + 4;
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
/// perform WAL replay. Only one thread can use the process at a time,
/// that is controlled by the Mutex. In the future, we might want to
/// launch a pool of processes to allow concurrent replay of multiple
/// records.
///
pub struct PostgresRedoManager {
    conf: &'static PageServerConf,
    handle: Handle,
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

        // convert it to an arc to avoid cloning it on batches
        let records: Arc<[(Lsn, NeonWalRecord)]> = records.into();

        let mut img: Option<Bytes> = base_img;
        let mut batch_neon = can_apply_in_neon(&records[0].1);
        let mut batch_start = 0;
        for i in 1..records.len() {
            let rec_neon = can_apply_in_neon(&records[i].1);

            if rec_neon != batch_neon {
                let result = if batch_neon {
                    self.apply_batch_neon(key, lsn, img, &records[batch_start..i])
                } else {
                    self.apply_batch_postgres(
                        key,
                        lsn,
                        img,
                        &records,
                        (batch_start..i).into(),
                        self.conf.wal_redo_timeout,
                        pg_version,
                    )
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
            self.apply_batch_postgres(
                key,
                lsn,
                img,
                &records,
                (batch_start..).into(),
                self.conf.wal_redo_timeout,
                pg_version,
            )
        }
    }
}

impl PostgresRedoManager {
    ///
    /// Create a new PostgresRedoManager.
    ///
    pub fn new(conf: &'static PageServerConf, tenant_id: TenantId) -> PostgresRedoManager {
        // The actual process is launched lazily, on first request.

        let h = WALREDO_RUNTIME.handle();
        let (handle, fut) = tokio_postgres_redo(conf, tenant_id, h);
        crate::task_mgr::spawn(
            h,
            crate::task_mgr::TaskKind::WalRedo,
            Some(tenant_id),
            None,
            "walredo",
            true,
            fut,
        );

        PostgresRedoManager { conf, handle }
    }

    ///
    /// Process one request for WAL redo using wal-redo postgres
    ///
    #[allow(clippy::too_many_arguments)]
    fn apply_batch_postgres(
        &self,
        key: Key,
        lsn: Lsn,
        base_img: Option<Bytes>,
        records: &Arc<[(Lsn, NeonWalRecord)]>,
        records_range: SliceRange,
        wal_redo_timeout: Duration,
        pg_version: u32,
    ) -> Result<Bytes, WalRedoError> {
        let (rel, blknum) = key_to_rel_block(key).or(Err(WalRedoError::InvalidRecord))?;

        let start_time = Instant::now();

        // Relational WAL records are applied using wal-redo-postgres
        let buf_tag = BufferTag { rel, blknum };

        let consumed_records = records_range.sub_slice(records);
        let record_count = consumed_records.len() as u64;

        // while walredo processes are async, and execute on separate thread, this is mostly called
        // from within async tasks but as a blocking invocation.
        let result = self
            .handle
            .request_redo(Request {
                target: buf_tag,
                base_img,
                records: records.clone(),
                records_range,
                timeout: wal_redo_timeout,
                pg_version,
                requeues: 0,
            })
            .map_err(|e| WalRedoError::IoError(std::io::Error::new(std::io::ErrorKind::Other, e)));

        let duration = start_time.elapsed();

        let len = record_count;
        let nbytes = consumed_records.iter().fold(0, |acumulator, record| {
            acumulator
                + match &record.1 {
                    NeonWalRecord::Postgres { rec, .. } => rec.len(),
                    _ => unreachable!("Only PostgreSQL records are accepted in this batch"),
                }
        });

        WAL_REDO_TIME.observe(duration.as_secs_f64());
        WAL_REDO_RECORDS_HISTOGRAM.observe(len as f64);
        WAL_REDO_BYTES_HISTOGRAM.observe(nbytes as f64);
        WAL_REDO_RECORD_COUNTER.inc_by(record_count);

        debug!(
            "postgres applied {} WAL records ({} bytes) in {} us to reconstruct page image at LSN {}",
            len,
            nbytes,
            duration.as_micros(),
            lsn
        );

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

/// Command with ability not to give all file descriptors to child process
trait CloseFileDescriptors {
    fn close_fds(&mut self) -> &mut tokio::process::Command;
}

impl CloseFileDescriptors for tokio::process::Command {
    fn close_fds(&mut self) -> &mut tokio::process::Command {
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

fn tokio_postgres_redo(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    _handle: &tokio::runtime::Handle,
) -> (
    Handle,
    impl std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
) {
    // outer mpmc queue size
    let expected_inflight = 4;
    // intermediate queue size, max pipelined. there might be a different kind of balance needed if
    // multiple processes are used. this already allows for 8 waiting request_redo callers, but
    // with 4 processes it might mean that only one of them is actually busy.
    let expected_pipelined = 4;

    let (tx, rx) = flume::bounded::<Payload>(expected_inflight);

    let handle = Handle { tx: tx.clone() };

    let ipc = async move {
        while let Ok(first) = {
            let recv_next = rx.recv_async();
            tokio::pin!(recv_next);

            let watcher = crate::task_mgr::shutdown_watcher();
            tokio::pin!(watcher);

            tokio::select! {
                recv = &mut recv_next => {
                    recv.map_err(|_| "handle dropped while waiting for first")
                },
                _ = &mut watcher => {
                    Err("tenant shutdown while waiting for first item")
                }
            }
        } {
            let child = launch_walredo(conf, tenant_id, first.0.pg_version).await?;
            let watcher = crate::task_mgr::shutdown_watcher();

            let fut = walredo_rpc(
                child,
                tx.clone(),
                rx.clone(),
                tenant_id,
                expected_pipelined,
                Some(first),
                watcher,
            );

            fut.await;
        }

        Ok(())
    };

    (handle, ipc)
}

#[instrument(skip(conf))]
async fn launch_walredo(
    conf: &PageServerConf,
    tenant_id: TenantId,
    pg_version: u32,
) -> anyhow::Result<tokio::process::Child> {
    let datadir = path_with_suffix_extension(
        conf.tenant_path(&tenant_id).join("wal-redo-datadir"),
        TEMP_FILE_SUFFIX,
    );

    match tokio::fs::remove_dir_all(&datadir).await {
        Ok(()) => {
            info!("removed existing data directory: {}", datadir.display());
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        other => other.with_context(|| {
            format!(
                "Failed to cleanup existing wal-redo-datadir at {}",
                datadir.display()
            )
        })?,
    }

    let pg_bin_dir_path = conf.pg_bin_dir(pg_version)?;
    let pg_lib_dir_path = conf.pg_lib_dir(pg_version)?;

    info!("running initdb in {}", datadir.display());

    let initdb = tokio::process::Command::new(pg_bin_dir_path.join("initdb"))
        .arg("-D")
        .arg(&datadir)
        .arg("-N")
        .env_clear()
        .env("LD_LIBRARY_PATH", &pg_lib_dir_path)
        .env("DYLD_LIBRARY_PATH", &pg_lib_dir_path)
        .close_fds()
        .output()
        .await
        .context("Failed to execute initdb for wal-redo")?;

    anyhow::ensure!(
        initdb.status.success(),
        "initdb failed\nstdout: {}\nstderr:\n {}",
        String::from_utf8_lossy(&initdb.stdout),
        String::from_utf8_lossy(&initdb.stderr)
    );

    info!("starting walredo process");

    tokio::process::Command::new(pg_bin_dir_path.join("postgres"))
        .arg("--wal-redo")
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .env_clear()
        .env("LD_LIBRARY_PATH", &pg_lib_dir_path)
        .env("DYLD_LIBRARY_PATH", &pg_lib_dir_path)
        .env("PGDATA", &datadir)
        .close_fds()
        // best effort is probably good enough for us
        .kill_on_drop(true)
        .spawn()
        .context("postgres --wal-redo command failed to start")
}

async fn walredo_rpc<F>(
    mut child: tokio::process::Child,
    work_tx: flume::Sender<Payload>,
    work_rx: flume::Receiver<Payload>,
    tenant_id: TenantId,
    expected_inflight: usize,
    initial: Option<Payload>,
    shutdown: F,
) where
    F: std::future::Future<Output = ()>,
{
    let pid = child
        .id()
        .expect("pid is present before killing the process");

    info!("Launched wal-redo process for {tenant_id}: {pid}");

    // we send the external the request in different commands
    let stdin = child.stdin.take().expect("not taken yet");

    // stdout is used to communicate the resulting page
    let stdout = child.stdout.take().expect("not taken yet");

    #[cfg(not_needed)]
    {
        use std::os::unix::io::AsRawFd;

        nix::fcntl::fcntl(
            stdout.as_raw_fd(),
            nix::fcntl::FcntlArg::F_SETPIPE_SZ(8192 * 4),
        )
        .unwrap();

        nix::fcntl::fcntl(
            stdin.as_raw_fd(),
            nix::fcntl::FcntlArg::F_SETPIPE_SZ(1048576),
        )
        .unwrap();

        let pipe_sizes =
            nix::fcntl::fcntl(stdin.as_raw_fd(), nix::fcntl::FcntlArg::F_GETPIPE_SZ).unwrap();
        let pipe_sizes = (
            pipe_sizes,
            nix::fcntl::fcntl(stdout.as_raw_fd(), nix::fcntl::FcntlArg::F_GETPIPE_SZ).unwrap(),
        );

        debug!(stdin = pipe_sizes.0, stdout = pipe_sizes.1, "pipe sizes");
    }

    // used to communicate hopefully utf-8 log messages
    let stderr = child.stderr.take().expect("not taken yet");

    let (inter_tx, inter_rx) = tokio::sync::mpsc::channel(expected_inflight);

    let stdin_task = stdin_task(stdin, work_tx, work_rx, inter_tx, initial);

    let stdin_task = async {
        tokio::pin!(stdin_task);
        tokio::pin!(shutdown);

        // because stdin and stdout are interconnected with a channel to faciliate
        // pipelining, we don't need to cancel all of the tasks, just the "feeder" task.
        //
        // using tokio::try_join! later we get the wanted "channel close or shutdown takes
        // down all tasks after finishing the in-flight work"
        tokio::select! {
            ret = &mut stdin_task => ret,
            _ = &mut shutdown => {
                // instead of cancelling all of the tasks at once by:
                //
                // Err("tenant shutdown, cancelling walredo work")
                //
                // just "close" channel connecting stdin and stdout tasks so that
                // whatever got through, will be processed.
                //
                // walredo process might end up seeing one more request we will not
                // read the response to, but it's most likely killed at that point
                // anyways.
                Ok(())
            }
        }
    };

    let stdout_task = stdout_task(stdout, inter_rx);

    let stderr_task = stderr_task(stderr);

    async move {
        tokio::pin!(stdin_task);
        tokio::pin!(stdout_task);
        tokio::pin!(stderr_task);

        let mut stdin_alive = true;
        let mut stderr_alive = true;

        loop {
            tokio::select! {
                res = &mut stdin_task, if stdin_alive => {
                    stdin_alive = false;
                    if let Err(e) = res {
                        warn!("stdin task failed: {e:?}");
                        break;
                    }
                    // don't stop polling on stdin task exit, because we might still have pipelined
                    // waiting for stdout task to complete them.
                },
                res = &mut stdout_task => {
                    if let Err(e) = res {
                        warn!("stdout task failed: {e:?}");
                    }
                    // always break after stdout task exits, as stderr never/rarely exits
                    break;
                },
                res = &mut stderr_task, if stderr_alive => {
                    stderr_alive = false;
                    if let Err(e) = res {
                        warn!("stderr task failed: {e:?}");
                        break;
                    }
                },
            };
        }

        // only reason why this would fail is that it has exited already, which is
        // possible by manually killing it outside of pageserver or by it dying while
        // processing.
        let killed = child.start_kill().is_ok();

        match child.wait().await {
            Ok(status) => {
                if !status.success() {
                    if killed {
                        // the status will never be success when we kill, at least on unix
                        debug!(?status, "wal-redo process did not exit successfully as pageserver killed it");
                    } else {
                        // situations like killing it from outside manually
                        warn!(?status, "wal-redo process did not exit successfully but pageserver did not kill it");
                    };
                }
            }
            Err(e) => {
                error!("failed to wait for child process to exit: {e}");
            }
        }
        info!("task exiting");
    }
    .instrument(info_span!("wal-redo", pid, %tenant_id))
    .await
}

/// Payload from stdin task to stdout task for completion carrying the response channel and the
/// deadline for reading from stdout.
type IntermediatePayload = (flume::Sender<anyhow::Result<Bytes>>, tokio::time::Instant);

#[derive(Debug)]
enum StdinTaskError {
    IntermediateChannelClosed,
}

/// Returns a human readable reason for stopping. Channel being closed or shutdown are turned into
/// `Ok(())`.
#[allow(clippy::too_many_arguments)]
#[instrument(name = "stdin", skip_all)]
async fn stdin_task<AW>(
    mut stdin: AW,
    work_tx: flume::Sender<Payload>,
    work_rx: flume::Receiver<Payload>,
    inter_tx: tokio::sync::mpsc::Sender<IntermediatePayload>,
    mut initial: Option<Payload>,
) -> Result<(), StdinTaskError>
where
    AW: tokio::io::AsyncWrite + Unpin,
{
    use futures::future::{poll_fn, TryFutureExt};
    use std::pin::Pin;
    use tokio::io::AsyncWriteExt;
    use tokio_util::io::poll_write_buf;

    let mut buffers = BufQueue::default();

    let have_vectored_stdin = stdin.is_write_vectored();

    let mut scratch = BytesMut::with_capacity(if have_vectored_stdin {
        // without vectoring we aim at 3 messages: begin, page, records + get_page,
        // with vectoring this will be very much enough
        1024 * 16
    } else {
        // without vectoring, use full buffer
        1024 * 64
    });

    loop {
        let ((request, response), reservation) = {
            // when running in a multiprocess configuration, we must acquire the write side before
            // trying to acquire more work from the shared queue, otherwise we'd be stuck holding
            // the global work while we've overcommited this one process.
            let reservation = match inter_tx.reserve().await {
                Ok(r) => r,
                Err(_send_error) => {
                    if let Some(mut initial) = initial {
                        if !initial.0.inc_requeued() {
                            drop(initial.1.send(Err(anyhow::anyhow!(
                                "Failed to start the walredo process too many times"
                            ))));
                        } else {
                            // re-queue (while reordering) the initial work item
                            drop(work_tx.send_async(initial).await);
                        }
                    }

                    return Err(StdinTaskError::IntermediateChannelClosed);
                }
            };

            let next = initial.take();
            let next = if next.is_none() {
                // shutdown is handled outside of this task
                work_rx.recv_async().await.ok()
            } else {
                next
            };

            match next {
                Some(t) => (t, reservation),
                None => {
                    // the Handle or the request queue sender have been dropped; return Ok(()) to keep
                    // processing any of already pipelined requests.
                    //
                    // the drop however does never seem to happen, but most likely the task is
                    // descheduled because of a shutdown_watcher.
                    return Ok(());
                }
            }
        };

        let timeout_at = tokio::time::Instant::now() + request.timeout;

        let records = request.records_range.sub_slice(&request.records);

        if have_vectored_stdin {
            build_vectored_messages(
                request.target,
                request.base_img,
                records,
                &mut scratch,
                &mut buffers,
            );
        } else {
            build_messages(
                request.target,
                request.base_img,
                records,
                &mut scratch,
                &mut buffers,
            );
        }

        let write_res = async {
            while buffers.has_remaining() {
                poll_fn(|cx| poll_write_buf(Pin::new(&mut stdin), cx, &mut buffers))
                    .await
                    .map_err(anyhow::Error::new)?;
            }
            // in general flush is not needed, does nothing on pipes, but since we now accept
            // AsyncWrite
            stdin.flush().await.map_err(anyhow::Error::new)
        };

        let write_res = tokio::time::timeout_at(timeout_at, write_res)
            .map_err(|_| anyhow::anyhow!("write timeout"));

        // wait the write to complete before sending the completion over, because we cannot fail
        // the request after it has been moved. this could be worked around by making the oneshot
        // into a normal mpsc queue of size 2 and making the read side race against the channel
        // closing and timeouted read.
        match write_res.await.and_then(|x| x) {
            Ok(()) => {
                // by writing and then completing later we achieve request pipelining with the
                // walredo process. some workloads fit well into the default 64KB buffer, so we
                // have an opportunity to keep walredo busy by buffering the requests.

                if inter_tx.is_closed() {
                    drop(response.send(Err(anyhow::anyhow!(
                        "Failed to read walredo response: stdout task closed already"
                    ))));
                    return Err(StdinTaskError::IntermediateChannelClosed);
                } else {
                    reservation.send((response, timeout_at));
                }
            }
            Err(io_or_timeout) => {
                // TODO: could we somehow manage to keep the request in case we need to
                // restart the process? see https://github.com/neondatabase/neon/issues/1700
                // we could send it back to the queue at least here, but not so easily when it's
                // inflight.

                if let Err(e) =
                    response.send(Err(io_or_timeout).context("Failed to write request to wal-redo"))
                {
                    let e = e.into_inner().expect_err("just created the value");
                    // log this here, even though we could return an error
                    warn!("stopping due to io or timeout error: {e:#}");
                }
                // we can still continue processing pipelined requests, if any. the
                // stdout task will exit upon seeing we've dropped the result_txs.
                return Ok(());
            }
        }
    }
}

#[derive(Debug)]
enum StdoutTaskError {
    ReadFailed(std::io::Error),
    StdoutClosed,
    ReadTimeout,
}

/// Always eventually fails, returning a developer friendly reason.
#[instrument(name = "stdout", skip_all)]
async fn stdout_task<AR>(
    mut stdout: AR,
    mut inter_rx: tokio::sync::mpsc::Receiver<IntermediatePayload>,
) -> Result<(), StdoutTaskError>
where
    AR: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    async fn read_page<AR>(mut stdout: AR, buffer: &mut Vec<u8>) -> Result<(), StdoutTaskError>
    where
        AR: tokio::io::AsyncRead + Unpin,
    {
        loop {
            let read = stdout
                .read_buf(buffer)
                .await
                .map_err(StdoutTaskError::ReadFailed)?;
            if read == 0 {
                return Err(StdoutTaskError::StdoutClosed);
            }
            if buffer.len() < 8192 {
                continue;
            }
            return Ok(());
        }
    }

    // the extra past one block is for normally reading and clearing out the readyness for the
    // stdout. tokio does not expose an api on ChildStdout for this, so we need to have this
    // "hack". this buffer is reused between reads.
    let mut page_buf = Vec::with_capacity(8192 + 4096);

    loop {
        let (completion, timeout_at) = match inter_rx.recv().await {
            Some(t) => t,
            None => return Ok(()),
        };

        let res = tokio::time::timeout_at(timeout_at, read_page(&mut stdout, &mut page_buf))
            .await
            .map_err(|_elapsed| StdoutTaskError::ReadTimeout)
            .and_then(|x| x);

        match res {
            Ok(()) => {
                let page = Bytes::copy_from_slice(&page_buf[..8192]);
                // we don't care about the result, because the caller could be gone
                drop(completion.into_send_async(Ok(page)).await);
                page_buf.drain(..8192);
            }
            Err(e @ StdoutTaskError::ReadFailed(_)) => {
                drop(completion.send(Err(anyhow::anyhow!(
                    "Failed to read response from wal-redo"
                ))));
                return Err(e);
            }
            Err(e @ StdoutTaskError::StdoutClosed) => {
                drop(completion.send(Err(anyhow::anyhow!(
                    "Failed to read response from wal-redo"
                ))));
                return Err(e);
            }
            Err(e @ StdoutTaskError::ReadTimeout) => {
                drop(completion.send(Err(anyhow::anyhow!(
                    "Timed out while waiting for the page from wal-redo"
                ))));
                return Err(e);
            }
        }
    }
}

#[derive(Debug)]
enum StderrTaskError {
    ReadFailed(std::io::Error),
    Closed,
}

#[instrument(name = "stderr", skip_all)]
async fn stderr_task<AR>(stderr: AR) -> Result<(), StderrTaskError>
where
    AR: tokio::io::AsyncRead,
{
    use tokio::io::AsyncBufReadExt;
    let stderr = tokio::io::BufReader::new(stderr);
    let mut buffer = Vec::new();

    tokio::pin!(stderr);

    loop {
        buffer.clear();
        match stderr.read_until(b'\n', &mut buffer).await {
            Ok(0) => return Err(StderrTaskError::Closed),
            Ok(read) => {
                let message = String::from_utf8_lossy(&buffer[..read]);
                error!("wal-redo-process: {}", message.trim());
            }
            Err(e) => {
                return Err(StderrTaskError::ReadFailed(e));
            }
        }
    }
}

/// Serializes the wal redo request into `buffers` with the help of scratch buffer `scratch`.
///
/// The request is combination of `B + P? + A* + G`.
///
/// Compared to [`build_vectored_messages`], this implementation builds at most 3 messages if the
/// base version of page is included (it's never copied to conserve the "scratch" space).
///
/// All of the messages have structure of `command + len + payload?`, where:
/// - `command` is a single ascii character `[BPAG]`
/// - `len` is payload length + length of len (4)
/// - payload is command specific
fn build_messages(
    target: BufferTag,
    base_img: Option<Bytes>,
    records: &[(Lsn, NeonWalRecord)],
    scratch: &mut BytesMut,
    buffers: &mut BufQueue,
) {
    let need = message_length(
        base_img.is_some(),
        usize::MAX,
        records.iter().map(|(_lsn, r)| match r {
            NeonWalRecord::Postgres { rec, .. } => rec.len(),
            _ => unreachable!(),
        }),
    );

    scratch.reserve(need);

    let tag = {
        target.ser_into(&mut scratch.writer()).unwrap();
        scratch.split().freeze()
    };

    build_begin_message(&tag, scratch);

    if let Some(page) = base_img {
        assert_eq!(page.len(), 8192);
        build_push_page_header(&tag, scratch);

        let out = scratch.split().freeze();

        buffers.push(out);
        buffers.push(page);
    }

    for (end_lsn, record) in records {
        let (_will_init, rec) = match record {
            NeonWalRecord::Postgres { will_init, rec } => (will_init, rec),
            _ => unreachable!(),
        };

        build_apply_record_header(end_lsn, rec.len() as u32, scratch);
        scratch.put(rec.clone());
    }

    build_get_page_message(&tag, scratch);

    let out = scratch.split().freeze();
    buffers.push(out);
}

/// Calculates the buffer space needed for the messages produced out of this request.
#[allow(clippy::identity_op)]
fn message_length<I>(base_img: bool, copy_walrecord_threshold: usize, records: I) -> usize
where
    I: Iterator<Item = usize>,
{
    let tag_len = BufferTag::LEN as usize;

    let begin = 1 + 4 + tag_len;
    let page = if base_img {
        1 + 4 + tag_len + /* let kernel copy page */ 0
    } else {
        0
    };

    let records = records
        .map(|rec_len| {
            1 + 4
                + 8
                + if rec_len < copy_walrecord_threshold {
                    rec_len
                } else {
                    0
                }
        })
        .sum::<usize>();

    let get_page = 1 + 4 + tag_len;

    begin + page + records + get_page
}

/// Compared to [`build_messages`] builds many small messages and aiming for vectored write
/// handling the gathering of the already allocated records.
fn build_vectored_messages(
    target: BufferTag,
    base_img: Option<Bytes>,
    records: &[(Lsn, NeonWalRecord)],
    scratch: &mut BytesMut,
    buffers: &mut BufQueue,
) {
    let copy_walrecord_threshold = 1024;

    let need = message_length(
        base_img.is_some(),
        copy_walrecord_threshold,
        records.iter().map(|(_lsn, r)| match r {
            NeonWalRecord::Postgres { rec, .. } => rec.len(),
            _ => unreachable!(),
        }),
    );

    scratch.reserve(need);

    let tag = {
        target.ser_into(&mut scratch.writer()).unwrap();
        scratch.split().freeze()
    };

    build_begin_message(&tag, scratch);

    if let Some(page) = base_img {
        build_push_page_header(&tag, scratch);
        buffers.push(scratch.split().freeze());
        buffers.push(page);
    }

    for (end_lsn, record) in records {
        let rec = match record {
            NeonWalRecord::Postgres { rec, .. } => rec,
            _ => unreachable!(),
        };

        let record_len = rec.len() as u32;
        build_apply_record_header(end_lsn, record_len, scratch);
        if rec.len() < copy_walrecord_threshold {
            scratch.put(rec.clone());
        } else {
            buffers.push(scratch.split().freeze());
            buffers.push(rec.clone());
        }
    }

    build_get_page_message(&tag, scratch);
    buffers.push(scratch.split().freeze());
}

fn build_begin_message(tag: &Bytes, scratch: &mut BytesMut) {
    scratch.put_u8(b'B');
    scratch.put_u32(4 + BufferTag::LEN);
    scratch.put(tag.clone());
}

fn build_push_page_header(tag: &Bytes, scratch: &mut BytesMut) {
    let page_len = 8192;
    scratch.put_u8(b'P');
    scratch.put_u32(4 + BufferTag::LEN + page_len);
    scratch.put(tag.clone());
}

fn build_apply_record_header(end_lsn: &Lsn, record_len: u32, scratch: &mut BytesMut) {
    scratch.put_u8(b'A');
    scratch.put_u32(4 + 8 + record_len);
    scratch.put_u64(end_lsn.0);
}

fn build_get_page_message(tag: &Bytes, scratch: &mut BytesMut) {
    scratch.put_u8(b'G');
    scratch.put_u32(4 + BufferTag::LEN);
    scratch.put(tag.clone());
}

type Payload = (Request, flume::Sender<anyhow::Result<Bytes>>);

/// WAL Redo request
struct Request {
    target: BufferTag,
    base_img: Option<Bytes>,
    records: Arc<[(Lsn, NeonWalRecord)]>,
    records_range: SliceRange,
    timeout: std::time::Duration,
    pg_version: u32,
    requeues: u8,
}

impl Request {
    /// Returns true, if this should be requeued or false, if too many requeuing have happened
    /// already.
    fn inc_requeued(&mut self) -> bool {
        self.requeues = if let Some(remaining) = self.requeues.checked_add(1) {
            remaining
        } else {
            return false;
        };

        self.requeues < 8
    }
}

#[derive(Clone)]
struct Handle {
    tx: flume::Sender<Payload>,
}

impl Handle {
    fn request_redo(&self, request: Request) -> anyhow::Result<Bytes> {
        let (result_tx, result_rx) = flume::bounded(1);

        self.tx
            .send((request, result_tx))
            .map_err(|_| anyhow::anyhow!("Failed to communicate with the walredo task"))?;

        result_rx
            .recv()
            .context("Failed to get a WAL Redo'd page back")
            .and_then(|x| x)
    }
}

enum SliceRange {
    InclusiveExclusive(std::ops::Range<usize>),
    RangeFrom(std::ops::RangeFrom<usize>),
}

impl From<std::ops::Range<usize>> for SliceRange {
    fn from(r: std::ops::Range<usize>) -> Self {
        SliceRange::InclusiveExclusive(r)
    }
}

impl From<std::ops::RangeFrom<usize>> for SliceRange {
    fn from(r: std::ops::RangeFrom<usize>) -> Self {
        SliceRange::RangeFrom(r)
    }
}

impl SliceRange {
    fn sub_slice<'a, T>(&self, full_slice: &'a [T]) -> &'a [T] {
        match self {
            SliceRange::InclusiveExclusive(r) => &full_slice[r.start..r.end],
            SliceRange::RangeFrom(r) => &full_slice[r.start..],
        }
    }
}

/// Conceptually `Vec<Bytes>` masquerading as `bytes::Buf`.
///
/// Used to build vectorized writes, in case we have a base page.
///
/// Adapted from https://github.com/tokio-rs/bytes/pull/371
struct BufQueue {
    bufs: std::collections::VecDeque<Bytes>,
    remaining: usize,
}

impl Default for BufQueue {
    fn default() -> Self {
        Self {
            bufs: std::collections::VecDeque::with_capacity(4),
            remaining: 0,
        }
    }
}

impl BufQueue {
    fn push(&mut self, buffer: Bytes) {
        let rem = buffer.remaining();
        if rem != 0 {
            self.bufs.push_back(buffer);
            self.remaining = self.remaining.checked_add(rem).expect("remaining overflow");
        }
    }

    #[allow(unused)]
    fn clear(&mut self) {
        self.bufs.clear();
        self.remaining = 0;
    }
}

impl bytes::Buf for BufQueue {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        match self.bufs.front() {
            Some(b) => b.chunk(),
            None => &[],
        }
    }

    fn chunks_vectored<'a>(&'a self, mut dst: &mut [std::io::IoSlice<'a>]) -> usize {
        let mut n = 0;

        for b in &self.bufs {
            if dst.is_empty() {
                break;
            }
            let next = b.chunks_vectored(dst);
            dst = &mut dst[next..];
            n += next;
        }

        n
    }

    fn advance(&mut self, mut cnt: usize) {
        while cnt != 0 {
            let front = self.bufs.front_mut().expect("mut not be empty");
            let rem = front.remaining();
            let advance = std::cmp::min(cnt, rem);
            front.advance(advance);
            if rem == advance {
                self.bufs.pop_front().unwrap();
            }
            cnt -= advance;
            self.remaining -= advance;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PostgresRedoManager, WalRedoManager};
    use crate::repository::Key;
    use crate::{config::PageServerConf, walrecord::NeonWalRecord};
    use bytes::Bytes;
    use std::str::FromStr;
    use utils::{id::TenantId, lsn::Lsn};

    #[test]
    fn short_v14_redo() {
        // prettier than embedding the 8192 bytes here though most of it are zeroes
        // PRE-MERGE: zstd would cut it to 263 bytes, consider in review?
        let expected = std::fs::read("fixtures/short_v14_redo.page").unwrap();

        let h = RedoHarness::new().unwrap();
        let page = h
            .manager
            .request_redo(
                Key {
                    field1: 0,
                    field2: 1663,
                    field3: 13010,
                    field4: 1259,
                    field5: 0,
                    field6: 0,
                },
                Lsn::from_str("0/16E2408").unwrap(),
                None,
                short_records(),
                14,
            )
            .unwrap();

        assert_eq!(&expected, &*page);
    }

    #[test]
    fn short_v14_fails_for_wrong_key_but_returns_zero_page() {
        let h = RedoHarness::new().unwrap();

        let page = h
            .manager
            .request_redo(
                Key {
                    field1: 0,
                    field2: 1663,
                    // key should be 13010
                    field3: 13130,
                    field4: 1259,
                    field5: 0,
                    field6: 0,
                },
                Lsn::from_str("0/16E2408").unwrap(),
                None,
                short_records(),
                14,
            )
            .unwrap();

        // TODO: there will be some stderr printout, which is forwarded to tracing that could
        // perhaps be captured as long as it's in the same thread.
        assert_eq!(page, crate::ZERO_PAGE);
    }

    #[tokio::test]
    async fn shutdown_via_taskmgr() {
        let h = RedoHarness::new().unwrap();

        let _page = h
            .manager
            .request_redo(
                Key {
                    field1: 0,
                    field2: 1663,
                    field3: 13010,
                    field4: 1259,
                    field5: 0,
                    field6: 0,
                },
                Lsn::from_str("0/16E2408").unwrap(),
                None,
                short_records(),
                14,
            )
            .unwrap();

        crate::task_mgr::shutdown_tasks(
            Some(crate::task_mgr::TaskKind::WalRedo),
            Some(h.tenant_id),
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn stdout_reads_two_pages() {
        // stdout reads 8192 + 1 bytes at time, this test makes sure we see all bytes in that case,
        // because cursor can be read for 8193 and 8191 bytes.
        let mut buffer = Vec::with_capacity(8192 * 2);
        buffer.extend(std::iter::repeat(0).take(8192));
        buffer.push(1);
        buffer.extend((2usize..).map(|x| x as u8).take(8192 - 1));
        let buffer = std::io::Cursor::new(buffer);

        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let (ret_tx1, ret_rx1) = flume::bounded(1);
        let (ret_tx2, ret_rx2) = flume::bounded(1);

        tx.send((ret_tx1, bogus_deadline())).await.unwrap();
        tx.send((ret_tx2, bogus_deadline())).await.unwrap();

        // important to drop the sender, since we currently wait for the next result_rx before
        // trying to read (when we would read 0)
        drop(tx);

        super::stdout_task(buffer, rx)
            .await
            .expect("stdout should return ok since all requests were processed");

        let page = ret_rx1.recv_async().await.unwrap().unwrap();
        let page = page.slice(..);
        assert!(page.iter().all(|&x| x == 0));
        assert_eq!(page.len(), 8192);

        let page = ret_rx2.recv_async().await.unwrap().unwrap();
        let page = page.slice(..);
        assert!(page.iter().enumerate().all(|(i, &x)| x == (i + 1) as u8));
        assert_eq!(page.len(), 8192);
    }

    #[tokio::test]
    async fn stdout_task_fails_on_closed_stdout() {
        use super::{stdout_task, StdoutTaskError};

        let buffer = tokio::io::empty();

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let (ret_tx, ret_rx) = flume::bounded(1);

        tx.send((ret_tx, bogus_deadline())).await.unwrap();

        let e = stdout_task(buffer, rx)
            .await
            .expect_err("task should fail because nothing could be read");
        assert!(matches!(e, StdoutTaskError::StdoutClosed), "{e:?}");

        ret_rx
            .recv_async()
            .await
            .unwrap()
            .expect_err("response should also be error");
    }

    fn bogus_deadline() -> tokio::time::Instant {
        tokio::time::Instant::now() + std::time::Duration::from_secs(2)
    }

    #[allow(clippy::octal_escapes)]
    fn short_records() -> Vec<(Lsn, NeonWalRecord)> {
        vec![
            (
                Lsn::from_str("0/16A9388").unwrap(),
                NeonWalRecord::Postgres {
                    will_init: true,
                    rec: Bytes::from_static(b"j\x03\0\0\0\x04\0\0\xe8\x7fj\x01\0\0\0\0\0\n\0\0\xd0\x16\x13Y\0\x10\0\04\x03\xd4\0\x05\x7f\x06\0\0\xd22\0\0\xeb\x04\0\0\0\0\0\0\xff\x03\0\0\0\0\x80\xeca\x01\0\0\x01\0\xd4\0\xa0\x1d\0 \x04 \0\0\0\0/\0\x01\0\xa0\x9dX\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0.\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\00\x9f\x9a\x01P\x9e\xb2\x01\0\x04\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02\0!\0\x01\x08 \xff\xff\xff?\0\0\0\0\0\0@\0\0another_table\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x98\x08\0\0\x02@\0\0\0\0\0\0\n\0\0\0\x02\0\0\0\0@\0\0\0\0\0\0\0\0\0\0\0\0\x80\xbf\0\0\0\0\0\0\0\0\0\0pr\x01\0\0\0\0\0\0\0\0\x01d\0\0\0\0\0\0\x04\0\0\x01\0\0\0\0\0\0\0\x0c\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0/\0!\x80\x03+ \xff\xff\xff\x7f\0\0\0\0\0\xdf\x04\0\0pg_type\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x0b\0\0\0G\0\0\0\0\0\0\0\n\0\0\0\x02\0\0\0\0\0\0\0\0\0\0\0\x0e\0\0\0\0@\x16D\x0e\0\0\0K\x10\0\0\x01\0pr \0\0\0\0\0\0\0\0\x01n\0\0\0\0\0\xd6\x02\0\0\x01\0\0\0[\x01\0\0\0\0\0\0\0\t\x04\0\0\x02\0\0\0\x01\0\0\0\n\0\0\0\n\0\0\0\x7f\0\0\0\0\0\0\0\n\0\0\0\x02\0\0\0\0\0\0C\x01\0\0\x15\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0.\0!\x80\x03+ \xff\xff\xff\x7f\0\0\0\0\0;\n\0\0pg_statistic\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x0b\0\0\0\xfd.\0\0\0\0\0\0\n\0\0\0\x02\0\0\0;\n\0\0\0\0\0\0\x13\0\0\0\0\0\xcbC\x13\0\0\0\x18\x0b\0\0\x01\0pr\x1f\0\0\0\0\0\0\0\0\x01n\0\0\0\0\0\xd6\x02\0\0\x01\0\0\0C\x01\0\0\0\0\0\0\0\t\x04\0\0\x01\0\0\0\x01\0\0\0\n\0\0\0\n\0\0\0\x7f\0\0\0\0\0\0\x02\0\x01")
                }
            ),
            (
                Lsn::from_str("0/16D4080").unwrap(),
                NeonWalRecord::Postgres {
                    will_init: false,
                    rec: Bytes::from_static(b"\xbc\0\0\0\0\0\0\0h?m\x01\0\0\0\0p\n\0\09\x08\xa3\xea\0 \x8c\0\x7f\x06\0\0\xd22\0\0\xeb\x04\0\0\0\0\0\0\xff\x02\0@\0\0another_table\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x98\x08\0\0\x02@\0\0\0\0\0\0\n\0\0\0\x02\0\0\0\0@\0\0\0\0\0\0\x05\0\0\0\0@zD\x05\0\0\0\0\0\0\0\0\0pr\x01\0\0\0\0\0\0\0\0\x01d\0\0\0\0\0\0\x04\0\0\x01\0\0\0\x02\0")
                }
            )
        ]
    }

    struct RedoHarness {
        // underscored because unused, except for removal at drop
        _repo_dir: tempfile::TempDir,
        manager: PostgresRedoManager,
        tenant_id: TenantId,
        // FIXME: this needs a own tokio reactor to use the same api
    }

    impl RedoHarness {
        fn new() -> anyhow::Result<Self> {
            let repo_dir = tempfile::tempdir()?;
            let conf = PageServerConf::dummy_conf(repo_dir.path().to_path_buf());
            let conf = Box::leak(Box::new(conf));
            let tenant_id = TenantId::generate();

            let manager = PostgresRedoManager::new(conf, tenant_id);

            Ok(RedoHarness {
                _repo_dir: repo_dir,
                manager,
                tenant_id,
            })
        }
    }
}
