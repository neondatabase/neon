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
use bytes::{Bytes, BytesMut};

use pageserver_api::models::WalRedoManagerStatus;
use pageserver_api::shard::TenantShardId;

use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::time::Instant;
use tracing::*;
use utils::lsn::Lsn;

#[cfg(feature = "testing")]
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::config::PageServerConf;
use crate::metrics::{
    WAL_REDO_BYTES_HISTOGRAM, WAL_REDO_PROCESS_LAUNCH_DURATION_HISTOGRAM,
    WAL_REDO_RECORDS_HISTOGRAM, WAL_REDO_TIME,
};
use crate::repository::Key;
use crate::walrecord::NeonWalRecord;

use pageserver_api::key::{key_to_rel_block, key_to_slru_block};
use pageserver_api::reltag::SlruKind;
use postgres_ffi::pg_constants;
use postgres_ffi::relfile_utils::VISIBILITYMAP_FORKNUM;
use postgres_ffi::v14::nonrelfile_utils::{
    mx_offset_to_flags_bitshift, mx_offset_to_flags_offset, mx_offset_to_member_offset,
    transaction_id_set_status,
};
use postgres_ffi::BLCKSZ;

mod process;
mod protocol;
use process::WalRedoProcess;

///
/// This is the real implementation that uses a Postgres process to
/// perform WAL replay. Only one thread can use the process at a time,
/// that is controlled by the Mutex. In the future, we might want to
/// launch a pool of processes to allow concurrent replay of multiple
/// records.
///
pub struct PostgresRedoManager {
    tenant_shard_id: TenantShardId,
    conf: &'static PageServerConf,
    last_redo_at: std::sync::Mutex<Option<Instant>>,
    redo_process: RwLock<Option<Arc<WalRedoProcess>>>,
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

///
/// Public interface of WAL redo manager
///
impl PostgresRedoManager {
    ///
    /// Request the WAL redo manager to apply some WAL records
    ///
    /// The WAL redo is handled by a separate thread, so this just sends a request
    /// to the thread and waits for response.
    ///
    /// # Cancel-Safety
    ///
    /// This method is cancellation-safe.
    pub async fn request_redo(
        &self,
        key: Key,
        lsn: Lsn,
        base_img: Option<(Lsn, Bytes)>,
        records: Vec<(Lsn, NeonWalRecord)>,
        pg_version: u32,
    ) -> anyhow::Result<Bytes> {
        if records.is_empty() {
            anyhow::bail!("invalid WAL redo request with no records");
        }

        let base_img_lsn = base_img.as_ref().map(|p| p.0).unwrap_or(Lsn::INVALID);
        let mut img = base_img.map(|p| p.1);
        let mut batch_neon = can_apply_in_neon(&records[0].1);
        let mut batch_start = 0;
        for (i, record) in records.iter().enumerate().skip(1) {
            let rec_neon = can_apply_in_neon(&record.1);

            if rec_neon != batch_neon {
                let result = if batch_neon {
                    self.apply_batch_neon(key, lsn, img, &records[batch_start..i])
                } else {
                    self.apply_batch_postgres(
                        key,
                        lsn,
                        img,
                        base_img_lsn,
                        &records[batch_start..i],
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
                base_img_lsn,
                &records[batch_start..],
                self.conf.wal_redo_timeout,
                pg_version,
            )
        }
    }

    pub(crate) fn status(&self) -> Option<WalRedoManagerStatus> {
        Some(WalRedoManagerStatus {
            last_redo_at: {
                let at = *self.last_redo_at.lock().unwrap();
                at.and_then(|at| {
                    let age = at.elapsed();
                    // map any chrono errors silently to None here
                    chrono::Utc::now().checked_sub_signed(chrono::Duration::from_std(age).ok()?)
                })
            },
            pid: self.redo_process.read().unwrap().as_ref().map(|p| p.id()),
        })
    }
}

impl PostgresRedoManager {
    ///
    /// Create a new PostgresRedoManager.
    ///
    pub fn new(
        conf: &'static PageServerConf,
        tenant_shard_id: TenantShardId,
    ) -> PostgresRedoManager {
        // The actual process is launched lazily, on first request.
        PostgresRedoManager {
            tenant_shard_id,
            conf,
            last_redo_at: std::sync::Mutex::default(),
            redo_process: RwLock::new(None),
        }
    }

    /// This type doesn't have its own background task to check for idleness: we
    /// rely on our owner calling this function periodically in its own housekeeping
    /// loops.
    pub(crate) fn maybe_quiesce(&self, idle_timeout: Duration) {
        if let Ok(g) = self.last_redo_at.try_lock() {
            if let Some(last_redo_at) = *g {
                if last_redo_at.elapsed() >= idle_timeout {
                    drop(g);
                    let mut guard = self.redo_process.write().unwrap();
                    *guard = None;
                }
            }
        }
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
        base_img_lsn: Lsn,
        records: &[(Lsn, NeonWalRecord)],
        wal_redo_timeout: Duration,
        pg_version: u32,
    ) -> anyhow::Result<Bytes> {
        *(self.last_redo_at.lock().unwrap()) = Some(Instant::now());

        let (rel, blknum) = key_to_rel_block(key).context("invalid record")?;
        const MAX_RETRY_ATTEMPTS: u32 = 1;
        let mut n_attempts = 0u32;
        loop {
            // launch the WAL redo process on first use
            let proc: Arc<WalRedoProcess> = {
                let proc_guard = self.redo_process.read().unwrap();
                match &*proc_guard {
                    None => {
                        // "upgrade" to write lock to launch the process
                        drop(proc_guard);
                        let mut proc_guard = self.redo_process.write().unwrap();
                        match &*proc_guard {
                            None => {
                                let start = Instant::now();
                                let proc = Arc::new(
                                    WalRedoProcess::launch(
                                        self.conf,
                                        self.tenant_shard_id,
                                        pg_version,
                                    )
                                    .context("launch walredo process")?,
                                );
                                let duration = start.elapsed();
                                WAL_REDO_PROCESS_LAUNCH_DURATION_HISTOGRAM
                                    .observe(duration.as_secs_f64());
                                info!(
                                    duration_ms = duration.as_millis(),
                                    pid = proc.id(),
                                    "launched walredo process"
                                );
                                *proc_guard = Some(Arc::clone(&proc));
                                proc
                            }
                            Some(proc) => Arc::clone(proc),
                        }
                    }
                    Some(proc) => Arc::clone(proc),
                }
            };

            let started_at = std::time::Instant::now();

            // Relational WAL records are applied using wal-redo-postgres
            let buf_tag = protocol::BufferTag { rel, blknum };
            let result = proc
                .apply_wal_records(buf_tag, &base_img, records, wal_redo_timeout)
                .context("apply_wal_records");

            let duration = started_at.elapsed();

            let len = records.len();
            let nbytes = records.iter().fold(0, |acumulator, record| {
                acumulator
                    + match &record.1 {
                        NeonWalRecord::Postgres { rec, .. } => rec.len(),
                        _ => unreachable!("Only PostgreSQL records are accepted in this batch"),
                    }
            });

            WAL_REDO_TIME.observe(duration.as_secs_f64());
            WAL_REDO_RECORDS_HISTOGRAM.observe(len as f64);
            WAL_REDO_BYTES_HISTOGRAM.observe(nbytes as f64);

            debug!(
                "postgres applied {} WAL records ({} bytes) in {} us to reconstruct page image at LSN {}",
                len,
                nbytes,
                duration.as_micros(),
                lsn
            );

            // If something went wrong, don't try to reuse the process. Kill it, and
            // next request will launch a new one.
            if let Err(e) = result.as_ref() {
                error!(
                    "error applying {} WAL records {}..{} ({} bytes) to base image with LSN {} to reconstruct page image at LSN {} n_attempts={}: {:?}",
                    records.len(),
                    records.first().map(|p| p.0).unwrap_or(Lsn(0)),
                    records.last().map(|p| p.0).unwrap_or(Lsn(0)),
                    nbytes,
                    base_img_lsn,
                    lsn,
                    n_attempts,
                    e,
                );
                // Avoid concurrent callers hitting the same issue.
                // We can't prevent it from happening because we want to enable parallelism.
                {
                    let mut guard = self.redo_process.write().unwrap();
                    match &*guard {
                        Some(current_field_value) => {
                            if Arc::ptr_eq(current_field_value, &proc) {
                                // We're the first to observe an error from `proc`, it's our job to take it out of rotation.
                                *guard = None;
                            }
                        }
                        None => {
                            // Another thread was faster to observe the error, and already took the process out of rotation.
                        }
                    }
                }
                // NB: there may still be other concurrent threads using `proc`.
                // The last one will send SIGKILL when the underlying Arc reaches refcount 0.
                // NB: it's important to drop(proc) after drop(guard). Otherwise we'd keep
                // holding the lock while waiting for the process to exit.
                // NB: the drop impl blocks the current threads with a wait() system call for
                // the child process. We dropped the `guard` above so that other threads aren't
                // affected. But, it's good that the current thread _does_ block to wait.
                // If we instead deferred the waiting into the background / to tokio, it could
                // happen that if walredo always fails immediately, we spawn processes faster
                // than we can SIGKILL & `wait` for them to exit. By doing it the way we do here,
                // we limit this risk of run-away to at most $num_runtimes * $num_executor_threads.
                // This probably needs revisiting at some later point.
                drop(proc);
            } else if n_attempts != 0 {
                info!(n_attempts, "retried walredo succeeded");
            }
            n_attempts += 1;
            if n_attempts > MAX_RETRY_ATTEMPTS || result.is_ok() {
                return result;
            }
        }
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
    ) -> anyhow::Result<Bytes> {
        let start_time = Instant::now();

        let mut page = BytesMut::new();
        if let Some(fpi) = base_img {
            // If full-page image is provided, then use it...
            page.extend_from_slice(&fpi[..]);
        } else {
            // All the current WAL record types that we can handle require a base image.
            anyhow::bail!("invalid neon WAL redo request with no base image");
        }

        // Apply all the WAL records in the batch
        for (record_lsn, record) in records.iter() {
            self.apply_record_neon(key, &mut page, *record_lsn, record)?;
        }
        // Success!
        let duration = start_time.elapsed();
        // FIXME: using the same metric here creates a bimodal distribution by default, and because
        // there could be multiple batch sizes this would be N+1 modal.
        WAL_REDO_TIME.observe(duration.as_secs_f64());

        debug!(
            "neon applied {} WAL records in {} us to reconstruct page image at LSN {}",
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
    ) -> anyhow::Result<()> {
        match record {
            NeonWalRecord::Postgres {
                will_init: _,
                rec: _,
            } => {
                anyhow::bail!("tried to pass postgres wal record to neon WAL redo");
            }
            NeonWalRecord::ClearVisibilityMapFlags {
                new_heap_blkno,
                old_heap_blkno,
                flags,
            } => {
                // sanity check that this is modifying the correct relation
                let (rel, blknum) = key_to_rel_block(key).context("invalid record")?;
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
                    key_to_slru_block(key).context("invalid record")?;
                assert_eq!(
                    slru_kind,
                    SlruKind::Clog,
                    "ClogSetCommitted record with unexpected key {}",
                    key
                );
                for &xid in xids {
                    let pageno = xid / pg_constants::CLOG_XACTS_PER_PAGE;
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
                    key_to_slru_block(key).context("invalid record")?;
                assert_eq!(
                    slru_kind,
                    SlruKind::Clog,
                    "ClogSetAborted record with unexpected key {}",
                    key
                );
                for &xid in xids {
                    let pageno = xid / pg_constants::CLOG_XACTS_PER_PAGE;
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
                    key_to_slru_block(key).context("invalid record")?;
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
                    key_to_slru_block(key).context("invalid record")?;
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

#[cfg(test)]
mod tests {
    use super::PostgresRedoManager;
    use crate::repository::Key;
    use crate::{config::PageServerConf, walrecord::NeonWalRecord};
    use bytes::Bytes;
    use pageserver_api::shard::TenantShardId;
    use std::str::FromStr;
    use utils::{id::TenantId, lsn::Lsn};

    #[tokio::test]
    async fn short_v14_redo() {
        let expected = std::fs::read("test_data/short_v14_redo.page").unwrap();

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
            .await
            .unwrap();

        assert_eq!(&expected, &*page);
    }

    #[tokio::test]
    async fn short_v14_fails_for_wrong_key_but_returns_zero_page() {
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
            .await
            .unwrap();

        // TODO: there will be some stderr printout, which is forwarded to tracing that could
        // perhaps be captured as long as it's in the same thread.
        assert_eq!(page, crate::ZERO_PAGE);
    }

    #[tokio::test]
    async fn test_stderr() {
        let h = RedoHarness::new().unwrap();
        h
            .manager
            .request_redo(
                Key::from_i128(0),
                Lsn::INVALID,
                None,
                short_records(),
                16, /* 16 currently produces stderr output on startup, which adds a nice extra edge */
            )
            .await
            .unwrap_err();
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
        _repo_dir: camino_tempfile::Utf8TempDir,
        manager: PostgresRedoManager,
    }

    impl RedoHarness {
        fn new() -> anyhow::Result<Self> {
            crate::tenant::harness::setup_logging();

            let repo_dir = camino_tempfile::tempdir()?;
            let conf = PageServerConf::dummy_conf(repo_dir.path().to_path_buf());
            let conf = Box::leak(Box::new(conf));
            let tenant_shard_id = TenantShardId::unsharded(TenantId::generate());

            let manager = PostgresRedoManager::new(conf, tenant_shard_id);

            Ok(RedoHarness {
                _repo_dir: repo_dir,
                manager,
            })
        }
    }
}
