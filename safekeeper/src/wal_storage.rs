//! This module has everything to deal with WAL -- reading and writing to disk.
//!
//! Safekeeper WAL is stored in the timeline directory, in format similar to pg_wal.
//! PG timeline is always 1, so WAL segments are usually have names like this:
//! - 000000010000000000000001
//! - 000000010000000000000002.partial
//!
//! Note that last file has `.partial` suffix, that's different from postgres.

use anyhow::{anyhow, bail, Context, Result};
use std::io::{self, Seek, SeekFrom};
use std::pin::Pin;
use tokio::io::AsyncRead;

use lazy_static::lazy_static;
use postgres_ffi::xlog_utils::{
    find_end_of_wal, IsPartialXLogFileName, IsXLogFileName, XLogFromFileName, XLogSegNo, PG_TLI,
};
use std::cmp::min;

use std::fs::{self, remove_file, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use tracing::*;

use utils::{lsn::Lsn, zid::ZTenantTimelineId};

use crate::safekeeper::SafeKeeperState;

use crate::wal_backup::read_object;
use crate::SafeKeeperConf;
use postgres_ffi::xlog_utils::{XLogFileName, XLOG_BLCKSZ};

use postgres_ffi::waldecoder::WalStreamDecoder;

use metrics::{register_histogram_vec, Histogram, HistogramVec, DISK_WRITE_SECONDS_BUCKETS};

use tokio::io::{AsyncReadExt, AsyncSeekExt};

lazy_static! {
    // The prometheus crate does not support u64 yet, i64 only (see `IntGauge`).
    // i64 is faster than f64, so update to u64 when available.
    static ref WRITE_WAL_BYTES: HistogramVec = register_histogram_vec!(
        "safekeeper_write_wal_bytes",
        "Bytes written to WAL in a single request, grouped by timeline",
        &["tenant_id", "timeline_id"],
        vec![1.0, 10.0, 100.0, 1024.0, 8192.0, 128.0 * 1024.0, 1024.0 * 1024.0, 10.0 * 1024.0 * 1024.0]
    )
    .expect("Failed to register safekeeper_write_wal_bytes histogram vec");
    static ref WRITE_WAL_SECONDS: HistogramVec = register_histogram_vec!(
        "safekeeper_write_wal_seconds",
        "Seconds spent writing and syncing WAL to a disk in a single request, grouped by timeline",
        &["tenant_id", "timeline_id"],
        DISK_WRITE_SECONDS_BUCKETS.to_vec()
    )
    .expect("Failed to register safekeeper_write_wal_seconds histogram vec");
    static ref FLUSH_WAL_SECONDS: HistogramVec = register_histogram_vec!(
        "safekeeper_flush_wal_seconds",
        "Seconds spent syncing WAL to a disk, grouped by timeline",
        &["tenant_id", "timeline_id"],
        DISK_WRITE_SECONDS_BUCKETS.to_vec()
    )
    .expect("Failed to register safekeeper_flush_wal_seconds histogram vec");
}

struct WalStorageMetrics {
    write_wal_bytes: Histogram,
    write_wal_seconds: Histogram,
    flush_wal_seconds: Histogram,
}

impl WalStorageMetrics {
    fn new(zttid: &ZTenantTimelineId) -> Self {
        let tenant_id = zttid.tenant_id.to_string();
        let timeline_id = zttid.timeline_id.to_string();
        Self {
            write_wal_bytes: WRITE_WAL_BYTES.with_label_values(&[&tenant_id, &timeline_id]),
            write_wal_seconds: WRITE_WAL_SECONDS.with_label_values(&[&tenant_id, &timeline_id]),
            flush_wal_seconds: FLUSH_WAL_SECONDS.with_label_values(&[&tenant_id, &timeline_id]),
        }
    }
}

pub trait Storage {
    /// LSN of last durably stored WAL record.
    fn flush_lsn(&self) -> Lsn;

    /// Init storage with wal_seg_size and read WAL from disk to get latest LSN.
    fn init_storage(&mut self, state: &SafeKeeperState) -> Result<()>;

    /// Write piece of WAL from buf to disk, but not necessarily sync it.
    fn write_wal(&mut self, startpos: Lsn, buf: &[u8]) -> Result<()>;

    /// Truncate WAL at specified LSN, which must be the end of WAL record.
    fn truncate_wal(&mut self, end_pos: Lsn) -> Result<()>;

    /// Durably store WAL on disk, up to the last written WAL record.
    fn flush_wal(&mut self) -> Result<()>;

    /// Remove all segments <= given segno. Returns closure as we want to do
    /// that without timeline lock.
    fn remove_up_to(&self) -> Box<dyn Fn(XLogSegNo) -> Result<()>>;
}

/// PhysicalStorage is a storage that stores WAL on disk. Writes are separated from flushes
/// for better performance. Storage must be initialized before use.
///
/// WAL is stored in segments, each segment is a file. Last segment has ".partial" suffix in
/// its filename and may be not fully flushed.
///
/// Relationship of LSNs:
/// `write_lsn` >= `write_record_lsn` >= `flush_record_lsn`
///
/// When storage is just created, all LSNs are zeroes and there are no segments on disk.
pub struct PhysicalStorage {
    metrics: WalStorageMetrics,
    zttid: ZTenantTimelineId,
    timeline_dir: PathBuf,
    conf: SafeKeeperConf,

    // fields below are filled upon initialization
    /// None if uninitialized, Some(usize) if storage is initialized.
    wal_seg_size: Option<usize>,

    /// Written to disk, but possibly still in the cache and not fully persisted.
    /// Also can be ahead of record_lsn, if happen to be in the middle of a WAL record.
    write_lsn: Lsn,

    /// The LSN of the last WAL record written to disk. Still can be not fully flushed.
    write_record_lsn: Lsn,

    /// The LSN of the last WAL record flushed to disk.
    flush_record_lsn: Lsn,

    /// Decoder is required for detecting boundaries of WAL records.
    decoder: WalStreamDecoder,

    /// Cached open file for the last segment.
    ///
    /// If Some(file) is open, then it always:
    /// - has ".partial" suffix
    /// - points to write_lsn, so no seek is needed for writing
    /// - doesn't point to the end of the segment
    file: Option<File>,
}

impl PhysicalStorage {
    pub fn new(zttid: &ZTenantTimelineId, conf: &SafeKeeperConf) -> PhysicalStorage {
        let timeline_dir = conf.timeline_dir(zttid);
        PhysicalStorage {
            metrics: WalStorageMetrics::new(zttid),
            zttid: *zttid,
            timeline_dir,
            conf: conf.clone(),
            wal_seg_size: None,
            write_lsn: Lsn(0),
            write_record_lsn: Lsn(0),
            flush_record_lsn: Lsn(0),
            decoder: WalStreamDecoder::new(Lsn(0)),
            file: None,
        }
    }

    /// Wrapper for flush_lsn updates that also updates metrics.
    fn update_flush_lsn(&mut self) {
        self.flush_record_lsn = self.write_record_lsn;
    }

    /// Call fdatasync if config requires so.
    fn fdatasync_file(&self, file: &mut File) -> Result<()> {
        if !self.conf.no_sync {
            self.metrics
                .flush_wal_seconds
                .observe_closure_duration(|| file.sync_data())?;
        }
        Ok(())
    }

    /// Call fsync if config requires so.
    fn fsync_file(&self, file: &mut File) -> Result<()> {
        if !self.conf.no_sync {
            self.metrics
                .flush_wal_seconds
                .observe_closure_duration(|| file.sync_all())?;
        }
        Ok(())
    }

    /// Open or create WAL segment file. Caller must call seek to the wanted position.
    /// Returns `file` and `is_partial`.
    fn open_or_create(&self, segno: XLogSegNo, wal_seg_size: usize) -> Result<(File, bool)> {
        let (wal_file_path, wal_file_partial_path) =
            wal_file_paths(&self.timeline_dir, segno, wal_seg_size)?;

        // Try to open already completed segment
        if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_path) {
            Ok((file, false))
        } else if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_partial_path) {
            // Try to open existing partial file
            Ok((file, true))
        } else {
            // Create and fill new partial file
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&wal_file_partial_path)
                .with_context(|| format!("Failed to open log file {:?}", &wal_file_path))?;

            write_zeroes(&mut file, wal_seg_size)?;
            self.fsync_file(&mut file)?;
            Ok((file, true))
        }
    }

    /// Write WAL bytes, which are known to be located in a single WAL segment.
    fn write_in_segment(
        &mut self,
        segno: u64,
        xlogoff: usize,
        buf: &[u8],
        wal_seg_size: usize,
    ) -> Result<()> {
        let mut file = if let Some(file) = self.file.take() {
            file
        } else {
            let (mut file, is_partial) = self.open_or_create(segno, wal_seg_size)?;
            assert!(is_partial, "unexpected write into non-partial segment file");
            file.seek(SeekFrom::Start(xlogoff as u64))?;
            file
        };

        file.write_all(buf)?;

        if xlogoff + buf.len() == wal_seg_size {
            // If we reached the end of a WAL segment, flush and close it.
            self.fdatasync_file(&mut file)?;

            // Rename partial file to completed file
            let (wal_file_path, wal_file_partial_path) =
                wal_file_paths(&self.timeline_dir, segno, wal_seg_size)?;
            fs::rename(&wal_file_partial_path, &wal_file_path)?;
        } else {
            // otherwise, file can be reused later
            self.file = Some(file);
        }

        Ok(())
    }

    /// Writes WAL to the segment files, until everything is writed. If some segments
    /// are fully written, they are flushed to disk. The last (partial) segment can
    /// be flushed separately later.
    ///
    /// Updates `write_lsn`.
    fn write_exact(&mut self, pos: Lsn, mut buf: &[u8]) -> Result<()> {
        let wal_seg_size = self
            .wal_seg_size
            .ok_or_else(|| anyhow!("wal_seg_size is not initialized"))?;

        if self.write_lsn != pos {
            // need to flush the file before discarding it
            if let Some(mut file) = self.file.take() {
                self.fdatasync_file(&mut file)?;
            }

            self.write_lsn = pos;
        }

        while !buf.is_empty() {
            // Extract WAL location for this block
            let xlogoff = self.write_lsn.segment_offset(wal_seg_size) as usize;
            let segno = self.write_lsn.segment_number(wal_seg_size);

            // If crossing a WAL boundary, only write up until we reach wal segment size.
            let bytes_write = if xlogoff + buf.len() > wal_seg_size {
                wal_seg_size - xlogoff
            } else {
                buf.len()
            };

            self.write_in_segment(segno, xlogoff, &buf[..bytes_write], wal_seg_size)?;
            self.write_lsn += bytes_write as u64;
            buf = &buf[bytes_write..];
        }

        Ok(())
    }
}

impl Storage for PhysicalStorage {
    /// flush_lsn returns LSN of last durably stored WAL record.
    fn flush_lsn(&self) -> Lsn {
        self.flush_record_lsn
    }

    /// Storage needs to know wal_seg_size to know which segment to read/write, but
    /// wal_seg_size is not always known at the moment of storage creation. This method
    /// allows to postpone its initialization.
    fn init_storage(&mut self, state: &SafeKeeperState) -> Result<()> {
        if state.server.wal_seg_size == 0 {
            // wal_seg_size is still unknown. This is dead path normally, should
            // be used only in tests.
            return Ok(());
        }

        if let Some(wal_seg_size) = self.wal_seg_size {
            // physical storage is already initialized
            assert_eq!(wal_seg_size, state.server.wal_seg_size as usize);
            return Ok(());
        }

        // initialize physical storage
        let wal_seg_size = state.server.wal_seg_size as usize;
        self.wal_seg_size = Some(wal_seg_size);

        // Find out where stored WAL ends, starting at commit_lsn which is a
        // known recent record boundary (unless we don't have WAL at all).
        self.write_lsn = if state.commit_lsn == Lsn(0) {
            Lsn(0)
        } else {
            Lsn(find_end_of_wal(&self.timeline_dir, wal_seg_size, true, state.commit_lsn)?.0)
        };

        self.write_record_lsn = self.write_lsn;

        // TODO: do we really know that write_lsn is fully flushed to disk?
        //      If not, maybe it's better to call fsync() here to be sure?
        self.update_flush_lsn();

        info!(
            "initialized storage for timeline {}, flush_lsn={}, commit_lsn={}, peer_horizon_lsn={}",
            self.zttid.timeline_id, self.flush_record_lsn, state.commit_lsn, state.peer_horizon_lsn,
        );
        if self.flush_record_lsn < state.commit_lsn
            || self.flush_record_lsn < state.peer_horizon_lsn
        {
            warn!("timeline {} potential data loss: flush_lsn by find_end_of_wal is less than either commit_lsn or peer_horizon_lsn from control file", self.zttid.timeline_id);
        }

        Ok(())
    }

    /// Write WAL to disk.
    fn write_wal(&mut self, startpos: Lsn, buf: &[u8]) -> Result<()> {
        // Disallow any non-sequential writes, which can result in gaps or overwrites.
        // If we need to move the pointer, use truncate_wal() instead.
        if self.write_lsn > startpos {
            bail!(
                "write_wal rewrites WAL written before, write_lsn={}, startpos={}",
                self.write_lsn,
                startpos
            );
        }
        if self.write_lsn < startpos && self.write_lsn != Lsn(0) {
            bail!(
                "write_wal creates gap in written WAL, write_lsn={}, startpos={}",
                self.write_lsn,
                startpos
            );
        }

        {
            let _timer = self.metrics.write_wal_seconds.start_timer();
            self.write_exact(startpos, buf)?;
        }

        // WAL is written, updating write metrics
        self.metrics.write_wal_bytes.observe(buf.len() as f64);

        // figure out last record's end lsn for reporting (if we got the
        // whole record)
        if self.decoder.available() != startpos {
            info!(
                "restart decoder from {} to {}",
                self.decoder.available(),
                startpos,
            );
            self.decoder = WalStreamDecoder::new(startpos);
        }
        self.decoder.feed_bytes(buf);
        loop {
            match self.decoder.poll_decode()? {
                None => break, // no full record yet
                Some((lsn, _rec)) => {
                    self.write_record_lsn = lsn;
                }
            }
        }

        Ok(())
    }

    fn flush_wal(&mut self) -> Result<()> {
        if self.flush_record_lsn == self.write_record_lsn {
            // no need to do extra flush
            return Ok(());
        }

        if let Some(mut unflushed_file) = self.file.take() {
            self.fdatasync_file(&mut unflushed_file)?;
            self.file = Some(unflushed_file);
        } else {
            // We have unflushed data (write_lsn != flush_lsn), but no file.
            // This should only happen if last file was fully written and flushed,
            // but haven't updated flush_lsn yet.
            assert!(self.write_lsn.segment_offset(self.wal_seg_size.unwrap()) == 0);
        }

        // everything is flushed now, let's update flush_lsn
        self.update_flush_lsn();
        Ok(())
    }

    /// Truncate written WAL by removing all WAL segments after the given LSN.
    /// end_pos must point to the end of the WAL record.
    fn truncate_wal(&mut self, end_pos: Lsn) -> Result<()> {
        let wal_seg_size = self
            .wal_seg_size
            .ok_or_else(|| anyhow!("wal_seg_size is not initialized"))?;

        // Streaming must not create a hole, so truncate cannot be called on non-written lsn
        assert!(self.write_lsn == Lsn(0) || self.write_lsn >= end_pos);

        // Close previously opened file, if any
        if let Some(mut unflushed_file) = self.file.take() {
            self.fdatasync_file(&mut unflushed_file)?;
        }

        let xlogoff = end_pos.segment_offset(wal_seg_size) as usize;
        let segno = end_pos.segment_number(wal_seg_size);
        let (mut file, is_partial) = self.open_or_create(segno, wal_seg_size)?;

        // Fill end with zeroes
        file.seek(SeekFrom::Start(xlogoff as u64))?;
        write_zeroes(&mut file, wal_seg_size - xlogoff)?;
        self.fdatasync_file(&mut file)?;

        if !is_partial {
            // Make segment partial once again
            let (wal_file_path, wal_file_partial_path) =
                wal_file_paths(&self.timeline_dir, segno, wal_seg_size)?;
            fs::rename(&wal_file_path, &wal_file_partial_path)?;
        }

        // Remove all subsequent segments
        let mut segno = segno;
        loop {
            segno += 1;
            let (wal_file_path, wal_file_partial_path) =
                wal_file_paths(&self.timeline_dir, segno, wal_seg_size)?;
            // TODO: better use fs::try_exists which is currently available only in nightly build
            if wal_file_path.exists() {
                fs::remove_file(&wal_file_path)?;
            } else if wal_file_partial_path.exists() {
                fs::remove_file(&wal_file_partial_path)?;
            } else {
                break;
            }
        }

        // Update LSNs
        self.write_lsn = end_pos;
        self.write_record_lsn = end_pos;
        self.update_flush_lsn();
        Ok(())
    }

    fn remove_up_to(&self) -> Box<dyn Fn(XLogSegNo) -> Result<()>> {
        let timeline_dir = self.timeline_dir.clone();
        let wal_seg_size = self.wal_seg_size.unwrap();
        Box::new(move |segno_up_to: XLogSegNo| {
            remove_up_to(&timeline_dir, wal_seg_size, segno_up_to)
        })
    }
}

/// Remove all WAL segments in timeline_dir <= given segno.
fn remove_up_to(timeline_dir: &Path, wal_seg_size: usize, segno_up_to: XLogSegNo) -> Result<()> {
    let mut n_removed = 0;
    for entry in fs::read_dir(&timeline_dir)? {
        let entry = entry?;
        let entry_path = entry.path();
        let fname = entry_path.file_name().unwrap();

        if let Some(fname_str) = fname.to_str() {
            /* Ignore files that are not XLOG segments */
            if !IsXLogFileName(fname_str) && !IsPartialXLogFileName(fname_str) {
                continue;
            }
            let (segno, _) = XLogFromFileName(fname_str, wal_seg_size);
            if segno <= segno_up_to {
                remove_file(entry_path)?;
                n_removed += 1;
            }
        }
    }
    let segno_from = segno_up_to - n_removed + 1;
    info!(
        "removed {} WAL segments [{}; {}]",
        n_removed,
        XLogFileName(PG_TLI, segno_from, wal_seg_size),
        XLogFileName(PG_TLI, segno_up_to, wal_seg_size)
    );
    Ok(())
}

pub struct WalReader {
    timeline_dir: PathBuf,
    wal_seg_size: usize,
    pos: Lsn,
    wal_segment: Option<Pin<Box<dyn AsyncRead>>>,

    enable_remote_read: bool,
    // S3 will be used to read WAL is LSN it not available locally
    local_start_lsn: Lsn,
}

impl WalReader {
    pub fn new(
        timeline_dir: PathBuf,
        state: &SafeKeeperState,
        start_pos: Lsn,
        enable_remote_read: bool,
    ) -> Result<Self> {
        if start_pos < state.timeline_start_lsn {
            bail!(
                "Requested streaming from {}, which is before the start of the timeline {}",
                start_pos,
                state.timeline_start_lsn
            );
        }

        if state.server.wal_seg_size == 0
            || state.timeline_start_lsn == Lsn(0)
            || state.local_start_lsn == Lsn(0)
        {
            bail!("state uninitialized, no data to read");
        }

        Ok(Self {
            timeline_dir,
            wal_seg_size: state.server.wal_seg_size as usize,
            pos: start_pos,
            wal_segment: None,
            enable_remote_read,
            local_start_lsn: state.local_start_lsn,
        })
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut wal_segment = match self.wal_segment.take() {
            Some(reader) => reader,
            None => self.open_segment().await?,
        };

        // How much to read and send in message? We cannot cross the WAL file
        // boundary, and we don't want send more than provided buffer.
        let xlogoff = self.pos.segment_offset(self.wal_seg_size) as usize;
        let send_size = min(buf.len(), self.wal_seg_size - xlogoff);

        // Read some data from the file.
        let buf = &mut buf[0..send_size];
        let send_size = wal_segment.read(buf).await?;
        self.pos += send_size as u64;

        // Decide whether to reuse this file. If we don't set wal_segment here
        // a new reader will be opened next time.
        if self.pos.segment_offset(self.wal_seg_size) != 0 {
            self.wal_segment = Some(wal_segment);
        }

        Ok(send_size)
    }

    /// Open WAL segment at the current position of the reader.
    async fn open_segment(&self) -> Result<Pin<Box<dyn AsyncRead>>> {
        let xlogoff = self.pos.segment_offset(self.wal_seg_size) as usize;
        let segno = self.pos.segment_number(self.wal_seg_size);
        let wal_file_name = XLogFileName(PG_TLI, segno, self.wal_seg_size);
        let wal_file_path = self.timeline_dir.join(wal_file_name);

        // Try to open local file, if we may have WAL locally
        if self.pos >= self.local_start_lsn {
            let res = Self::open_wal_file(&wal_file_path).await;
            match res {
                Ok(mut file) => {
                    file.seek(SeekFrom::Start(xlogoff as u64)).await?;
                    return Ok(Box::pin(file));
                }
                Err(e) => {
                    let is_not_found = e.chain().any(|e| {
                        if let Some(e) = e.downcast_ref::<io::Error>() {
                            e.kind() == io::ErrorKind::NotFound
                        } else {
                            false
                        }
                    });
                    if !is_not_found {
                        return Err(e);
                    }
                    // NotFound is expected, fall through to remote read
                }
            };
        }

        // Try to open remote file, if remote reads are enabled
        if self.enable_remote_read {
            let (reader, _) = read_object(wal_file_path, xlogoff as u64).await;
            return Ok(Box::pin(reader));
        }

        bail!("WAL segment is not found")
    }

    /// Helper function for opening a wal file.
    async fn open_wal_file(wal_file_path: &Path) -> Result<tokio::fs::File> {
        // First try to open the .partial file.
        let mut partial_path = wal_file_path.to_owned();
        partial_path.set_extension("partial");
        if let Ok(opened_file) = tokio::fs::File::open(&partial_path).await {
            return Ok(opened_file);
        }

        // If that failed, try it without the .partial extension.
        tokio::fs::File::open(&wal_file_path)
            .await
            .with_context(|| format!("Failed to open WAL file {:?}", wal_file_path))
            .map_err(|e| {
                error!("{}", e);
                e
            })
    }
}

/// Zero block for filling created WAL segments.
const ZERO_BLOCK: &[u8] = &[0u8; XLOG_BLCKSZ];

/// Helper for filling file with zeroes.
fn write_zeroes(file: &mut File, mut count: usize) -> Result<()> {
    while count >= XLOG_BLCKSZ {
        file.write_all(ZERO_BLOCK)?;
        count -= XLOG_BLCKSZ;
    }
    file.write_all(&ZERO_BLOCK[0..count])?;
    Ok(())
}

/// Helper returning full path to WAL segment file and its .partial brother.
fn wal_file_paths(
    timeline_dir: &Path,
    segno: XLogSegNo,
    wal_seg_size: usize,
) -> Result<(PathBuf, PathBuf)> {
    let wal_file_name = XLogFileName(PG_TLI, segno, wal_seg_size);
    let wal_file_path = timeline_dir.join(wal_file_name.clone());
    let wal_file_partial_path = timeline_dir.join(wal_file_name + ".partial");
    Ok((wal_file_path, wal_file_partial_path))
}
