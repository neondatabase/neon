//! This module has everything to deal with WAL -- reading and writing to disk.
//!
//! Safekeeper WAL is stored in the timeline directory, in format similar to pg_wal.
//! PG timeline is always 1, so WAL segments are usually have names like this:
//! - 000000010000000000000001
//! - 000000010000000000000002.partial
//!
//! Note that last file has `.partial` suffix, that's different from postgres.

use anyhow::{anyhow, bail, Context, Result};
use std::io::{Read, Seek, SeekFrom};

use lazy_static::lazy_static;
use postgres_ffi::xlog_utils::{find_end_of_wal, XLogSegNo, PG_TLI};
use std::cmp::min;

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use tracing::*;

use zenith_utils::lsn::Lsn;
use zenith_utils::zid::ZTenantTimelineId;

use crate::safekeeper::SafeKeeperState;

use crate::SafeKeeperConf;
use postgres_ffi::xlog_utils::{XLogFileName, XLOG_BLCKSZ};

use postgres_ffi::waldecoder::WalStreamDecoder;

use zenith_metrics::{
    register_gauge_vec, register_histogram_vec, Gauge, GaugeVec, Histogram, HistogramVec,
    DISK_WRITE_SECONDS_BUCKETS,
};

lazy_static! {
    // The prometheus crate does not support u64 yet, i64 only (see `IntGauge`).
    // i64 is faster than f64, so update to u64 when available.
    static ref FLUSH_LSN_GAUGE: GaugeVec = register_gauge_vec!(
        "safekeeper_flush_lsn",
        "Current flush_lsn, grouped by timeline",
        &["tenant_id", "timeline_id"]
    )
    .expect("Failed to register safekeeper_flush_lsn gauge vec");
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
    flush_lsn: Gauge,
    write_wal_bytes: Histogram,
    write_wal_seconds: Histogram,
    flush_wal_seconds: Histogram,
}

impl WalStorageMetrics {
    fn new(zttid: &ZTenantTimelineId) -> Self {
        let tenant_id = zttid.tenant_id.to_string();
        let timeline_id = zttid.timeline_id.to_string();
        Self {
            flush_lsn: FLUSH_LSN_GAUGE.with_label_values(&[&tenant_id, &timeline_id]),
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
    /// None if unitialized, Some(usize) if storage is initialized.
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
        self.metrics.flush_lsn.set(self.flush_record_lsn.0 as f64);
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

    /// Write an empty XLOG page
    ///
    /// Note: offset is a segment offset of the first log record to be written on the page 
    fn write_canned_page(&self, file: &mut File, segno: XLogSegNo, offset: usize, wal_seg_size: usize) -> Result<usize> {
        let page_off =  offset - offset % XLOG_BLCKSZ;

        if  page_off != 0 {
            file.seek(SeekFrom::Start(page_off as u64))?;
        }

        // xlp_magic - 0xd10d -                 2 bytes
        file.write_all(&0xd10du16.to_le_bytes())?;

        if page_off == 0 {
            // in order to skip to the log recrod we will pretend that the page has cont record
            // until real data begins
            // xlp_info  - 0x0002 XLP_LONG_HEADER - 2 bytes
            file.write_all(&0x01u16.to_le_bytes())?;

        } else {
            if offset % XLOG_BLCKSZ == 24 {
                // xlp_info  - 0x00 - 2 bytes
                file.write_all(&0x00u16.to_le_bytes())?;
            } else {
                // xlp_info  - 0x01 XLP_FIRST_IS_CONTRECORD  - 2 bytes
                file.write_all(&0x02u16.to_le_bytes())?;
            }
        }

        // xlp_tli   - 0x01 -                   4 bytes
        file.write_all(&0x01u32.to_le_bytes())?;

        // xlp_pageaddr - .... -                8 bytes
        let x = segno * (wal_seg_size as u64) + page_off as u64;
        file.write_all(&x.to_le_bytes())?;

        if page_off > 0 {
            let hdr_bytes = 24;
            let y = (offset - page_off - hdr_bytes) as u32;
            // xlp_rem_len - 0x00                   4 bytes
            file.write_all(&y.to_le_bytes())?;
            // padding                             4 bytes
            file.write_all(&0x00u32.to_le_bytes())?;
            // write 0 for the rest of the page
            file.write_all(&ZERO_BLOCK[0..XLOG_BLCKSZ-hdr_bytes])?;
        } else {
            // xlp_rem_len - 0x00                   4 bytes
            file.write_all(&0x00u32.to_le_bytes())?;
            // padding                             4 bytes
            file.write_all(&0x00u32.to_le_bytes())?;

            // first page requires a long header
            //
            // xlp_sysid - 0x00                     8 bytes
            file.write_all(&0x00u64.to_le_bytes())?;
            // xlp_seg_size = 0x1000000 ....        4 bytes
            file.write_all(&0x1000000u32.to_le_bytes())?;
            // xlp_xlog_blcksz 0x2000               4 bytes
            file.write_all(&0x2000u32.to_le_bytes())?;
            file.write_all(&ZERO_BLOCK[0..XLOG_BLCKSZ-40])?;
        }


        Ok(XLOG_BLCKSZ)
    }

    /// Open or create WAL segment file. Caller must call seek to the wanted position.
    /// Returns `file` and `is_partial`.
    fn open_or_create(&self, segno: XLogSegNo, wal_seg_size: usize) -> Result<(File, bool, bool)> {
        let (wal_file_path, wal_file_partial_path) =
            wal_file_paths(&self.timeline_dir, segno, wal_seg_size)?;

        // Try to open already completed segment
        if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_path) {
            warn!("aaa: open already completed segment {}", segno);
            Ok((file, false, false))
        } else if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_partial_path) {
            // Try to open existing partial file
            warn!("aaa: open existing partial file {}", segno);
            Ok((file, true, false))
        } else {
            // Create and fill new partial file
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&wal_file_partial_path)
                .with_context(|| format!("Failed to open log file {:?}", &wal_file_path))?;

            warn!("aaa: Create and fill new partial file {}", segno);

            // Write segment header only on the first segment to help pg_waldump
            if segno == 1 {
	            let b = self.write_canned_page(&mut file, segno, 0, wal_seg_size)?;
                write_zeroes(&mut file, wal_seg_size - b)?;
            } else {
                write_zeroes(&mut file, wal_seg_size)?;
            }

            self.fsync_file(&mut file)?;
            Ok((file, true, true))
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
            let (mut file, is_partial, _is_created) = self.open_or_create(segno, wal_seg_size)?;

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
            // wal_seg_size is still unknown
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

        // we need to read WAL from disk to know which LSNs are stored on disk
        self.write_lsn =
            Lsn(find_end_of_wal(&self.timeline_dir, wal_seg_size, true, state.wal_start_lsn)?.0);

        self.write_record_lsn = self.write_lsn;

        // TODO: do we really know that write_lsn is fully flushed to disk?
        //      If not, maybe it's better to call fsync() here to be sure?
        self.update_flush_lsn();

        info!(
            "initialized storage for timeline {}, flush_lsn={}, commit_lsn={}, truncate_lsn={}",
            self.zttid.timeline_id, self.flush_record_lsn, state.commit_lsn, state.truncate_lsn,
        );
        if self.flush_record_lsn < state.commit_lsn || self.flush_record_lsn < state.truncate_lsn {
            warn!("timeline {} potential data loss: flush_lsn by find_end_of_wal is less than either commit_lsn or truncate_lsn from control file", self.zttid.timeline_id);
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
        let (mut file, is_partial, is_created) = self.open_or_create(segno, wal_seg_size)?;

        if is_created && segno == 1 {
            warn!("ddd: primeed a page for log {} in segment {}", xlogoff, segno);
            self.write_canned_page(&mut file, segno, xlogoff, wal_seg_size)?;
        }


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
            // TODO: better use fs::try_exists which is currenty avaialble only in nightly build
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
}

pub struct WalReader {
    timeline_dir: PathBuf,
    wal_seg_size: usize,
    pos: Lsn,
    file: Option<File>,
}

impl WalReader {
    pub fn new(timeline_dir: PathBuf, wal_seg_size: usize, pos: Lsn) -> Self {
        Self {
            timeline_dir,
            wal_seg_size,
            pos,
            file: None,
        }
    }

    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        // Take the `File` from `wal_file`, or open a new file.
        let mut file = match self.file.take() {
            Some(file) => file,
            None => {
                // Open a new file.
                let segno = self.pos.segment_number(self.wal_seg_size);
                let wal_file_name = XLogFileName(PG_TLI, segno, self.wal_seg_size);
                let wal_file_path = self.timeline_dir.join(wal_file_name);
                Self::open_wal_file(&wal_file_path)?
            }
        };

        let xlogoff = self.pos.segment_offset(self.wal_seg_size) as usize;

        // How much to read and send in message? We cannot cross the WAL file
        // boundary, and we don't want send more than provided buffer.
        let send_size = min(buf.len(), self.wal_seg_size - xlogoff);

        // Read some data from the file.
        let buf = &mut buf[0..send_size];
        file.seek(SeekFrom::Start(xlogoff as u64))
            .and_then(|_| file.read_exact(buf))
            .context("Failed to read data from WAL file")?;

        self.pos += send_size as u64;

        // Decide whether to reuse this file. If we don't set wal_file here
        // a new file will be opened next time.
        if self.pos.segment_offset(self.wal_seg_size) != 0 {
            self.file = Some(file);
        }

        Ok(send_size)
    }

    /// Helper function for opening a wal file.
    fn open_wal_file(wal_file_path: &Path) -> Result<File> {
        // First try to open the .partial file.
        let mut partial_path = wal_file_path.to_owned();
        partial_path.set_extension("partial");
        if let Ok(opened_file) = File::open(&partial_path) {
            return Ok(opened_file);
        }

        // If that failed, try it without the .partial extension.
        File::open(&wal_file_path)
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
