//! This module has everything to deal with WAL -- reading and writing to disk.
//!
//! Safekeeper WAL is stored in the timeline directory, in format similar to pg_wal.
//! PG timeline is always 1, so WAL segments are usually have names like this:
//! - 000000010000000000000001
//! - 000000010000000000000002.partial
//!
//! Note that last file has `.partial` suffix, that's different from postgres.

use anyhow::{anyhow, Context, Result};
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
}

struct WalStorageMetrics {
    flush_lsn: Gauge,
    write_wal_bytes: Histogram,
    write_wal_seconds: Histogram,
}

impl WalStorageMetrics {
    fn new(zttid: &ZTenantTimelineId) -> Self {
        let tenant_id = zttid.tenant_id.to_string();
        let timeline_id = zttid.timeline_id.to_string();
        Self {
            flush_lsn: FLUSH_LSN_GAUGE.with_label_values(&[&tenant_id, &timeline_id]),
            write_wal_bytes: WRITE_WAL_BYTES.with_label_values(&[&tenant_id, &timeline_id]),
            write_wal_seconds: WRITE_WAL_SECONDS.with_label_values(&[&tenant_id, &timeline_id]),
        }
    }
}

pub trait Storage {
    /// lsn of last durably stored WAL record.
    fn flush_lsn(&self) -> Lsn;

    /// Init storage with wal_seg_size and read WAL from disk to get latest lsn.
    fn init_storage(&mut self, state: &SafeKeeperState) -> Result<()>;

    /// Write piece of wal in buf to disk and sync it.
    fn write_wal(&mut self, startpos: Lsn, buf: &[u8]) -> Result<()>;

    // Truncate WAL at specified LSN.
    fn truncate_wal(&mut self, end_pos: Lsn) -> Result<()>;
}

pub struct PhysicalStorage {
    metrics: WalStorageMetrics,
    zttid: ZTenantTimelineId,
    timeline_dir: PathBuf,
    conf: SafeKeeperConf,

    // fields below are filled upon initialization

    // None if unitialized, Some(lsn) if storage is initialized
    wal_seg_size: Option<usize>,

    // Relationship of lsns:
    // `write_lsn` >= `write_record_lsn` >= `flush_record_lsn`
    //
    // All lsns are zeroes, if storage is just created, and there are no segments on disk.

    // Written to disk, but possibly still in the cache and not fully persisted.
    // Also can be ahead of record_lsn, if happen to be in the middle of a WAL record.
    write_lsn: Lsn,

    // The LSN of the last WAL record written to disk. Still can be not fully flushed.
    write_record_lsn: Lsn,

    // The LSN of the last WAL record flushed to disk.
    flush_record_lsn: Lsn,

    // Decoder is required for detecting boundaries of WAL records.
    decoder: WalStreamDecoder,
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
        }
    }

    // wrapper for flush_lsn updates that also updates metrics
    fn update_flush_lsn(&mut self) {
        self.flush_record_lsn = self.write_record_lsn;
        self.metrics.flush_lsn.set(self.flush_record_lsn.0 as f64);
    }

    /// Helper returning full path to WAL segment file and its .partial brother.
    fn wal_file_paths(&self, segno: XLogSegNo) -> Result<(PathBuf, PathBuf)> {
        let wal_seg_size = self
            .wal_seg_size
            .ok_or_else(|| anyhow!("wal_seg_size is not initialized"))?;

        let wal_file_name = XLogFileName(PG_TLI, segno, wal_seg_size);
        let wal_file_path = self.timeline_dir.join(wal_file_name.clone());
        let wal_file_partial_path = self.timeline_dir.join(wal_file_name + ".partial");
        Ok((wal_file_path, wal_file_partial_path))
    }

    // TODO: this function is going to be refactored soon, what will change:
    //      - flush will be called separately from write_wal, this function
    //        will only write bytes to disk
    //      - File will be cached in PhysicalStorage, to remove extra syscalls,
    //        such as open(), seek(), close()
    fn write_and_flush(&mut self, startpos: Lsn, buf: &[u8]) -> Result<()> {
        let wal_seg_size = self
            .wal_seg_size
            .ok_or_else(|| anyhow!("wal_seg_size is not initialized"))?;

        let mut bytes_left: usize = buf.len();
        let mut bytes_written: usize = 0;
        let mut partial;
        let mut start_pos = startpos;
        const ZERO_BLOCK: &[u8] = &[0u8; XLOG_BLCKSZ];

        /* Extract WAL location for this block */
        let mut xlogoff = start_pos.segment_offset(wal_seg_size) as usize;

        while bytes_left != 0 {
            let bytes_to_write;

            /*
             * If crossing a WAL boundary, only write up until we reach wal
             * segment size.
             */
            if xlogoff + bytes_left > wal_seg_size {
                bytes_to_write = wal_seg_size - xlogoff;
            } else {
                bytes_to_write = bytes_left;
            }

            /* Open file */
            let segno = start_pos.segment_number(wal_seg_size);
            let (wal_file_path, wal_file_partial_path) = self.wal_file_paths(segno)?;
            {
                let mut wal_file: File;
                /* Try to open already completed segment */
                if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_path) {
                    wal_file = file;
                    partial = false;
                } else if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_partial_path)
                {
                    /* Try to open existed partial file */
                    wal_file = file;
                    partial = true;
                } else {
                    /* Create and fill new partial file */
                    partial = true;
                    match OpenOptions::new()
                        .create(true)
                        .write(true)
                        .open(&wal_file_partial_path)
                    {
                        Ok(mut file) => {
                            for _ in 0..(wal_seg_size / XLOG_BLCKSZ) {
                                file.write_all(ZERO_BLOCK)?;
                            }
                            wal_file = file;
                        }
                        Err(e) => {
                            error!("Failed to open log file {:?}: {}", &wal_file_path, e);
                            return Err(e.into());
                        }
                    }
                }
                wal_file.seek(SeekFrom::Start(xlogoff as u64))?;
                wal_file.write_all(&buf[bytes_written..(bytes_written + bytes_to_write)])?;

                // Flush file, if not said otherwise
                if !self.conf.no_sync {
                    wal_file.sync_all()?;
                }
            }
            /* Write was successful, advance our position */
            bytes_written += bytes_to_write;
            bytes_left -= bytes_to_write;
            start_pos += bytes_to_write as u64;
            xlogoff += bytes_to_write;

            /* Did we reach the end of a WAL segment? */
            if start_pos.segment_offset(wal_seg_size) == 0 {
                xlogoff = 0;
                if partial {
                    fs::rename(&wal_file_partial_path, &wal_file_path)?;
                }
            }
        }
        Ok(())
    }
}

impl Storage for PhysicalStorage {
    // flush_lsn returns lsn of last durably stored WAL record.
    fn flush_lsn(&self) -> Lsn {
        self.flush_record_lsn
    }

    // Storage needs to know wal_seg_size to know which segment to read/write, but
    // wal_seg_size is not always known at the moment of storage creation. This method
    // allows to postpone its initialization.
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

    // Write and flush WAL to disk.
    fn write_wal(&mut self, startpos: Lsn, buf: &[u8]) -> Result<()> {
        if self.write_lsn > startpos {
            warn!(
                "write_wal rewrites WAL written before, write_lsn={}, startpos={}",
                self.write_lsn, startpos
            );
        }
        if self.write_lsn < startpos {
            warn!(
                "write_wal creates gap in written WAL, write_lsn={}, startpos={}",
                self.write_lsn, startpos
            );
            // TODO: return error if write_lsn is not zero
        }

        {
            let _timer = self.metrics.write_wal_seconds.start_timer();
            self.write_and_flush(startpos, buf)?;
        }

        // WAL is written and flushed, updating lsns
        self.write_lsn = startpos + buf.len() as u64;
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

        self.update_flush_lsn();
        Ok(())
    }

    // Truncate written WAL by removing all WAL segments after the given LSN.
    // end_pos must point to the end of the WAL record.
    fn truncate_wal(&mut self, end_pos: Lsn) -> Result<()> {
        let wal_seg_size = self
            .wal_seg_size
            .ok_or_else(|| anyhow!("wal_seg_size is not initialized"))?;

        // TODO: cross check divergence point

        // nothing to truncate
        if self.write_lsn == Lsn(0) {
            return Ok(());
        }

        // Streaming must not create a hole, so truncate cannot be called on non-written lsn
        assert!(self.write_lsn >= end_pos);

        // open segment files and delete or fill end with zeroes

        let partial;
        const ZERO_BLOCK: &[u8] = &[0u8; XLOG_BLCKSZ];

        /* Extract WAL location for this block */
        let mut xlogoff = end_pos.segment_offset(wal_seg_size) as usize;

        /* Open file */
        let mut segno = end_pos.segment_number(wal_seg_size);
        let (wal_file_path, wal_file_partial_path) = self.wal_file_paths(segno)?;
        {
            let mut wal_file: File;
            /* Try to open already completed segment */
            if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_path) {
                wal_file = file;
                partial = false;
            } else {
                wal_file = OpenOptions::new()
                    .write(true)
                    .open(&wal_file_partial_path)?;
                partial = true;
            }
            wal_file.seek(SeekFrom::Start(xlogoff as u64))?;
            while xlogoff < wal_seg_size {
                let bytes_to_write = min(XLOG_BLCKSZ, wal_seg_size - xlogoff);
                wal_file.write_all(&ZERO_BLOCK[0..bytes_to_write])?;
                xlogoff += bytes_to_write;
            }
            // Flush file, if not said otherwise
            if !self.conf.no_sync {
                wal_file.sync_all()?;
            }
        }
        if !partial {
            // Make segment partial once again
            fs::rename(&wal_file_path, &wal_file_partial_path)?;
        }
        // Remove all subsequent segments
        loop {
            segno += 1;
            let (wal_file_path, wal_file_partial_path) = self.wal_file_paths(segno)?;
            // TODO: better use fs::try_exists which is currenty avaialble only in nightly build
            if wal_file_path.exists() {
                fs::remove_file(&wal_file_path)?;
            } else if wal_file_partial_path.exists() {
                fs::remove_file(&wal_file_partial_path)?;
            } else {
                break;
            }
        }

        // Update lsns
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
