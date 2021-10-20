//!
//! Zenith repository implementation that keeps old data in files on disk, and
//! the recent changes in memory. See layered_repository/*_layer.rs files.
//! The functions here are responsible for locating the correct layer for the
//! get/put call, tracing timeline branching history as needed.
//!
//! The files are stored in the .zenith/tenants/<tenantid>/timelines/<timelineid>
//! directory. See layered_repository/README for how the files are managed.
//! In addition to the layer files, there is a metadata file in the same
//! directory that contains information about the timeline, in particular its
//! parent timeline, and the last LSN that has been written to disk.
//!

use anyhow::{anyhow, bail, ensure, Context, Result};
use bookfile::Book;
use bytes::Bytes;
use lazy_static::lazy_static;
use postgres_ffi::pg_constants::BLCKSZ;
use serde::{Deserialize, Serialize};
use tracing::*;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::{BTreeSet, HashSet};
use std::convert::TryInto;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::{Bound::Included, Deref};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::relish::*;
use crate::relish_storage::schedule_timeline_upload;
use crate::repository::{GcResult, Repository, Timeline, TimelineWriter, WALRecord};
use crate::tenant_mgr;
use crate::walreceiver;
use crate::walreceiver::IS_WAL_RECEIVER;
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};

use zenith_metrics::{
    register_histogram, register_int_gauge_vec, Histogram, IntGauge, IntGaugeVec,
};
use zenith_metrics::{register_histogram_vec, HistogramVec};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::crashsafe_dir;
use zenith_utils::lsn::{AtomicLsn, Lsn, RecordLsn};
use zenith_utils::seqwait::SeqWait;

mod blob;
mod delta_layer;
mod filename;
mod image_layer;
mod inmemory_layer;
mod interval_tree;
mod layer_map;
mod page_versions;
mod storage_layer;

use delta_layer::DeltaLayer;
use image_layer::ImageLayer;

use inmemory_layer::InMemoryLayer;
use layer_map::LayerMap;
use storage_layer::{
    Layer, PageReconstructData, PageReconstructResult, SegmentTag, RELISH_SEG_SIZE,
};

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

// Timeout when waiting for WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(60);

// Taken from PG_CONTROL_MAX_SAFE_SIZE
const METADATA_MAX_SAFE_SIZE: usize = 512;
const METADATA_CHECKSUM_SIZE: usize = std::mem::size_of::<u32>();
const METADATA_MAX_DATA_SIZE: usize = METADATA_MAX_SAFE_SIZE - METADATA_CHECKSUM_SIZE;

// Metrics collected on operations on the storage repository.
lazy_static! {
    static ref STORAGE_TIME: HistogramVec = register_histogram_vec!(
        "pageserver_storage_time",
        "Time spent on storage operations",
        &["operation"]
    )
    .expect("failed to define a metric");
}

// Metrics collected on operations on the storage repository.
lazy_static! {
    static ref RECONSTRUCT_TIME: Histogram = register_histogram!(
        "pageserver_getpage_reconstruct_time",
        "FIXME Time spent on storage operations"
    )
    .expect("failed to define a metric");
}

lazy_static! {
    // NOTE: can be zero if pageserver was restarted and there hasn't been any
    // activity yet.
    static ref LOGICAL_TIMELINE_SIZE: IntGaugeVec = register_int_gauge_vec!(
        "pageserver_logical_timeline_size",
        "Logical timeline size (bytes)",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric");
}

/// The name of the metadata file pageserver creates per timeline.
pub const METADATA_FILE_NAME: &str = "metadata";

///
/// Repository consists of multiple timelines. Keep them in a hash table.
///
pub struct LayeredRepository {
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    timelines: Mutex<HashMap<ZTimelineId, Arc<LayeredTimeline>>>,

    walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
    /// Makes evey repo's timelines to backup their files to remote storage,
    /// when they get frozen.
    upload_relishes: bool,
}

/// Public interface
impl Repository for LayeredRepository {
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        Ok(self.get_timeline_locked(timelineid, &mut timelines)?)
    }

    fn create_empty_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        // Create the timeline directory, and write initial metadata to file.
        crashsafe_dir::create_dir_all(self.conf.timeline_path(&timelineid, &self.tenantid))?;

        let metadata = TimelineMetadata {
            disk_consistent_lsn: Lsn(0),
            prev_record_lsn: None,
            ancestor_timeline: None,
            ancestor_lsn: Lsn(0),
        };
        Self::save_metadata(self.conf, timelineid, self.tenantid, &metadata, true)?;

        let timeline = LayeredTimeline::new(
            self.conf,
            metadata,
            None,
            timelineid,
            self.tenantid,
            Arc::clone(&self.walredo_mgr),
            0,
            false,
        )?;

        let timeline_rc = Arc::new(timeline);
        let r = timelines.insert(timelineid, timeline_rc.clone());
        assert!(r.is_none());
        Ok(timeline_rc)
    }

    /// Branch a timeline
    fn branch_timeline(&self, src: ZTimelineId, dst: ZTimelineId, start_lsn: Lsn) -> Result<()> {
        let src_timeline = self.get_timeline(src)?;

        let RecordLsn {
            last: src_last,
            prev: src_prev,
        } = src_timeline.get_last_record_rlsn();

        // Use src_prev from the source timeline only if we branched at the last record.
        let dst_prev = if src_last == start_lsn {
            Some(src_prev)
        } else {
            None
        };

        // Create the metadata file, noting the ancestor of the new timeline.
        // There is initially no data in it, but all the read-calls know to look
        // into the ancestor.
        let metadata = TimelineMetadata {
            disk_consistent_lsn: start_lsn,
            prev_record_lsn: dst_prev,
            ancestor_timeline: Some(src),
            ancestor_lsn: start_lsn,
        };
        crashsafe_dir::create_dir_all(self.conf.timeline_path(&dst, &self.tenantid))?;
        Self::save_metadata(self.conf, dst, self.tenantid, &metadata, true)?;

        info!("branched timeline {} from {} at {}", dst, src, start_lsn);

        Ok(())
    }

    /// Public entry point to GC. All the logic is in the private
    /// gc_iteration_internal function, this public facade just wraps it for
    /// metrics collection.
    fn gc_iteration(
        &self,
        target_timelineid: Option<ZTimelineId>,
        horizon: u64,
        checkpoint_before_gc: bool,
    ) -> Result<GcResult> {
        STORAGE_TIME
            .with_label_values(&["gc"])
            .observe_closure_duration(|| {
                self.gc_iteration_internal(target_timelineid, horizon, checkpoint_before_gc)
            })
    }

    // Wait for all threads to complete and persist repository data before pageserver shutdown.
    fn shutdown(&self) -> Result<()> {
        trace!("LayeredRepository shutdown for tenant {}", self.tenantid);

        let timelines = self.timelines.lock().unwrap();
        for (timelineid, timeline) in timelines.iter() {
            walreceiver::stop_wal_receiver(*timelineid);
            // Wait for syncing data to disk
            trace!("repo shutdown. checkpoint timeline {}", timelineid);
            timeline.checkpoint()?;

            //TODO Wait for walredo process to shutdown too
        }

        Ok(())
    }
}

/// Private functions
impl LayeredRepository {
    // Implementation of the public `get_timeline` function. This differs from the public
    // interface in that the caller must already hold the mutex on the 'timelines' hashmap.
    fn get_timeline_locked(
        &self,
        timelineid: ZTimelineId,
        timelines: &mut HashMap<ZTimelineId, Arc<LayeredTimeline>>,
    ) -> Result<Arc<LayeredTimeline>> {
        match timelines.get(&timelineid) {
            Some(timeline) => Ok(timeline.clone()),
            None => {
                let metadata = Self::load_metadata(self.conf, timelineid, self.tenantid)?;
                let disk_consistent_lsn = metadata.disk_consistent_lsn;

                // Recurse to look up the ancestor timeline.
                //
                // TODO: If you have a very deep timeline history, this could become
                // expensive. Perhaps delay this until we need to look up a page in
                // ancestor.
                let ancestor = if let Some(ancestor_timelineid) = metadata.ancestor_timeline {
                    Some(self.get_timeline_locked(ancestor_timelineid, timelines)?)
                } else {
                    None
                };

                let _enter =
                    info_span!("loading timeline", timeline = %timelineid, tenant = %self.tenantid)
                        .entered();

                let mut timeline = LayeredTimeline::new(
                    self.conf,
                    metadata,
                    ancestor,
                    timelineid,
                    self.tenantid,
                    Arc::clone(&self.walredo_mgr),
                    0, // init with 0 and update after layers are loaded,
                    self.upload_relishes,
                )?;

                // List the layers on disk, and load them into the layer map
                let _loaded_layers = timeline.load_layer_map(disk_consistent_lsn)?;
                if self.upload_relishes {
                    schedule_timeline_upload(());
                    // schedule_timeline_upload(
                    //     self.tenantid,
                    //     timelineid,
                    //     loaded_layers,
                    //     disk_consistent_lsn,
                    // );
                }

                // needs to be after load_layer_map
                timeline.init_current_logical_size()?;

                let timeline = Arc::new(timeline);
                timelines.insert(timelineid, timeline.clone());
                Ok(timeline)
            }
        }
    }

    pub fn new(
        conf: &'static PageServerConf,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        tenantid: ZTenantId,
        upload_relishes: bool,
    ) -> LayeredRepository {
        LayeredRepository {
            tenantid,
            conf,
            timelines: Mutex::new(HashMap::new()),
            walredo_mgr,
            upload_relishes,
        }
    }

    ///
    /// Launch the checkpointer thread in given repository.
    ///
    pub fn launch_checkpointer_thread(
        conf: &'static PageServerConf,
        rc: Arc<LayeredRepository>,
    ) -> JoinHandle<()> {
        std::thread::Builder::new()
            .name("Checkpointer thread".into())
            .spawn(move || {
                // FIXME: relaunch it? Panic is not good.
                rc.checkpoint_loop(conf).expect("Checkpointer thread died");
            })
            .unwrap()
    }

    ///
    /// Checkpointer thread's main loop
    ///
    fn checkpoint_loop(&self, conf: &'static PageServerConf) -> Result<()> {
        while !tenant_mgr::shutdown_requested() {
            std::thread::sleep(conf.checkpoint_period);
            info!("checkpointer thread for tenant {} waking up", self.tenantid);

            // checkpoint timelines that have accumulated more than CHECKPOINT_DISTANCE
            // bytes of WAL since last checkpoint.
            {
                let timelines = self.timelines.lock().unwrap();
                for (timelineid, timeline) in timelines.iter() {
                    let _entered =
                        info_span!("checkpoint", timeline = %timelineid, tenant = %self.tenantid)
                            .entered();

                    STORAGE_TIME
                        .with_label_values(&["checkpoint_timed"])
                        .observe_closure_duration(|| {
                            timeline.checkpoint_internal(conf.checkpoint_distance, false)
                        })?
                }
                // release lock on 'timelines'
            }
        }
        trace!("Checkpointer thread shut down");
        Ok(())
    }

    ///
    /// Launch the GC thread in given repository.
    ///
    pub fn launch_gc_thread(
        conf: &'static PageServerConf,
        rc: Arc<LayeredRepository>,
    ) -> JoinHandle<()> {
        std::thread::Builder::new()
            .name("GC thread".into())
            .spawn(move || {
                // FIXME: relaunch it? Panic is not good.
                rc.gc_loop(conf).expect("GC thread died");
            })
            .unwrap()
    }

    ///
    /// GC thread's main loop
    ///
    fn gc_loop(&self, conf: &'static PageServerConf) -> Result<()> {
        while !tenant_mgr::shutdown_requested() {
            // Garbage collect old files that are not needed for PITR anymore
            if conf.gc_horizon > 0 {
                self.gc_iteration(None, conf.gc_horizon, false).unwrap();
            }

            // TODO Write it in more adequate way using
            // condvar.wait_timeout() or something
            let mut sleep_time = conf.gc_period.as_secs();
            while sleep_time > 0 && !tenant_mgr::shutdown_requested() {
                sleep_time -= 1;
                std::thread::sleep(Duration::from_secs(1));
            }
            info!("gc thread for tenant {} waking up", self.tenantid);
        }
        Ok(())
    }

    /// Save timeline metadata to file
    fn save_metadata(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        data: &TimelineMetadata,
        first_save: bool,
    ) -> Result<()> {
        let _enter = info_span!("saving metadata").entered();
        let path = metadata_path(conf, timelineid, tenantid);
        // use OpenOptions to ensure file presence is consistent with first_save
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(first_save)
            .open(&path)?;

        let mut metadata_bytes = TimelineMetadata::ser(data)?;

        assert!(metadata_bytes.len() <= METADATA_MAX_DATA_SIZE);
        metadata_bytes.resize(METADATA_MAX_SAFE_SIZE, 0u8);

        let checksum = crc32c::crc32c(&metadata_bytes[..METADATA_MAX_DATA_SIZE]);
        metadata_bytes[METADATA_MAX_DATA_SIZE..].copy_from_slice(&u32::to_le_bytes(checksum));

        if file.write(&metadata_bytes)? != metadata_bytes.len() {
            bail!("Could not write all the metadata bytes in a single call");
        }
        file.sync_all()?;

        // fsync the parent directory to ensure the directory entry is durable
        if first_save {
            let timeline_dir = File::open(
                &path
                    .parent()
                    .expect("Metadata should always have a parent dir"),
            )?;
            timeline_dir.sync_all()?;
        }

        Ok(())
    }

    fn load_metadata(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
    ) -> Result<TimelineMetadata> {
        let path = metadata_path(conf, timelineid, tenantid);
        let metadata_bytes = std::fs::read(&path)?;
        ensure!(metadata_bytes.len() == METADATA_MAX_SAFE_SIZE);

        let data = &metadata_bytes[..METADATA_MAX_DATA_SIZE];
        let calculated_checksum = crc32c::crc32c(data);

        let checksum_bytes: &[u8; METADATA_CHECKSUM_SIZE] =
            metadata_bytes[METADATA_MAX_DATA_SIZE..].try_into()?;
        let expected_checksum = u32::from_le_bytes(*checksum_bytes);
        ensure!(calculated_checksum == expected_checksum);

        let data = TimelineMetadata::des_prefix(data)?;
        assert!(data.disk_consistent_lsn.is_aligned());

        Ok(data)
    }

    //
    // How garbage collection works:
    //
    //                    +--bar------------->
    //                   /
    //             +----+-----foo---------------->
    //            /
    // ----main--+-------------------------->
    //                \
    //                 +-----baz-------->
    //
    //
    // 1. Grab a mutex to prevent new timelines from being created
    // 2. Scan all timelines, and on each timeline, make note of the
    //    all the points where other timelines have been branched off.
    //    We will refrain from removing page versions at those LSNs.
    // 3. For each timeline, scan all layer files on the timeline.
    //    Remove all files for which a newer file exists and which
    //    don't cover any branch point LSNs.
    //
    // TODO:
    // - if a relation has a non-incremental persistent layer on a child branch, then we
    //   don't need to keep that in the parent anymore. But currently
    //   we do.
    fn gc_iteration_internal(
        &self,
        target_timelineid: Option<ZTimelineId>,
        horizon: u64,
        checkpoint_before_gc: bool,
    ) -> Result<GcResult> {
        let mut totals: GcResult = Default::default();
        let now = Instant::now();

        // grab mutex to prevent new timelines from being created here.
        // TODO: We will hold it for a long time
        let mut timelines = self.timelines.lock().unwrap();

        // Scan all timelines. For each timeline, remember the timeline ID and
        // the branch point where it was created.
        //
        let mut timelineids: Vec<ZTimelineId> = Vec::new();

        // We scan the directory, not the in-memory hash table, because the hash
        // table only contains entries for timelines that have been accessed. We
        // need to take all timelines into account, not only the active ones.
        let timelines_path = self.conf.timelines_path(&self.tenantid);

        for direntry in fs::read_dir(timelines_path)? {
            let direntry = direntry?;
            if let Some(fname) = direntry.file_name().to_str() {
                if let Ok(timelineid) = fname.parse::<ZTimelineId>() {
                    timelineids.push(timelineid);
                }
            }
        }

        //Now collect info about branchpoints
        let mut all_branchpoints: BTreeSet<(ZTimelineId, Lsn)> = BTreeSet::new();
        for timelineid in &timelineids {
            let timeline = self.get_timeline_locked(*timelineid, &mut *timelines)?;

            if let Some(ancestor_timeline) = &timeline.ancestor_timeline {
                // If target_timeline is specified, we only need to know branchpoints of its children
                if let Some(timelineid) = target_timelineid {
                    if ancestor_timeline.timelineid == timelineid {
                        all_branchpoints
                            .insert((ancestor_timeline.timelineid, timeline.ancestor_lsn));
                    }
                }
                // Collect branchpoints for all timelines
                else {
                    all_branchpoints.insert((ancestor_timeline.timelineid, timeline.ancestor_lsn));
                }
            }
        }

        // Ok, we now know all the branch points.
        // Perform GC for each timeline.
        for timelineid in timelineids {
            if tenant_mgr::shutdown_requested() {
                return Ok(totals);
            }

            // We have already loaded all timelines above
            // so this operation is just a quick map lookup.
            let timeline = self.get_timeline_locked(timelineid, &mut *timelines)?;

            // If target_timeline is specified, only GC it
            if let Some(target_timelineid) = target_timelineid {
                if timelineid != target_timelineid {
                    continue;
                }
            }

            if let Some(cutoff) = timeline.get_last_record_lsn().checked_sub(horizon) {
                let branchpoints: Vec<Lsn> = all_branchpoints
                    .range((
                        Included((timelineid, Lsn(0))),
                        Included((timelineid, Lsn(u64::MAX))),
                    ))
                    .map(|&x| x.1)
                    .collect();

                // If requested, force flush all in-memory layers to disk first,
                // so that they too can be garbage collected. That's
                // used in tests, so we want as deterministic results as possible.
                if checkpoint_before_gc {
                    timeline.checkpoint()?;
                    info!("timeline {} checkpoint_before_gc done", timelineid);
                }

                let result = timeline.gc_timeline(branchpoints, cutoff)?;

                totals += result;
            }
        }

        totals.elapsed = now.elapsed();
        Ok(totals)
    }
}

/// Metadata stored on disk for each timeline
///
/// The fields correspond to the values we hold in memory, in LayeredTimeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineMetadata {
    disk_consistent_lsn: Lsn,

    // This is only set if we know it. We track it in memory when the page
    // server is running, but we only track the value corresponding to
    // 'last_record_lsn', not 'disk_consistent_lsn' which can lag behind by a
    // lot. We only store it in the metadata file when we flush *all* the
    // in-memory data so that 'last_record_lsn' is the same as
    // 'disk_consistent_lsn'.  That's OK, because after page server restart, as
    // soon as we reprocess at least one record, we will have a valid
    // 'prev_record_lsn' value in memory again. This is only really needed when
    // doing a clean shutdown, so that there is no more WAL beyond
    // 'disk_consistent_lsn'
    prev_record_lsn: Option<Lsn>,

    ancestor_timeline: Option<ZTimelineId>,
    ancestor_lsn: Lsn,
}

pub struct LayeredTimeline {
    conf: &'static PageServerConf,

    tenantid: ZTenantId,
    timelineid: ZTimelineId,

    layers: Mutex<LayerMap>,

    // WAL redo manager
    walredo_mgr: Arc<dyn WalRedoManager + Sync + Send>,

    // What page versions do we hold in the repository? If we get a
    // request > last_record_lsn, we need to wait until we receive all
    // the WAL up to the request. The SeqWait provides functions for
    // that. TODO: If we get a request for an old LSN, such that the
    // versions have already been garbage collected away, we should
    // throw an error, but we don't track that currently.
    //
    // last_record_lsn.load().last points to the end of last processed WAL record.
    //
    // We also remember the starting point of the previous record in
    // 'last_record_lsn.load().prev'. It's used to set the xl_prev pointer of the
    // first WAL record when the node is started up. But here, we just
    // keep track of it.
    last_record_lsn: SeqWait<RecordLsn, Lsn>,

    // All WAL records have been processed and stored durably on files on
    // local disk, up to this LSN. On crash and restart, we need to re-process
    // the WAL starting from this point.
    //
    // Some later WAL records might have been processed and also flushed to disk
    // already, so don't be surprised to see some, but there's no guarantee on
    // them yet.
    disk_consistent_lsn: AtomicLsn,

    // Parent timeline that this timeline was branched from, and the LSN
    // of the branch point.
    ancestor_timeline: Option<Arc<LayeredTimeline>>,
    ancestor_lsn: Lsn,

    // this variable indicates how much space is used from user's point of view,
    // e.g. we do not account here for multiple versions of data and so on.
    // this is counted incrementally based on physical relishes (excluding FileNodeMap)
    // current_logical_size is not stored no disk and initialized on timeline creation using
    // get_current_logical_size_non_incremental in init_current_logical_size
    // this is needed because when we save it in metadata it can become out of sync
    // because current_logical_size is consistent on last_record_lsn, not ondisk_consistent_lsn
    // NOTE: current_logical_size also includes size of the ancestor
    current_logical_size: AtomicUsize, // bytes

    // To avoid calling .with_label_values and formatting the tenant and timeline IDs to strings
    // every time the logical size is updated, keep a direct reference to the Gauge here.
    // unfortunately it doesnt forward atomic methods like .fetch_add
    // so use two fields: actual size and metric
    // see https://github.com/zenithdb/zenith/issues/622 for discussion
    // TODO: it is possible to combine these two fields into single one using custom metric which uses SeqCst
    // ordering for its operations, but involves private modules, and macro trickery
    current_logical_size_gauge: IntGauge,

    /// If `true`, will backup its timeline files to remote storage after freezing.
    upload_relishes: bool,

    /// Ensures layers aren't frozen by checkpointer between
    /// [`LayeredTimeline::get_layer_for_write`] and layer reads.
    /// Locked automatically by [`LayeredTimelineWriter`] and checkpointer.
    /// Must always be acquired before the layer map/individual layer lock
    /// to avoid deadlock.
    write_lock: Mutex<()>,
}

/// Public interface functions
impl Timeline for LayeredTimeline {
    fn get_ancestor_lsn(&self) -> Lsn {
        self.ancestor_lsn
    }

    /// Wait until WAL has been received up to the given LSN.
    fn wait_lsn(&self, lsn: Lsn) -> Result<()> {
        // This should never be called from the WAL receiver thread, because that could lead
        // to a deadlock.
        assert!(
            !IS_WAL_RECEIVER.with(|c| c.get()),
            "wait_lsn called by WAL receiver thread"
        );

        self.last_record_lsn
            .wait_for_timeout(lsn, TIMEOUT)
            .with_context(|| {
                format!(
                    "Timed out while waiting for WAL record at LSN {} to arrive, disk consistent LSN={}",
                    lsn, self.get_disk_consistent_lsn()
                )
            })?;

        Ok(())
    }

    /// Look up given page version.
    fn get_page_at_lsn(&self, rel: RelishTag, blknum: u32, lsn: Lsn) -> Result<Bytes> {
        if !rel.is_blocky() && blknum != 0 {
            bail!(
                "invalid request for block {} for non-blocky relish {}",
                blknum,
                rel
            );
        }
        debug_assert!(lsn <= self.get_last_record_lsn());

        let seg = SegmentTag::from_blknum(rel, blknum);

        if let Some((layer, lsn)) = self.get_layer_for_read(seg, lsn)? {
            RECONSTRUCT_TIME
                .observe_closure_duration(|| self.materialize_page(seg, blknum, lsn, &*layer))
        } else {
            bail!("relish {} not found at {}", rel, lsn);
        }
    }

    fn get_relish_size(&self, rel: RelishTag, lsn: Lsn) -> Result<Option<u32>> {
        if !rel.is_blocky() {
            bail!(
                "invalid get_relish_size request for non-blocky relish {}",
                rel
            );
        }
        debug_assert!(lsn <= self.get_last_record_lsn());

        let mut segno = 0;
        loop {
            let seg = SegmentTag { rel, segno };

            let segsize;
            if let Some((layer, lsn)) = self.get_layer_for_read(seg, lsn)? {
                segsize = layer.get_seg_size(lsn)?;
                trace!("get_seg_size: {} at {} -> {}", seg, lsn, segsize);
            } else {
                if segno == 0 {
                    return Ok(None);
                }
                segsize = 0;
            }

            if segsize != RELISH_SEG_SIZE {
                let result = segno * RELISH_SEG_SIZE + segsize;
                return Ok(Some(result));
            }
            segno += 1;
        }
    }

    fn get_rel_exists(&self, rel: RelishTag, lsn: Lsn) -> Result<bool> {
        debug_assert!(lsn <= self.get_last_record_lsn());

        let seg = SegmentTag { rel, segno: 0 };

        let result;
        if let Some((layer, lsn)) = self.get_layer_for_read(seg, lsn)? {
            result = layer.get_seg_exists(lsn)?;
        } else {
            result = false;
        }

        trace!("get_rel_exists: {} at {} -> {}", rel, lsn, result);
        Ok(result)
    }

    fn list_rels(&self, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelishTag>> {
        let request_tag = RelTag {
            spcnode,
            dbnode,
            relnode: 0,
            forknum: 0,
        };

        self.list_relishes(Some(request_tag), lsn)
    }

    fn list_nonrels(&self, lsn: Lsn) -> Result<HashSet<RelishTag>> {
        info!("list_nonrels called at {}", lsn);

        self.list_relishes(None, lsn)
    }

    fn list_relishes(&self, tag: Option<RelTag>, lsn: Lsn) -> Result<HashSet<RelishTag>> {
        trace!("list_relishes called at {}", lsn);
        debug_assert!(lsn <= self.get_last_record_lsn());

        // List of all relishes along with a flag that marks if they exist at the given lsn.
        let mut all_relishes_map: HashMap<RelishTag, bool> = HashMap::new();
        let mut result = HashSet::new();
        let mut timeline = self;

        // Iterate through layers back in time and find the most
        // recent state of the relish. Don't add relish to the list
        // if newer version is already there.
        //
        // This most recent version can represent dropped or existing relish.
        // We will filter dropped relishes below.
        //
        loop {
            let rels = timeline.layers.lock().unwrap().list_relishes(tag, lsn)?;

            for (&new_relish, &new_relish_exists) in rels.iter() {
                match all_relishes_map.entry(new_relish) {
                    Entry::Occupied(o) => {
                        trace!(
                            "Newer version of the object {} is already found: exists {}",
                            new_relish,
                            o.get(),
                        );
                    }
                    Entry::Vacant(v) => {
                        v.insert(new_relish_exists);
                        trace!(
                            "Newer version of the object {} NOT found. Insert NEW: exists {}",
                            new_relish,
                            new_relish_exists
                        );
                    }
                }
            }

            if let Some(ancestor) = timeline.ancestor_timeline.as_ref() {
                timeline = ancestor;
                continue;
            } else {
                break;
            }
        }

        // Filter out dropped relishes
        for (&new_relish, &new_relish_exists) in all_relishes_map.iter() {
            if new_relish_exists {
                result.insert(new_relish);
                trace!("List object {}", new_relish);
            } else {
                trace!("Filtered out dropped object {}", new_relish);
            }
        }

        Ok(result)
    }

    /// Public entry point for checkpoint(). All the logic is in the private
    /// checkpoint_internal function, this public facade just wraps it for
    /// metrics collection.
    fn checkpoint(&self) -> Result<()> {
        STORAGE_TIME
            .with_label_values(&["checkpoint_force"])
            //pass checkpoint_distance=0 to force checkpoint
            .observe_closure_duration(|| self.checkpoint_internal(0, true))
    }

    fn get_last_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load().last
    }

    fn get_prev_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load().prev
    }

    fn get_last_record_rlsn(&self) -> RecordLsn {
        self.last_record_lsn.load()
    }

    fn get_start_lsn(&self) -> Lsn {
        if let Some(ancestor) = self.ancestor_timeline.as_ref() {
            ancestor.get_start_lsn()
        } else {
            self.ancestor_lsn
        }
    }

    fn get_current_logical_size(&self) -> usize {
        self.current_logical_size.load(Ordering::Acquire) as usize
    }

    fn get_current_logical_size_non_incremental(&self, lsn: Lsn) -> Result<usize> {
        let mut total_blocks: usize = 0;

        let _enter = info_span!("calc logical size", %lsn).entered();

        // list of all relations in this timeline, including ancestor timelines
        let all_rels = self.list_rels(0, 0, lsn)?;

        for rel in all_rels {
            if let Some(size) = self.get_relish_size(rel, lsn)? {
                total_blocks += size as usize;
            }
        }

        let non_rels = self.list_nonrels(lsn)?;
        for non_rel in non_rels {
            // TODO support TwoPhase
            if matches!(non_rel, RelishTag::Slru { slru: _, segno: _ }) {
                if let Some(size) = self.get_relish_size(non_rel, lsn)? {
                    total_blocks += size as usize;
                }
            }
        }

        Ok(total_blocks * BLCKSZ as usize)
    }

    fn get_disk_consistent_lsn(&self) -> Lsn {
        self.disk_consistent_lsn.load()
	}

    fn writer<'a>(&'a self) -> Box<dyn TimelineWriter + 'a> {
        Box::new(LayeredTimelineWriter {
            tl: self,
            _write_guard: self.write_lock.lock().unwrap(),
        })
    }
}

impl LayeredTimeline {
    /// Open a Timeline handle.
    ///
    /// Loads the metadata for the timeline into memory, but not the layer map.
    #[allow(clippy::too_many_arguments)]
    fn new(
        conf: &'static PageServerConf,
        metadata: TimelineMetadata,
        ancestor: Option<Arc<LayeredTimeline>>,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        current_logical_size: usize,
        upload_relishes: bool,
    ) -> Result<LayeredTimeline> {
        let current_logical_size_gauge = LOGICAL_TIMELINE_SIZE
            .get_metric_with_label_values(&[&tenantid.to_string(), &timelineid.to_string()])
            .unwrap();
        let timeline = LayeredTimeline {
            conf,
            timelineid,
            tenantid,
            layers: Mutex::new(LayerMap::default()),

            walredo_mgr,

            // initialize in-memory 'last_record_lsn' from 'disk_consistent_lsn'.
            last_record_lsn: SeqWait::new(RecordLsn {
                last: metadata.disk_consistent_lsn,
                prev: metadata.prev_record_lsn.unwrap_or(Lsn(0)),
            }),
            disk_consistent_lsn: AtomicLsn::new(metadata.disk_consistent_lsn.0),

            ancestor_timeline: ancestor,
            ancestor_lsn: metadata.ancestor_lsn,
            current_logical_size: AtomicUsize::new(current_logical_size),
            current_logical_size_gauge,
            upload_relishes,

            write_lock: Mutex::new(()),
        };
        Ok(timeline)
    }

    ///
    /// Scan the timeline directory to populate the layer map.
    /// Returns all timeline-related files that were found and loaded.
    ///
    fn load_layer_map(&self, disk_consistent_lsn: Lsn) -> anyhow::Result<Vec<PathBuf>> {
        let mut layers = self.layers.lock().unwrap();
        let mut num_layers = 0;
        let (imgfilenames, deltafilenames) =
            filename::list_files(self.conf, self.timelineid, self.tenantid)?;

        let timeline_path = self.conf.timeline_path(&self.timelineid, &self.tenantid);
        let mut local_layers = Vec::with_capacity(imgfilenames.len() + deltafilenames.len());
        // First create ImageLayer structs for each image file.
        for filename in &imgfilenames {
            if filename.lsn > disk_consistent_lsn {
                warn!(
                    "found future image layer {} on timeline {}",
                    filename, self.timelineid
                );

                rename_to_backup(timeline_path.join(filename.to_string()))?;
                continue;
            }

            let layer = ImageLayer::new(self.conf, self.timelineid, self.tenantid, filename);

            trace!("found layer {}", layer.filename().display());
            local_layers.push(layer.path());
            layers.insert_historic(Arc::new(layer));
            num_layers += 1;
        }

        for filename in &deltafilenames {
            ensure!(filename.start_lsn < filename.end_lsn);
            if filename.end_lsn > disk_consistent_lsn {
                warn!(
                    "found future delta layer {} on timeline {}",
                    filename, self.timelineid
                );

                rename_to_backup(timeline_path.join(filename.to_string()))?;
                continue;
            }

            let layer = DeltaLayer::new(self.conf, self.timelineid, self.tenantid, filename);

            trace!("found layer {}", layer.filename().display());
            local_layers.push(layer.path());
            layers.insert_historic(Arc::new(layer));
            num_layers += 1;
        }
        info!("loaded layer map with {} layers", num_layers);

        Ok(local_layers)
    }

    ///
    /// Used to init current logical size on startup
    ///
    fn init_current_logical_size(&mut self) -> Result<()> {
        if self.current_logical_size.load(Ordering::Relaxed) != 0 {
            bail!("cannot init already initialized current logical size")
        };
        let lsn = self.get_last_record_lsn();
        self.current_logical_size =
            AtomicUsize::new(self.get_current_logical_size_non_incremental(lsn)?);
        trace!(
            "current_logical_size initialized to {}",
            self.current_logical_size.load(Ordering::Relaxed)
        );
        Ok(())
    }

    ///
    /// Get a handle to a Layer for reading.
    ///
    /// The returned Layer might be from an ancestor timeline, if the
    /// segment hasn't been updated on this timeline yet.
    ///
    fn get_layer_for_read(
        &self,
        seg: SegmentTag,
        lsn: Lsn,
    ) -> Result<Option<(Arc<dyn Layer>, Lsn)>> {
        let self_layers = self.layers.lock().unwrap();
        self.get_layer_for_read_locked(seg, lsn, &self_layers)
    }

    ///
    /// Get a handle to a Layer for reading.
    ///
    /// The returned Layer might be from an ancestor timeline, if the
    /// segment hasn't been updated on this timeline yet.
    ///
    /// This function takes the current timeline's locked LayerMap as an argument,
    /// so callers can avoid potential race conditions.
    fn get_layer_for_read_locked(
        &self,
        seg: SegmentTag,
        lsn: Lsn,
        self_layers: &MutexGuard<LayerMap>,
    ) -> Result<Option<(Arc<dyn Layer>, Lsn)>> {
        trace!("get_layer_for_read called for {} at {}", seg, lsn);

        // If you requested a page at an older LSN, before the branch point, dig into
        // the right ancestor timeline. This can only happen if you launch a read-only
        // node with an old LSN, a primary always uses a recent LSN in its requests.
        let mut timeline = self;
        let mut lsn = lsn;

        while lsn < timeline.ancestor_lsn {
            trace!("going into ancestor {} ", timeline.ancestor_lsn);
            timeline = timeline.ancestor_timeline.as_ref().unwrap();
        }

        // Now we have the right starting timeline for our search.
        loop {
            let layers_owned: MutexGuard<LayerMap>;
            let layers = if self as *const LayeredTimeline != timeline as *const LayeredTimeline {
                layers_owned = timeline.layers.lock().unwrap();
                &layers_owned
            } else {
                self_layers
            };

            //
            // FIXME: If the relation has been dropped, does this return the right
            // thing? The compute node should not normally request dropped relations,
            // but if OID wraparound happens the same relfilenode might get reused
            // for an unrelated relation.
            //

            // Do we have a layer on this timeline?
            if let Some(layer) = layers.get(&seg, lsn) {
                trace!(
                    "found layer in cache: {} {}-{}",
                    timeline.timelineid,
                    layer.get_start_lsn(),
                    layer.get_end_lsn()
                );

                assert!(layer.get_start_lsn() <= lsn);

                if layer.is_dropped() && layer.get_end_lsn() <= lsn {
                    return Ok(None);
                }

                return Ok(Some((layer.clone(), lsn)));
            }

            // If not, check if there's a layer on the ancestor timeline
            if let Some(ancestor) = &timeline.ancestor_timeline {
                lsn = timeline.ancestor_lsn;
                timeline = ancestor.as_ref();
                trace!("recursing into ancestor at {}/{}", timeline.timelineid, lsn);
                continue;
            }
            return Ok(None);
        }
    }

    ///
    /// Get a handle to the latest layer for appending.
    ///
    fn get_layer_for_write(&self, seg: SegmentTag, lsn: Lsn) -> Result<Arc<InMemoryLayer>> {
        let mut layers = self.layers.lock().unwrap();

        assert!(lsn.is_aligned());

        let last_record_lsn = self.get_last_record_lsn();
        assert!(
            lsn > last_record_lsn,
            "cannot modify relation after advancing last_record_lsn (incoming_lsn={}, last_record_lsn={})",
            lsn,
            last_record_lsn,
        );

        // Do we have a layer open for writing already?
        let layer;
        if let Some(open_layer) = layers.get_open(&seg) {
            if open_layer.get_start_lsn() > lsn {
                bail!("unexpected open layer in the future");
            }

            // Open layer exists, but it is dropped, so create a new one.
            if open_layer.is_dropped() {
                assert!(!open_layer.is_writeable());
                // Layer that is created after dropped one represents a new relish segment.
                trace!(
                    "creating layer for write for new relish segment after dropped layer {} at {}/{}",
                    seg,
                    self.timelineid,
                    lsn
                );

                layer = InMemoryLayer::create(
                    self.conf,
                    self.timelineid,
                    self.tenantid,
                    seg,
                    lsn,
                    lsn,
                )?;
            } else {
                return Ok(open_layer);
            }
        }
        // No writeable layer for this relation. Create one.
        //
        // Is this a completely new relation? Or the first modification after branching?
        //
        else if let Some((prev_layer, _prev_lsn)) =
            self.get_layer_for_read_locked(seg, lsn, &layers)?
        {
            // Create new entry after the previous one.
            let start_lsn;
            if prev_layer.get_timeline_id() != self.timelineid {
                // First modification on this timeline
                start_lsn = self.ancestor_lsn + 1;
                trace!(
                    "creating layer for write for {} at branch point {}",
                    seg,
                    start_lsn
                );
            } else {
                start_lsn = prev_layer.get_end_lsn();
                trace!(
                    "creating layer for write for {} after previous layer {}",
                    seg,
                    start_lsn
                );
            }
            trace!(
                "prev layer is at {}/{} - {}",
                prev_layer.get_timeline_id(),
                prev_layer.get_start_lsn(),
                prev_layer.get_end_lsn()
            );
            layer = InMemoryLayer::create_successor_layer(
                self.conf,
                prev_layer,
                self.timelineid,
                self.tenantid,
                start_lsn,
                lsn,
            )?;
        } else {
            // New relation.
            trace!(
                "creating layer for write for new rel {} at {}/{}",
                seg,
                self.timelineid,
                lsn
            );

            layer =
                InMemoryLayer::create(self.conf, self.timelineid, self.tenantid, seg, lsn, lsn)?;
        }

        let layer_rc: Arc<InMemoryLayer> = Arc::new(layer);
        layers.insert_open(Arc::clone(&layer_rc));

        Ok(layer_rc)
    }

    ///
    /// Flush to disk all data that was written with the put_* functions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL.
    fn checkpoint_internal(&self, checkpoint_distance: u64, forced: bool) -> Result<()> {
        let mut write_guard = self.write_lock.lock().unwrap();
        let mut layers = self.layers.lock().unwrap();

        // Bump the generation number in the layer map, so that we can distinguish
        // entries inserted after the checkpoint started
        let current_generation = layers.increment_generation();

        let RecordLsn {
            last: last_record_lsn,
            prev: prev_record_lsn,
        } = self.last_record_lsn.load();

        trace!("checkpoint starting at {}", last_record_lsn);

        // Take the in-memory layer with the oldest WAL record. If it's older
        // than the threshold, write it out to disk as a new image and delta file.
        // Repeat until all remaining in-memory layers are within the threshold.
        //
        // That's necessary to limit the amount of WAL that needs to be kept
        // in the safekeepers, and that needs to be reprocessed on page server
        // crash. TODO: It's not a great policy for keeping memory usage in
        // check, though. We should also aim at flushing layers that consume
        // a lot of memory and/or aren't receiving much updates anymore.
        let mut disk_consistent_lsn = last_record_lsn;

        let mut created_historics = false;
        let mut layer_uploads = Vec::new();
        while let Some((oldest_layer, oldest_generation)) = layers.peek_oldest_open() {
            let oldest_pending_lsn = oldest_layer.get_oldest_pending_lsn();

            if tenant_mgr::shutdown_requested() && !forced {
                return Ok(());
            }

            // Does this layer need freezing?
            //
            // Write out all in-memory layers that contain WAL older than CHECKPOINT_DISTANCE.
            // If we reach a layer with the same
            // generation number, we know that we have cycled through all layers that were open
            // when we started. We don't want to process layers inserted after we started, to
            // avoid getting into an infinite loop trying to process again entries that we
            // inserted ourselves.
            let distance = last_record_lsn.widening_sub(oldest_pending_lsn);
            if distance < 0
                || distance < checkpoint_distance.into()
                || oldest_generation == current_generation
            {
                info!(
                    "the oldest layer is now {} which is {} bytes behind last_record_lsn",
                    oldest_layer.filename().display(),
                    distance
                );
                disk_consistent_lsn = oldest_pending_lsn;
                break;
            }

            // Mark the layer as no longer accepting writes and record the end_lsn.
            // This happens in-place, no new layers are created now.
            // We call `get_last_record_lsn` again, which may be different from the
            // original load, as we may have released the write lock since then.
            oldest_layer.freeze(self.get_last_record_lsn());

            // The layer is no longer open, update the layer map to reflect this.
            // We will replace it with on-disk historics below.
            layers.pop_oldest_open();
            layers.insert_historic(oldest_layer.clone());

            // Write the now-frozen layer to disk. That could take a while, so release the lock while do it
            drop(layers);
            drop(write_guard);

            let new_historics = oldest_layer.write_to_disk(self)?;

            write_guard = self.write_lock.lock().unwrap();
            layers = self.layers.lock().unwrap();

            if !new_historics.is_empty() {
                created_historics = true;
            }

            // Finally, replace the frozen in-memory layer with the new on-disk layers
            layers.remove_historic(oldest_layer);

            // Add the historics to the LayerMap
            for delta_layer in new_historics.delta_layers {
                layer_uploads.push(delta_layer.path());
                layers.insert_historic(Arc::new(delta_layer));
            }
            for image_layer in new_historics.image_layers {
                layer_uploads.push(image_layer.path());
                layers.insert_historic(Arc::new(image_layer));
            }
        }

        // Call unload() on all frozen layers, to release memory.
        // This shouldn't be much memory, as only metadata is slurped
        // into memory.
        for layer in layers.iter_historic_layers() {
            layer.unload()?;
        }

        drop(layers);
        drop(write_guard);

        if created_historics {
            // We must fsync the timeline dir to ensure the directory entries for
            // new layer files are durable
            let timeline_dir =
                File::open(self.conf.timeline_path(&self.timelineid, &self.tenantid))?;
            timeline_dir.sync_all()?;
        }

        // If we were able to advance 'disk_consistent_lsn', save it the metadata file.
        // After crash, we will restart WAL streaming and processing from that point.
        let old_disk_consistent_lsn = self.disk_consistent_lsn.load();
        if disk_consistent_lsn != old_disk_consistent_lsn {
            assert!(disk_consistent_lsn > old_disk_consistent_lsn);

            // We can only save a valid 'prev_record_lsn' value on disk if we
            // flushed *all* in-memory changes to disk. We only track
            // 'prev_record_lsn' in memory for the latest processed record, so we
            // don't remember what the correct value that corresponds to some old
            // LSN is. But if we flush everything, then the value corresponding
            // current 'last_record_lsn' is correct and we can store it on disk.
            let ondisk_prev_record_lsn = if disk_consistent_lsn == last_record_lsn {
                Some(prev_record_lsn)
            } else {
                None
            };

            let ancestor_timelineid = self.ancestor_timeline.as_ref().map(|x| x.timelineid);

            let metadata = TimelineMetadata {
                disk_consistent_lsn,
                prev_record_lsn: ondisk_prev_record_lsn,
                ancestor_timeline: ancestor_timelineid,
                ancestor_lsn: self.ancestor_lsn,
            };
            LayeredRepository::save_metadata(
                self.conf,
                self.timelineid,
                self.tenantid,
                &metadata,
                false,
            )?;

            // Also update the in-memory copy
            self.disk_consistent_lsn.store(disk_consistent_lsn);

            if self.upload_relishes {
                schedule_timeline_upload(())
                // schedule_timeline_upload(
                //     self.tenantid,
                //     self.timelineid,
                //     layer_uploads,
                //     disk_consistent_lsn,
                // });
            }
        }

        Ok(())
    }

    ///
    /// Garbage collect layer files on a timeline that are no longer needed.
    ///
    /// The caller specifies how much history is needed with the two arguments:
    ///
    /// retain_lsns: keep a version of each page at these LSNs
    /// cutoff: also keep everything newer than this LSN
    ///
    /// The 'retain_lsns' list is currently used to prevent removing files that
    /// are needed by child timelines. In the future, the user might be able to
    /// name additional points in time to retain. The caller is responsible for
    /// collecting that information.
    ///
    /// The 'cutoff' point is used to retain recent versions that might still be
    /// needed by read-only nodes. (As of this writing, the caller just passes
    /// the latest LSN subtracted by a constant, and doesn't do anything smart
    /// to figure out what read-only nodes might actually need.)
    ///
    /// Currently, we don't make any attempt at removing unneeded page versions
    /// within a layer file. We can only remove the whole file if it's fully
    /// obsolete.
    ///
    pub fn gc_timeline(&self, retain_lsns: Vec<Lsn>, cutoff: Lsn) -> Result<GcResult> {
        let now = Instant::now();
        let mut result: GcResult = Default::default();

        let _enter = info_span!("garbage collection", timeline = %self.timelineid, tenant = %self.tenantid, cutoff = %cutoff).entered();

        info!("GC starting");

        debug!("retain_lsns: {:?}", retain_lsns);

        let mut layers_to_remove: Vec<Arc<dyn Layer>> = Vec::new();

        // Scan all on-disk layers in the timeline.
        //
        // Garbage collect the layer if all conditions are satisfied:
        // 1. it is older than cutoff LSN;
        // 2. it doesn't need to be retained for 'retain_lsns';
        // 3. newer on-disk layer exists (only for non-dropped segments);
        // 4. this layer doesn't serve as a tombstone for some older layer;
        //
        let mut layers = self.layers.lock().unwrap();
        'outer: for l in layers.iter_historic_layers() {
            let seg = l.get_seg_tag();

            if seg.rel.is_relation() {
                result.ondisk_relfiles_total += 1;
            } else {
                result.ondisk_nonrelfiles_total += 1;
            }

            // 1. Is it newer than cutoff point?
            if l.get_end_lsn() > cutoff {
                info!(
                    "keeping {} {}-{} because it's newer than cutoff {}",
                    seg,
                    l.get_start_lsn(),
                    l.get_end_lsn(),
                    cutoff
                );
                if seg.rel.is_relation() {
                    result.ondisk_relfiles_needed_by_cutoff += 1;
                } else {
                    result.ondisk_nonrelfiles_needed_by_cutoff += 1;
                }
                continue 'outer;
            }

            // 2. Is it needed by a child branch?
            for retain_lsn in &retain_lsns {
                // start_lsn is inclusive and end_lsn is exclusive
                if l.get_start_lsn() <= *retain_lsn && *retain_lsn < l.get_end_lsn() {
                    info!(
                        "keeping {} {}-{} because it's needed by branch point {}",
                        seg,
                        l.get_start_lsn(),
                        l.get_end_lsn(),
                        *retain_lsn
                    );
                    if seg.rel.is_relation() {
                        result.ondisk_relfiles_needed_by_branches += 1;
                    } else {
                        result.ondisk_nonrelfiles_needed_by_branches += 1;
                    }
                    continue 'outer;
                }
            }

            // 3. Is there a later on-disk layer for this relation?
            if !l.is_dropped() && !layers.newer_image_layer_exists(l.get_seg_tag(), l.get_end_lsn())
            {
                info!(
                    "keeping {} {}-{} because it is the latest layer",
                    seg,
                    l.get_start_lsn(),
                    l.get_end_lsn()
                );
                if seg.rel.is_relation() {
                    result.ondisk_relfiles_not_updated += 1;
                } else {
                    result.ondisk_nonrelfiles_not_updated += 1;
                }
                continue 'outer;
            }

            // 4. Does this layer serve as a tombstome for some older layer?
            if l.is_dropped() {
                let prior_lsn = l.get_start_lsn().checked_sub(1u64).unwrap();

                // Check if this layer serves as a tombstone for this timeline
                // We have to do this separately from timeline check below,
                // because LayerMap of this timeline is already locked.
                let mut is_tombstone = layers.layer_exists_at_lsn(l.get_seg_tag(), prior_lsn)?;
                if is_tombstone {
                    info!(
                        "earlier layer exists at {} in {}",
                        prior_lsn, self.timelineid
                    );
                }
                // Now check ancestor timelines, if any
                else if let Some(ancestor) = &self.ancestor_timeline {
                    let prior_lsn = ancestor.get_last_record_lsn();
                    if seg.rel.is_blocky() {
                        info!(
                            "check blocky relish size {} at {} in {} for layer {}-{}",
                            seg,
                            prior_lsn,
                            ancestor.timelineid,
                            l.get_start_lsn(),
                            l.get_end_lsn()
                        );
                        match ancestor.get_relish_size(seg.rel, prior_lsn).unwrap() {
                            Some(size) => {
                                let last_live_seg = SegmentTag::from_blknum(seg.rel, size - 1);
                                info!(
                                    "blocky rel size is {} last_live_seg.segno {} seg.segno {}",
                                    size, last_live_seg.segno, seg.segno
                                );
                                if last_live_seg.segno >= seg.segno {
                                    is_tombstone = true;
                                }
                            }
                            _ => {
                                info!("blocky rel doesn't exist");
                            }
                        }
                    } else {
                        info!(
                            "check non-blocky relish existence {} at {} in {} for layer {}-{}",
                            seg,
                            prior_lsn,
                            ancestor.timelineid,
                            l.get_start_lsn(),
                            l.get_end_lsn()
                        );
                        is_tombstone = ancestor.get_rel_exists(seg.rel, prior_lsn).unwrap_or(false);
                    }
                }

                if is_tombstone {
                    info!(
                        "keeping {} {}-{} because this layer servers as a tombstome for older layer",
                        seg,
                        l.get_start_lsn(),
                        l.get_end_lsn()
                    );

                    if seg.rel.is_relation() {
                        result.ondisk_relfiles_needed_as_tombstone += 1;
                    } else {
                        result.ondisk_nonrelfiles_needed_as_tombstone += 1;
                    }
                    continue 'outer;
                }
            }

            // We didn't find any reason to keep this file, so remove it.
            info!(
                "garbage collecting {} {}-{} {}",
                l.get_seg_tag(),
                l.get_start_lsn(),
                l.get_end_lsn(),
                l.is_dropped()
            );
            layers_to_remove.push(Arc::clone(&l));
        }

        // Actually delete the layers from disk and remove them from the map.
        // (couldn't do this in the loop above, because you cannot modify a collection
        // while iterating it. BTreeMap::retain() would be another option)
        for doomed_layer in layers_to_remove {
            doomed_layer.delete()?;
            layers.remove_historic(doomed_layer.clone());

            match (
                doomed_layer.is_dropped(),
                doomed_layer.get_seg_tag().rel.is_relation(),
            ) {
                (true, true) => result.ondisk_relfiles_dropped += 1,
                (true, false) => result.ondisk_nonrelfiles_dropped += 1,
                (false, true) => result.ondisk_relfiles_removed += 1,
                (false, false) => result.ondisk_nonrelfiles_removed += 1,
            }
        }

        result.elapsed = now.elapsed();
        Ok(result)
    }

    ///
    /// Reconstruct a page version from given Layer
    ///
    fn materialize_page(
        &self,
        seg: SegmentTag,
        blknum: u32,
        lsn: Lsn,
        layer: &dyn Layer,
    ) -> Result<Bytes> {
        let mut data = PageReconstructData {
            records: Vec::new(),
            page_img: None,
        };

        // Holds an Arc reference to 'layer_ref' when iterating in the loop below.
        let mut layer_arc: Arc<dyn Layer>;

        // Call the layer's get_page_reconstruct_data function to get the base image
        // and WAL records needed to materialize the page. If it returns 'Continue',
        // call it again on the predecessor layer until we have all the required data.
        let mut layer_ref = layer;
        let mut curr_lsn = lsn;
        loop {
            match layer_ref.get_page_reconstruct_data(blknum, curr_lsn, &mut data)? {
                PageReconstructResult::Complete => break,
                PageReconstructResult::Continue(cont_lsn) => {
                    // Fetch base image / more WAL from the returned predecessor layer
                    if let Some((cont_layer, cont_lsn)) = self.get_layer_for_read(seg, cont_lsn)? {
                        layer_arc = cont_layer;
                        layer_ref = &*layer_arc;
                        curr_lsn = cont_lsn;
                        continue;
                    } else {
                        bail!(
                            "could not find predecessor layer of segment {} at {}",
                            seg.rel,
                            cont_lsn
                        );
                    }
                }
                PageReconstructResult::Missing(lsn) => {
                    // Oops, we could not reconstruct the page.
                    if data.records.is_empty() {
                        // no records, and no base image. This can happen if PostgreSQL extends a relation
                        // but never writes the page.
                        //
                        // Would be nice to detect that situation better.
                        warn!("Page {} blk {} at {} not found", seg.rel, blknum, lsn);
                        return Ok(ZERO_PAGE.clone());
                    }
                    bail!(
                        "No base image found for page {} blk {} at {}/{}",
                        seg.rel,
                        blknum,
                        self.timelineid,
                        lsn,
                    );
                }
            }
        }

        self.reconstruct_page(seg.rel, blknum, lsn, data)
    }

    ///
    /// Reconstruct a page version, using the given base image and WAL records in 'data'.
    ///
    fn reconstruct_page(
        &self,
        rel: RelishTag,
        blknum: u32,
        request_lsn: Lsn,
        mut data: PageReconstructData,
    ) -> Result<Bytes> {
        // Perform WAL redo if needed
        data.records.reverse();

        // If we have a page image, and no WAL, we're all set
        if data.records.is_empty() {
            if let Some(img) = &data.page_img {
                trace!(
                    "found page image for blk {} in {} at {}, no WAL redo required",
                    blknum,
                    rel,
                    request_lsn
                );
                Ok(img.clone())
            } else {
                // FIXME: this ought to be an error?
                warn!("Page {} blk {} at {} not found", rel, blknum, request_lsn);
                Ok(ZERO_PAGE.clone())
            }
        } else {
            // We need to do WAL redo.
            //
            // If we don't have a base image, then the oldest WAL record better initialize
            // the page
            if data.page_img.is_none() && !data.records.first().unwrap().1.will_init {
                // FIXME: this ought to be an error?
                warn!(
                    "Base image for page {}/{} at {} not found, but got {} WAL records",
                    rel,
                    blknum,
                    request_lsn,
                    data.records.len()
                );
                Ok(ZERO_PAGE.clone())
            } else {
                if data.page_img.is_some() {
                    trace!("found {} WAL records and a base image for blk {} in {} at {}, performing WAL redo", data.records.len(), blknum, rel, request_lsn);
                } else {
                    trace!("found {} WAL records that will init the page for blk {} in {} at {}, performing WAL redo", data.records.len(), blknum, rel, request_lsn);
                }
                let img = self.walredo_mgr.request_redo(
                    rel,
                    blknum,
                    request_lsn,
                    data.page_img.clone(),
                    data.records,
                )?;

                Ok(img)
            }
        }
    }

    ///
    /// This is a helper function to increase current_total_relation_size
    ///
    fn increase_current_logical_size(&self, diff: u32) {
        let val = self
            .current_logical_size
            .fetch_add(diff as usize, Ordering::SeqCst);
        trace!(
            "increase_current_logical_size: {} + {} = {}",
            val,
            diff,
            val + diff as usize,
        );
        self.current_logical_size_gauge
            .set(val as i64 + diff as i64);
    }

    ///
    /// This is a helper function to decrease current_total_relation_size
    ///
    fn decrease_current_logical_size(&self, diff: u32) {
        let val = self
            .current_logical_size
            .fetch_sub(diff as usize, Ordering::SeqCst);
        trace!(
            "decrease_current_logical_size: {} - {} = {}",
            val,
            diff,
            val - diff as usize,
        );
        self.current_logical_size_gauge
            .set(val as i64 - diff as i64);
    }
}

struct LayeredTimelineWriter<'a> {
    tl: &'a LayeredTimeline,
    _write_guard: MutexGuard<'a, ()>,
}

impl Deref for LayeredTimelineWriter<'_> {
    type Target = dyn Timeline;

    fn deref(&self) -> &Self::Target {
        self.tl
    }
}

impl<'a> TimelineWriter for LayeredTimelineWriter<'a> {
    fn put_wal_record(&self, lsn: Lsn, rel: RelishTag, blknum: u32, rec: WALRecord) -> Result<()> {
        if !rel.is_blocky() && blknum != 0 {
            bail!(
                "invalid request for block {} for non-blocky relish {}",
                blknum,
                rel
            );
        }
        ensure!(lsn.is_aligned(), "unaligned record LSN");

        let seg = SegmentTag::from_blknum(rel, blknum);
        let layer = self.tl.get_layer_for_write(seg, lsn)?;
        let delta_size = layer.put_wal_record(lsn, blknum, rec);
        self.tl
            .increase_current_logical_size(delta_size * BLCKSZ as u32);
        Ok(())
    }

    fn put_page_image(&self, rel: RelishTag, blknum: u32, lsn: Lsn, img: Bytes) -> Result<()> {
        if !rel.is_blocky() && blknum != 0 {
            bail!(
                "invalid request for block {} for non-blocky relish {}",
                blknum,
                rel
            );
        }
        ensure!(lsn.is_aligned(), "unaligned record LSN");

        let seg = SegmentTag::from_blknum(rel, blknum);

        let layer = self.tl.get_layer_for_write(seg, lsn)?;
        let delta_size = layer.put_page_image(blknum, lsn, img);

        self.tl
            .increase_current_logical_size(delta_size * BLCKSZ as u32);
        Ok(())
    }

    fn put_truncation(&self, rel: RelishTag, lsn: Lsn, relsize: u32) -> Result<()> {
        if !rel.is_blocky() {
            bail!("invalid truncation for non-blocky relish {}", rel);
        }
        ensure!(lsn.is_aligned(), "unaligned record LSN");

        debug!("put_truncation: {} to {} blocks at {}", rel, relsize, lsn);

        let oldsize = self
            .tl
            .get_relish_size(rel, self.tl.get_last_record_lsn())?
            .ok_or_else(|| {
                anyhow!(
                    "attempted to truncate non-existent relish {} at {}",
                    rel,
                    lsn
                )
            })?;

        if oldsize <= relsize {
            return Ok(());
        }
        let old_last_seg = (oldsize - 1) / RELISH_SEG_SIZE;

        let last_remain_seg = if relsize == 0 {
            0
        } else {
            (relsize - 1) / RELISH_SEG_SIZE
        };

        // Drop segments beyond the last remaining segment.
        for remove_segno in (last_remain_seg + 1)..=old_last_seg {
            let seg = SegmentTag {
                rel,
                segno: remove_segno,
            };

            let layer = self.tl.get_layer_for_write(seg, lsn)?;
            layer.drop_segment(lsn);
        }

        // Truncate the last remaining segment to the specified size
        if relsize == 0 || relsize % RELISH_SEG_SIZE != 0 {
            let seg = SegmentTag {
                rel,
                segno: last_remain_seg,
            };
            let layer = self.tl.get_layer_for_write(seg, lsn)?;
            layer.put_truncation(lsn, relsize % RELISH_SEG_SIZE)
        }
        self.tl
            .decrease_current_logical_size((oldsize - relsize) * BLCKSZ as u32);
        Ok(())
    }

    fn drop_relish(&self, rel: RelishTag, lsn: Lsn) -> Result<()> {
        trace!("drop_segment: {} at {}", rel, lsn);

        if rel.is_blocky() {
            if let Some(oldsize) = self
                .tl
                .get_relish_size(rel, self.tl.get_last_record_lsn())?
            {
                let old_last_seg = if oldsize == 0 {
                    0
                } else {
                    (oldsize - 1) / RELISH_SEG_SIZE
                };

                // Drop all segments of the relish
                for remove_segno in 0..=old_last_seg {
                    let seg = SegmentTag {
                        rel,
                        segno: remove_segno,
                    };
                    let layer = self.tl.get_layer_for_write(seg, lsn)?;
                    layer.drop_segment(lsn);
                }
                self.tl
                    .decrease_current_logical_size(oldsize * BLCKSZ as u32);
            } else {
                warn!(
                    "drop_segment called on non-existent relish {} at {}",
                    rel, lsn
                );
            }
        } else {
            // TODO handle TwoPhase relishes
            let seg = SegmentTag::from_blknum(rel, 0);
            let layer = self.tl.get_layer_for_write(seg, lsn)?;
            layer.drop_segment(lsn);
        }

        Ok(())
    }

    ///
    /// Remember the (end of) last valid WAL record remembered in the timeline.
    ///
    fn advance_last_record_lsn(&self, new_lsn: Lsn) {
        assert!(new_lsn.is_aligned());

        self.tl.last_record_lsn.advance(new_lsn);
    }
}

/// Dump contents of a layer file to stdout.
pub fn dump_layerfile_from_path(path: &Path) -> Result<()> {
    let file = File::open(path)?;
    let book = Book::new(file)?;

    match book.magic() {
        delta_layer::DELTA_FILE_MAGIC => {
            DeltaLayer::new_for_path(path, &book)?.dump()?;
        }
        image_layer::IMAGE_FILE_MAGIC => {
            ImageLayer::new_for_path(path, &book)?.dump()?;
        }
        magic => bail!("unrecognized magic identifier: {:?}", magic),
    }

    Ok(())
}

fn metadata_path(
    conf: &'static PageServerConf,
    timelineid: ZTimelineId,
    tenantid: ZTenantId,
) -> PathBuf {
    conf.timeline_path(&timelineid, &tenantid)
        .join(METADATA_FILE_NAME)
}

/// Add a suffix to a layer file's name: .{num}.old
/// Uses the first available num (starts at 0)
fn rename_to_backup(path: PathBuf) -> anyhow::Result<()> {
    let filename = path.file_name().unwrap().to_str().unwrap();
    let mut new_path = path.clone();

    for i in 0u32.. {
        new_path.set_file_name(format!("{}.{}.old", filename, i));
        if !new_path.exists() {
            std::fs::rename(&path, &new_path)?;
            return Ok(());
        }
    }

    Err(anyhow!(
        "couldn't find an unused backup number for {:?}",
        path
    ))
}
