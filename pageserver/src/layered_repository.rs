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
use tracing::*;

use std::cmp;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::{Bound::Included, Deref};
use std::path::{Path, PathBuf};
use std::sync::atomic::{self, AtomicBool, AtomicUsize};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use self::metadata::{metadata_path, TimelineMetadata, METADATA_FILE_NAME};
use crate::config::PageServerConf;
use crate::page_cache;
use crate::relish::*;
use crate::remote_storage::{schedule_timeline_checkpoint_upload, schedule_timeline_download};
use crate::repository::{
    BlockNumber, GcResult, Repository, RepositoryTimeline, Timeline, TimelineSyncState,
    TimelineWriter, ZenithWalRecord,
};
use crate::tenant_mgr;
use crate::walreceiver;
use crate::walreceiver::IS_WAL_RECEIVER;
use crate::walredo::WalRedoManager;
use crate::CheckpointConfig;
use crate::{ZTenantId, ZTimelineId};

use zenith_metrics::{
    register_histogram, register_int_gauge_vec, Histogram, IntGauge, IntGaugeVec,
};
use zenith_metrics::{register_histogram_vec, HistogramVec};
use zenith_utils::crashsafe_dir;
use zenith_utils::lsn::{AtomicLsn, Lsn, RecordLsn};
use zenith_utils::seqwait::SeqWait;

mod delta_layer;
mod ephemeral_file;
mod filename;
mod global_layer_map;
mod image_layer;
mod inmemory_layer;
mod interval_tree;
mod layer_map;
pub mod metadata;
mod page_versions;
mod par_fsync;
mod storage_layer;

use delta_layer::DeltaLayer;
use ephemeral_file::is_ephemeral_file;
use filename::{DeltaFileName, ImageFileName};
use global_layer_map::{LayerId, GLOBAL_LAYER_MAP};
use image_layer::ImageLayer;
use inmemory_layer::InMemoryLayer;
use layer_map::LayerMap;
use storage_layer::{
    Layer, PageReconstructData, PageReconstructResult, SegmentBlk, SegmentTag, RELISH_SEG_SIZE,
};

// re-export this function so that page_cache.rs can use it.
pub use crate::layered_repository::ephemeral_file::writeback as writeback_ephemeral_file;

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

// Timeout when waiting for WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(60);

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

/// Parts of the `.zenith/tenants/<tenantid>/timelines/<timelineid>` directory prefix.
pub const TIMELINES_SEGMENT_NAME: &str = "timelines";

///
/// Repository consists of multiple timelines. Keep them in a hash table.
///
pub struct LayeredRepository {
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    timelines: Mutex<HashMap<ZTimelineId, LayeredTimelineEntry>>,
    // This mutex prevents creation of new timelines during GC.
    // Adding yet another mutex (in addition to `timelines`) is needed because holding
    // `timelines` mutex during all GC iteration (especially with enforced checkpoint)
    // may block for a long time `get_timeline`, `get_timelines_state`,... and other operations
    // with timelines, which in turn may cause dropping replication connection, expiration of wait_for_lsn
    // timeout...
    gc_cs: Mutex<()>,
    walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
    /// Makes every timeline to backup their files to remote storage.
    upload_relishes: bool,
}

/// Public interface
impl Repository for LayeredRepository {
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<RepositoryTimeline> {
        let mut timelines = self.timelines.lock().unwrap();
        Ok(
            match self.get_or_init_timeline(timelineid, &mut timelines)? {
                LayeredTimelineEntry::Local(local) => RepositoryTimeline::Local(local),
                LayeredTimelineEntry::Remote {
                    id,
                    disk_consistent_lsn,
                    awaits_download,
                } => RepositoryTimeline::Remote {
                    id,
                    disk_consistent_lsn,
                    awaits_download,
                },
            },
        )
    }

    fn create_empty_timeline(
        &self,
        timelineid: ZTimelineId,
        initdb_lsn: Lsn,
    ) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        // Create the timeline directory, and write initial metadata to file.
        crashsafe_dir::create_dir_all(self.conf.timeline_path(&timelineid, &self.tenantid))?;

        let metadata = TimelineMetadata::new(Lsn(0), None, None, Lsn(0), Lsn(0), initdb_lsn);
        Self::save_metadata(self.conf, timelineid, self.tenantid, &metadata, true)?;

        let timeline = LayeredTimeline::new(
            self.conf,
            metadata,
            None,
            timelineid,
            self.tenantid,
            Arc::clone(&self.walredo_mgr),
            0,
            self.upload_relishes,
        );

        let timeline_rc = Arc::new(timeline);
        let r = timelines.insert(timelineid, LayeredTimelineEntry::Local(timeline_rc.clone()));
        assert!(r.is_none());
        Ok(timeline_rc)
    }

    /// Branch a timeline
    fn branch_timeline(&self, src: ZTimelineId, dst: ZTimelineId, start_lsn: Lsn) -> Result<()> {
        // We need to hold this lock to prevent GC from starting at the same time. GC scans the directory to learn
        // about timelines, so otherwise a race condition is possible, where we create new timeline and GC
        // concurrently removes data that is needed by the new timeline.
        let _gc_cs = self.gc_cs.lock().unwrap();

        let mut timelines = self.timelines.lock().unwrap();
        let src_timeline = match self.get_or_init_timeline(src, &mut timelines)? {
            LayeredTimelineEntry::Local(timeline) => timeline,
            LayeredTimelineEntry::Remote { .. } => {
                bail!("Cannot branch off the timeline {} that's not local", src)
            }
        };

        src_timeline
            .check_lsn_is_in_scope(start_lsn)
            .context("invalid branch start lsn")?;

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

        // create a new timeline directory
        let timelinedir = self.conf.timeline_path(&dst, &self.tenantid);

        crashsafe_dir::create_dir(&timelinedir)?;

        // Create the metadata file, noting the ancestor of the new timeline.
        // There is initially no data in it, but all the read-calls know to look
        // into the ancestor.
        let metadata = TimelineMetadata::new(
            start_lsn,
            dst_prev,
            Some(src),
            start_lsn,
            src_timeline.latest_gc_cutoff_lsn.load(),
            src_timeline.initdb_lsn,
        );
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

    fn checkpoint_iteration(&self, cconf: CheckpointConfig) -> Result<()> {
        // Scan through the hashmap and collect a list of all the timelines,
        // while holding the lock. Then drop the lock and actually perform the
        // checkpoints.  We don't want to block everything else while the
        // checkpoint runs.
        let timelines = self.timelines.lock().unwrap();
        let timelines_to_checkpoint = timelines
            .iter()
            .map(|(timelineid, timeline)| (*timelineid, timeline.clone()))
            .collect::<Vec<_>>();
        drop(timelines);

        for (timelineid, timeline) in &timelines_to_checkpoint {
            let _entered =
                info_span!("checkpoint", timeline = %timelineid, tenant = %self.tenantid).entered();
            match timeline {
                LayeredTimelineEntry::Local(timeline) => timeline.checkpoint(cconf)?,
                LayeredTimelineEntry::Remote { .. } => debug!(
                    "Cannot run the checkpoint for remote timeline {}",
                    timelineid
                ),
            }
        }

        Ok(())
    }

    // Wait for all threads to complete and persist repository data before pageserver shutdown.
    fn shutdown(&self) -> Result<()> {
        trace!("LayeredRepository shutdown for tenant {}", self.tenantid);

        let timelines = self.timelines.lock().unwrap();
        for (timelineid, timeline) in timelines.iter() {
            shutdown_timeline(self.tenantid, *timelineid, timeline)?;
        }

        Ok(())
    }

    // TODO this method currentlly does not do anything to prevent (or react to) state updates between a sync task schedule and a sync task end (that causes this update).
    // Sync task is enqueued and can error and be rescheduled, so some significant time may pass between the events.
    //
    /// Reacts on the timeline sync state change, changing pageserver's memory state for this timeline (unload or load of the timeline files).
    fn set_timeline_state(
        &self,
        timeline_id: ZTimelineId,
        new_state: TimelineSyncState,
    ) -> Result<()> {
        let mut timelines_accessor = self.timelines.lock().unwrap();

        let timeline_to_shutdown = match new_state {
            TimelineSyncState::Ready(_) => {
                let reloaded_timeline =
                    self.init_local_timeline(timeline_id, &mut timelines_accessor)?;
                timelines_accessor
                    .insert(timeline_id, LayeredTimelineEntry::Local(reloaded_timeline));
                None
            }
            TimelineSyncState::Evicted(_) => timelines_accessor.remove(&timeline_id),
            TimelineSyncState::AwaitsDownload(disk_consistent_lsn) => timelines_accessor.insert(
                timeline_id,
                LayeredTimelineEntry::Remote {
                    id: timeline_id,
                    disk_consistent_lsn,
                    awaits_download: true,
                },
            ),
            TimelineSyncState::CloudOnly(disk_consistent_lsn) => timelines_accessor.insert(
                timeline_id,
                LayeredTimelineEntry::Remote {
                    id: timeline_id,
                    disk_consistent_lsn,
                    awaits_download: false,
                },
            ),
        };
        drop(timelines_accessor);

        if let Some(timeline) = timeline_to_shutdown {
            shutdown_timeline(self.tenantid, timeline_id, &timeline)?;
        }

        Ok(())
    }

    /// Layered repo does not store anything but
    /// * local, fully loaded timelines, ready for usage
    /// * remote timelines, that need a download task scheduled first before they can be used
    ///
    /// [`TimelineSyncState::Evicted`] and other non-local and non-remote states are not stored in the layered repo at all,
    /// hence their statuses cannot be returned by the repo.
    fn get_timeline_state(&self, timeline_id: ZTimelineId) -> Option<TimelineSyncState> {
        let timelines_accessor = self.timelines.lock().unwrap();
        let timeline_entry = timelines_accessor.get(&timeline_id)?;
        Some(
            if timeline_entry
                .local_or_schedule_download(self.tenantid)
                .is_some()
            {
                TimelineSyncState::Ready(timeline_entry.disk_consistent_lsn())
            } else {
                TimelineSyncState::CloudOnly(timeline_entry.disk_consistent_lsn())
            },
        )
    }
}

fn shutdown_timeline(
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    timeline: &LayeredTimelineEntry,
) -> Result<(), anyhow::Error> {
    match timeline {
        LayeredTimelineEntry::Local(timeline) => {
            timeline
                .upload_relishes
                .store(false, atomic::Ordering::Relaxed);
            walreceiver::stop_wal_receiver(timeline_id);
            trace!("repo shutdown. checkpoint timeline {}", timeline_id);
            // Do not reconstruct pages to reduce shutdown time
            timeline.checkpoint(CheckpointConfig::Flush)?;
            //TODO Wait for walredo process to shutdown too
        }
        LayeredTimelineEntry::Remote { .. } => warn!(
            "Skipping shutdown of a remote timeline {} for tenant {}",
            timeline_id, tenant_id
        ),
    }
    Ok(())
}

#[derive(Clone)]
enum LayeredTimelineEntry {
    Local(Arc<LayeredTimeline>),
    Remote {
        id: ZTimelineId,
        /// metadata contents of the latest successfully uploaded checkpoint
        disk_consistent_lsn: Lsn,
        awaits_download: bool,
    },
}

impl LayeredTimelineEntry {
    fn timeline_id(&self) -> ZTimelineId {
        match self {
            LayeredTimelineEntry::Local(timeline) => timeline.timelineid,
            LayeredTimelineEntry::Remote { id, .. } => *id,
        }
    }

    /// Gets local timeline data, if it's present. Otherwise schedules a download fot the remote timeline and returns `None`.
    fn local_or_schedule_download(&self, tenant_id: ZTenantId) -> Option<&LayeredTimeline> {
        match self {
            Self::Local(local) => Some(local.as_ref()),
            Self::Remote {
                id: timeline_id,
                awaits_download,
                ..
            } => {
                if *awaits_download {
                    debug!("timeline {} already scheduled for download", timeline_id,);
                } else {
                    debug!(
                        "Accessed a remote timeline {} for tenant {}, scheduling a timeline download",
                        timeline_id, tenant_id
                    );
                    schedule_timeline_download(tenant_id, *timeline_id);
                }
                None
            }
        }
    }

    /// Gets a current (latest for the remote case) disk consistent Lsn for the timeline.
    fn disk_consistent_lsn(&self) -> Lsn {
        match self {
            Self::Local(local) => local.disk_consistent_lsn.load(),
            Self::Remote {
                disk_consistent_lsn,
                ..
            } => *disk_consistent_lsn,
        }
    }
}

/// Private functions
impl LayeredRepository {
    // Implementation of the public `get_timeline` function. This differs from the public
    // interface in that the caller must already hold the mutex on the 'timelines' hashmap.
    fn get_or_init_timeline(
        &self,
        timelineid: ZTimelineId,
        timelines: &mut HashMap<ZTimelineId, LayeredTimelineEntry>,
    ) -> Result<LayeredTimelineEntry> {
        match timelines.get(&timelineid) {
            Some(timeline_entry) => {
                let _ = timeline_entry.local_or_schedule_download(self.tenantid);
                Ok(timeline_entry.clone())
            }
            None => {
                let timeline = self.init_local_timeline(timelineid, timelines)?;
                timelines.insert(
                    timelineid,
                    LayeredTimelineEntry::Local(Arc::clone(&timeline)),
                );
                Ok(LayeredTimelineEntry::Local(timeline))
            }
        }
    }

    fn init_local_timeline(
        &self,
        timelineid: ZTimelineId,
        timelines: &mut HashMap<ZTimelineId, LayeredTimelineEntry>,
    ) -> anyhow::Result<Arc<LayeredTimeline>> {
        let metadata = Self::load_metadata(self.conf, timelineid, self.tenantid)
            .context("failed to load metadata")?;
        let disk_consistent_lsn = metadata.disk_consistent_lsn();

        let ancestor = metadata
            .ancestor_timeline()
            .map(|ancestor_timelineid| self.get_or_init_timeline(ancestor_timelineid, timelines))
            .transpose()?;
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
        );
        timeline
            .load_layer_map(disk_consistent_lsn)
            .context("failed to load layermap")?;
        timeline.init_current_logical_size()?;

        Ok(Arc::new(timeline))
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
            gc_cs: Mutex::new(()),
            walredo_mgr,
            upload_relishes,
        }
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

        let metadata_bytes = data.to_bytes().context("Failed to get metadata bytes")?;

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
        info!("loading metadata from {}", path.display());
        let metadata_bytes = std::fs::read(&path)?;
        TimelineMetadata::from_bytes(&metadata_bytes)
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
        let _gc_cs = self.gc_cs.lock().unwrap();

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
        for &timelineid in &timelineids {
            let timeline = match self.get_or_init_timeline(timelineid, &mut timelines)? {
                LayeredTimelineEntry::Local(timeline) => timeline,
                LayeredTimelineEntry::Remote { .. } => {
                    warn!(
                        "Timeline {} is not local, cannot proceed with gc",
                        timelineid
                    );
                    return Ok(totals);
                }
            };

            if let Some(ancestor_timeline) = &timeline.ancestor_timeline {
                drop(timelines);
                let ancestor_timeline =
                    match ancestor_timeline.local_or_schedule_download(self.tenantid) {
                        Some(timeline) => timeline,
                        None => {
                            warn!(
                                "Timeline {} has ancestor {} is not local, cannot proceed with gc",
                                timelineid,
                                ancestor_timeline.timeline_id()
                            );
                            return Ok(totals);
                        }
                    };
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
                timelines = self.timelines.lock().unwrap();
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
            let timeline = match self.get_or_init_timeline(timelineid, &mut *timelines)? {
                LayeredTimelineEntry::Local(timeline) => timeline,
                LayeredTimelineEntry::Remote { .. } => {
                    debug!("Skipping GC for non-local timeline {}", timelineid);
                    continue;
                }
            };

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
                    timeline.checkpoint(CheckpointConfig::Forced)?;
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
    ancestor_timeline: Option<LayeredTimelineEntry>,
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

    /// If `true`, will backup its files that appear after each checkpointing to the remote storage.
    upload_relishes: AtomicBool,

    /// Ensures layers aren't frozen by checkpointer between
    /// [`LayeredTimeline::get_layer_for_write`] and layer reads.
    /// Locked automatically by [`LayeredTimelineWriter`] and checkpointer.
    /// Must always be acquired before the layer map/individual layer lock
    /// to avoid deadlock.
    write_lock: Mutex<()>,

    // Needed to ensure that we can't create a branch at a point that was already garbage collected
    latest_gc_cutoff_lsn: AtomicLsn,

    // It may change across major versions so for simplicity
    // keep it after running initdb for a timeline.
    // It is needed in checks when we want to error on some operations
    // when they are requested for pre-initdb lsn.
    // It can be unified with latest_gc_cutoff_lsn under some "first_valid_lsn",
    // though lets keep them both for better error visibility.
    initdb_lsn: Lsn,
}

/// Public interface functions
impl Timeline for LayeredTimeline {
    fn get_ancestor_lsn(&self) -> Lsn {
        self.ancestor_lsn
    }

    fn get_ancestor_timeline_id(&self) -> Option<ZTimelineId> {
        self.ancestor_timeline
            .as_ref()
            .map(LayeredTimelineEntry::timeline_id)
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
                    "Timed out while waiting for WAL record at LSN {} to arrive, last_record_lsn {} disk consistent LSN={}",
                    lsn, self.get_last_record_lsn(), self.get_disk_consistent_lsn()
                )
            })?;

        Ok(())
    }

    /// Look up given page version.
    fn get_page_at_lsn(&self, rel: RelishTag, rel_blknum: BlockNumber, lsn: Lsn) -> Result<Bytes> {
        if !rel.is_blocky() && rel_blknum != 0 {
            bail!(
                "invalid request for block {} for non-blocky relish {}",
                rel_blknum,
                rel
            );
        }
        debug_assert!(lsn <= self.get_last_record_lsn());
        let latest_gc_cutoff_lsn = self.latest_gc_cutoff_lsn.load();
        // error instead of assert to simplify testing
        ensure!(
            lsn >= latest_gc_cutoff_lsn,
            "tried to request a page version that was garbage collected. requested at {} gc cutoff {}",
            lsn, latest_gc_cutoff_lsn
        );

        let (seg, seg_blknum) = SegmentTag::from_blknum(rel, rel_blknum);

        if let Some((layer, lsn)) = self.get_layer_for_read(seg, lsn)? {
            RECONSTRUCT_TIME
                .observe_closure_duration(|| self.materialize_page(seg, seg_blknum, lsn, &*layer))
        } else {
            // FIXME: This can happen if PostgreSQL extends a relation but never writes
            // the page. See https://github.com/zenithdb/zenith/issues/841
            //
            // Would be nice to detect that situation better.
            if seg.segno > 0 && self.get_rel_exists(rel, lsn)? {
                warn!("Page {} blk {} at {} not found", rel, rel_blknum, lsn);
                return Ok(ZERO_PAGE.clone());
            }

            bail!("segment {} not found at {}", rel, lsn);
        }
    }

    fn get_relish_size(&self, rel: RelishTag, lsn: Lsn) -> Result<Option<BlockNumber>> {
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

            match &timeline.ancestor_timeline {
                None => break,
                Some(ancestor_entry) => {
                    match ancestor_entry.local_or_schedule_download(self.tenantid) {
                        Some(ancestor) => {
                            timeline = ancestor;
                            continue;
                        }
                        None => bail!("Cannot list relishes for timeline {} tenant {} due to its ancestor being remote only", self.timelineid, self.tenantid),
                    }
                }
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
    fn checkpoint(&self, cconf: CheckpointConfig) -> Result<()> {
        match cconf {
            CheckpointConfig::Flush => STORAGE_TIME
                .with_label_values(&["flush checkpoint"])
                .observe_closure_duration(|| self.checkpoint_internal(0, false)),
            CheckpointConfig::Forced => STORAGE_TIME
                .with_label_values(&["forced checkpoint"])
                .observe_closure_duration(|| self.checkpoint_internal(0, true)),
            CheckpointConfig::Distance(distance) => STORAGE_TIME
                .with_label_values(&["checkpoint"])
                .observe_closure_duration(|| self.checkpoint_internal(distance, true)),
        }
    }

    ///
    /// Validate lsn against initdb_lsn and latest_gc_cutoff_lsn.
    ///
    fn check_lsn_is_in_scope(&self, lsn: Lsn) -> Result<()> {
        let initdb_lsn = self.initdb_lsn;
        ensure!(
            lsn >= initdb_lsn,
            "LSN {} is earlier than initdb lsn {}",
            lsn,
            initdb_lsn,
        );

        let latest_gc_cutoff_lsn = self.latest_gc_cutoff_lsn.load();
        ensure!(
            lsn >= latest_gc_cutoff_lsn,
            "LSN {} is earlier than latest GC horizon {} (we might've already garbage collected needed data)",
            lsn,
            latest_gc_cutoff_lsn,
        );
        Ok(())
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
        self.ancestor_timeline
            .as_ref()
            .and_then(|ancestor_entry| ancestor_entry.local_or_schedule_download(self.tenantid))
            .map(Timeline::get_start_lsn)
            .unwrap_or(self.ancestor_lsn)
    }

    fn get_current_logical_size(&self) -> usize {
        self.current_logical_size.load(atomic::Ordering::Acquire) as usize
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

    fn upgrade_to_layered_timeline(&self) -> &crate::layered_repository::LayeredTimeline {
        self
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
        ancestor: Option<LayeredTimelineEntry>,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        current_logical_size: usize,
        upload_relishes: bool,
    ) -> LayeredTimeline {
        let current_logical_size_gauge = LOGICAL_TIMELINE_SIZE
            .get_metric_with_label_values(&[&tenantid.to_string(), &timelineid.to_string()])
            .unwrap();
        LayeredTimeline {
            conf,
            timelineid,
            tenantid,
            layers: Mutex::new(LayerMap::default()),

            walredo_mgr,

            // initialize in-memory 'last_record_lsn' from 'disk_consistent_lsn'.
            last_record_lsn: SeqWait::new(RecordLsn {
                last: metadata.disk_consistent_lsn(),
                prev: metadata.prev_record_lsn().unwrap_or(Lsn(0)),
            }),
            disk_consistent_lsn: AtomicLsn::new(metadata.disk_consistent_lsn().0),

            ancestor_timeline: ancestor,
            ancestor_lsn: metadata.ancestor_lsn(),
            current_logical_size: AtomicUsize::new(current_logical_size),
            current_logical_size_gauge,
            upload_relishes: AtomicBool::new(upload_relishes),

            write_lock: Mutex::new(()),

            latest_gc_cutoff_lsn: AtomicLsn::from(metadata.latest_gc_cutoff_lsn()),
            initdb_lsn: metadata.initdb_lsn(),
        }
    }

    ///
    /// Scan the timeline directory to populate the layer map.
    /// Returns all timeline-related files that were found and loaded.
    ///
    fn load_layer_map(&self, disk_consistent_lsn: Lsn) -> anyhow::Result<()> {
        let mut layers = self.layers.lock().unwrap();
        let mut num_layers = 0;

        // Scan timeline directory and create ImageFileName and DeltaFilename
        // structs representing all files on disk
        let timeline_path = self.conf.timeline_path(&self.timelineid, &self.tenantid);

        for direntry in fs::read_dir(timeline_path)? {
            let direntry = direntry?;
            let fname = direntry.file_name();
            let fname = fname.to_str().unwrap();

            if let Some(imgfilename) = ImageFileName::parse_str(fname) {
                // create an ImageLayer struct for each image file.
                if imgfilename.lsn > disk_consistent_lsn {
                    warn!(
                        "found future image layer {} on timeline {}",
                        imgfilename, self.timelineid
                    );

                    rename_to_backup(direntry.path())?;
                    continue;
                }

                let layer =
                    ImageLayer::new(self.conf, self.timelineid, self.tenantid, &imgfilename);

                trace!("found layer {}", layer.filename().display());
                layers.insert_historic(Arc::new(layer));
                num_layers += 1;
            } else if let Some(deltafilename) = DeltaFileName::parse_str(fname) {
                // Create a DeltaLayer struct for each delta file.
                ensure!(deltafilename.start_lsn < deltafilename.end_lsn);
                // The end-LSN is exclusive, while disk_consistent_lsn is
                // inclusive. For example, if disk_consistent_lsn is 100, it is
                // OK for a delta layer to have end LSN 101, but if the end LSN
                // is 102, then it might not have been fully flushed to disk
                // before crash.
                if deltafilename.end_lsn > disk_consistent_lsn + 1 {
                    warn!(
                        "found future delta layer {} on timeline {}",
                        deltafilename, self.timelineid
                    );

                    rename_to_backup(direntry.path())?;
                    continue;
                }

                let layer =
                    DeltaLayer::new(self.conf, self.timelineid, self.tenantid, &deltafilename);

                trace!("found layer {}", layer.filename().display());
                layers.insert_historic(Arc::new(layer));
                num_layers += 1;
            } else if fname == METADATA_FILE_NAME || fname.ends_with(".old") {
                // ignore these
            } else if is_ephemeral_file(fname) {
                // Delete any old ephemeral files
                trace!("deleting old ephemeral file in timeline dir: {}", fname);
                fs::remove_file(direntry.path())?;
            } else {
                warn!("unrecognized filename in timeline dir: {}", fname);
            }
        }

        info!("loaded layer map with {} layers", num_layers);

        Ok(())
    }

    ///
    /// Used to init current logical size on startup
    ///
    fn init_current_logical_size(&mut self) -> Result<()> {
        if self.current_logical_size.load(atomic::Ordering::Relaxed) != 0 {
            bail!("cannot init already initialized current logical size")
        };
        let lsn = self.get_last_record_lsn();
        self.current_logical_size =
            AtomicUsize::new(self.get_current_logical_size_non_incremental(lsn)?);
        trace!(
            "current_logical_size initialized to {}",
            self.current_logical_size.load(atomic::Ordering::Relaxed)
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
            timeline = match timeline
                .ancestor_timeline
                .as_ref()
                .and_then(|ancestor_entry| ancestor_entry.local_or_schedule_download(self.tenantid))
            {
                Some(timeline) => timeline,
                None => {
                    bail!(
                        "Cannot get the whole layer for read locked: timeline {} is not present locally",
                        self.timelineid
                    )
                }
            };
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
            match &timeline.ancestor_timeline {
                Some(ancestor_entry) => {
                    match ancestor_entry.local_or_schedule_download(self.tenantid) {
                        Some(ancestor) => {
                            lsn = timeline.ancestor_lsn;
                            timeline = ancestor;
                            trace!("recursing into ancestor at {}/{}", timeline.timelineid, lsn);
                            continue;
                        }
                        None => bail!(
                            "Cannot get a layer for read from remote ancestor timeline {}",
                            self.timelineid
                        ),
                    }
                }
                None => return Ok(None),
            }
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
    fn checkpoint_internal(&self, checkpoint_distance: u64, reconstruct_pages: bool) -> Result<()> {
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

        let mut layer_paths = Vec::new();
        while let Some((oldest_layer_id, oldest_layer, oldest_generation)) =
            layers.peek_oldest_open()
        {
            let oldest_pending_lsn = oldest_layer.get_oldest_pending_lsn();

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

            drop(layers);
            drop(write_guard);

            let mut this_layer_paths = self.evict_layer(oldest_layer_id, reconstruct_pages)?;
            layer_paths.append(&mut this_layer_paths);

            write_guard = self.write_lock.lock().unwrap();
            layers = self.layers.lock().unwrap();
        }

        // Call unload() on all frozen layers, to release memory.
        // This shouldn't be much memory, as only metadata is slurped
        // into memory.
        for layer in layers.iter_historic_layers() {
            layer.unload()?;
        }

        drop(layers);
        drop(write_guard);

        if !layer_paths.is_empty() {
            // We must fsync the timeline dir to ensure the directory entries for
            // new layer files are durable
            layer_paths.push(self.conf.timeline_path(&self.timelineid, &self.tenantid));

            // Fsync all the layer files and directory using multiple threads to
            // minimize latency.
            par_fsync::par_fsync(&layer_paths)?;

            layer_paths.pop().unwrap();
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

            let ancestor_timelineid = self
                .ancestor_timeline
                .as_ref()
                .map(LayeredTimelineEntry::timeline_id);

            let metadata = TimelineMetadata::new(
                disk_consistent_lsn,
                ondisk_prev_record_lsn,
                ancestor_timelineid,
                self.ancestor_lsn,
                self.latest_gc_cutoff_lsn.load(),
                self.initdb_lsn,
            );

            LayeredRepository::save_metadata(
                self.conf,
                self.timelineid,
                self.tenantid,
                &metadata,
                false,
            )?;
            if self.upload_relishes.load(atomic::Ordering::Relaxed) {
                schedule_timeline_checkpoint_upload(
                    self.tenantid,
                    self.timelineid,
                    layer_paths,
                    metadata,
                );
            }

            // Also update the in-memory copy
            self.disk_consistent_lsn.store(disk_consistent_lsn);
        }

        Ok(())
    }

    fn evict_layer(&self, layer_id: LayerId, reconstruct_pages: bool) -> Result<Vec<PathBuf>> {
        // Mark the layer as no longer accepting writes and record the end_lsn.
        // This happens in-place, no new layers are created now.
        // We call `get_last_record_lsn` again, which may be different from the
        // original load, as we may have released the write lock since then.

        let mut write_guard = self.write_lock.lock().unwrap();
        let mut layers = self.layers.lock().unwrap();

        let mut layer_paths = Vec::new();

        let global_layer_map = GLOBAL_LAYER_MAP.read().unwrap();
        if let Some(oldest_layer) = global_layer_map.get(&layer_id) {
            drop(global_layer_map);
            oldest_layer.freeze(self.get_last_record_lsn());

            // The layer is no longer open, update the layer map to reflect this.
            // We will replace it with on-disk historics below.
            layers.remove_open(layer_id);
            layers.insert_historic(oldest_layer.clone());

            // Write the now-frozen layer to disk. That could take a while, so release the lock while do it
            drop(layers);
            drop(write_guard);

            let new_historics = oldest_layer.write_to_disk(self, reconstruct_pages)?;

            write_guard = self.write_lock.lock().unwrap();
            layers = self.layers.lock().unwrap();

            // Finally, replace the frozen in-memory layer with the new on-disk layers
            layers.remove_historic(oldest_layer);

            // Add the historics to the LayerMap
            for delta_layer in new_historics.delta_layers {
                layer_paths.push(delta_layer.path());
                layers.insert_historic(Arc::new(delta_layer));
            }
            for image_layer in new_historics.image_layers {
                layer_paths.push(image_layer.path());
                layers.insert_historic(Arc::new(image_layer));
            }
        }
        drop(layers);
        drop(write_guard);

        Ok(layer_paths)
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

        // We need to ensure that no one branches at a point before latest_gc_cutoff_lsn.
        // See branch_timeline() for details.
        self.latest_gc_cutoff_lsn.store(cutoff);

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
            // This layer is in the process of being flushed to disk.
            // It will be swapped out of the layer map, replaced with
            // on-disk layers containing the same data.
            // We can't GC it, as it's not on disk. We can't remove it
            // from the layer map yet, as it would make its data
            // inaccessible.
            if l.is_in_memory() {
                continue;
            }

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
            // NOTE With that wee would keep data that
            // might be referenced by child branches forever.
            // We can track this in child timeline GC and delete parent layers when
            // they are no longer needed. This might be complicated with long inheritance chains.
            for retain_lsn in &retain_lsns {
                // start_lsn is inclusive
                if &l.get_start_lsn() <= retain_lsn {
                    info!(
                        "keeping {} {}-{} because it's still might be referenced by child branch forked at {} is_dropped: {} is_incremental: {}",
                        seg,
                        l.get_start_lsn(),
                        l.get_end_lsn(),
                        retain_lsn,
                        l.is_dropped(),
                        l.is_incremental(),
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

            // 4. Does this layer serve as a tombstone for some older layer?
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
                // Now check ancestor timelines, if any are present locally
                else if let Some(ancestor) =
                    self.ancestor_timeline.as_ref().and_then(|timeline_entry| {
                        timeline_entry.local_or_schedule_download(self.tenantid)
                    })
                {
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
                                let (last_live_seg, _rel_blknum) =
                                    SegmentTag::from_blknum(seg.rel, size - 1);
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
                        "keeping {} {}-{} because this layer serves as a tombstone for older layer",
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
                "garbage collecting {} {}-{} is_dropped: {} is_incremental: {}",
                l.get_seg_tag(),
                l.get_start_lsn(),
                l.get_end_lsn(),
                l.is_dropped(),
                l.is_incremental(),
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

    fn lookup_cached_page(
        &self,
        rel: &RelishTag,
        rel_blknum: BlockNumber,
        lsn: Lsn,
    ) -> Option<(Lsn, Bytes)> {
        let cache = page_cache::get();
        if let RelishTag::Relation(rel_tag) = &rel {
            let (lsn, read_guard) = cache.lookup_materialized_page(
                self.tenantid,
                self.timelineid,
                *rel_tag,
                rel_blknum,
                lsn,
            )?;
            let img = Bytes::from(read_guard.to_vec());
            Some((lsn, img))
        } else {
            None
        }
    }

    ///
    /// Reconstruct a page version from given Layer
    ///
    fn materialize_page(
        &self,
        seg: SegmentTag,
        seg_blknum: SegmentBlk,
        lsn: Lsn,
        layer: &dyn Layer,
    ) -> Result<Bytes> {
        // Check the page cache. We will get back the most recent page with lsn <= `lsn`.
        // The cached image can be returned directly if there is no WAL between the cached image
        // and requested LSN. The cached image can also be used to reduce the amount of WAL needed
        // for redo.
        let rel = seg.rel;
        let rel_blknum = seg.segno * RELISH_SEG_SIZE + seg_blknum;
        let (cached_lsn_opt, cached_page_opt) = match self.lookup_cached_page(&rel, rel_blknum, lsn)
        {
            Some((cached_lsn, cached_img)) => {
                match cached_lsn.cmp(&lsn) {
                    cmp::Ordering::Less => {} // there might be WAL between cached_lsn and lsn, we need to check
                    cmp::Ordering::Equal => return Ok(cached_img), // exact LSN match, return the image
                    cmp::Ordering::Greater => panic!(), // the returned lsn should never be after the requested lsn
                }
                (Some(cached_lsn), Some((cached_lsn, cached_img)))
            }
            None => (None, None),
        };

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
            let result = layer_ref
                .get_page_reconstruct_data(seg_blknum, curr_lsn, cached_lsn_opt, &mut data)
                .with_context(|| {
                    format!(
                        "Failed to get reconstruct data {} {:?} {} {} {:?}",
                        layer_ref.get_seg_tag(),
                        layer_ref.filename(),
                        seg_blknum,
                        curr_lsn,
                        cached_lsn_opt,
                    )
                })?;
            match result {
                PageReconstructResult::Complete => break,
                PageReconstructResult::Continue(cont_lsn) => {
                    // Fetch base image / more WAL from the returned predecessor layer
                    if let Some((cont_layer, cont_lsn)) = self.get_layer_for_read(seg, cont_lsn)? {
                        if cont_lsn == curr_lsn {
                            // We landed on the same layer again. Shouldn't happen, but if it does,
                            // don't get stuck in an infinite loop.
                            bail!(
                                "could not find predecessor of layer {} at {}, layer returned its own LSN",
                                layer_ref.filename().display(),
                                cont_lsn
                            );
                        }
                        layer_arc = cont_layer;
                        layer_ref = &*layer_arc;
                        curr_lsn = cont_lsn;
                        continue;
                    } else {
                        bail!(
                            "could not find predecessor of layer {} at {}",
                            layer_ref.filename().display(),
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
                        warn!("Page {} blk {} at {} not found", rel, rel_blknum, lsn);
                        return Ok(ZERO_PAGE.clone());
                    }
                    bail!(
                        "No base image found for page {} blk {} at {}/{}",
                        rel,
                        rel_blknum,
                        self.timelineid,
                        lsn,
                    );
                }
                PageReconstructResult::Cached => {
                    let (cached_lsn, cached_img) = cached_page_opt.unwrap();
                    assert!(data.page_img.is_none());
                    if let Some((first_rec_lsn, first_rec)) = data.records.first() {
                        assert!(&cached_lsn < first_rec_lsn);
                        assert!(!first_rec.will_init());
                    }
                    data.page_img = Some(cached_img);
                    break;
                }
            }
        }

        self.reconstruct_page(rel, rel_blknum, lsn, data)
    }

    ///
    /// Reconstruct a page version, using the given base image and WAL records in 'data'.
    ///
    fn reconstruct_page(
        &self,
        rel: RelishTag,
        rel_blknum: BlockNumber,
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
                    rel_blknum,
                    rel,
                    request_lsn
                );
                Ok(img.clone())
            } else {
                // FIXME: this ought to be an error?
                warn!(
                    "Page {} blk {} at {} not found",
                    rel, rel_blknum, request_lsn
                );
                Ok(ZERO_PAGE.clone())
            }
        } else {
            // We need to do WAL redo.
            //
            // If we don't have a base image, then the oldest WAL record better initialize
            // the page
            if data.page_img.is_none() && !data.records.first().unwrap().1.will_init() {
                // FIXME: this ought to be an error?
                warn!(
                    "Base image for page {}/{} at {} not found, but got {} WAL records",
                    rel,
                    rel_blknum,
                    request_lsn,
                    data.records.len()
                );
                Ok(ZERO_PAGE.clone())
            } else {
                if data.page_img.is_some() {
                    trace!("found {} WAL records and a base image for blk {} in {} at {}, performing WAL redo", data.records.len(), rel_blknum, rel, request_lsn);
                } else {
                    trace!("found {} WAL records that will init the page for blk {} in {} at {}, performing WAL redo", data.records.len(), rel_blknum, rel, request_lsn);
                }

                let last_rec_lsn = data.records.last().unwrap().0;

                let img = self.walredo_mgr.request_redo(
                    rel,
                    rel_blknum,
                    request_lsn,
                    data.page_img.clone(),
                    data.records,
                )?;

                if let RelishTag::Relation(rel_tag) = &rel {
                    let cache = page_cache::get();
                    cache.memorize_materialized_page(
                        self.tenantid,
                        self.timelineid,
                        *rel_tag,
                        rel_blknum,
                        last_rec_lsn,
                        &img,
                    );
                }

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
            .fetch_add(diff as usize, atomic::Ordering::SeqCst);
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
            .fetch_sub(diff as usize, atomic::Ordering::SeqCst);
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
    fn put_wal_record(
        &self,
        lsn: Lsn,
        rel: RelishTag,
        rel_blknum: u32,
        rec: ZenithWalRecord,
    ) -> Result<()> {
        if !rel.is_blocky() && rel_blknum != 0 {
            bail!(
                "invalid request for block {} for non-blocky relish {}",
                rel_blknum,
                rel
            );
        }
        ensure!(lsn.is_aligned(), "unaligned record LSN");

        let (seg, seg_blknum) = SegmentTag::from_blknum(rel, rel_blknum);
        let layer = self.tl.get_layer_for_write(seg, lsn)?;
        let delta_size = layer.put_wal_record(lsn, seg_blknum, rec)?;
        self.tl
            .increase_current_logical_size(delta_size * BLCKSZ as u32);
        Ok(())
    }

    fn put_page_image(
        &self,
        rel: RelishTag,
        rel_blknum: BlockNumber,
        lsn: Lsn,
        img: Bytes,
    ) -> Result<()> {
        if !rel.is_blocky() && rel_blknum != 0 {
            bail!(
                "invalid request for block {} for non-blocky relish {}",
                rel_blknum,
                rel
            );
        }
        ensure!(lsn.is_aligned(), "unaligned record LSN");

        let (seg, seg_blknum) = SegmentTag::from_blknum(rel, rel_blknum);

        let layer = self.tl.get_layer_for_write(seg, lsn)?;
        let delta_size = layer.put_page_image(seg_blknum, lsn, img)?;

        self.tl
            .increase_current_logical_size(delta_size * BLCKSZ as u32);
        Ok(())
    }

    fn put_truncation(&self, rel: RelishTag, lsn: Lsn, relsize: BlockNumber) -> Result<()> {
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
            let (seg, _seg_blknum) = SegmentTag::from_blknum(rel, 0);
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
