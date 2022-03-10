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

use anyhow::{bail, ensure, Context, Result};
use bookfile::Book;
use bytes::Bytes;
use fail::fail_point;
use itertools::Itertools;
use lazy_static::lazy_static;
use tracing::*;

use std::cmp::{max, min, Ordering};
use std::collections::hash_map::Entry;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::{Bound::Included, Deref, Range};
use std::path::{Path, PathBuf};
use std::sync::atomic::{self, AtomicBool};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard};
use std::time::Instant;

use self::metadata::{metadata_path, TimelineMetadata, METADATA_FILE_NAME};
use crate::config::PageServerConf;
use crate::keyspace::KeyPartitioning;
use crate::remote_storage::{schedule_timeline_checkpoint_upload, schedule_timeline_download};
use crate::repository::{
    GcResult, Repository, RepositoryTimeline, Timeline, TimelineSyncState, TimelineWriter,
};
use crate::repository::{Key, Value};
use crate::thread_mgr;
use crate::virtual_file::VirtualFile;
use crate::walreceiver::IS_WAL_RECEIVER;
use crate::walredo::WalRedoManager;
use crate::CheckpointConfig;
use crate::{ZTenantId, ZTimelineId};

use zenith_metrics::{register_histogram_vec, HistogramVec};
use zenith_utils::crashsafe_dir;
use zenith_utils::lsn::{AtomicLsn, Lsn, RecordLsn};
use zenith_utils::seqwait::SeqWait;

mod delta_layer;
mod ephemeral_file;
mod filename;
mod image_layer;
mod inmemory_layer;
mod layer_map;
pub mod metadata;
mod par_fsync;
mod storage_layer;
mod utils;

use delta_layer::{DeltaLayer, DeltaLayerWriter};
use ephemeral_file::is_ephemeral_file;
use filename::{DeltaFileName, ImageFileName};
use image_layer::{ImageLayer, ImageLayerWriter};
use inmemory_layer::InMemoryLayer;
use layer_map::LayerMap;
use layer_map::SearchResult;
use storage_layer::{Layer, ValueReconstructResult, ValueReconstructState};

use crate::keyspace::TARGET_FILE_SIZE_BYTES;

// re-export this function so that page_cache.rs can use it.
pub use crate::layered_repository::ephemeral_file::writeback as writeback_ephemeral_file;

// Metrics collected on operations on the storage repository.
lazy_static! {
    static ref STORAGE_TIME: HistogramVec = register_histogram_vec!(
        "pageserver_storage_time",
        "Time spent on storage operations",
        &["operation"]
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
    type Timeline = LayeredTimeline;

    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<RepositoryTimeline<LayeredTimeline>> {
        let mut timelines = self.timelines.lock().unwrap();
        Ok(
            match self.get_or_init_timeline(timelineid, &mut timelines)? {
                LayeredTimelineEntry::Local(local) => RepositoryTimeline::Local(local),
                LayeredTimelineEntry::Remote {
                    id,
                    disk_consistent_lsn,
                } => RepositoryTimeline::Remote {
                    id,
                    disk_consistent_lsn,
                },
            },
        )
    }

    fn create_empty_timeline(
        &self,
        timelineid: ZTimelineId,
        initdb_lsn: Lsn,
    ) -> Result<Arc<LayeredTimeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        // Create the timeline directory, and write initial metadata to file.
        crashsafe_dir::create_dir_all(self.conf.timeline_path(&timelineid, &self.tenantid))?;

        let metadata = TimelineMetadata::new(Lsn(0), None, None, Lsn(0), initdb_lsn, initdb_lsn);
        Self::save_metadata(self.conf, timelineid, self.tenantid, &metadata, true)?;

        let timeline = LayeredTimeline::new(
            self.conf,
            metadata,
            None,
            timelineid,
            self.tenantid,
            Arc::clone(&self.walredo_mgr),
            self.upload_relishes,
        );
        timeline.layers.lock().unwrap().next_open_layer_at = Some(initdb_lsn);

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
        let latest_gc_cutoff_lsn = src_timeline.get_latest_gc_cutoff_lsn();

        src_timeline
            .check_lsn_is_in_scope(start_lsn, &latest_gc_cutoff_lsn)
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
            *src_timeline.latest_gc_cutoff_lsn.read().unwrap(),
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

    // Detaches the timeline from the repository.
    fn detach_timeline(&self, timeline_id: ZTimelineId) -> Result<()> {
        let mut timelines = self.timelines.lock().unwrap();
        match timelines.entry(timeline_id) {
            Entry::Vacant(_) => {
                bail!("cannot detach non existing timeline");
            }
            Entry::Occupied(mut entry) => {
                let timeline_entry = entry.get_mut();

                let timeline = match timeline_entry {
                    LayeredTimelineEntry::Remote { .. } => {
                        bail!("cannot detach remote timeline {}", timeline_id);
                    }
                    LayeredTimelineEntry::Local(timeline) => timeline,
                };

                // TODO (rodionov) keep local state in timeline itself (refactoring related to https://github.com/zenithdb/zenith/issues/997 and #1104)

                // FIXME this is local disk consistent lsn, need to keep the latest succesfully uploaded checkpoint lsn in timeline (metadata?)
                //  https://github.com/zenithdb/zenith/issues/1104
                let remote_disk_consistent_lsn = timeline.disk_consistent_lsn.load();
                // reference to timeline is dropped here
                entry.insert(LayeredTimelineEntry::Remote {
                    id: timeline_id,
                    disk_consistent_lsn: remote_disk_consistent_lsn,
                });
            }
        };
        // Release the lock to shutdown and remove the files without holding it
        drop(timelines);
        // shutdown the timeline (this shuts down the walreceiver)
        thread_mgr::shutdown_threads(None, Some(self.tenantid), Some(timeline_id));

        // remove timeline files (maybe avoid this for ease of debugging if something goes wrong)
        fs::remove_dir_all(self.conf.timeline_path(&timeline_id, &self.tenantid))?;
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
        debug!(
            "set_timeline_state: timeline_id: {}, new_state: {:?}",
            timeline_id, new_state
        );
        let mut timelines_accessor = self.timelines.lock().unwrap();

        match new_state {
            TimelineSyncState::Ready(_) => {
                let reloaded_timeline =
                    self.init_local_timeline(timeline_id, &mut timelines_accessor)?;
                timelines_accessor
                    .insert(timeline_id, LayeredTimelineEntry::Local(reloaded_timeline));
                None
            }
            TimelineSyncState::Evicted(_) => timelines_accessor.remove(&timeline_id),
            TimelineSyncState::AwaitsDownload(disk_consistent_lsn)
            | TimelineSyncState::CloudOnly(disk_consistent_lsn) => timelines_accessor.insert(
                timeline_id,
                LayeredTimelineEntry::Remote {
                    id: timeline_id,
                    disk_consistent_lsn,
                },
            ),
        };
        // NOTE we do not delete local data in case timeline became cloud only, this is performed in detach_timeline
        drop(timelines_accessor);

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

#[derive(Clone)]
enum LayeredTimelineEntry {
    Local(Arc<LayeredTimeline>),
    Remote {
        id: ZTimelineId,
        /// metadata contents of the latest successfully uploaded checkpoint
        disk_consistent_lsn: Lsn,
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
    fn local_or_schedule_download(&self, tenant_id: ZTenantId) -> Option<Arc<LayeredTimeline>> {
        match self {
            Self::Local(local) => Some(Arc::clone(local)),
            Self::Remote {
                id: timeline_id, ..
            } => {
                debug!(
                    "Accessed a remote timeline {} for tenant {}, scheduling a timeline download",
                    timeline_id, tenant_id
                );
                schedule_timeline_download(tenant_id, *timeline_id);
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
        let timeline = LayeredTimeline::new(
            self.conf,
            metadata,
            ancestor,
            timelineid,
            self.tenantid,
            Arc::clone(&self.walredo_mgr),
            self.upload_relishes,
        );
        timeline
            .load_layer_map(disk_consistent_lsn)
            .context("failed to load layermap")?;

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
        let mut file = VirtualFile::open_with_options(
            &path,
            OpenOptions::new().write(true).create_new(first_save),
        )?;

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

        // Now collect info about branchpoints
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
            }
        }

        // Ok, we now know all the branch points.
        // Perform GC for each timeline.
        for timelineid in timelineids {
            if thread_mgr::is_shutdown_requested() {
                // We were requested to shut down. Stop and return with the progress we
                // made.
                break;
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
                drop(timelines);
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
                timeline.update_gc_info(branchpoints, cutoff);
                let result = timeline.gc()?;

                totals += result;
                timelines = self.timelines.lock().unwrap();
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

    /// If `true`, will backup its files that appear after each checkpointing to the remote storage.
    upload_relishes: AtomicBool,

    /// Ensures layers aren't frozen by checkpointer between
    /// [`LayeredTimeline::get_layer_for_write`] and layer reads.
    /// Locked automatically by [`LayeredTimelineWriter`] and checkpointer.
    /// Must always be acquired before the layer map/individual layer lock
    /// to avoid deadlock.
    write_lock: Mutex<()>,

    // Prevent concurrent checkpoints.
    // Checkpoints are normally performed by one thread. But checkpoint can also be manually requested by admin
    // (that's used in tests), and shutdown also forces a checkpoint. These forced checkpoints run in a different thread
    // and could be triggered at the same time as a normal checkpoint.
    checkpoint_cs: Mutex<()>,

    // Needed to ensure that we can't create a branch at a point that was already garbage collected
    latest_gc_cutoff_lsn: RwLock<Lsn>,

    // List of child timelines and their branch points. This is needed to avoid
    // garbage collecting data that is still needed by the child timelines.
    gc_info: RwLock<GcInfo>,

    partitioning: RwLock<Option<(KeyPartitioning, Lsn)>>,

    // It may change across major versions so for simplicity
    // keep it after running initdb for a timeline.
    // It is needed in checks when we want to error on some operations
    // when they are requested for pre-initdb lsn.
    // It can be unified with latest_gc_cutoff_lsn under some "first_valid_lsn",
    // though lets keep them both for better error visibility.
    initdb_lsn: Lsn,
}

struct GcInfo {
    retain_lsns: Vec<Lsn>,
    cutoff: Lsn,
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
            .wait_for_timeout(lsn, self.conf.wait_lsn_timeout)
            .with_context(|| {
                format!(
                    "Timed out while waiting for WAL record at LSN {} to arrive, last_record_lsn {} disk consistent LSN={}",
                    lsn, self.get_last_record_lsn(), self.get_disk_consistent_lsn()
                )
            })?;

        Ok(())
    }

    fn get_latest_gc_cutoff_lsn(&self) -> RwLockReadGuard<Lsn> {
        self.latest_gc_cutoff_lsn.read().unwrap()
    }

    /// Look up the value with the given a key
    fn get(&self, key: Key, lsn: Lsn) -> Result<Bytes> {
        debug_assert!(lsn <= self.get_last_record_lsn());

        let mut reconstruct_state = ValueReconstructState {
            records: Vec::new(),
            img: None, // FIXME: check page cache and put the img here
        };

        self.get_reconstruct_data(key, lsn, &mut reconstruct_state)?;

        self.reconstruct_value(key, lsn, reconstruct_state)
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
    fn check_lsn_is_in_scope(
        &self,
        lsn: Lsn,
        latest_gc_cutoff_lsn: &RwLockReadGuard<Lsn>,
    ) -> Result<()> {
        ensure!(
            lsn >= **latest_gc_cutoff_lsn,
            "LSN {} is earlier than latest GC horizon {} (we might've already garbage collected needed data)",
            lsn,
            **latest_gc_cutoff_lsn,
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

    fn get_disk_consistent_lsn(&self) -> Lsn {
        self.disk_consistent_lsn.load()
    }

    fn hint_partitioning(&self, partitioning: KeyPartitioning, lsn: Lsn) -> Result<()> {
        self.partitioning
            .write()
            .unwrap()
            .replace((partitioning, lsn));
        Ok(())
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
        ancestor: Option<LayeredTimelineEntry>,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        upload_relishes: bool,
    ) -> LayeredTimeline {
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
            upload_relishes: AtomicBool::new(upload_relishes),

            write_lock: Mutex::new(()),
            checkpoint_cs: Mutex::new(()),

            gc_info: RwLock::new(GcInfo {
                retain_lsns: Vec::new(),
                cutoff: Lsn(0),
            }),
            partitioning: RwLock::new(None),

            latest_gc_cutoff_lsn: RwLock::new(metadata.latest_gc_cutoff_lsn()),
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
                        "found future image layer {} on timeline {} disk_consistent_lsn is {}",
                        imgfilename, self.timelineid, disk_consistent_lsn
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
                // The end-LSN is exclusive, while disk_consistent_lsn is
                // inclusive. For example, if disk_consistent_lsn is 100, it is
                // OK for a delta layer to have end LSN 101, but if the end LSN
                // is 102, then it might not have been fully flushed to disk
                // before crash.
                if deltafilename.lsn_range.end > disk_consistent_lsn + 1 {
                    warn!(
                        "found future delta layer {} on timeline {} disk_consistent_lsn is {}",
                        deltafilename, self.timelineid, disk_consistent_lsn
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

        layers.next_open_layer_at = Some(Lsn(disk_consistent_lsn.0) + 1);

        info!(
            "loaded layer map with {} layers at {}",
            num_layers, disk_consistent_lsn
        );

        Ok(())
    }

    ///
    /// Get a handle to a Layer for reading.
    ///
    /// The returned Layer might be from an ancestor timeline, if the
    /// segment hasn't been updated on this timeline yet.
    ///
    /// This function takes the current timeline's locked LayerMap as an argument,
    /// so callers can avoid potential race conditions.
    fn get_reconstruct_data(
        &self,
        key: Key,
        request_lsn: Lsn,
        reconstruct_state: &mut ValueReconstructState,
    ) -> Result<()> {
        // Start from the current timeline.
        let mut timeline_owned;
        let mut timeline = self;

        let mut path: Vec<(ValueReconstructResult, Lsn, Arc<dyn Layer>)> = Vec::new();

        // 'prev_lsn' tracks the last LSN that we were at in our search. It's used
        // to check that each iteration make some progress, to break infinite
        // looping if something goes wrong.
        let mut prev_lsn = Lsn(u64::MAX);

        let mut result = ValueReconstructResult::Continue;
        let mut cont_lsn = Lsn(request_lsn.0 + 1);

        loop {
            // The function should have updated 'state'
            //info!("CALLED for {} at {}: {:?} with {} records", reconstruct_state.key, reconstruct_state.lsn, result, reconstruct_state.records.len());
            match result {
                ValueReconstructResult::Complete => return Ok(()),
                ValueReconstructResult::Continue => {
                    if prev_lsn <= cont_lsn {
                        // Didn't make any progress in last iteration. Error out to avoid
                        // getting stuck in the loop.

                        // For debugging purposes, print the path of layers that we traversed
                        // through.
                        for (r, c, l) in path {
                            error!(
                                "PATH: result {:?}, cont_lsn {}, layer: {}",
                                r,
                                c,
                                l.filename().display()
                            );
                        }
                        bail!("could not find layer with more data for key {} at LSN {}, request LSN {}, ancestor {}",
                          key,
                          Lsn(cont_lsn.0 - 1),
                              request_lsn,
                        timeline.ancestor_lsn)
                    }
                    prev_lsn = cont_lsn;
                }
                ValueReconstructResult::Missing => {
                    bail!(
                        "could not find data for key {} at LSN {}, for request at LSN {}",
                        key,
                        cont_lsn,
                        request_lsn
                    )
                }
            }

            // Recurse into ancestor if needed
            if Lsn(cont_lsn.0 - 1) <= timeline.ancestor_lsn {
                trace!(
                    "going into ancestor {}, cont_lsn is {}",
                    timeline.ancestor_lsn,
                    cont_lsn
                );
                let ancestor = timeline.get_ancestor_timeline()?;
                timeline_owned = ancestor;
                timeline = &*timeline_owned;
                prev_lsn = Lsn(u64::MAX);
                continue;
            }

            let layers = timeline.layers.lock().unwrap();

            // Check the open and frozen in-memory layers first
            if let Some(open_layer) = &layers.open_layer {
                let start_lsn = open_layer.get_lsn_range().start;
                if cont_lsn > start_lsn {
                    //info!("CHECKING for {} at {} on open layer {}", key, cont_lsn, open_layer.filename().display());
                    result = open_layer.get_value_reconstruct_data(
                        key,
                        open_layer.get_lsn_range().start..cont_lsn,
                        reconstruct_state,
                    )?;
                    cont_lsn = start_lsn;
                    path.push((result, cont_lsn, open_layer.clone()));
                    continue;
                }
            }
            if let Some(frozen_layer) = &layers.frozen_layer {
                let start_lsn = frozen_layer.get_lsn_range().start;
                if cont_lsn > start_lsn {
                    //info!("CHECKING for {} at {} on frozen layer {}", key, cont_lsn, frozen_layer.filename().display());
                    result = frozen_layer.get_value_reconstruct_data(
                        key,
                        frozen_layer.get_lsn_range().start..cont_lsn,
                        reconstruct_state,
                    )?;
                    cont_lsn = start_lsn;
                    path.push((result, cont_lsn, frozen_layer.clone()));
                    continue;
                }
            }

            if let Some(SearchResult { lsn_floor, layer }) = layers.search(key, cont_lsn)? {
                //info!("CHECKING for {} at {} on historic layer {}", key, cont_lsn, layer.filename().display());

                result = layer.get_value_reconstruct_data(
                    key,
                    lsn_floor..cont_lsn,
                    reconstruct_state,
                )?;
                cont_lsn = lsn_floor;
                path.push((result, cont_lsn, layer));
            } else if self.ancestor_timeline.is_some() {
                // Nothing on this timeline. Traverse to parent
                result = ValueReconstructResult::Continue;
                cont_lsn = Lsn(self.ancestor_lsn.0 + 1);
            } else {
                // Nothing found
                result = ValueReconstructResult::Missing;
            }
        }
    }

    fn get_ancestor_timeline(&self) -> Result<Arc<LayeredTimeline>> {
        let ancestor_entry = self
            .ancestor_timeline
            .as_ref()
            .expect("get_ancestor_timeline() called on timeline with no parent");

        let timeline = match ancestor_entry.local_or_schedule_download(self.tenantid) {
            Some(timeline) => timeline,
            None => {
                bail!(
                    "Cannot get the whole layer for read locked: ancestor of timeline {} is not present locally",
                    self.timelineid
                )
            }
        };
        Ok(timeline)
    }

    ///
    /// Get a handle to the latest layer for appending.
    ///
    fn get_layer_for_write(&self, lsn: Lsn) -> Result<Arc<InMemoryLayer>> {
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
        if let Some(open_layer) = &layers.open_layer {
            if open_layer.get_lsn_range().start > lsn {
                bail!("unexpected open layer in the future");
            }

            layer = Arc::clone(open_layer);
        } else {
            // No writeable layer yet. Create one.
            let start_lsn = layers.next_open_layer_at.unwrap();

            trace!(
                "creating layer for write at {}/{} for record at {}",
                self.timelineid,
                start_lsn,
                lsn
            );
            let new_layer =
                InMemoryLayer::create(self.conf, self.timelineid, self.tenantid, start_lsn, lsn)?;
            let layer_rc = Arc::new(new_layer);

            layers.open_layer = Some(Arc::clone(&layer_rc));
            layers.next_open_layer_at = None;

            layer = layer_rc;
        }
        Ok(layer)
    }

    fn put_value(&self, key: Key, lsn: Lsn, val: Value) -> Result<()> {
        //info!("PUT: key {} at {}", key, lsn);
        let layer = self.get_layer_for_write(lsn)?;
        layer.put_value(key, lsn, val)?;

        Ok(())
    }

    fn put_tombstone(&self, key_range: Range<Key>, lsn: Lsn) -> Result<()> {
        let layer = self.get_layer_for_write(lsn)?;
        layer.put_tombstone(key_range, lsn)?;

        Ok(())
    }

    ///
    /// Flush to disk all data that was written with the put_* functions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL.
    fn checkpoint_internal(&self, checkpoint_distance: u64, reconstruct_pages: bool) -> Result<()> {
        info!("checkpoint starting");
        // Prevent concurrent checkpoints
        let _checkpoint_cs = self.checkpoint_cs.lock().unwrap();

        // If the in-memory layer is larger than 'checkpoint_distance', write it
        // to a delta file.  That's necessary to limit the amount of WAL that
        // needs to be kept in the safekeepers, and that needs to be reprocessed
        // on page server crash.
        //
        // TODO: It's not a great policy for keeping memory usage in check,
        // though. We should also aim at flushing layers that consume a lot of
        // memory and/or aren't receiving much updates anymore.
        loop {
            // Do we have a frozen in-memory layer that we need to write out?
            // If we do, write it out now. Otherwise, check if the current
            // in-memory layer is old enough that we should freeze and write it out.
            let write_guard = self.write_lock.lock().unwrap();
            let mut layers = self.layers.lock().unwrap();
            if let Some(frozen_layer) = &layers.frozen_layer {
                // Write out the frozen in-memory layer to disk, as a delta file
                let frozen_layer = Arc::clone(frozen_layer);
                drop(write_guard);
                drop(layers);
                self.flush_frozen_layer(frozen_layer)?;
            } else {
                // Freeze the current open in-memory layer, if it's larger than
                // 'checkpoint_distance'. It will be written to disk on next
                // iteration.
                if let Some(open_layer) = &layers.open_layer {
                    // Does this layer need freezing?
                    let RecordLsn {
                        last: last_record_lsn,
                        prev: _prev_record_lsn,
                    } = self.last_record_lsn.load();
                    let oldest_lsn = open_layer.get_oldest_lsn();
                    let distance = last_record_lsn.widening_sub(oldest_lsn);
                    if distance < 0 || distance < checkpoint_distance.into() {
                        info!(
                            "the oldest layer is now {} which is {} bytes behind last_record_lsn",
                            open_layer.filename().display(),
                            distance
                        );
                        break;
                    }
                    let end_lsn = Lsn(self.get_last_record_lsn().0 + 1);
                    open_layer.freeze(end_lsn);

                    // The layer is no longer open, update the layer map to reflect this.
                    // We will replace it with on-disk historics below.
                    layers.frozen_layer = Some(Arc::clone(open_layer));
                    layers.open_layer = None;
                    layers.next_open_layer_at = Some(end_lsn);
                } else {
                    break;
                }
                // We will write the now-frozen layer to disk on next iteration.
                // That could take a while, so release the lock while do it
                drop(layers);
                drop(write_guard);
            }
        }

        // Create new image layers to allow GC and to reduce read latency
        if reconstruct_pages {
            // TODO: the threshold for how often we create image layers is
            // currently hard-coded at 3. It means, write out a new image layer,
            // if there are at least three delta layers on top of it.
            self.compact(TARGET_FILE_SIZE_BYTES as usize)?;
        }

        // TODO: We should also compact existing delta layers here.

        // Call unload() on all frozen layers, to release memory.
        // This shouldn't be much memory, as only metadata is slurped
        // into memory.
        let layers = self.layers.lock().unwrap();
        for layer in layers.iter_historic_layers() {
            layer.unload()?;
        }
        drop(layers);

        Ok(())
    }

    fn flush_frozen_layer(&self, frozen_layer: Arc<InMemoryLayer>) -> Result<()> {
        // Do we have a frozen in-memory layer that we need to write out?
        let new_delta = frozen_layer.write_to_disk()?;

        // Finally, replace the frozen in-memory layer with the new on-disk layers
        let write_guard = self.write_lock.lock().unwrap();
        let mut layers = self.layers.lock().unwrap();
        layers.frozen_layer = None;

        // Add the new delta layer to the LayerMap
        let mut layer_paths = vec![new_delta.path()];
        layers.insert_historic(Arc::new(new_delta));

        drop(write_guard);
        drop(layers);

        // Sync layers
        if !layer_paths.is_empty() {
            // We must fsync the timeline dir to ensure the directory entries for
            // new layer files are durable
            layer_paths.push(self.conf.timeline_path(&self.timelineid, &self.tenantid));

            // Fsync all the layer files and directory using multiple threads to
            // minimize latency.
            par_fsync::par_fsync(&layer_paths)?;

            layer_paths.pop().unwrap();
        }

        // Compute new 'disk_consistent_lsn'
        let disk_consistent_lsn;
        disk_consistent_lsn = Lsn(frozen_layer.get_lsn_range().end.0 - 1);

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
            let RecordLsn {
                last: last_record_lsn,
                prev: prev_record_lsn,
            } = self.last_record_lsn.load();
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
                *self.latest_gc_cutoff_lsn.read().unwrap(),
                self.initdb_lsn,
            );

            fail_point!("checkpoint-before-saving-metadata", |x| bail!(
                "{}",
                x.unwrap()
            ));

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

    fn compact(&self, target_file_size: usize) -> Result<()> {
        //
        // High level strategy for compaction / image creation:
        //
        // 1. First, calculate the desired "partitioning" of the
        // currently in-use key space. The goal is to partition the
        // key space into TARGET_FILE_SIZE chunks, but also take into
        // account any existing image layers, and try to align the
        // chunk boundaries with the existing image layers to avoid
        // too much churn. Also try to align chunk boundaries with
        // relation boundaries.  In principle, we don't know about
        // relation boundaries here, we just deal with key-value
        // pairs, and the code in pgdatadir_mapping.rs knows how to
        // map relations into key-value pairs. But in practice we know
        // that 'field6' is the block number, and the fields 1-5
        // identify a relation. This is just an optimization,
        // though.
        //
        // 2. Once we know the partitioning, for each partition,
        // decide if it's time to create a new image layer. The
        // criteria is: there has been too much "churn" since the last
        // image layer? The "churn" is fuzzy concept, it's a
        // combination of too many delta files, or too much WAL in
        // total in the delta file. Or perhaps: if creating an image
        // file would allow to delete some older files.
        //
        // 3. After that, we compact all level0 delta files if there
        // are too many of them.  While compacting, we also garbage
        // collect any page versions that are no longer needed because
        // of the new image layers we created in step 2.
        //
        // TODO: This hight level strategy hasn't been implemented yet.
        // Below are functions compact_level0() and create_image_layers()
        // but they are a bit ad hoc and don't quite work like it's explained
        // above. Rewrite it.

        // 1. The partitioning was already done by the code in
        // pgdatadir_mapping.rs. We just use it here.
        let partitioning_guard = self.partitioning.read().unwrap();
        if let Some((partitioning, lsn)) = partitioning_guard.as_ref() {
            // Make a copy of the partitioning, so that we can release
            // the lock. Otherwise we could block the WAL receiver.
            let lsn = *lsn;
            let partitions = partitioning.partitions.clone();
            drop(partitioning_guard);

            // 2. Create new image layers for partitions that have been modified
            // "enough".
            for partition in partitions.iter() {
                if self.time_for_new_image_layer(partition, lsn, 3)? {
                    self.create_image_layer(partition, lsn)?;
                }
            }

            // 3. Compact
            self.compact_level0(target_file_size)?;
        } else {
            info!("Could not compact because no partitioning specified yet");
        }
        Ok(())
    }

    // Is it time to create a new image layer for the given partition?
    fn time_for_new_image_layer(
        &self,
        partition: &[Range<Key>],
        lsn: Lsn,
        threshold: usize,
    ) -> Result<bool> {
        let layers = self.layers.lock().unwrap();

        for part_range in partition {
            let image_coverage = layers.image_coverage(part_range, lsn)?;
            for (img_range, last_img) in image_coverage {
                let img_lsn = if let Some(ref last_img) = last_img {
                    last_img.get_lsn_range().end
                } else {
                    Lsn(0)
                };

                let num_deltas = layers.count_deltas(&img_range, &(img_lsn..lsn))?;

                info!(
                    "range {}-{}, has {} deltas on this timeline",
                    img_range.start, img_range.end, num_deltas
                );
                if num_deltas >= threshold {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    fn create_image_layer(&self, partition: &[Range<Key>], lsn: Lsn) -> Result<()> {
        let img_range = partition.first().unwrap().start..partition.last().unwrap().end;
        let mut image_layer_writer =
            ImageLayerWriter::new(self.conf, self.timelineid, self.tenantid, &img_range, lsn)?;

        for range in partition {
            let mut key = range.start;
            while key < range.end {
                let img = self.get(key, lsn)?;
                image_layer_writer.put_image(key, &img)?;
                key = key.next();
            }
        }
        let image_layer = image_layer_writer.finish()?;

        let mut layers = self.layers.lock().unwrap();
        layers.insert_historic(Arc::new(image_layer));
        drop(layers);
        // FIXME: need to fsync?

        Ok(())
    }

    fn compact_level0(&self, target_file_size: usize) -> Result<()> {
        let layers = self.layers.lock().unwrap();

        // We compact or "shuffle" the level-0 delta layers when 10 have
        // accumulated.
        static COMPACT_THRESHOLD: usize = 10;

        let level0_deltas = layers.get_level0_deltas()?;

        if level0_deltas.len() < COMPACT_THRESHOLD {
            return Ok(());
        }
        drop(layers);

        // FIXME: this function probably won't work correctly if there's overlap
        // in the deltas.
        let lsn_range = level0_deltas
            .iter()
            .map(|l| l.get_lsn_range())
            .reduce(|a, b| min(a.start, b.start)..max(a.end, b.end))
            .unwrap();

        let all_values_iter = level0_deltas.iter().map(|l| l.iter()).kmerge_by(|a, b| {
            if let Ok((a_key, a_lsn, _)) = a {
                if let Ok((b_key, b_lsn, _)) = b {
                    match a_key.cmp(b_key) {
                        Ordering::Less => true,
                        Ordering::Equal => a_lsn <= b_lsn,
                        Ordering::Greater => false,
                    }
                } else {
                    false
                }
            } else {
                true
            }
        });

        // Merge the contents of all the input delta layers into a new set
        // of delta layers, based on the current partitioning.
        //
        // TODO: this actually divides the layers into fixed-size chunks, not
        // based on the partitioning.
        //
        // TODO: we should also opportunistically materialize and
        // garbage collect what we can.
        let mut new_layers = Vec::new();
        let mut prev_key: Option<Key> = None;
        let mut writer: Option<DeltaLayerWriter> = None;
        for x in all_values_iter {
            let (key, lsn, value) = x?;

            if let Some(prev_key) = prev_key {
                if key != prev_key && writer.is_some() {
                    let size = writer.as_mut().unwrap().size();
                    if size > target_file_size as u64 {
                        new_layers.push(writer.take().unwrap().finish(prev_key.next())?);
                        writer = None;
                    }
                }
            }

            if writer.is_none() {
                writer = Some(DeltaLayerWriter::new(
                    self.conf,
                    self.timelineid,
                    self.tenantid,
                    key,
                    lsn_range.clone(),
                )?);
            }

            writer.as_mut().unwrap().put_value(key, lsn, value)?;
            prev_key = Some(key);
        }
        if let Some(writer) = writer {
            new_layers.push(writer.finish(prev_key.unwrap().next())?);
        }

        let mut layers = self.layers.lock().unwrap();
        for l in new_layers {
            layers.insert_historic(Arc::new(l));
        }

        // Now that we have reshuffled the data to set of new delta layers, we can
        // delete the old ones
        for l in level0_deltas {
            l.delete()?;
            layers.remove_historic(l.clone());
        }
        drop(layers);

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
    fn update_gc_info(&self, retain_lsns: Vec<Lsn>, cutoff: Lsn) {
        let mut gc_info = self.gc_info.write().unwrap();
        gc_info.retain_lsns = retain_lsns;
        gc_info.cutoff = cutoff;
    }

    fn gc(&self) -> Result<GcResult> {
        let now = Instant::now();
        let mut result: GcResult = Default::default();
        let disk_consistent_lsn = self.get_disk_consistent_lsn();
        let _checkpoint_cs = self.checkpoint_cs.lock().unwrap();

        let gc_info = self.gc_info.read().unwrap();
        let retain_lsns = &gc_info.retain_lsns;
        let cutoff = gc_info.cutoff;

        let _enter = info_span!("garbage collection", timeline = %self.timelineid, tenant = %self.tenantid, cutoff = %cutoff).entered();

        // We need to ensure that no one branches at a point before latest_gc_cutoff_lsn.
        // See branch_timeline() for details.
        *self.latest_gc_cutoff_lsn.write().unwrap() = cutoff;

        info!("GC starting");

        debug!("retain_lsns: {:?}", retain_lsns);

        let mut layers_to_remove: Vec<Arc<dyn Layer>> = Vec::new();

        // Scan all on-disk layers in the timeline.
        //
        // Garbage collect the layer if all conditions are satisfied:
        // 1. it is older than cutoff LSN;
        // 2. it doesn't need to be retained for 'retain_lsns';
        // 3. newer on-disk image layers cover the layer's whole key range
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

            result.layers_total += 1;

            // 1. Is it newer than cutoff point?
            if l.get_lsn_range().end > cutoff {
                info!(
                    "keeping {} because it's newer than cutoff {}",
                    l.filename().display(),
                    cutoff
                );
                result.layers_needed_by_cutoff += 1;
                continue 'outer;
            }

            // 2. Is it needed by a child branch?
            // NOTE With that wee would keep data that
            // might be referenced by child branches forever.
            // We can track this in child timeline GC and delete parent layers when
            // they are no longer needed. This might be complicated with long inheritance chains.
            for retain_lsn in retain_lsns {
                // start_lsn is inclusive
                if &l.get_lsn_range().start <= retain_lsn {
                    info!(
                        "keeping {} because it's still might be referenced by child branch forked at {} is_dropped: xx is_incremental: {}",
                        l.filename().display(),
                        retain_lsn,
                        l.is_incremental(),
                    );
                    result.layers_needed_by_branches += 1;
                    continue 'outer;
                }
            }

            // 3. Is there a later on-disk layer for this relation?
            //
            // The end-LSN is exclusive, while disk_consistent_lsn is
            // inclusive. For example, if disk_consistent_lsn is 100, it is
            // OK for a delta layer to have end LSN 101, but if the end LSN
            // is 102, then it might not have been fully flushed to disk
            // before crash.
            if !layers.newer_image_layer_exists(
                &l.get_key_range(),
                l.get_lsn_range().end,
                disk_consistent_lsn + 1,
            )? {
                info!(
                    "keeping {} because it is the latest layer",
                    l.filename().display()
                );
                result.layers_not_updated += 1;
                continue 'outer;
            }

            // We didn't find any reason to keep this file, so remove it.
            info!(
                "garbage collecting {} is_dropped: xx is_incremental: {}",
                l.filename().display(),
                l.is_incremental(),
            );
            layers_to_remove.push(Arc::clone(l));
        }

        // Actually delete the layers from disk and remove them from the map.
        // (couldn't do this in the loop above, because you cannot modify a collection
        // while iterating it. BTreeMap::retain() would be another option)
        for doomed_layer in layers_to_remove {
            doomed_layer.delete()?;
            layers.remove_historic(doomed_layer.clone());

            result.layers_removed += 1;
        }

        result.elapsed = now.elapsed();
        Ok(result)
    }

    ///
    /// Reconstruct a value, using the given base image and WAL records in 'data'.
    ///
    fn reconstruct_value(
        &self,
        key: Key,
        request_lsn: Lsn,
        mut data: ValueReconstructState,
    ) -> Result<Bytes> {
        // Perform WAL redo if needed
        data.records.reverse();

        // If we have a page image, and no WAL, we're all set
        if data.records.is_empty() {
            if let Some((img_lsn, img)) = &data.img {
                trace!(
                    "found page image for key {} at {}, no WAL redo required",
                    key,
                    img_lsn
                );
                Ok(img.clone())
            } else {
                bail!("base image for {} at {} not found", key, request_lsn);
            }
        } else {
            // We need to do WAL redo.
            //
            // If we don't have a base image, then the oldest WAL record better initialize
            // the page
            if data.img.is_none() && !data.records.first().unwrap().1.will_init() {
                bail!(
                    "Base image for {} at {} not found, but got {} WAL records",
                    key,
                    request_lsn,
                    data.records.len()
                );
            } else {
                let base_img = if let Some((_lsn, img)) = data.img {
                    trace!(
                        "found {} WAL records and a base image for {} at {}, performing WAL redo",
                        data.records.len(),
                        key,
                        request_lsn
                    );
                    Some(img)
                } else {
                    trace!("found {} WAL records that will init the page for {} at {}, performing WAL redo", data.records.len(), key, request_lsn);
                    None
                };

                //let last_rec_lsn = data.records.last().unwrap().0;

                let img =
                    self.walredo_mgr
                        .request_redo(key, request_lsn, base_img, data.records)?;

                // FIXME: page caching
                /*
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
                */

                Ok(img)
            }
        }
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

impl<'a> TimelineWriter<'_> for LayeredTimelineWriter<'a> {
    fn put(&self, key: Key, lsn: Lsn, value: Value) -> Result<()> {
        self.tl.put_value(key, lsn, value)
    }

    fn delete(&self, key_range: Range<Key>, lsn: Lsn) -> Result<()> {
        self.tl.put_tombstone(key_range, lsn)
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

    bail!("couldn't find an unused backup number for {:?}", path)
}

///
/// Tests that are specific to the layered storage format.
///
/// There are more unit tests in repository.rs that work through the
/// Repository interface and are expected to work regardless of the
/// file format and directory layout. The test here are more low level.
///
#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::repo_harness::*;
    use rand::thread_rng;
    use rand::Rng;

    #[test]
    fn corrupt_metadata() -> Result<()> {
        const TEST_NAME: &str = "corrupt_metadata";
        let harness = RepoHarness::create(TEST_NAME)?;
        let repo = harness.load();

        repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        drop(repo);

        let metadata_path = harness.timeline_path(&TIMELINE_ID).join(METADATA_FILE_NAME);

        assert!(metadata_path.is_file());

        let mut metadata_bytes = std::fs::read(&metadata_path)?;
        assert_eq!(metadata_bytes.len(), 512);
        metadata_bytes[512 - 4 - 2] ^= 1;
        std::fs::write(metadata_path, metadata_bytes)?;

        let new_repo = harness.load();
        let err = new_repo.get_timeline(TIMELINE_ID).err().unwrap();
        assert_eq!(err.to_string(), "failed to load metadata");
        assert_eq!(
            err.source().unwrap().to_string(),
            "metadata checksum mismatch"
        );

        Ok(())
    }

    // Target file size in the unit tests. In production, the target
    // file size is much larger, maybe 1 GB. But a small size makes it
    // much faster to exercise all the logic for creating the files,
    // garbage collection, compaction etc.
    const TEST_FILE_SIZE: usize = 4 * 1024 * 1024;

    #[test]
    fn test_images() -> Result<()> {
        let repo = RepoHarness::create("test_images")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        #[allow(non_snake_case)]
        let TEST_KEY: Key = Key::from_hex("112222222233333333444444445500000001").unwrap();

        let writer = tline.writer();
        writer.put(TEST_KEY, Lsn(0x10), Value::Image(TEST_IMG("foo at 0x10")))?;
        writer.advance_last_record_lsn(Lsn(0x10));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced)?;
        tline.compact(TEST_FILE_SIZE)?;

        let writer = tline.writer();
        writer.put(TEST_KEY, Lsn(0x20), Value::Image(TEST_IMG("foo at 0x20")))?;
        writer.advance_last_record_lsn(Lsn(0x20));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced)?;
        tline.compact(TEST_FILE_SIZE)?;

        let writer = tline.writer();
        writer.put(TEST_KEY, Lsn(0x30), Value::Image(TEST_IMG("foo at 0x30")))?;
        writer.advance_last_record_lsn(Lsn(0x30));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced)?;
        tline.compact(TEST_FILE_SIZE)?;

        let writer = tline.writer();
        writer.put(TEST_KEY, Lsn(0x40), Value::Image(TEST_IMG("foo at 0x40")))?;
        writer.advance_last_record_lsn(Lsn(0x40));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced)?;
        tline.compact(TEST_FILE_SIZE)?;

        assert_eq!(tline.get(TEST_KEY, Lsn(0x10))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(TEST_KEY, Lsn(0x1f))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(TEST_KEY, Lsn(0x20))?, TEST_IMG("foo at 0x20"));
        assert_eq!(tline.get(TEST_KEY, Lsn(0x30))?, TEST_IMG("foo at 0x30"));
        assert_eq!(tline.get(TEST_KEY, Lsn(0x40))?, TEST_IMG("foo at 0x40"));

        Ok(())
    }

    //
    // Insert 1000 key-value pairs with increasing keys, checkpoint,
    // repeat 50 times.
    //
    #[test]
    fn test_bulk_insert() -> Result<()> {
        let repo = RepoHarness::create("test_bulk_insert")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        let mut lsn = Lsn(0x10);

        let mut parts = KeyPartitioning::new();

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();
        let mut blknum = 0;
        for _ in 0..50 {
            for _ in 0..1000 {
                test_key.field6 = blknum;
                let writer = tline.writer();
                writer.put(
                    test_key,
                    lsn,
                    Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                )?;
                writer.advance_last_record_lsn(lsn);
                drop(writer);

                parts.add_key(test_key);

                lsn = Lsn(lsn.0 + 0x10);
                blknum += 1;
            }

            let cutoff = tline.get_last_record_lsn();
            parts.repartition(TEST_FILE_SIZE as u64);
            tline.hint_partitioning(parts.clone(), lsn)?;

            tline.update_gc_info(Vec::new(), cutoff);
            tline.checkpoint(CheckpointConfig::Forced)?;
            tline.compact(TEST_FILE_SIZE)?;
            tline.gc()?;
        }

        Ok(())
    }

    #[test]
    fn test_random_updates() -> Result<()> {
        let repo = RepoHarness::create("test_random_updates")?.load();
        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        const NUM_KEYS: usize = 1000;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();

        let mut parts = KeyPartitioning::new();

        // Track when each page was last modified. Used to assert that
        // a read sees the latest page version.
        let mut updated = [Lsn(0); NUM_KEYS];

        let mut lsn = Lsn(0);
        #[allow(clippy::needless_range_loop)]
        for blknum in 0..NUM_KEYS {
            lsn = Lsn(lsn.0 + 0x10);
            test_key.field6 = blknum as u32;
            let writer = tline.writer();
            writer.put(
                test_key,
                lsn,
                Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
            )?;
            writer.advance_last_record_lsn(lsn);
            updated[blknum] = lsn;
            drop(writer);

            parts.add_key(test_key);
        }

        parts.repartition(TEST_FILE_SIZE as u64);
        tline.hint_partitioning(parts, lsn)?;

        for _ in 0..50 {
            for _ in 0..NUM_KEYS {
                lsn = Lsn(lsn.0 + 0x10);
                let blknum = thread_rng().gen_range(0..NUM_KEYS);
                test_key.field6 = blknum as u32;
                let writer = tline.writer();
                writer.put(
                    test_key,
                    lsn,
                    Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                )?;
                println!("updating {} at {}", blknum, lsn);
                writer.advance_last_record_lsn(lsn);
                drop(writer);
                updated[blknum] = lsn;
            }

            // Read all the blocks
            for (blknum, last_lsn) in updated.iter().enumerate() {
                test_key.field6 = blknum as u32;
                assert_eq!(
                    tline.get(test_key, lsn)?,
                    TEST_IMG(&format!("{} at {}", blknum, last_lsn))
                );
            }

            // Perform a cycle of checkpoint, compaction, and GC
            println!("checkpointing {}", lsn);
            let cutoff = tline.get_last_record_lsn();
            tline.update_gc_info(Vec::new(), cutoff);
            tline.checkpoint(CheckpointConfig::Forced)?;
            tline.compact(TEST_FILE_SIZE)?;
            tline.gc()?;
        }

        Ok(())
    }

    #[test]
    fn test_traverse_branches() -> Result<()> {
        let repo = RepoHarness::create("test_traverse_branches")?.load();
        let mut tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        const NUM_KEYS: usize = 1000;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();

        let mut parts = KeyPartitioning::new();

        // Track when each page was last modified. Used to assert that
        // a read sees the latest page version.
        let mut updated = [Lsn(0); NUM_KEYS];

        let mut lsn = Lsn(0);
        #[allow(clippy::needless_range_loop)]
        for blknum in 0..NUM_KEYS {
            lsn = Lsn(lsn.0 + 0x10);
            test_key.field6 = blknum as u32;
            let writer = tline.writer();
            writer.put(
                test_key,
                lsn,
                Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
            )?;
            writer.advance_last_record_lsn(lsn);
            updated[blknum] = lsn;
            drop(writer);

            parts.add_key(test_key);
        }

        parts.repartition(TEST_FILE_SIZE as u64);
        tline.hint_partitioning(parts, lsn)?;

        let mut tline_id = TIMELINE_ID;
        for _ in 0..50 {
            let new_tline_id = ZTimelineId::generate();
            repo.branch_timeline(tline_id, new_tline_id, lsn)?;
            tline = if let RepositoryTimeline::Local(local) = repo.get_timeline(new_tline_id)? {
                local
            } else {
                panic!("unexpected timeline state");
            };
            tline_id = new_tline_id;

            for _ in 0..NUM_KEYS {
                lsn = Lsn(lsn.0 + 0x10);
                let blknum = thread_rng().gen_range(0..NUM_KEYS);
                test_key.field6 = blknum as u32;
                let writer = tline.writer();
                writer.put(
                    test_key,
                    lsn,
                    Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                )?;
                println!("updating {} at {}", blknum, lsn);
                writer.advance_last_record_lsn(lsn);
                drop(writer);
                updated[blknum] = lsn;
            }

            // Read all the blocks
            for (blknum, last_lsn) in updated.iter().enumerate() {
                test_key.field6 = blknum as u32;
                assert_eq!(
                    tline.get(test_key, lsn)?,
                    TEST_IMG(&format!("{} at {}", blknum, last_lsn))
                );
            }

            // Perform a cycle of checkpoint, compaction, and GC
            println!("checkpointing {}", lsn);
            let cutoff = tline.get_last_record_lsn();
            tline.update_gc_info(Vec::new(), cutoff);
            tline.checkpoint(CheckpointConfig::Forced)?;
            tline.compact(TEST_FILE_SIZE)?;
            tline.gc()?;
        }

        Ok(())
    }
}
