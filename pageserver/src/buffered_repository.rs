//!
//! Zenith repository implementation that keeps old data in files on disk, and
//! the recent changes in memory. See buffered_repository/*_layer.rs files.
//! The functions here are responsible for locating the correct layer for the
//! get/put call, tracing timeline branching history as needed.
//!
//! The files are stored in the .zenith/tenants/<tenantid>/timelines/<timelineid>
//! directory. See buffered_repository/README for how the files are managed.
//! In addition to the layer files, there is a metadata file in the same
//! directory that contains information about the timeline, in particular its
//! parent timeline, and the last LSN that has been written to disk.
//!

use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use lazy_static::lazy_static;
use postgres_ffi::pg_constants::BLCKSZ;
use serde::{Deserialize, Serialize};
use tracing::*;

use std::collections::HashMap;
use std::collections::{BTreeSet, HashSet};
use std::convert::TryInto;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::{Bound::Included, Deref};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::relish::*;
use crate::relish_storage::schedule_timeline_upload;
use crate::repository::{
    GcResult, PageReconstructData, PageReconstructResult, PageVersion, Repository, Timeline,
    TimelineWriter, WALRecord,
};
use crate::tenant_mgr;
use crate::toast_store::ToastStore;
use crate::walreceiver;
use crate::walreceiver::IS_WAL_RECEIVER;
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};

use crate::layered_repository::delta_layer::DeltaLayer;
use crate::layered_repository::filename;
use crate::layered_repository::image_layer::ImageLayer;
use crate::layered_repository::storage_layer::{Layer, SegmentTag, RELISH_SEG_SIZE};

use zenith_metrics::{register_histogram, register_int_gauge_vec, Histogram, IntGaugeVec};
use zenith_metrics::{register_histogram_vec, HistogramVec};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::crashsafe_dir;
use zenith_utils::lsn::{AtomicLsn, Lsn, RecordLsn};
use zenith_utils::seqwait::SeqWait;
use zenith_utils::vec_map::VecMap;

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

// Timeout when waiting for WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(60);

// Taken from PG_CONTROL_MAX_SAFE_SIZE
const METADATA_MAX_SAFE_SIZE: usize = 512;
const METADATA_CHECKSUM_SIZE: usize = std::mem::size_of::<u32>();
const METADATA_MAX_DATA_SIZE: usize = METADATA_MAX_SAFE_SIZE - METADATA_CHECKSUM_SIZE;

const ZERO_TAG: RelTag = RelTag {
    spcnode: 0,
    dbnode: 0,
    relnode: 0,
    forknum: 0,
};

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
pub struct BufferedRepository {
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    timelines: Mutex<HashMap<ZTimelineId, Arc<BufferedTimeline>>>,

    walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
    /// Makes evey repo's timelines to backup their files to remote storage,
    /// when they get frozen.
    upload_relishes: bool,
}

//
// All entries in KV storage use this key
//
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum StoreKey {
    Metadata(MetadataKey), // for relish size
    Data(DataKey),         // for relish content
}

//
// Key used for relish blocks
//
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct DataKey {
    rel: RelishTag,
    blknum: u32,
    lsn: Lsn,
}

//
// Relish metadata key
//
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct MetadataKey {
    rel: RelishTag,
    lsn: Lsn,
}

//
// Value associated with MetadataKey
//
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct MetadataValue {
    size: Option<u32>, // empty for dropped relations
}

//
// Struct used for caching most recent metadata values.
// We do not need to use Option here, because entries corresponding to dropped relation are removed from map
//
struct MetadataSnapshot {
    size: u32,
    lsn: Lsn,
}

//
// Relish store consists of persistent KV store and transient metadata cache loadedon demand
//
struct RelishStore {
    data: ToastStore,
    meta: Option<HashMap<RelishTag, MetadataSnapshot>>,
}

/// Public interface
impl Repository for BufferedRepository {
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

        let timeline = BufferedTimeline::new(
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
        trace!("BufferedRepository shutdown for tenant {}", self.tenantid);

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
impl BufferedRepository {
    fn get_buffered_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<BufferedTimeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        Ok(self.get_timeline_locked(timelineid, &mut timelines)?)
    }

    // Implementation of the public `get_timeline` function. This differs from the public
    // interface in that the caller must already hold the mutex on the 'timelines' hashmap.
    fn get_timeline_locked(
        &self,
        timelineid: ZTimelineId,
        timelines: &mut HashMap<ZTimelineId, Arc<BufferedTimeline>>,
    ) -> Result<Arc<BufferedTimeline>> {
        match timelines.get(&timelineid) {
            Some(timeline) => Ok(timeline.clone()),
            None => {
                let metadata = Self::load_metadata(self.conf, timelineid, self.tenantid)?;

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

                let mut timeline = BufferedTimeline::new(
                    self.conf,
                    metadata,
                    ancestor,
                    timelineid,
                    self.tenantid,
                    Arc::clone(&self.walredo_mgr),
                    0, // init with 0 and update after layers are loaded,
                    self.upload_relishes,
                )?;

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
    ) -> BufferedRepository {
        BufferedRepository {
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
        rc: Arc<BufferedRepository>,
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
                let timelines: Vec<(ZTimelineId, Arc<BufferedTimeline>)> = self
                    .timelines
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|pair| (pair.0.clone(), pair.1.clone()))
                    .collect();
                //let timelines = self.timelines.lock().unwrap();
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
        rc: Arc<BufferedRepository>,
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
                let result = self.gc_iteration(None, conf.gc_horizon, false)?;
                info!("GC result: {:?}", result);
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
        //let mut timelines = self.timelines.lock().unwrap();

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
            let timeline = self.get_buffered_timeline(*timelineid)?;

            if let Some(ancestor_timeline) = &timeline.ancestor_timeline {
                // If target_timeline is specified, we only need to know branchpoints of its childs
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
            // We have already loaded all timelines above
            // so this operation is just a quick map lookup.
            let timeline = self.get_buffered_timeline(timelineid)?;

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
                        Included((timelineid, Lsn::MAX)),
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
/// The fields correspond to the values we hold in memory, in BufferedTimeline.
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

pub struct BufferedTimeline {
    conf: &'static PageServerConf,

    tenantid: ZTenantId,
    timelineid: ZTimelineId,

    store: RwLock<RelishStore>, // provide MURSIW access to the storage

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
    ancestor_timeline: Option<Arc<BufferedTimeline>>,
    ancestor_lsn: Lsn,

    // this variable indicates how much space is used from user's point of view,
    // e.g. we do not account here for multiple versions of data and so on.
    // this is counted incrementally based on physical relishes (excluding FileNodeMap)
    // current_logical_size is not stored no disk and initialized on timeline creation using
    // get_current_logical_size_non_incremental in init_current_logical_size
    // this is needed because when we save it in metadata it can become out of sync
    // because current_logical_size is consistent on last_record_lsn, not ondisk_consistent_lsn
    // NOTE: current_logical_size also includes size of the ancestor
    //
    // FIXME-KK: it is not properly maintained. Do we really need to track logical size of database or its physical size on the disk?
    // With compressed KV storage them are completely different.
    current_logical_size: AtomicUsize, // bytes

    /// If `true`, will backup its timeline files to remote storage after freezing.
    upload_relishes: bool,

    /// Ensures layers aren't frozen by checkpointer between
    /// [`BufferedTimeline::get_layer_for_write`] and layer reads.
    /// Locked automatically by [`BufferedTimelineWriter`] and checkpointer.
    /// Must always be acquired before the layer map/individual layer lock
    /// to avoid deadlock.
    write_lock: Mutex<()>,
}

/// Public interface functions
impl Timeline for BufferedTimeline {
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
                    "Timed out while waiting for WAL record at LSN {} to arrive",
                    lsn
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
        // FIXME-KK; navigation to ancestor timelines is not yet supported
        assert!(self.ancestor_timeline.is_none());
        debug_assert!(lsn <= self.get_last_record_lsn());

        let from = StoreKey::Data(DataKey {
            rel,
            blknum,
            lsn: Lsn(0),
        })
        .ser()?
        .to_vec();
        let till = StoreKey::Data(DataKey { rel, blknum, lsn }).ser()?.to_vec();
        //let mut reconstruct_key: Option<DataKey> = None;
        let result = {
            let store = self.store.read().unwrap();
            let mut iter = store.data.range(&from..=&till);

            // locate latest version with LSN <= than requested
            if let Some(entry) = iter.next_back() {
                let pair = entry?;
                let key = StoreKey::des(&pair.0)?;
                if let StoreKey::Data(dk) = key {
                    let ver = PageVersion::des(&pair.1)?;
                    match ver {
                        PageVersion::Page(img) => Ok(img), // already materialized: we are done
                        PageVersion::Wal(rec) => {
                            let mut will_init = rec.will_init;
                            let mut data = PageReconstructData {
                                records: Vec::new(),
                                page_img: None,
                            };
                            //reconstruct_key = Some(dk);
                            data.records.push((dk.lsn, rec));
                            // loop until we locate full page image or initialization WAL record
                            // FIXME-KK: cross-timelines histories are not handled now
                            while !will_init {
                                if let Some(entry) = iter.next_back() {
                                    let pair = entry?;
                                    let key = StoreKey::des(&pair.0)?;
                                    let ver = PageVersion::des(&pair.1)?;
                                    if let StoreKey::Data(dk) = key {
                                        assert!(dk.rel == rel); // check that we don't jump to previous relish before locating full image
                                        match ver {
                                            PageVersion::Page(img) => {
                                                data.page_img = Some(img);
                                                break;
                                            }
                                            PageVersion::Wal(rec) => {
                                                will_init = rec.will_init;
                                                data.records.push((dk.lsn, rec));
                                            }
                                        }
                                    } else {
                                        bail!("Unexpected key type {:?}", key);
                                    }
                                } else {
                                    bail!("Base image not found for relish {} at {}", rel, lsn);
                                }
                            }
                            RECONSTRUCT_TIME.observe_closure_duration(|| {
                                self.reconstruct_page(rel, blknum, lsn, data)
                            })
                        }
                    }
                } else {
                    bail!("Unexpected key type {:?}", key);
                }
            } else {
                warn!("block {} of relish {} not found at {}", blknum, rel, lsn);
                Ok(ZERO_PAGE.clone())
            }
        };
        /*
        if let Some(key) = reconstruct_key {
            if let Ok(img) = &result {
                let mut store = self.store.write().unwrap();
                store
                    .data
                    .put(&StoreKey::Data(key).ser()?, &PageVersion::Image(img.clone()).ser()?)?;
            }
        }
        */
        result
    }

    fn get_relish_size(&self, rel: RelishTag, lsn: Lsn) -> Result<Option<u32>> {
        if !rel.is_blocky() {
            bail!(
                "invalid get_relish_size request for non-blocky relish {}",
                rel
            );
        }
        debug_assert!(lsn <= self.get_last_record_lsn());

        let store = self.store.read().unwrap();
        // Use metadata hash only if it was loaded
        if let Some(hash) = &store.meta {
            if let Some(snap) = hash.get(&rel) {
                // We can used cached version only of requested LSN is >= than LSN of last version.
                // Otherwise extract historical value from KV storage.
                if snap.lsn <= lsn {
                    return Ok(Some(snap.size));
                }
            }
        }
        let from = StoreKey::Metadata(MetadataKey { rel, lsn: Lsn(0) })
            .ser()?
            .to_vec();
        let till = StoreKey::Metadata(MetadataKey { rel, lsn }).ser()?.to_vec();
        // locate last version with LSN <= than requested
        let mut iter = store.data.range(&from..=&till);

        if let Some(pair) = iter.next_back() {
            let meta = MetadataValue::des(&pair?.1)?;
            Ok(meta.size)
        } else {
            Ok(None)
        }
    }

    fn get_rel_exists(&self, rel: RelishTag, lsn: Lsn) -> Result<bool> {
        self.get_relish_size(rel, lsn).map(|meta| meta.is_some())
    }

    fn list_rels(&self, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelishTag>> {
        let from = RelishTag::Relation(RelTag {
            spcnode,
            dbnode,
            relnode: 0,
            forknum: 0,
        });
        let till = RelishTag::Relation(RelTag {
            spcnode,
            dbnode,
            relnode: u32::MAX,
            forknum: u8::MAX,
        });

        self.list_relishes(from, till, lsn)
    }

    fn list_nonrels(&self, lsn: Lsn) -> Result<HashSet<RelishTag>> {
        let from = RelishTag::Relation(RelTag {
            spcnode: u32::MAX,
            dbnode: u32::MAX,
            relnode: u32::MAX,
            forknum: u8::MAX,
        });
        let till = RelishTag::Checkpoint;

        self.list_relishes(from, till, lsn)
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

    ///
    /// Export data as delats and image layers between 'start_lsn' to 'end_lsn'. The
    /// start is inclusive, and end is exclusive.
    ///
    fn export_timeline(&self, start_lsn: Lsn, end_lsn: Lsn) -> Result<()> {
        let now = Instant::now();
        let _enter = info_span!("export timeline", timeline = %self.timelineid, tenant = %self.tenantid, start_lsn = %start_lsn, end_lsn = %end_lsn).entered();

        info!("exporting timeline");
        let zero_rel = RelishTag::Relation(ZERO_TAG);
        let mut from_rel = zero_rel;
        let mut from = StoreKey::Metadata(MetadataKey {
            rel: from_rel,
            lsn: start_lsn,
        });
        let mut relsizes: HashMap<RelishTag, VecMap<Lsn, u32>> = HashMap::new();
        let mut dropped: HashSet<RelishTag> = HashSet::new();
        let store = self.store.read().unwrap();
        'meta: loop {
            let mut iter = store.data.range(&from.ser()?..);

            while let Some(entry) = iter.next() {
                let pair = entry?;
                if let StoreKey::Metadata(dk) = StoreKey::des(&pair.0)? {
                    // processing metadata
                    from_rel = dk.rel;
                    if dk.lsn < start_lsn {
                        from = StoreKey::Metadata(MetadataKey {
                            rel: from_rel,
                            lsn: start_lsn,
                        });
                        continue 'meta;
                    } else if dk.lsn >= end_lsn {
                        from = StoreKey::Metadata(MetadataKey {
                            rel: from_rel,
                            lsn: Lsn::MAX,
                        });
                        continue 'meta;
                    } else {
                        let meta = MetadataValue::des(&pair.1)?;
                        if let Some(size) = meta.size {
                            if let Some(sizes) = relsizes.get_mut(&dk.rel) {
                                sizes.append(dk.lsn, size).unwrap();
                            } else {
                                let mut sizes = VecMap::default();
                                sizes.append(dk.lsn, size).unwrap();
                                relsizes.insert(dk.rel, sizes);
                            }
                        } else {
                            dropped.insert(dk.rel);
                        }
                    }
                } else {
                    // End of metadata
                    break 'meta;
                }
            }
            break;
        }

        from_rel = zero_rel;
        from = StoreKey::Data(DataKey {
            rel: from_rel,
            blknum: 0,
            lsn: Lsn(0),
        });

        // currently proceed block number
        let mut from_blknum = 0;
        let mut page_versions: Vec<(u32, Lsn, PageVersion)> = Vec::new();
        'pages: loop {
            let mut iter = store.data.range(&from.ser()?..);
            while let Some(entry) = iter.next() {
                let pair = entry?;
                if let StoreKey::Data(dk) = StoreKey::des(&pair.0)? {
                    let same_seg = from_rel == dk.rel
                        && dk.blknum / RELISH_SEG_SIZE < from_blknum / RELISH_SEG_SIZE;
                    if !same_seg && from_rel != zero_rel {
                        let is_dropped = dropped.contains(&from_rel);
                        let segtag = SegmentTag::from_blknum(from_rel, from_blknum);
                        if !page_versions.is_empty() {
                            DeltaLayer::create(
                                self.conf,
                                self.timelineid,
                                self.tenantid,
                                segtag,
                                start_lsn,
                                end_lsn,
                                is_dropped,
                                page_versions.iter().map(|t| (t.0, t.1, &t.2)),
                                relsizes[&from_rel].clone(),
                            )?;
                            page_versions.clear();
                        }
                        if !is_dropped {
                            let mut images: Vec<Bytes> =
                                Vec::with_capacity(RELISH_SEG_SIZE as usize);
                            let first_blknum = from_blknum & !RELISH_SEG_SIZE;
                            let last_blknum = u32::min(
                                first_blknum + RELISH_SEG_SIZE,
                                relsizes[&from_rel].last().map(|p| p.1).unwrap_or(0),
                            );
                            if first_blknum < last_blknum {
                                for blk in first_blknum..last_blknum {
                                    images.push(self.get_page_at_lsn(from_rel, blk, end_lsn)?);
                                }
                                ImageLayer::create(
                                    self.conf,
                                    self.timelineid,
                                    self.tenantid,
                                    segtag,
                                    end_lsn,
                                    images,
                                )?;
                            }
                        }
                    }
                    from_rel = dk.rel;
                    from_blknum = dk.blknum;
                    if dk.lsn < start_lsn {
                        from = StoreKey::Data(DataKey {
                            rel: from_rel,
                            blknum: from_blknum,
                            lsn: start_lsn,
                        });
                    } else if dk.lsn >= start_lsn {
                        from_blknum += 1;
                        from = StoreKey::Data(DataKey {
                            rel: from_rel,
                            blknum: from_blknum,
                            lsn: start_lsn,
                        });
                    } else {
                        page_versions.push((dk.blknum, dk.lsn, PageVersion::des(&pair.1)?));
                        continue;
                    }
                    continue 'pages;
                } else {
                    break 'pages;
                }
            }
            break;
        }
        if from_rel != zero_rel {
            let is_dropped = dropped.contains(&from_rel);
            let segtag = SegmentTag::from_blknum(from_rel, from_blknum);
            if !page_versions.is_empty() {
                DeltaLayer::create(
                    self.conf,
                    self.timelineid,
                    self.tenantid,
                    segtag,
                    start_lsn,
                    end_lsn,
                    is_dropped,
                    page_versions.iter().map(|t| (t.0, t.1, &t.2)),
                    relsizes[&from_rel].clone(),
                )?;
            }
            if !is_dropped {
                let mut images: Vec<Bytes> = Vec::with_capacity(RELISH_SEG_SIZE as usize);
                let first_blknum = from_blknum & !RELISH_SEG_SIZE;
                let last_blknum = u32::min(
                    first_blknum + RELISH_SEG_SIZE,
                    relsizes[&from_rel].last().map(|p| p.1).unwrap_or(0),
                );
                if first_blknum < last_blknum {
                    for blk in first_blknum..last_blknum {
                        images.push(self.get_page_at_lsn(from_rel, blk, end_lsn)?);
                    }
                    ImageLayer::create(
                        self.conf,
                        self.timelineid,
                        self.tenantid,
                        segtag,
                        end_lsn,
                        images,
                    )?;
                }
            }
        }
        info!("Export time line in {:?}", now.elapsed());
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

    fn writer<'a>(&'a self) -> Box<dyn TimelineWriter + 'a> {
        Box::new(BufferedTimelineWriter {
            tl: self,
            _write_guard: self.write_lock.lock().unwrap(),
        })
    }
}

impl RelishStore {
    fn load_metadata(&mut self) -> Result<&'_ mut HashMap<RelishTag, MetadataSnapshot>> {
        if self.meta.is_none() {
            let mut meta: HashMap<RelishTag, MetadataSnapshot> = HashMap::new();
            let mut till = StoreKey::Metadata(MetadataKey {
                rel: RelishTag::Checkpoint,
                lsn: Lsn::MAX,
            });
            loop {
                let mut iter = self.data.range(..&till.ser()?);
                if let Some(entry) = iter.next_back() {
                    let pair = entry?;
                    let key = StoreKey::des(&pair.0)?;
                    if let StoreKey::Metadata(last) = key {
                        let metadata = MetadataValue::des(&pair.1)?;
                        if let Some(size) = metadata.size {
                            // igonore dropped relations
                            meta.insert(
                                last.rel,
                                MetadataSnapshot {
                                    size,
                                    lsn: last.lsn,
                                },
                            );
                        }
                        till = StoreKey::Metadata(MetadataKey {
                            rel: last.rel,
                            lsn: Lsn(0),
                        });
                    } else {
                        bail!("Storage is corrupted: unexpected key: {:?}", key);
                    }
                } else {
                    break;
                }
            }
            self.meta = Some(meta)
        }
        Ok(self.meta.as_mut().unwrap())
    }

    fn _unload_metadata(&mut self) {
        self.meta = None;
    }
}

impl BufferedTimeline {
    /// Open a Timeline handle.
    ///
    /// Loads the metadata for the timeline into memory, but not the layer map.
    #[allow(clippy::too_many_arguments)]
    fn new(
        conf: &'static PageServerConf,
        metadata: TimelineMetadata,
        ancestor: Option<Arc<BufferedTimeline>>,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        current_logical_size: usize,
        upload_relishes: bool,
    ) -> Result<BufferedTimeline> {
        let path = conf.timeline_path(&timelineid, &tenantid);
        let timeline = BufferedTimeline {
            conf,
            timelineid,
            tenantid,
            store: RwLock::new(RelishStore {
                data: ToastStore::new(&path)?,
                meta: None,
            }),

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
            upload_relishes,

            write_lock: Mutex::new(()),
        };
        Ok(timeline)
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

    //
    // List all relish in inclsive range [from_rel, till_rel] exists at the specfied LSN
    //
    fn list_relishes(
        &self,
        from_rel: RelishTag,
        till_rel: RelishTag,
        lsn: Lsn,
    ) -> Result<HashSet<RelishTag>> {
        let mut result = HashSet::new();

        // from boundary is constant and till updated at each iteration
        let from = StoreKey::Metadata(MetadataKey {
            rel: from_rel,
            lsn: Lsn(0),
        })
        .ser()?;
        let mut till = StoreKey::Metadata(MetadataKey {
            rel: till_rel,
            lsn: Lsn::MAX,
        })
        .ser()?; // Lsn::MAX tranforms inclusive boundary to exclusive

        let store = self.store.read().unwrap();
        // Iterate through relish in reverse order (to locae last version)
        loop {
            // Use exclusive boundary for till to be able to skip to previous relish
            let mut iter = store.data.range(&from..&till);
            if let Some(entry) = iter.next_back() {
                // locate last version
                let pair = entry?;
                let key = StoreKey::des(&pair.0)?;
                if let StoreKey::Metadata(mk) = key {
                    if mk.lsn <= lsn {
                        // if LSN of last version is <= than requested, then we are done with this relish
                        let meta = MetadataValue::des(&pair.1)?;
                        if meta.size.is_some() {
                            // if relish was not dropped
                            result.insert(mk.rel);
                        }
                    } else {
                        // we need some older version
                        let from = StoreKey::Metadata(MetadataKey {
                            rel: mk.rel,
                            lsn: Lsn(0),
                        })
                        .ser()?;
                        let till = StoreKey::Metadata(MetadataKey { rel: mk.rel, lsn }).ser()?;

                        let mut iter = store.data.range(&from..=&till);
                        if let Some(entry) = iter.next_back() {
                            // locate visible version
                            let pair = entry?;
                            let key = StoreKey::des(&pair.0)?;
                            if let StoreKey::Metadata(mk) = key {
                                let meta = MetadataValue::des(&pair.1)?;
                                if meta.size.is_some() {
                                    result.insert(mk.rel);
                                }
                            } else {
                                bail!("Unexpected key {:?}", key);
                            }
                        }
                    }
                    // Jump to next relish by setting Lsn=0 and use it as exclusive boundary
                    till = StoreKey::Metadata(MetadataKey {
                        rel: mk.rel,
                        lsn: Lsn(0),
                    })
                    .ser()?;
                } else {
                    bail!("Unexpected key {:?}", key);
                }
            } else {
                break; // no more entries
            }
        }
        Ok(result)
    }

    ///
    /// Matrialize last page versions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL.
    /// checkpoint_interval is used to measure total length of applied WAL records.
    /// It can be used to prevent to frequent materialization of page. We can avoid store materialized page if history of changes is not so long
    /// and can be fast replayed. Alternatively we can measure interval from last version LSN:
    /// it will enforce materialization of "stabilized" pages. But there is a risk that permanently updated page will never be materialized.
    ///
    fn checkpoint_internal(&self, checkpoint_distance: u64, _forced: bool) -> Result<()> {
        // From boundary is constant and till boundary is changed at each iteration.
        let from = StoreKey::Data(DataKey {
            rel: RelishTag::Relation(ZERO_TAG),
            blknum: 0,
            lsn: Lsn(0),
        })
        .ser()?;

        let mut till = StoreKey::Data(DataKey {
            rel: RelishTag::Relation(RelTag {
                spcnode: u32::MAX,
                dbnode: u32::MAX,
                relnode: u32::MAX,
                forknum: u8::MAX,
            }),
            blknum: u32::MAX,
            lsn: Lsn::MAX,
        })
        .ser()?; // this MAX values allows to use this boundary as exclusive

        let mut n_checkpointed_records = 0;
        loop {
            let store = self.store.read().unwrap();

            let mut iter = store.data.range(&from..&till);
            if let Some(entry) = iter.next_back() {
                let pair = entry?;
                let key = pair.0;
                debug_assert!(key < till);
                if let StoreKey::Data(dk) = StoreKey::des(&key)? {
                    let ver = PageVersion::des(&pair.1)?;
                    if let PageVersion::Wal(rec) = ver {
                        // ignore already materialized pages
                        let mut will_init = rec.will_init;
                        let mut data = PageReconstructData {
                            records: Vec::new(),
                            page_img: None,
                        };
                        // Calculate total length of applied WAL records
                        let mut history_len = rec.rec.len();
                        data.records.push((dk.lsn, rec));
                        // loop until we locate full page image or initialization WAL record
                        // FIXME-KK: cross-timelines histories are not handled now
                        while !will_init {
                            if let Some(entry) = iter.next_back() {
                                let pair = entry?;
                                let key = StoreKey::des(&pair.0)?;
                                let ver = PageVersion::des(&pair.1)?;
                                if let StoreKey::Data(dk2) = key {
                                    assert!(dk.rel == dk2.rel); // check that we don't jump to previous relish before locating full image
                                    match ver {
                                        PageVersion::Page(img) => {
                                            data.page_img = Some(img);
                                            break;
                                        }
                                        PageVersion::Wal(rec) => {
                                            will_init = rec.will_init;
                                            history_len += rec.rec.len();
                                            data.records.push((dk2.lsn, rec));
                                        }
                                    }
                                } else {
                                    bail!("Unexpected key type {:?}", key);
                                }
                            } else {
                                bail!("Base image not found for relish {} at {}", dk.rel, dk.lsn);
                            }
                        }
                        // release locks and  reconstruct page withut blocking storage
                        drop(iter);
                        drop(store);
                        // See comment above. May be we should also enforce here checkpointing of too old versions.
                        if history_len as u64 >= checkpoint_distance {
                            let img = RECONSTRUCT_TIME.observe_closure_duration(|| {
                                self.reconstruct_page(dk.rel, dk.blknum, dk.lsn, data)
                            });

                            let mut store = self.store.write().unwrap();
                            store.data.put(&key, &PageVersion::Page(img?).ser()?)?;
                            n_checkpointed_records += 1;
                        }
                    }
                    // Jump to next page. Setting lsn=0 and using it as exclusive boundary allows us to jump to previous page.
                    till = StoreKey::Data(DataKey {
                        rel: dk.rel,
                        blknum: dk.blknum,
                        lsn: Lsn(0),
                    })
                    .ser()?;
                } else {
                    bail!("Unexpected key {:?}", key);
                }
            } else {
                break;
            }
        }
        let ancestor_timelineid = self.ancestor_timeline.as_ref().map(|x| x.timelineid);
        let ondisk_prev_record_lsn = self.get_prev_record_lsn();
        let metadata = TimelineMetadata {
            disk_consistent_lsn: self.disk_consistent_lsn.load(),
            prev_record_lsn: Some(ondisk_prev_record_lsn),
            ancestor_timeline: ancestor_timelineid,
            ancestor_lsn: self.ancestor_lsn,
        };

        BufferedRepository::save_metadata(
            self.conf,
            self.timelineid,
            self.tenantid,
            &metadata,
            false,
        )?;
        info!("Checkpoint {} records", n_checkpointed_records);

        if self.upload_relishes {
            schedule_timeline_upload(())
            // schedule_timeline_upload(
            //     self.tenantid,
            //     self.timelineid,
            //     layer_uploads,
            //     disk_consistent_lsn,
            // });
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
    pub fn gc_timeline(&self, _retain_lsns: Vec<Lsn>, cutoff: Lsn) -> Result<GcResult> {
        let now = Instant::now();
        let mut result: GcResult = Default::default();

        let _enter = info_span!("garbage collection", timeline = %self.timelineid, tenant = %self.tenantid, cutoff = %cutoff).entered();

        info!("GC starting");

        let zero_rel = RelishTag::Relation(ZERO_TAG);
        let mut from_rel = zero_rel;
        let mut from = StoreKey::Metadata(MetadataKey {
            rel: from_rel,
            lsn: Lsn(0),
        });
        // Keep tracked dropped relish
        let mut dropped: HashSet<RelishTag> = HashSet::new();

        'meta: loop {
            let store = self.store.read().unwrap();
            let mut iter = store.data.range(&from.ser()?..);
            // We can not remove deteriorated version immediately, we need to check first that successor exists
            let mut last_key: Option<yakv::storage::Key> = None;

            while let Some(entry) = iter.next() {
                let pair = entry?;
                let raw_key = pair.0;
                let key = StoreKey::des(&raw_key)?;
                if let StoreKey::Metadata(dk) = key {
                    // processing metadata
                    let same_rel = from_rel == dk.rel;
                    if !same_rel {
                        // we jumped to the next relation
                        result.meta_total += 1;
                        from_rel = dk.rel;
                        from = StoreKey::Metadata(MetadataKey {
                            rel: from_rel,
                            lsn: Lsn(0),
                        });
                    }
                    if dk.lsn < cutoff {
                        // we have something deteriorated
                        // Has previos version
                        if let Some(prev_key) = last_key {
                            // If we are still on the same relish...
                            if same_rel {
                                // then drop previus version as it is not needed any more
                                drop(store);
                                let mut store = self.store.write().unwrap();
                                store.data.remove(&prev_key)?;
                                result.meta_removed += 1;
                                // We should reset iterator and start from the current point
                                continue 'meta;
                            }
                        }
                        let meta = MetadataValue::des(&pair.1)?;
                        if meta.size.is_none() {
                            // object was dropped, so we can immediately remove deteriorated version
                            drop(store);
                            let mut store = self.store.write().unwrap();
                            store.data.remove(&raw_key)?;
                            dropped.insert(dk.rel);
                            result.meta_dropped += 1;
                            // We should reset iterator and start from the current point
                            continue 'meta;
                        } else {
                            // Remember key as candidate for deletion and contiune iteration
                            last_key = Some(raw_key);
                        }
                    } else {
                        // We reached version which should be preserved to enable PITR, so jump to the next object
                        from = StoreKey::Metadata(MetadataKey {
                            rel: from_rel,
                            lsn: Lsn::MAX,
                        });
                        continue 'meta;
                    }
                } else {
                    // End of metadata
                    break 'meta;
                }
            }
            break;
        }

        from_rel = zero_rel;
        from = StoreKey::Data(DataKey {
            rel: from_rel,
            blknum: 0,
            lsn: Lsn(0),
        });

        // Array to accumulate keys we can remove.
        // Place it outside main loop to reduce number of dynamic memory allocations
        let mut deteriorated: Vec<yakv::storage::Key> = Vec::new();
        // currently proceed block number
        let mut from_blknum = 0;
        'pages: loop {
            let store = self.store.read().unwrap();
            let mut iter = store.data.range(&from.ser()?..);
            deteriorated.clear();
            while let Some(entry) = iter.next() {
                let pair = entry?;
                let raw_key = pair.0;
                let key = StoreKey::des(&raw_key)?;
                if let StoreKey::Data(dk) = key {
                    let same_page = from_rel == dk.rel && from_blknum == dk.blknum;
                    if !same_page {
                        result.pages_total += 1;
                        from_rel = dk.rel;
                        from_blknum = dk.blknum;
                        from = StoreKey::Data(DataKey {
                            rel: from_rel,
                            blknum: from_blknum,
                            lsn: Lsn(0),
                        });
                    }
                    if dk.lsn < cutoff {
                        // we have something deteriorated
                        // If we still on the same page...
                        if same_page {
                            if !deteriorated.is_empty() {
                                // .. and have something to remove
                                // ... and have page image
                                let ver = PageVersion::des(&pair.1)?;
                                if let PageVersion::Page(_) = ver {
                                    // ... then remove all previously accumulated deltas and images, as them are not needed any more
                                    drop(store);
                                    let mut store = self.store.write().unwrap();
                                    for key in deteriorated.iter() {
                                        store.data.remove(key)?;
                                    }
                                    result.pages_removed += deteriorated.len() as u64;
                                    // We should reset iterator and start from the current point
                                    continue 'pages;
                                }
                            }
                        } else {
                            // we jumped to the next relation
                            deteriorated.clear();
                        }
                        if dropped.contains(&dk.rel) {
                            // This relations was dropped beyond PITR interval:
                            // we can remove all its pages
                            assert!(deteriorated.is_empty()); // we should not append anything to `deteriorated` for dropped relation
                            drop(store);
                            let mut store = self.store.write().unwrap();
                            // We should reset iterator and start from the current point
                            store.data.remove(&raw_key)?;
                            result.pages_dropped += 1;
                            continue 'pages;
                        }
                        // Remember key as candidate for deletion and continue iteration
                        deteriorated.push(raw_key);
                    } else {
                        // We reached version which should be preserved to enable PITR, so jump to the next object
                        from = StoreKey::Data(DataKey {
                            rel: from_rel,
                            blknum: from_blknum + 1,
                            lsn: Lsn(0),
                        });
                    }
                } else {
                    break 'pages;
                }
            }
            break;
        }

        result.elapsed = now.elapsed();
        Ok(result)
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
}

struct BufferedTimelineWriter<'a> {
    tl: &'a BufferedTimeline,
    _write_guard: MutexGuard<'a, ()>,
}

impl Deref for BufferedTimelineWriter<'_> {
    type Target = dyn Timeline;

    fn deref(&self) -> &Self::Target {
        self.tl
    }
}

impl<'a> BufferedTimelineWriter<'a> {
    fn put_page_version(
        &self,
        rel: RelishTag,
        blknum: u32,
        lsn: Lsn,
        ver: PageVersion,
    ) -> Result<()> {
        if !rel.is_blocky() && blknum != 0 {
            bail!(
                "invalid request for block {} for non-blocky relish {}",
                blknum,
                rel
            );
        }
        ensure!(lsn.is_aligned(), "unaligned record LSN");
        let key = StoreKey::Data(DataKey { rel, blknum, lsn });
        let mut store = self.tl.store.write().unwrap();
        store.data.put(&key.ser()?, &ver.ser()?)?;

        // Update metadata
        let meta_hash = store.load_metadata()?;
        let rel_size = meta_hash.get(&rel).map(|m| m.size).unwrap_or(0);
        if rel_size <= blknum {
            meta_hash.insert(
                rel,
                MetadataSnapshot {
                    size: blknum + 1,
                    lsn,
                },
            );
            let mk = StoreKey::Metadata(MetadataKey { rel, lsn });
            let mv = MetadataValue {
                size: Some(blknum + 1),
            };
            store.data.put(&mk.ser()?, &mv.ser()?)?;
            /* Looks like we do not need to explicitly fill gap, because we in any case have to handle situation when
             * page in accessed before been wal logged
                        //  Fill gap with zero pages
                        for blk in rel_size..blknum {
                            let key = StoreKey::Data(DataKey {
                                rel,
                                blknum: blk,
                                lsn,
                            });
                            store
                                .data
                                .put(&key.ser()?, &PageVersion::Image(ZERO_PAGE.clone()).ser()?)?;
                        }
            */
        }
        if store.data.committed {
            self.tl.disk_consistent_lsn.store(lsn); // each update is flushed to the disk
        }
        Ok(())
    }
}

impl<'a> TimelineWriter for BufferedTimelineWriter<'a> {
    ///
    /// Import data from layer files
    ///
    fn import_timeline(&self, snapshot_lsn: Lsn) -> Result<()> {
        let now = Instant::now();
        let (imgfilenames, deltafilenames) =
            filename::list_files(self.tl.conf, self.tl.timelineid, self.tl.tenantid)?;

        let mut data = PageReconstructData {
            records: Vec::new(),
            page_img: None,
        };

        for filename in &imgfilenames {
            if filename.lsn == snapshot_lsn {
                let layer =
                    ImageLayer::new(self.tl.conf, self.tl.timelineid, self.tl.tenantid, filename);
                let seg_size = layer.get_seg_size(snapshot_lsn)?;
                for blknum in 0..seg_size {
                    match layer.get_page_reconstruct_data(blknum, snapshot_lsn, &mut data)? {
                        PageReconstructResult::Complete => {
                            if let Some(page) = data.page_img.take() {
                                self.put_page_image(
                                    filename.seg.rel,
                                    filename.seg.segno * RELISH_SEG_SIZE + blknum,
                                    snapshot_lsn,
                                    page,
                                )?;
                            }
                        }
                        PageReconstructResult::Continue(_) => bail!("Branches not supported"),
                        PageReconstructResult::Missing(_) => bail!("Failed to extract page image"),
                    }
                }
            }
        }

        for filename in &deltafilenames {
            ensure!(filename.start_lsn < filename.end_lsn);
            if filename.start_lsn >= snapshot_lsn {
                let layer =
                    DeltaLayer::new(self.tl.conf, self.tl.timelineid, self.tl.tenantid, filename);
                for (blk, lsn, ver) in layer.versions()? {
                    self.put_page_version(filename.seg.rel, blk, lsn, ver)?;
                }
            }
        }
        info!("Import timeline completed in {:?}", now.elapsed());
        Ok(())
    }

    fn put_wal_record(&self, lsn: Lsn, rel: RelishTag, blknum: u32, rec: WALRecord) -> Result<()> {
        self.put_page_version(rel, blknum, lsn, PageVersion::Wal(rec))
    }

    fn put_page_image(&self, rel: RelishTag, blknum: u32, lsn: Lsn, img: Bytes) -> Result<()> {
        self.put_page_version(rel, blknum, lsn, PageVersion::Page(img))
    }

    fn put_truncation(&self, rel: RelishTag, lsn: Lsn, relsize: u32) -> Result<()> {
        if !rel.is_blocky() {
            bail!("invalid truncation for non-blocky relish {}", rel);
        }
        ensure!(lsn.is_aligned(), "unaligned record LSN");

        debug!("put_truncation: {} to {} blocks at {}", rel, relsize, lsn);

        let mut store = self.tl.store.write().unwrap();
        let meta_hash = store.load_metadata()?;
        meta_hash.insert(rel, MetadataSnapshot { size: relsize, lsn });
        let mk = StoreKey::Metadata(MetadataKey { rel, lsn });
        let mv = MetadataValue {
            size: Some(relsize),
        };
        store.data.put(&mk.ser()?, &mv.ser()?)?;
        if store.data.committed {
            self.tl.disk_consistent_lsn.store(lsn); // each update is flushed to the disk
        }
        Ok(())
    }

    fn drop_relish(&self, rel: RelishTag, lsn: Lsn) -> Result<()> {
        trace!("drop_segment: {} at {}", rel, lsn);

        let mut store = self.tl.store.write().unwrap();
        let meta_hash = store.load_metadata()?;
        meta_hash.remove(&rel);
        let mk = StoreKey::Metadata(MetadataKey { rel, lsn });
        let mv = MetadataValue { size: None }; // None indicates dropped relation
        store.data.put(&mk.ser()?, &mv.ser()?)?;

        if store.data.committed {
            self.tl.disk_consistent_lsn.store(lsn); // each update is flushed to the disk
        }
        Ok(())
    }

    ///
    /// Complete all delayed commits and advance disk_consistent_lsn
    ///
    fn checkpoint(&self) -> Result<()> {
        let store = self.tl.store.write().unwrap();
        store.data.checkpoint()?;
        self.tl
            .disk_consistent_lsn
            .store(self.tl.get_last_record_lsn());
        Ok(())
    }

    ///
    /// Remember the (end of) last valid WAL record remembered in the timeline.
    ///
    fn advance_last_record_lsn(&self, new_lsn: Lsn) {
        self.tl.last_record_lsn.advance(new_lsn);
    }
}

fn metadata_path(
    conf: &'static PageServerConf,
    timelineid: ZTimelineId,
    tenantid: ZTenantId,
) -> PathBuf {
    conf.timeline_path(&timelineid, &tenantid)
        .join(METADATA_FILE_NAME)
}
