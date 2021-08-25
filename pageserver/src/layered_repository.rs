//!
//! Zenith repository implementation that keeps old data in "snapshot files", and
//! the recent changes in memory. See layered_repository/snapshot_layer.rs and
//! layered_repository/inmemory_layer.rs, respectively. The functions here are
//! responsible for locating the correct layer for the get/put call, tracing
//! timeline branching history as needed.
//!
//! The snapshot files are stored in the .zenith/tenants/<tenantid>/timelines/<timelineid>
//! directory. See layered_repository/README for how the files are managed.
//! In addition to the snapshot files, there is a metadata file in the same
//! directory that contains information about the timeline, in particular its
//! parent timeline, and the last LSN that has been written to disk.
//!

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use lazy_static::lazy_static;
use log::*;
use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::fs::File;
use std::io::Write;
use std::ops::Bound::Included;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::relish::*;
use crate::repository::{GcResult, Repository, Timeline, WALRecord};
use crate::restore_local_repo::import_timeline_wal;
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};

use zenith_metrics::{register_histogram_vec, HistogramVec};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::{AtomicLsn, Lsn};
use zenith_utils::seqwait::SeqWait;

mod filename;
mod inmemory_layer;
mod layer_map;
mod snapshot_layer;
mod storage_layer;

use inmemory_layer::InMemoryLayer;
use layer_map::LayerMap;
use snapshot_layer::SnapshotLayer;
use storage_layer::{Layer, PageReconstructData, SegmentTag, RELISH_SEG_SIZE};

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

// Timeout when waiting for WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(60);

// Flush out an inmemory layer, if it's holding WAL older than this.
// This puts a backstop on how much WAL needs to be re-digested if the
// page server crashes.
//
// FIXME: This current value is very low. I would imagine something like 1 GB or 10 GB
// would be more appropriate. But a low value forces the code to be exercised more,
// which is good for now to trigger bugs.
static OLDEST_INMEM_DISTANCE: u64 = 16 * 1024 * 1024;

// Metrics collected on operations on the storage repository.
lazy_static! {
    static ref STORAGE_TIME: HistogramVec = register_histogram_vec!(
        "pageserver_storage_time",
        "Time spent on storage operations",
        &["operation"]
    )
    .expect("failed to define a metric");
}

///
/// Repository consists of multiple timelines. Keep them in a hash table.
///
pub struct LayeredRepository {
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    timelines: Mutex<HashMap<ZTimelineId, Arc<LayeredTimeline>>>,

    walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
}

/// Public interface
impl Repository for LayeredRepository {
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        Ok(self.get_timeline_locked(timelineid, &mut timelines)?)
    }

    fn create_empty_timeline(
        &self,
        timelineid: ZTimelineId,
        start_lsn: Lsn,
    ) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        // Create the timeline directory, and write initial metadata to file.
        std::fs::create_dir_all(self.conf.timeline_path(&timelineid, &self.tenantid))?;

        let metadata = TimelineMetadata {
            disk_consistent_lsn: start_lsn,
            prev_record_lsn: None,
            ancestor_timeline: None,
            ancestor_lsn: start_lsn,
        };
        Self::save_metadata(self.conf, timelineid, self.tenantid, &metadata)?;

        let timeline = LayeredTimeline::new(
            self.conf,
            metadata,
            None,
            timelineid,
            self.tenantid,
            self.walredo_mgr.clone(),
        )?;

        let timeline_rc = Arc::new(timeline);
        let r = timelines.insert(timelineid, timeline_rc.clone());
        assert!(r.is_none());
        Ok(timeline_rc)
    }

    /// Branch a timeline
    fn branch_timeline(&self, src: ZTimelineId, dst: ZTimelineId, start_lsn: Lsn) -> Result<()> {
        let src_timeline = self.get_timeline(src)?;

        // Create the metadata file, noting the ancestor of the new timeline.
        // There is initially no data in it, but all the read-calls know to look
        // into the ancestor.
        let metadata = TimelineMetadata {
            disk_consistent_lsn: start_lsn,
            prev_record_lsn: Some(src_timeline.get_prev_record_lsn()), // FIXME not atomic with start_lsn
            ancestor_timeline: Some(src),
            ancestor_lsn: start_lsn,
        };
        std::fs::create_dir_all(self.conf.timeline_path(&dst, &self.tenantid))?;
        Self::save_metadata(self.conf, dst, self.tenantid, &metadata)?;

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
        compact: bool,
    ) -> Result<GcResult> {
        STORAGE_TIME
            .with_label_values(&["gc"])
            .observe_closure_duration(|| {
                self.gc_iteration_internal(target_timelineid, horizon, compact)
            })
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

                let timeline = LayeredTimeline::new(
                    self.conf,
                    metadata,
                    ancestor,
                    timelineid,
                    self.tenantid,
                    self.walredo_mgr.clone(),
                )?;

                // List the snapshot layers on disk, and load them into the layer map
                timeline.load_layer_map()?;

                // Load any new WAL after the last checkpoint into memory.
                info!(
                    "Loading WAL for timeline {} starting at {}",
                    timelineid,
                    timeline.get_last_record_lsn()
                );
                let wal_dir = self
                    .conf
                    .timeline_path(&timelineid, &self.tenantid)
                    .join("wal");
                import_timeline_wal(&wal_dir, &timeline, timeline.last_record_lsn.load())?;

                let timeline_rc = Arc::new(timeline);
                timelines.insert(timelineid, timeline_rc.clone());
                Ok(timeline_rc)
            }
        }
    }

    pub fn new(
        conf: &'static PageServerConf,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        tenantid: ZTenantId,
    ) -> LayeredRepository {
        LayeredRepository {
            tenantid: tenantid,
            conf: conf,
            timelines: Mutex::new(HashMap::new()),
            walredo_mgr,
        }
    }

    ///
    /// Launch the checkpointer thread in given repository.
    ///
    pub fn launch_checkpointer_thread(conf: &'static PageServerConf, rc: Arc<LayeredRepository>) {
        let _thread = std::thread::Builder::new()
            .name("Checkpointer thread".into())
            .spawn(move || {
                // FIXME: relaunch it? Panic is not good.
                rc.checkpoint_loop(conf).expect("Checkpointer thread died");
            })
            .unwrap();
    }

    ///
    /// Checkpointer thread's main loop
    ///
    fn checkpoint_loop(&self, conf: &'static PageServerConf) -> Result<()> {
        loop {
            std::thread::sleep(conf.gc_period);

            info!("checkpointer thread for tenant {} waking up", self.tenantid);

            // checkpoint timelines that have accumulated more than CHECKPOINT_INTERVAL
            // bytes of WAL since last checkpoint.
            {
                let timelines = self.timelines.lock().unwrap();
                for (_timelineid, timeline) in timelines.iter() {
                    STORAGE_TIME
                        .with_label_values(&["checkpoint_timed"])
                        .observe_closure_duration(|| timeline.checkpoint_internal(false))?
                }
                // release lock on 'timelines'
            }

            // Garbage collect old files that are not needed for PITR anymore
            if conf.gc_horizon > 0 {
                self.gc_iteration(None, conf.gc_horizon, false).unwrap();
            }
        }
    }

    /// Save timeline metadata to file
    fn save_metadata(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        data: &TimelineMetadata,
    ) -> Result<()> {
        let path = conf.timeline_path(&timelineid, &tenantid).join("metadata");
        let mut file = File::create(&path)?;

        info!("saving metadata {}", path.display());

        file.write_all(&TimelineMetadata::ser(data)?)?;

        Ok(())
    }

    fn load_metadata(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
    ) -> Result<TimelineMetadata> {
        let path = conf.timeline_path(&timelineid, &tenantid).join("metadata");
        let data = std::fs::read(&path)?;

        Ok(TimelineMetadata::des(&data)?)
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
    // 3. For each timeline, scan all snapshot files on the timeline.
    //    Remove all files for which a newer file exists and which
    //    don't cover any branch point LSNs.
    //
    // TODO:
    // - if a relation has been modified on a child branch, then we
    //   don't need to keep that in the parent anymore. But currently
    //   we do.
    fn gc_iteration_internal(
        &self,
        target_timelineid: Option<ZTimelineId>,
        horizon: u64,
        compact: bool,
    ) -> Result<GcResult> {
        let mut totals: GcResult = Default::default();
        let now = Instant::now();

        // grab mutex to prevent new timelines from being created here.
        // TODO: We will hold it for a long time
        let mut timelines = self.timelines.lock().unwrap();

        // Scan all timelines. For each timeline, remember the timeline ID and
        // the branch point where it was created.
        //
        // We scan the directory, not the in-memory hash table, because the hash
        // table only contains entries for timelines that have been accessed. We
        // need to take all timelines into account, not only the active ones.
        let mut timelineids: Vec<ZTimelineId> = Vec::new();
        let mut all_branchpoints: BTreeSet<(ZTimelineId, Lsn)> = BTreeSet::new();
        let timelines_path = self.conf.timelines_path(&self.tenantid);
        for direntry in fs::read_dir(timelines_path)? {
            let direntry = direntry?;
            if let Some(fname) = direntry.file_name().to_str() {
                if let Ok(timelineid) = fname.parse::<ZTimelineId>() {
                    timelineids.push(timelineid);

                    // Read the metadata of this timeline to get its parent timeline.
                    //
                    // We read the ancestor information directly from the file, instead
                    // of calling get_timeline(). We don't want to load the timeline
                    // into memory just for GC.
                    //
                    // FIXME: we open the timeline in the loop below with
                    // get_timeline_locked() anyway, so maybe we should just do it
                    // here, too.
                    let metadata = Self::load_metadata(self.conf, timelineid, self.tenantid)?;
                    if let Some(ancestor_timeline) = metadata.ancestor_timeline {
                        all_branchpoints.insert((ancestor_timeline, metadata.ancestor_lsn));
                    }
                }
            }
        }

        // Ok, we now know all the branch points. Iterate through them.
        for timelineid in timelineids {
            // If a target timeline was specified, leave the other timelines alone.
            // This is a bit inefficient, because we still collect the information for
            // all the timelines above.
            if let Some(x) = target_timelineid {
                if x != timelineid {
                    continue;
                }
            }

            let branchpoints: Vec<Lsn> = all_branchpoints
                .range((
                    Included((timelineid, Lsn(0))),
                    Included((timelineid, Lsn(u64::MAX))),
                ))
                .map(|&x| x.1)
                .collect();

            let timeline = self.get_timeline_locked(timelineid, &mut *timelines)?;
            let last_lsn = timeline.get_last_valid_lsn();

            if let Some(cutoff) = last_lsn.checked_sub(horizon) {
                // If GC was explicitly requested by the admin, force flush all in-memory
                // layers to disk first, so that they too can be garbage collected. That's
                // used in tests, so we want as deterministic results as possible.
                if compact {
                    timeline.checkpoint()?;
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
    // request > last_valid_lsn, we need to wait until we receive all
    // the WAL up to the request. The SeqWait provides functions for
    // that. TODO: If we get a request for an old LSN, such that the
    // versions have already been garbage collected away, we should
    // throw an error, but we don't track that currently.
    //
    // last_record_lsn points to the end of last processed WAL record.
    // It can lag behind last_valid_lsn, if the WAL receiver has
    // received some WAL after the end of last record, but not the
    // whole next record yet. In the page cache, we care about
    // last_valid_lsn, but if the WAL receiver needs to restart the
    // streaming, it needs to restart at the end of last record, so we
    // track them separately. last_record_lsn should perhaps be in
    // walreceiver.rs instead of here, but it seems convenient to keep
    // all three values together.
    //
    // We also remember the starting point of the previous record in
    // 'prev_record_lsn'. It's used to set the xl_prev pointer of the
    // first WAL record when the node is started up. But here, we just
    // keep track of it. FIXME: last_record_lsn and prev_record_lsn
    // should be updated atomically together.
    //
    last_valid_lsn: SeqWait<Lsn>,
    last_record_lsn: AtomicLsn,
    prev_record_lsn: AtomicLsn,

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
}

/// Public interface functions
impl Timeline for LayeredTimeline {
    /// Look up given page in the cache.
    fn get_page_at_lsn(&self, rel: RelishTag, blknum: u32, lsn: Lsn) -> Result<Bytes> {
        let lsn = self.wait_lsn(lsn)?;

        self.get_page_at_lsn_nowait(rel, blknum, lsn)
    }

    fn get_page_at_lsn_nowait(&self, rel: RelishTag, blknum: u32, lsn: Lsn) -> Result<Bytes> {
        if !rel.is_blocky() && blknum != 0 {
            bail!(
                "invalid request for block {} for non-blocky relish {}",
                blknum,
                rel
            );
        }

        let seg = SegmentTag::from_blknum(rel, blknum);

        if let Some((layer, lsn)) = self.get_layer_for_read(seg, lsn)? {
            self.materialize_page(seg, blknum, lsn, &*layer)
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

        let lsn = self.wait_lsn(lsn)?;

        let mut segno = 0;
        loop {
            let seg = SegmentTag { rel, segno };

            let segsize;
            if let Some((layer, lsn)) = self.get_layer_for_read(seg, lsn)? {
                segsize = layer.get_seg_size(lsn)?;
                trace!(
                    "get_seg_size: {} at {}/{} -> {}",
                    seg,
                    self.timelineid,
                    lsn,
                    segsize
                );
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
        let lsn = self.wait_lsn(lsn)?;

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

    fn list_rels(&self, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelTag>> {
        trace!("list_rels called at {}", lsn);

        // List all rels in this timeline, and all its ancestors.
        let mut all_rels = HashSet::new();
        let mut timeline = self;
        loop {
            let rels = timeline.layers.lock().unwrap().list_rels(spcnode, dbnode)?;

            all_rels.extend(rels.iter());

            if let Some(ancestor) = timeline.ancestor_timeline.as_ref() {
                timeline = ancestor;
                continue;
            } else {
                break;
            }
        }

        // Now we have a list of all rels that appeared anywhere in the history. Filter
        // out relations that were dropped.
        //
        // FIXME: We should pass the LSN argument to the calls above, and avoid scanning
        // dropped relations in the first place.
        let mut res: Result<()> = Ok(());
        all_rels.retain(
            |reltag| match self.get_rel_exists(RelishTag::Relation(*reltag), lsn) {
                Ok(exists) => {
                    info!("retain: {} -> {}", *reltag, exists);
                    exists
                }
                Err(err) => {
                    res = Err(err);
                    false
                }
            },
        );
        res?;

        Ok(all_rels)
    }

    fn list_nonrels(&self, lsn: Lsn) -> Result<HashSet<RelishTag>> {
        info!("list_nonrels called at {}", lsn);

        // List all nonrels in this timeline, and all its ancestors.
        let mut all_rels = HashSet::new();
        let mut timeline = self;
        loop {
            let rels = timeline.layers.lock().unwrap().list_nonrels(lsn)?;

            all_rels.extend(rels.iter());

            if let Some(ancestor) = timeline.ancestor_timeline.as_ref() {
                timeline = ancestor;
                continue;
            } else {
                break;
            }
        }

        // Now we have a list of all nonrels that appeared anywhere in the history. Filter
        // out dropped ones.
        //
        // FIXME: We should pass the LSN argument to the calls above, and avoid scanning
        // dropped relations in the first place.
        let mut res: Result<()> = Ok(());
        all_rels.retain(|tag| match self.get_rel_exists(*tag, lsn) {
            Ok(exists) => {
                info!("retain: {} -> {}", *tag, exists);
                exists
            }
            Err(err) => {
                res = Err(err);
                false
            }
        });
        res?;

        Ok(all_rels)
    }

    fn put_wal_record(&self, rel: RelishTag, blknum: u32, rec: WALRecord) -> Result<()> {
        if !rel.is_blocky() && blknum != 0 {
            bail!(
                "invalid request for block {} for non-blocky relish {}",
                blknum,
                rel
            );
        }

        let seg = SegmentTag::from_blknum(rel, blknum);

        let layer = self.get_layer_for_write(seg, rec.lsn)?;
        layer.put_wal_record(blknum, rec)
    }

    fn put_truncation(&self, rel: RelishTag, lsn: Lsn, relsize: u32) -> anyhow::Result<()> {
        if !rel.is_blocky() {
            bail!("invalid truncation for non-blocky relish {}", rel);
        }

        debug!("put_truncation: {} to {} blocks at {}", rel, relsize, lsn);

        let oldsize = self
            .get_relish_size(rel, self.last_valid_lsn.load())?
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

        // Unlink segments beyond the last remaining segment.
        for remove_segno in (last_remain_seg + 1)..=old_last_seg {
            let seg = SegmentTag {
                rel,
                segno: remove_segno,
            };
            let layer = self.get_layer_for_write(seg, lsn)?;
            layer.put_unlink(lsn)?;
        }

        // Truncate the last remaining segment to the specified size
        if relsize == 0 || relsize % RELISH_SEG_SIZE != 0 {
            let seg = SegmentTag {
                rel,
                segno: last_remain_seg,
            };
            let layer = self.get_layer_for_write(seg, lsn)?;
            layer.put_truncation(lsn, relsize % RELISH_SEG_SIZE)?;
        }

        Ok(())
    }

    fn put_unlink(&self, rel: RelishTag, lsn: Lsn) -> Result<()> {
        trace!("put_unlink: {} at {}", rel, lsn);

        if rel.is_blocky() {
            let oldsize_opt = self.get_relish_size(rel, self.last_valid_lsn.load())?;
            if let Some(oldsize) = oldsize_opt {
                let old_last_seg = if oldsize == 0 {
                    0
                } else {
                    (oldsize - 1) / RELISH_SEG_SIZE
                };

                // Unlink all segments
                for remove_segno in 0..=old_last_seg {
                    let seg = SegmentTag {
                        rel,
                        segno: remove_segno,
                    };
                    let layer = self.get_layer_for_write(seg, lsn)?;
                    layer.put_unlink(lsn)?;
                }
            } else {
                warn!(
                    "put_unlink called on non-existent relish {} at {}",
                    rel, lsn
                );
            }
        } else {
            let seg = SegmentTag::from_blknum(rel, 0);
            let layer = self.get_layer_for_write(seg, lsn)?;
            layer.put_unlink(lsn)?;
        }

        Ok(())
    }

    fn put_page_image(
        &self,
        rel: RelishTag,
        blknum: u32,
        lsn: Lsn,
        img: Bytes,
        _update_meta: bool,
    ) -> Result<()> {
        if !rel.is_blocky() && blknum != 0 {
            bail!(
                "invalid request for block {} for non-blocky relish {}",
                blknum,
                rel
            );
        }

        let seg = SegmentTag::from_blknum(rel, blknum);

        let layer = self.get_layer_for_write(seg, lsn)?;
        layer.put_page_image(blknum, lsn, img)
    }

    /// Public entry point for checkpoint(). All the logic is in the private
    /// checkpoint_internal function, this public facade just wraps it for
    /// metrics collection.
    fn checkpoint(&self) -> Result<()> {
        STORAGE_TIME
            .with_label_values(&["checkpoint_force"])
            .observe_closure_duration(|| self.checkpoint_internal(true))
    }

    /// Remember that WAL has been received and added to the page cache up to the given LSN
    fn advance_last_valid_lsn(&self, lsn: Lsn) {
        let old = self.last_valid_lsn.advance(lsn);

        // The last valid LSN cannot move backwards, but when WAL
        // receiver is restarted after having only partially processed
        // a record, it can call this with an lsn older than previous
        // last valid LSN, when it restarts processing that record.
        if lsn < old {
            // Should never be called with an LSN older than the last
            // record LSN, though.
            let last_record_lsn = self.last_record_lsn.load();
            if lsn < last_record_lsn {
                warn!(
                    "attempted to move last valid LSN backwards beyond last record LSN (last record {}, new {})",
                    last_record_lsn, lsn
                );
            }
        }
    }

    fn init_valid_lsn(&self, lsn: Lsn) {
        let old = self.last_valid_lsn.advance(lsn);
        assert!(old == Lsn(0));
        let old = self.last_record_lsn.fetch_max(lsn);
        assert!(old == Lsn(0));
        self.prev_record_lsn.store(Lsn(0));
    }

    fn get_last_valid_lsn(&self) -> Lsn {
        self.last_valid_lsn.load()
    }

    ///
    /// Remember the (end of) last valid WAL record remembered in the page cache.
    ///
    /// NOTE: this updates last_valid_lsn as well.
    ///
    fn advance_last_record_lsn(&self, lsn: Lsn) {
        // Can't move backwards.
        let old = self.last_record_lsn.fetch_max(lsn);
        assert!(old <= lsn);

        // Use old value of last_record_lsn as prev_record_lsn
        self.prev_record_lsn.fetch_max(old);

        // Also advance last_valid_lsn
        let old = self.last_valid_lsn.advance(lsn);
        // Can't move backwards.
        if lsn < old {
            warn!(
                "attempted to move last record LSN backwards (was {}, new {})",
                old, lsn
            );
        }
    }

    fn get_last_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load()
    }

    fn get_prev_record_lsn(&self) -> Lsn {
        self.prev_record_lsn.load()
    }
}

impl LayeredTimeline {
    /// Open a Timeline handle.
    ///
    /// Loads the metadata for the timeline into memory, but not the layer map.
    fn new(
        conf: &'static PageServerConf,
        metadata: TimelineMetadata,
        ancestor: Option<Arc<LayeredTimeline>>,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
    ) -> Result<LayeredTimeline> {
        let timeline = LayeredTimeline {
            conf,
            timelineid,
            tenantid,
            layers: Mutex::new(LayerMap::default()),

            walredo_mgr,

            // initialize in-memory 'last_valid_lsn' and 'last_record_lsn' from
            // 'disk_consistent_lsn'.
            last_valid_lsn: SeqWait::new(metadata.disk_consistent_lsn),
            last_record_lsn: AtomicLsn::new(metadata.disk_consistent_lsn.0),

            prev_record_lsn: AtomicLsn::new(metadata.prev_record_lsn.unwrap_or(Lsn(0)).0),
            disk_consistent_lsn: AtomicLsn::new(metadata.disk_consistent_lsn.0),

            ancestor_timeline: ancestor,
            ancestor_lsn: metadata.ancestor_lsn,
        };
        Ok(timeline)
    }

    ///
    /// Load the list of snapshot files from disk, populating the layer map
    ///
    fn load_layer_map(&self) -> anyhow::Result<()> {
        info!(
            "loading layer map for timeline {} into memory",
            self.timelineid
        );
        let mut layers = self.layers.lock().unwrap();
        let snapfilenames =
            filename::list_snapshot_files(self.conf, self.timelineid, self.tenantid)?;

        for filename in snapfilenames.iter() {
            let layer = SnapshotLayer::load_snapshot_layer(self.conf, self.timelineid, self.tenantid, filename)?;

            info!(
                "found layer {} {}-{} {} on timeline {}",
                layer.get_seg_tag(),
                layer.get_start_lsn(),
                layer.get_end_lsn(),
                layer.is_dropped(),
                self.timelineid
            );
            layers.insert_historic(Arc::new(layer));
        }

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
        trace!(
            "get_layer_for_read called for {} at {}/{}",
            seg,
            self.timelineid,
            lsn
        );

        // If you requested a page at an older LSN, before the branch point, dig into
        // the right ancestor timeline. This can only happen if you launch a read-only
        // node with an old LSN, a primary always uses a recent LSN in its requests.
        let mut timeline = self;
        let mut lsn = lsn;

        while lsn < timeline.ancestor_lsn {
            trace!("going into ancestor {} ", timeline.ancestor_lsn);
            timeline = &timeline.ancestor_timeline.as_ref().unwrap();
        }

        // Now we have the right starting timeline for our search.
        loop {
            let layers = timeline.layers.lock().unwrap();
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
                    // The segment was unlinked
                    return Ok(None);
                }

                return Ok(Some((layer.clone(), lsn)));
            }

            // If not, check if there's a layer on the ancestor timeline
            if let Some(ancestor) = &timeline.ancestor_timeline {
                lsn = timeline.ancestor_lsn;
                timeline = &ancestor.as_ref();
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
        let layers = self.layers.lock().unwrap();

        if lsn < self.last_valid_lsn.load() {
            bail!("cannot modify relation after advancing last_valid_lsn");
        }

        // Do we have a layer open for writing already?
        if let Some(layer) = layers.get_open(&seg) {
            if layer.get_start_lsn() > lsn {
                bail!("unexpected open layer in the future");
            }
            return Ok(layer);
        }

        // No (writeable) layer for this relation yet. Create one.
        //
        // Is this a completely new relation? Or the first modification after branching?
        //

        // FIXME: race condition, if another thread creates the layer while
        // we're busy looking up the previous one. We should hold the mutex throughout
        // this operation, but for that we'll need a version of get_layer_for_read()
        // that doesn't try to also grab the mutex.
        drop(layers);

        let layer;
        if let Some((prev_layer, _prev_lsn)) = self.get_layer_for_read(seg, lsn)? {
            // Create new entry after the previous one.
            let start_lsn;
            if prev_layer.get_timeline_id() != self.timelineid {
                // First modification on this timeline
                start_lsn = self.ancestor_lsn;
                trace!(
                    "creating file for write for {} at branch point {}/{}",
                    seg,
                    self.timelineid,
                    start_lsn
                );
            } else {
                start_lsn = prev_layer.get_end_lsn();
                trace!(
                    "creating file for write for {} after previous layer {}/{}",
                    seg,
                    self.timelineid,
                    start_lsn
                );
            }
            trace!(
                "prev layer is at {}/{} - {}",
                prev_layer.get_timeline_id(),
                prev_layer.get_start_lsn(),
                prev_layer.get_end_lsn()
            );
            layer = InMemoryLayer::copy_snapshot(
                self.conf,
                &self,
                &*prev_layer,
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

        let mut layers = self.layers.lock().unwrap();
        let layer_rc: Arc<InMemoryLayer> = Arc::new(layer);
        layers.insert_open(Arc::clone(&layer_rc));

        Ok(layer_rc)
    }

    ///
    /// Wait until WAL has been received up to the given LSN.
    ///
    fn wait_lsn(&self, mut lsn: Lsn) -> anyhow::Result<Lsn> {
        // When invalid LSN is requested, it means "don't wait, return latest version of the page"
        // This is necessary for bootstrap.
        if lsn == Lsn(0) {
            let last_valid_lsn = self.last_valid_lsn.load();
            trace!(
                "walreceiver doesn't work yet last_valid_lsn {}, requested {}",
                last_valid_lsn,
                lsn
            );
            lsn = last_valid_lsn;
        }

        self.last_valid_lsn
            .wait_for_timeout(lsn, TIMEOUT)
            .with_context(|| {
                format!(
                    "Timed out while waiting for WAL record at LSN {} to arrive",
                    lsn
                )
            })?;

        Ok(lsn)
    }

    ///
    /// Flush to disk all data that was written with the put_* functions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL. We don't
    /// know anything about them here in the repository.
    fn checkpoint_internal(&self, force: bool) -> Result<()> {
        // FIXME: these should be fetched atomically.
        let last_valid_lsn = self.last_valid_lsn.load();
        let last_record_lsn = self.last_record_lsn.load();
        let prev_record_lsn = self.prev_record_lsn.load();
        trace!(
            "checkpointing timeline {} at {}",
            self.timelineid,
            last_valid_lsn
        );

        // Grab lock on the layer map.
        //
        // TODO: We hold it locked throughout the checkpoint operation. That's bad,
        // the checkpointing could take many seconds, and any incoming get_page_at_lsn()
        // requests will block.
        let mut layers = self.layers.lock().unwrap();

        // Take the in-memory layer with the oldest WAL record. If it's older
        // than the threshold, write it out to disk as a new snapshot file.
        // Repeat until all remaining in-memory layers are within the threshold.
        //
        // That's necessary to limit the amount of WAL that needs to be kept
        // in the safekeepers, and that needs to be reprocessed on page server
        // crash. TODO: It's not a great policy for keeping memory usage in
        // check, though. We should also aim at flushing layers that consume
        // a lot of memory and/or aren't receiving much updates anymore.
        let mut disk_consistent_lsn = last_record_lsn;
        while let Some(oldest_layer) = layers.peek_oldest_open() {
            // Does this layer need freezing?
            let oldest_pending_lsn = oldest_layer.get_oldest_pending_lsn();
            let distance = last_valid_lsn.0 - oldest_pending_lsn.0;
            if !force && distance < OLDEST_INMEM_DISTANCE {
                info!(
                    "the oldest layer is now {} which is {} bytes behind last_valid_lsn",
                    oldest_layer.get_seg_tag(),
                    distance
                );
                disk_consistent_lsn = oldest_pending_lsn;
                break;
            }

            // freeze it
            let (new_historic, new_open) = oldest_layer.freeze(last_valid_lsn, &self)?;

            // replace this layer with the new layers that 'freeze' returned
            layers.pop_oldest_open();
            if let Some(n) = new_open {
                layers.insert_open(n);
            }
            layers.insert_historic(new_historic);
        }

        // Call unload() on all frozen layers, to release memory.
        // TODO: On-disk layers shouldn't consume much memory to begin with,
        // so this shouldn't be necessary. But currently the SnapshotLayer
        // code slurps the whole file into memory, so they do in fact consume
        // a lot of memory.
        for layer in layers.iter_historic_layers() {
            layer.unload()?;
        }

        // Save the metadata, with updated 'disk_consistent_lsn', to a
        // file in the timeline dir. After crash, we will restart WAL
        // streaming and processing from that point.

        // We can only save a valid 'prev_record_lsn' value on disk if we
        // flushed *all* in-memory changes to disk. We only track
        // 'prev_record_lsn' in memory for the latest processed record, so we
        // don't remember what the correct value that corresponds to some old
        // LSN is. But if we flush everything, then the value corresponding
        // current 'last_record_lsn' is correct and we can store it on disk.
        let ondisk_prev_record_lsn = {
            if disk_consistent_lsn == last_record_lsn {
                Some(prev_record_lsn)
            } else {
                None
            }
        };

        let ancestor_timelineid = self.ancestor_timeline.as_ref().map(|x| x.timelineid);

        let metadata = TimelineMetadata {
            disk_consistent_lsn: disk_consistent_lsn,
            prev_record_lsn: ondisk_prev_record_lsn,
            ancestor_timeline: ancestor_timelineid,
            ancestor_lsn: self.ancestor_lsn,
        };
        LayeredRepository::save_metadata(self.conf, self.timelineid, self.tenantid, &metadata)?;

        // Also update the in-memory copy
        self.disk_consistent_lsn.store(disk_consistent_lsn);

        Ok(())
    }

    ///
    /// Garbage collect snapshot files on a timeline that are no longer needed.
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
    /// within a snapshot file. We can only remove the whole file if it's fully
    /// obsolete.
    ///
    pub fn gc_timeline(&self, retain_lsns: Vec<Lsn>, cutoff: Lsn) -> Result<GcResult> {
        let now = Instant::now();
        let mut result: GcResult = Default::default();

        info!(
            "running GC on timeline {}, cutoff {}",
            self.timelineid, cutoff
        );

        let mut layers_to_remove: Vec<Arc<SnapshotLayer>> = Vec::new();

        // Scan all snapshot files in the directory. For each file, if a newer file
        // exists, we can remove the old one.
        //
        // Determine for each file if it needs to be retained
        // FIXME: also scan open in-memory layers. Normally we cannot remove the
        // latest layer of any seg, but if it was unlinked it's possible
        let mut layers = self.layers.lock().unwrap();
        'outer: for l in layers.iter_historic_layers() {
            let seg = l.get_seg_tag();

            if seg.rel.is_relation() {
                result.snapshot_relfiles_total += 1;
            } else {
                result.snapshot_nonrelfiles_total += 1;
            }

            // Is it newer than cutoff point?
            if l.get_end_lsn() > cutoff {
                info!(
                    "keeping {} {}-{} because it's newer than cutoff {}",
                    seg,
                    l.get_start_lsn(),
                    l.get_end_lsn(),
                    cutoff
                );
                if seg.rel.is_relation() {
                    result.snapshot_relfiles_needed_by_cutoff += 1;
                } else {
                    result.snapshot_nonrelfiles_needed_by_cutoff += 1;
                }
                continue 'outer;
            }

            // Is it needed by a child branch?
            for retain_lsn in &retain_lsns {
                // FIXME: are the bounds inclusive or exclusive?
                if l.get_start_lsn() <= *retain_lsn && *retain_lsn <= l.get_end_lsn() {
                    info!(
                        "keeping {} {}-{} because it's needed by branch point {}",
                        seg,
                        l.get_start_lsn(),
                        l.get_end_lsn(),
                        *retain_lsn
                    );
                    if seg.rel.is_relation() {
                        result.snapshot_relfiles_needed_by_branches += 1;
                    } else {
                        result.snapshot_nonrelfiles_needed_by_branches += 1;
                    }
                    continue 'outer;
                }
            }

            // Unless the relation was dropped, is there a later snapshot file for this relation?
            if !l.is_dropped() && !layers.newer_layer_exists(l.get_seg_tag(), l.get_end_lsn()) {
                if seg.rel.is_relation() {
                    result.snapshot_relfiles_not_updated += 1;
                } else {
                    result.snapshot_nonrelfiles_not_updated += 1;
                }
                continue 'outer;
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
            layers.remove_historic(&*doomed_layer);

            if doomed_layer.is_dropped() {
                if doomed_layer.get_seg_tag().rel.is_relation() {
                    result.snapshot_relfiles_dropped += 1;
                } else {
                    result.snapshot_nonrelfiles_dropped += 1;
                }
            } else {
                if doomed_layer.get_seg_tag().rel.is_relation() {
                    result.snapshot_relfiles_removed += 1;
                } else {
                    result.snapshot_nonrelfiles_removed += 1;
                }
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

        if let Some(_cont_lsn) = layer.get_page_reconstruct_data(blknum, lsn, &mut data)? {
            // The layers are currently fully self-contained, so we should have found all
            // the data we need to reconstruct the page in the layer.
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
                    "found page image for blk {} in {} at {}/{}, no WAL redo required",
                    blknum,
                    rel,
                    self.timelineid,
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
            if data.page_img.is_none() && !data.records.first().unwrap().will_init {
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
                    trace!("found {} WAL records and a base image for blk {} in {} at {}/{}, performing WAL redo", data.records.len(), blknum, rel, self.timelineid, request_lsn);
                } else {
                    trace!("found {} WAL records that will init the page for blk {} in {} at {}/{}, performing WAL redo", data.records.len(), blknum, rel, self.timelineid, request_lsn);
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
