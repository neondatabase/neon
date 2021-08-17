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
use crate::repository::{GcResult, History, Repository, Timeline, WALRecord};
use crate::restore_local_repo::import_timeline_wal;
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};

use zenith_metrics::{register_histogram_vec, HistogramVec};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::{AtomicLsn, Lsn};
use zenith_utils::seqwait::SeqWait;

mod inmemory_layer;
mod layer_map;
mod snapshot_layer;
mod storage_layer;

use inmemory_layer::InMemoryLayer;
use layer_map::LayerMap;
use snapshot_layer::SnapshotLayer;
use storage_layer::{Layer, SegmentTag, RELISH_SEG_SIZE};

// Timeout when waiting for WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(60);

// Perform a checkpoint in the GC thread, when the LSN has advanced this much since
// last checkpoint. This puts a backstop on how much WAL needs to be re-digested if
// the page server is restarted.
//
// FIXME: This current value is very low. I would imagine something like 1 GB or 10 GB
// would be more appropriate. But a low value forces the code to be exercised more,
// which is good for now to trigger bugs.
static CHECKPOINT_INTERVAL: u64 = 16 * 1024 * 1024;

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
            last_valid_lsn: start_lsn,
            last_record_lsn: start_lsn,
            prev_record_lsn: Lsn(0),
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
            last_valid_lsn: start_lsn,
            last_record_lsn: start_lsn,
            prev_record_lsn: src_timeline.get_prev_record_lsn(),
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
                import_timeline_wal(&wal_dir, &timeline, timeline.get_last_record_lsn())?;

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
                    let distance = u64::from(timeline.last_valid_lsn.load())
                        - u64::from(timeline.last_checkpoint_lsn.load());
                    if distance > CHECKPOINT_INTERVAL {
                        timeline.checkpoint()?;
                    }
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
        _compact: bool,
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
                let result = timeline.gc_timeline(branchpoints, cutoff)?;

                totals += result;
            }
        }

        totals.elapsed = now.elapsed();
        Ok(totals)
    }
}

/// Metadata stored on disk for each timeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineMetadata {
    last_valid_lsn: Lsn,
    last_record_lsn: Lsn,
    prev_record_lsn: Lsn,
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

    last_checkpoint_lsn: AtomicLsn,

    // Parent timeline that this timeline was branched from, and the LSN
    // of the branch point.
    ancestor_timeline: Option<Arc<LayeredTimeline>>,
    ancestor_lsn: Lsn,
}

/// Public interface functions
impl Timeline for LayeredTimeline {
    /// Look up given page in the cache.
    fn get_page_at_lsn(&self, rel: RelishTag, blknum: u32, lsn: Lsn) -> Result<Bytes> {
        if !rel.is_blocky() && blknum != 0 {
            bail!(
                "invalid request for block {} for non-blocky relish {}",
                blknum,
                rel
            );
        }
        let lsn = self.wait_lsn(lsn)?;

        let seg = SegmentTag::from_blknum(rel, blknum);

        if let Some((layer, lsn)) = self.get_layer_for_read(seg, lsn)? {
            layer.get_page_at_lsn(&*self.walredo_mgr, blknum, lsn)
        } else {
            bail!("relish {} not found at {}", rel, lsn);
        }
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
            layer.get_page_at_lsn(&*self.walredo_mgr, blknum, lsn)
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

    fn history<'a>(&'a self) -> Result<Box<dyn History + 'a>> {
        // This is needed by the push/pull functionality. Not implemented yet.
        todo!();
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

    fn put_raw_data(
        &self,
        _tag: crate::object_key::ObjectTag,
        _lsn: Lsn,
        _data: &[u8],
    ) -> Result<()> {
        // FIXME: This doesn't make much sense for the layered storage format,
        // it's pretty tightly coupled with the way the object store stores
        // things.
        bail!("put_raw_data not implemented");
    }

    /// Public entry point for checkpoint(). All the logic is in the private
    /// checkpoint_internal function, this public facade just wraps it for
    /// metrics collection.
    fn checkpoint(&self) -> Result<()> {
        STORAGE_TIME
            .with_label_values(&["checkpoint"])
            .observe_closure_duration(|| self.checkpoint_internal())
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

            last_valid_lsn: SeqWait::new(metadata.last_valid_lsn),
            last_record_lsn: AtomicLsn::new(metadata.last_record_lsn.0),
            prev_record_lsn: AtomicLsn::new(metadata.prev_record_lsn.0),
            last_checkpoint_lsn: AtomicLsn::new(metadata.last_valid_lsn.0),

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
        let snapfiles =
            SnapshotLayer::list_snapshot_files(self.conf, self.timelineid, self.tenantid)?;

        for layer_rc in snapfiles.iter() {
            info!(
                "found layer {} {}-{} {} on timeline {}",
                layer_rc.get_seg_tag(),
                layer_rc.get_start_lsn(),
                layer_rc.get_end_lsn(),
                layer_rc.is_dropped(),
                self.timelineid
            );
            layers.insert(Arc::clone(layer_rc));
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
            if let Some(layer) = layers.get(seg, lsn) {
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
    fn get_layer_for_write(&self, seg: SegmentTag, lsn: Lsn) -> Result<Arc<dyn Layer>> {
        if lsn < self.last_valid_lsn.load() {
            bail!("cannot modify relation after advancing last_valid_lsn");
        }

        // Look up the correct layer.
        let layers = self.layers.lock().unwrap();
        if let Some(layer) = layers.get(seg, lsn) {
            // If it's writeable, good, return it.
            if !layer.is_frozen() {
                return Ok(Arc::clone(&layer));
            }
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
            let lsn;
            if prev_layer.get_timeline_id() != self.timelineid {
                // First modification on this timeline
                lsn = self.ancestor_lsn;
                trace!(
                    "creating file for write for {} at branch point {}/{}",
                    seg,
                    self.timelineid,
                    lsn
                );
            } else {
                lsn = prev_layer.get_end_lsn();
                trace!(
                    "creating file for write for {} after previous layer {}/{}",
                    seg,
                    self.timelineid,
                    lsn
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
                &*self.walredo_mgr,
                &*prev_layer,
                self.timelineid,
                self.tenantid,
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

            layer = InMemoryLayer::create(self.conf, self.timelineid, self.tenantid, seg, lsn)?;
        }

        let mut layers = self.layers.lock().unwrap();
        let layer_rc: Arc<dyn Layer> = Arc::new(layer);
        layers.insert(Arc::clone(&layer_rc));

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
    fn checkpoint_internal(&self) -> Result<()> {
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

        // Walk through each in-memory, and write any dirty data to disk,
        // as snapshot files.
        //
        // We currently write a new snapshot file for every relation
        // that was modified, if there has been any changes at all.
        // It would be smarter to only flush out in-memory layers that
        // have accumulated a fair amount of changes. Note that the
        // start and end LSNs of snapshot files belonging to different
        // relations don't have to line up, although currently they do
        // because of the way this works. So you could have a snapshot
        // file covering LSN range 100-200 for one relation, and a
        // snapshot file covering 150-250 for another relation. The
        // read functions should even cope with snapshot files
        // covering overlapping ranges for the same relation, although
        // that situation never arises currently.
        //
        // Note: We aggressively freeze and unload all the layer
        // structs. Even if a layer is actively being used. This
        // keeps memory usage in check, but is probably too
        // aggressive. Some kind of LRU policy would be appropriate.
        //

        // It is not possible to modify a BTreeMap while you're iterating
        // it. So we have to make a temporary copy, and iterate through that,
        // while we modify the original.
        let old_layers = layers.inner.clone();

        // Call freeze() on any unfrozen layers (that is, layers that haven't
        // been written to disk yet).
        // Call unload() on all frozen layers, to release memory.
        for layer in old_layers.values() {
            if !layer.is_frozen() {
                let new_layers = layer.freeze(last_valid_lsn, &*self.walredo_mgr)?;

                // replace this layer with the new layers that 'freeze' returned
                layers.remove(&**layer);
                for new_layer in new_layers {
                    trace!(
                        "freeze returned layer {} {}-{}",
                        new_layer.get_seg_tag(),
                        new_layer.get_start_lsn(),
                        new_layer.get_end_lsn()
                    );
                    layers.insert(Arc::clone(&new_layer));
                }
            } else {
                layer.unload()?;
            }
        }

        // Also save the metadata, with updated last_valid_lsn and last_record_lsn, to a
        // file in the timeline dir. The metadata reflects the last_valid_lsn as it was
        // when we *started* the checkpoint, so that after crash, the WAL receiver knows
        // to restart the streaming from that WAL position.
        let ancestor_timelineid = if let Some(x) = &self.ancestor_timeline {
            Some(x.timelineid)
        } else {
            None
        };
        let metadata = TimelineMetadata {
            last_valid_lsn: last_valid_lsn,
            last_record_lsn: last_record_lsn,
            prev_record_lsn: prev_record_lsn,
            ancestor_timeline: ancestor_timelineid,
            ancestor_lsn: self.ancestor_lsn,
        };
        LayeredRepository::save_metadata(self.conf, self.timelineid, self.tenantid, &metadata)?;

        self.last_checkpoint_lsn.store(last_valid_lsn);

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

        // Scan all snapshot files in the directory. For each file, if a newer file
        // exists, we can remove the old one.
        self.checkpoint()?;

        let mut layers = self.layers.lock().unwrap();

        info!(
            "running GC on timeline {}, cutoff {}",
            self.timelineid, cutoff
        );

        let mut layers_to_remove: Vec<Arc<dyn Layer>> = Vec::new();

        // Determine for each file if it needs to be retained
        'outer: for ((seg, _lsn), l) in layers.inner.iter() {
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
            layers_to_remove.push(Arc::clone(l));
        }

        // Actually delete the layers from disk and remove them from the map.
        // (couldn't do this in the loop above, because you cannot modify a collection
        // while iterating it. BTreeMap::retain() would be another option)
        for doomed_layer in layers_to_remove {
            doomed_layer.delete()?;
            layers.remove(&*doomed_layer);

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
}
