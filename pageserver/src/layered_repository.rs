//!
//! Zenith repository implementation that keeps old data in "snapshot files", and
//! the recent changes in memory. See layered_repository/snapshot_layer.rs and
//! layered_repository/inmemory_layer.rs, respectively. The functions here are
//! responsible for locating the correct layer for the get/put call, tracing
//! timeline branching history as needed.
//!
//! The snapshot files are stored in the .zenith/timelines/<timelineid> directory.
//! In addition to the snapshot files, there is a metadata file in the
//! same directory that contains information about the timline, in particular its
//! parent timeline, and the last LSN that has been written to disk.
//!
//! This is based on the design at https://github.com/zenithdb/rfcs/pull/8.
//! Some notable differences and details not covered by the RFC:
//!
//! - A snapshot layer doesn't contain a snapshot at a specific LSN, but all page
//!   versions in a range of LSNs. So each snapshot file has a start and end LSN.
//!
//!
//! Each layer contains a full snapshot of the relish at the start LSN. In addition
//! to that, it contains WAL (or more page images) needed to recontruct any page
//! version up to the end LSN.
//!

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use log::*;
use serde::{Deserialize, Serialize};

use std::collections::{BTreeMap, HashMap};
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
use crate::{ZTimelineId, ZTenantId};

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::{AtomicLsn, Lsn};
use zenith_utils::seqwait::SeqWait;

mod inmemory_layer;
mod snapshot_layer;
mod storage_layer;

use inmemory_layer::InMemoryLayer;
use snapshot_layer::SnapshotLayer;
use storage_layer::Layer;

// Timeout when waiting or WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(60);

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

        std::fs::create_dir_all(self.conf.timeline_path(&timelineid, &self.tenantid))?;

        // Write initial metadata.
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

        // Create the metadata file, noting the ancestor of th new timeline. There is initially
        // no data in it, but all the read-calls know to look into the ancestor.
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

                // Load any new WAL after the last checkpoint into memory.
                info!(
                    "Loading WAL for timeline {} starting at {}",
                    timelineid,
                    timeline.get_last_record_lsn()
                );
                let wal_dir = self.conf.timeline_path(&timelineid, &self.tenantid).join("wal");
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

    /// Save metadata to file
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
    // How garbage collection works
    // --------
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
    //   we don't need to keep that in the parent anymore.
    //
    // - Currently, this is only triggered manually by the 'do_gc' command.
    //   There is no background thread to do it automatically.
    fn gc_iteration(conf: &'static PageServerConf, tenantid: ZTenantId, horizon: u64) -> Result<GcResult> {
        let mut totals: GcResult = Default::default();
        let now = Instant::now();

        // TODO: grab mutex to prevent new timelines from being created here.

        // Scan all timelines for the branch points.
        let mut all_branchpoints: BTreeSet<(ZTimelineId, Lsn)> = BTreeSet::new();

        // Remember timelineid and its last_record_lsn for each timeline
        let mut timelines: Vec<(ZTimelineId, Lsn)> = Vec::new();

        let timelines_path = conf.timelines_path(&tenantid);
        for direntry in fs::read_dir(timelines_path)? {
            let direntry = direntry?;
            if let Some(fname) = direntry.file_name().to_str() {
                if let Ok(timelineid) = fname.parse::<ZTimelineId>() {
                    // Read the metadata of this timeline to get its parent timeline.
                    let metadata = Self::load_metadata(conf, timelineid, tenantid)?;

                    timelines.push((timelineid, metadata.last_record_lsn));

                    if let Some(ancestor_timeline) = metadata.ancestor_timeline {
                        all_branchpoints.insert((ancestor_timeline, metadata.ancestor_lsn));
                    }
                }
            }
        }

        // Ok, we now know all the branch points. Iterate through them.
        for (timelineid, last_lsn) in timelines {
            let branchpoints: Vec<Lsn> = all_branchpoints
                .range((
                    Included((timelineid, Lsn(0))),
                    Included((timelineid, Lsn(u64::MAX))),
                ))
                .map(|&x| x.1)
                .collect();

            if let Some(cutoff) = last_lsn.checked_sub(horizon) {
                let result = SnapshotLayer::gc_timeline(conf, timelineid, tenantid, branchpoints, cutoff)?;

                totals += result;
            }
        }

        totals.elapsed = now.elapsed();
        Ok(totals)
    }
}

/// LayerMap is a BTreeMap keyed by RelishTag and the layer's start LSN.
/// It provides a couple of convenience functions over a plain BTreeMap
struct LayerMap(BTreeMap<(RelishTag, Lsn), Arc<dyn Layer>>);

impl LayerMap {
    ///
    /// Look up using the given rel tag and LSN. This differs from a plain
    /// key-value lookup in that if there is any layer that covers the
    /// given LSN, or precedes the given LSN, it is returned. In other words,
    /// you don't need to know the exact start LSN of the layer.
    ///
    fn get(&self, tag: RelishTag, lsn: Lsn) -> Option<Arc<dyn Layer>> {
        let startkey = (tag, Lsn(0));
        let endkey = (tag, lsn);

        if let Some((_k, v)) = self
            .0
            .range((Included(startkey), Included(endkey)))
            .next_back()
        {
            Some(Arc::clone(v))
        } else {
            None
        }
    }

    fn insert(&mut self, layer: Arc<dyn Layer>) {
        let rel = layer.get_relish_tag();
        let start_lsn = layer.get_start_lsn();

        self.0.insert((rel, start_lsn), Arc::clone(&layer));
    }
}

impl Default for LayerMap {
    fn default() -> Self {
        LayerMap(BTreeMap::new())
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
    last_valid_lsn: SeqWait<Lsn>,
    last_record_lsn: AtomicLsn,
    prev_record_lsn: AtomicLsn,

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

        if let Some((layer, lsn)) = self.get_layer_for_read(rel, lsn)? {
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

        if let Some((layer, lsn)) = self.get_layer_for_read(rel, lsn)? {
            layer.get_page_at_lsn(&*self.walredo_mgr, blknum, lsn)
        } else {
            bail!("relish {} not found at {}", rel, lsn);
        }
    }

    fn get_rel_size(&self, rel: RelishTag, lsn: Lsn) -> Result<u32> {
        if !rel.is_blocky() {
            bail!("invalid get_rel_size request for non-blocky relish {}", rel);
        }

        let lsn = self.wait_lsn(lsn)?;

        if let Some((layer, lsn)) = self.get_layer_for_read(rel, lsn)? {
            let result = layer.get_rel_size(lsn);
            trace!(
                "get_relsize: rel {} at {}/{} -> {:?}",
                rel,
                self.timelineid,
                lsn,
                result
            );
            result
        } else {
            warn!(
                "get_relsize: rel {} at {}/{} -> not found",
                rel, self.timelineid, lsn
            );
            bail!("relation {} not found at {}", rel, lsn);
        }
    }

    fn get_rel_exists(&self, rel: RelishTag, lsn: Lsn) -> Result<bool> {
        let lsn = self.wait_lsn(lsn)?;

        let result;
        if let Some((layer, lsn)) = self.get_layer_for_read(rel, lsn)? {
            result = layer.get_rel_exists(lsn)?;
        } else {
            result = false;
        }

        trace!("get_relsize_exists: {} at {} -> {}", rel, lsn, result);
        Ok(result)
    }

    fn list_rels(&self, spcnode: u32, dbnode: u32, _lsn: Lsn) -> Result<HashSet<RelTag>> {
        // SnapshotLayer::list_rels works by scanning the directory on disk. Make sure
        // we have a file on disk for each relation.
        self.checkpoint()?;

        // List all rels in this timeline, and all its ancestors.
        let mut all_rels = HashSet::new();
        let mut timeline = self;
        loop {
            let rels = SnapshotLayer::list_rels(self.conf, timeline.timelineid, timeline.tenantid, spcnode, dbnode)?;

            // FIXME: We should filter out relations that don't exist at the given LSN.
            all_rels.extend(rels.iter());

            if let Some(ancestor) = timeline.ancestor_timeline.as_ref() {
                timeline = ancestor;
                continue;
            } else {
                break;
            }
        }

        Ok(all_rels)
    }

    fn list_nonrels(&self, lsn: Lsn) -> Result<HashSet<RelishTag>> {
        info!("list_nonrels called at {}", lsn);

        // List all rels in this timeline, and all its ancestors.
        let mut all_rels = HashSet::new();
        let mut timeline = self;
        loop {
            // SnapshotFile::list_rels works by scanning the directory on disk. Make sure
            // we have a file on disk for each relation.
            timeline.checkpoint()?;

            let rels = SnapshotLayer::list_nonrels(self.conf, timeline.timelineid, timeline.tenantid, lsn)?;

            // FIXME: We should filter out relishes that don't exist at the given LSN.
            all_rels.extend(rels.iter());

            if let Some(ancestor) = timeline.ancestor_timeline.as_ref() {
                timeline = ancestor;
                continue;
            } else {
                break;
            }
        }

        Ok(all_rels)
    }

    fn history<'a>(&'a self) -> Result<Box<dyn History + 'a>> {
        // TODO
        todo!();
    }

    fn gc_iteration(&self, horizon: u64, _compact: bool) -> Result<GcResult> {
        // In the layered repository, event to GC a single timeline,
        // we have to scan all the timelines to determine what child
        // timelines there are, so that we know to retain snapshot
        // files that are still needed by the children. So we just do
        // GC on the whole repository.
        //
        // FIXME: This makes writing repeatable tests harder, if
        // activity on other timelines can affect the counters that
        // we return

        // But do flush the in-memory layers to disk first.
        self.checkpoint()?;

        LayeredRepository::gc_iteration(self.conf, self.tenantid, horizon)
    }

    fn put_wal_record(&self, rel: RelishTag, blknum: u32, rec: WALRecord) -> Result<()> {
        if !rel.is_blocky() && blknum != 0 {
            bail!(
                "invalid request for block {} for non-blocky relish {}",
                blknum,
                rel
            );
        }
        let layer = self.get_layer_for_write(rel, rec.lsn)?;
        layer.put_wal_record(blknum, rec)
    }

    fn put_truncation(&self, rel: RelishTag, lsn: Lsn, relsize: u32) -> anyhow::Result<()> {
        if !rel.is_blocky() {
            bail!("invalid truncation for non-blocky relish {}", rel);
        }

        debug!("put_truncation: {} to {} blocks at {}", rel, relsize, lsn);

        let layer = self.get_layer_for_write(rel, lsn)?;
        layer.put_truncation(lsn, relsize)
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

        let layer = self.get_layer_for_write(rel, lsn)?;
        layer.put_page_image(blknum, lsn, img)
    }

    fn put_unlink(&self, rel: RelishTag, lsn: Lsn) -> Result<()> {
        debug!("put_unlink: {} at {}", rel, lsn);

        let layer = self.get_layer_for_write(rel, lsn)?;
        layer.put_unlink(lsn)
    }

    fn put_raw_data(&self, _tag: crate::object_key::ObjectTag, _lsn: Lsn, _data: &[u8]) -> Result<()> {

        // FIXME: This doesn't make much sense for the layered storage format,
        // it's pretty tightly coupled with the way the object store stores
        // things.
        bail!("put_raw_data not implemented");
    }

    fn get_next_tag(
        &self,
        _tag: crate::object_key::ObjectTag,
    ) -> Result<Option<crate::object_key::ObjectTag>> {
        todo!();
    }

    ///
    /// Flush to disk all data that was written with the put_* functions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL. We don't
    /// know anything about them here in the repository.
    fn checkpoint(&self) -> Result<()> {

        let last_valid_lsn = self.last_valid_lsn.load();
        trace!("checkpointing timeline {} at {}", self.timelineid, last_valid_lsn);

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
        // Note: We release all the layer structs, and start fresh
        // with an empty map. This keeps memory usage in check, but is
        // probably too aggressive. Some kind of LRU policy would be
        // appropriate.
        //
        let old_layers = std::mem::take(&mut *layers);
        for old_layer in old_layers.0.values() {
            if !old_layer.is_frozen() {
                if let Some(new_layer) = old_layer.freeze(last_valid_lsn)? {
                    layers.insert(Arc::clone(&new_layer));
                }
            }
        }

        // Also save the metadata, with updated last_valid_lsn and last_record_lsn, to a
        // file in the timeline dir
        let ancestor_timelineid = if let Some(x) = &self.ancestor_timeline {
            Some(x.timelineid)
        } else {
            None
        };

        let metadata = TimelineMetadata {
            last_valid_lsn: self.last_valid_lsn.load(),
            last_record_lsn: self.last_record_lsn.load(),
            prev_record_lsn: self.prev_record_lsn.load(),
            ancestor_timeline: ancestor_timelineid,
            ancestor_lsn: self.ancestor_lsn,
        };
        LayeredRepository::save_metadata(self.conf, self.timelineid, self.tenantid, &metadata)?;

        // If there were any concurrent updates on the timeline, we would have to work
        // harder to make sure we don't lose the new updates. Currently, that shouldn't
        // happen, because the WAL receiver process is responsible for both updating
        // the timeline and calling checkpoint()
        assert!(self.last_valid_lsn.load() == last_valid_lsn);

        Ok(())
    }

    /// Remember that WAL has been received and added to the page cache up to the given LSN
    fn advance_last_valid_lsn(&self, lsn: Lsn) {
        let old = self.last_valid_lsn.advance(lsn);

        // Can't move backwards.
        if lsn < old {
            warn!(
                "attempted to move last valid LSN backwards (was {}, new {})",
                old, lsn
            );
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
    /// Loads the metadata for the timeline into memory.
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

            ancestor_timeline: ancestor,
            ancestor_lsn: metadata.ancestor_lsn,
        };
        Ok(timeline)
    }

    ///
    /// Get a handle to a Layer for reading.
    ///
    /// The returned SnapshotFile might be from an ancestor timeline, if the
    /// relation hasn't been updated on this timeline yet.
    ///
    fn get_layer_for_read(
        &self,
        rel: RelishTag,
        lsn: Lsn,
    ) -> Result<Option<(Arc<dyn Layer>, Lsn)>> {
        // First dig the right ancestor timeline
        let mut timeline = self;
        let mut lsn = lsn;
        trace!(
            "get_layer_for_read called for {} at {}/{}",
            rel,
            self.timelineid,
            lsn
        );

        // If you requested a page at an older LSN, before the branch point, dig into
        // the right ancestor timeline. This can only happen if you launch a read-only
        // node with an old LSN. A primary always uses a recent LSN in its requests.
        while lsn < timeline.ancestor_lsn {
            trace!("going into ancestor {} ", timeline.ancestor_lsn);
            timeline = &timeline.ancestor_timeline.as_ref().unwrap();
        }

        loop {
            let mut layers = timeline.layers.lock().unwrap();
            //
            // FIXME: If the relation has been dropped, does this return the right
            // thing? The compute node should not normally request dropped relations,
            // but if OID wraparound happens the same relfilenode might get reused
            // for an unrelated relation.
            //
            let mut best_candidate = None;
            let mut best_end_lsn = Lsn(0);

            // First, see if we have loaded a layer in the cache ready.
            if let Some(layer) = layers.get(rel, lsn) {
                trace!(
                    "found layer in cache: {} {}-{}",
                    timeline.timelineid,
                    layer.get_start_lsn(),
                    layer.get_end_lsn()
                );

                assert!(layer.get_start_lsn() <= lsn);

                // If this layer's LSN range contains the request LSN, it is an "exact" match,
                // and we can return it directly. If it's not an exact match there might be
                // a more recent layer on disk than what we have in cache.
                //
                // For example, imagine that the following snapshot files exist:
                //
                // 100-200 [cached]
                // 200-300
                // 300-400
                //
                // A request comes in for LSN 250. We already have the layer 100-200 in cache,
                // so we find it here. But there's a newer layer on disk for 200-300, that's the
                // correct one we need to return from this function. If the 200-300 snapshot file
                // didn't exist (because there were no modifications to the relation after LSN
                // 200), then the 100-200 layer was the correct one
                //
                // So if we find a layer in cache with end-LSN before the request LSN, remember
                // that, but fall through to check if there is a newer snapshot file on disk before
                // returning it.
                if layer.get_end_lsn() >= lsn {
                    return Ok(Some((layer.clone(), lsn)));
                }
                best_candidate = Some(layer.clone());
                best_end_lsn = layer.get_end_lsn();
            }

            // Proceed to check if there is a (better) snapshot file on disk.
            if let Some(layer) =
                SnapshotLayer::load(timeline.conf, timeline.timelineid, timeline.tenantid, rel, best_end_lsn, lsn)?
            {
                trace!(
                    "found snapshot file on disk: {}-{}",
                    layer.get_start_lsn(),
                    layer.get_end_lsn()
                );
                let layer_rc: Arc<dyn Layer> = Arc::new(layer);
                layers.insert(Arc::clone(&layer_rc));

                return Ok(Some((layer_rc, lsn)));
            } else {
                // No (better) snapshot files for this relation on this timeline. If we found
                // something in cache, return that.
                if let Some(result) = best_candidate {
                    return Ok(Some((result, lsn)));
                }

                // If we got nothing on this timeline, check if there's a layer on the ancestor
                // timeline
                if let Some(ancestor) = &timeline.ancestor_timeline {
                    lsn = timeline.ancestor_lsn;
                    timeline = &ancestor.as_ref();
                    trace!("recursing into ancestor at {}/{}", timeline.timelineid, lsn);
                    continue;
                }
                return Ok(None);
            }
        }
    }

    ///
    /// Get a handle to the latest layer for appending.
    ///
    fn get_layer_for_write(&self, rel: RelishTag, lsn: Lsn) -> Result<Arc<dyn Layer>> {
        if lsn < self.last_valid_lsn.load() {
            bail!("cannot modify relation after advancing last_valid_lsn");
        }

        // Look up the snapshot file
        let layers = self.layers.lock().unwrap();
        if let Some(layer) = layers.get(rel, lsn) {
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
        if let Some((prev_layer, _prev_lsn)) = self.get_layer_for_read(rel, lsn)? {
            // Create new entry after the previous one.
            let lsn;
            if prev_layer.get_timeline_id() != self.timelineid {
                // First modification on this timeline
                lsn = self.ancestor_lsn;
                trace!(
                    "creating file for write for {} at branch point {}/{}",
                    rel,
                    self.timelineid,
                    lsn
                );
            } else {
                lsn = prev_layer.get_end_lsn();
                trace!(
                    "creating file for write for {} after previous layer {}/{}",
                    rel,
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
                rel,
                self.timelineid,
                lsn
            );

            // Scan the directory for latest existing file.
            // FIXME: if this is truly a new rel, none should exist right?
            let start_lsn;
            if let Some((_start, end, dropped)) = SnapshotLayer::find_latest_snapshot_file(
                self.conf,
                self.timelineid,
                self.tenantid,
                rel,
                Lsn(0),
                Lsn(u64::MAX),
            )? {
                if dropped {
                    start_lsn = lsn;
                } else {
                    start_lsn = end;
                }
            } else {
                start_lsn = lsn;
            }
            layer = InMemoryLayer::create(self.conf, self.timelineid, self.tenantid, rel, start_lsn)?;
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
}
