//!
//! Zenith repository implementation based on "snapshot files"
//!
//! This is based on the design at https://github.com/zenithdb/rfcs/pull/8.
//! Some notable differences and details not covered by the RFC:
//!
//! - a snapshot file doesn't contain a snapshot at a specific LSN, but all page
//!   versions in a range of LSNs. So each snapshot file has a start and end LSN.
//!
//! - A snapshot file is actually a pair of files: one contains the page versions,
//!   and the other the relsizes.
//!
//!
//! The files are stored in .zenith/timelines/<timelineid>/inmemory-storage/ directory.
//! Currently, there are no subdirectories, and each snapshot file is named like this:
//!
//!    <spcnode>_<dbnode>_<relnode>_<forknum>_<start LSN>_<end LSN>
//!
//! And the corresponding file containing the relation size information has _relsizes
//! suffix. For example:
//!
//!    1663_13990_2609_0_000000000169C348_000000000169C349
//!    1663_13990_2609_0_000000000169C348_000000000169C349_relsizes
//!

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use log::*;
use serde::{Deserialize, Serialize};

use std::collections::{BTreeMap, HashMap};
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::ops::Bound::Included;

use crate::repository::{BufferTag, GcResult, History, RelTag, Repository, Timeline, WALRecord};
use crate::restore_local_repo::import_timeline_wal;
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::ZTimelineId;

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::{AtomicLsn, Lsn};
use zenith_utils::seqwait::SeqWait;

mod snapshotfile;

use snapshotfile::SnapshotFile;

// Timeout when waiting or WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(60);

///
/// Repository consists of multiple timelines. Keep them in a hash table.
///
pub struct InMemoryRepository {
    conf: &'static PageServerConf,
    timelines: Mutex<HashMap<ZTimelineId, Arc<InMemoryTimeline>>>,

    walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
}

/// Public interface
impl Repository for InMemoryRepository {

    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        Ok(self.get_timeline_locked(timelineid, &mut timelines)?)
    }

    fn create_empty_timeline(&self, timelineid: ZTimelineId, start_lsn: Lsn) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        std::fs::create_dir_all(self.conf.timeline_path(timelineid))?;
        //std::fs::create_dir(self.conf.snapshots_path(timelineid))?;
        //std::fs::create_dir(self.conf.timeline_path(timelineid).join("wal"))?;
        std::fs::create_dir_all(self.conf.timeline_path(timelineid).join("inmemory-storage"))?;

        // Write initial metadata.
        let metadata = TimelineMetadata {
            last_valid_lsn: start_lsn,
            last_record_lsn: start_lsn,
            ancestor_timeline: None,
            ancestor_lsn: start_lsn,
        };
        Self::save_metadata(self.conf, timelineid, &metadata)?;

        let timeline = InMemoryTimeline::new(self.conf, metadata, None, timelineid, self.walredo_mgr.clone())?;

        let timeline_rc = Arc::new(timeline);
        let r = timelines.insert(timelineid, timeline_rc.clone());
        assert!(r.is_none());
        Ok(timeline_rc)
    }

    /// Branch a timeline
    fn branch_timeline(&self, src: ZTimelineId, dst: ZTimelineId, start_lsn: Lsn) -> Result<()> {
        // just to check the source timeline exists
        let _src_timeline = self.get_timeline(src)?;

        // Create the metadata file, noting the ancestor of th new timeline. There is initially
        // no data in it, but all the read-calls know to look into the ancestor.
        let metadata = TimelineMetadata {
            last_valid_lsn: start_lsn,
            last_record_lsn: start_lsn,
            ancestor_timeline: Some(src),
            ancestor_lsn: start_lsn,
        };
        std::fs::create_dir_all(self.conf.timeline_path(dst).join("inmemory-storage"))?;
        Self::save_metadata(self.conf, dst, &metadata)?;

        info!("branched timeline {} from {} at {}", dst, src, start_lsn);

        Ok(())
    }
}

/// Private functions
impl InMemoryRepository {

    // Implementation of the public `get_timeline` function. This differs from the public
    // interface in that the caller must already hold the mutex on the 'timelines' hashmap.
    fn get_timeline_locked(&self, timelineid: ZTimelineId, timelines: &mut HashMap<ZTimelineId, Arc<InMemoryTimeline>>) -> Result<Arc<InMemoryTimeline>> {
        match timelines.get(&timelineid) {
            Some(timeline) => Ok(timeline.clone()),
            None => {
                let metadata = Self::load_metadata(self.conf, timelineid)?;

                let ancestor =
                    if let Some(ancestor_timelineid) = metadata.ancestor_timeline {
                        Some(self.get_timeline_locked(ancestor_timelineid, timelines)?)
                    } else {
                        None
                    };

                let timeline = InMemoryTimeline::new(self.conf, metadata, ancestor, timelineid, self.walredo_mgr.clone())?;

                // Load any new WAL after the last checkpoint into memory.
                info!(
                    "Loading WAL for timeline {} starting at {}",
                    timelineid,
                    timeline.get_last_record_lsn()
                );
                let wal_dir = self.conf.timeline_path(timelineid).join("wal");
                import_timeline_wal(&wal_dir, &timeline, timeline.get_last_record_lsn())?;

                let timeline_rc = Arc::new(timeline);
                timelines.insert(timelineid, timeline_rc.clone());
                Ok(timeline_rc)
            }
        }
    }

    pub fn new(conf: &'static PageServerConf, walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>) -> InMemoryRepository {
        InMemoryRepository {
            conf: conf,
            timelines: Mutex::new(HashMap::new()),
            walredo_mgr,
        }
    }

    /// Save metadata to file
    fn save_metadata(conf: &'static PageServerConf, timelineid: ZTimelineId, data: &TimelineMetadata) -> Result<()> {
        let path = conf.timeline_path(timelineid).join("metadata");
        let mut file = File::create(&path)?;

        file.write_all(&TimelineMetadata::ser(data)?)?;

        Ok(())
     }

    fn load_metadata(conf: &'static PageServerConf, timelineid: ZTimelineId) -> Result<TimelineMetadata> {
        let path = conf.timeline_path(timelineid).join("metadata");
        let data = std::fs::read(&path)?;

        Ok(TimelineMetadata::des(&data)?)
     }
}

/// SnapshotFileMap is a BTreeMap keyed by RelTag and the snapshot file's start LSN.
///
/// This provides a couple of convenience functions over a plain BTreeMap.
struct SnapshotFileMap (BTreeMap<(RelTag, Lsn), Arc<SnapshotFile>>);

impl SnapshotFileMap {
    ///
    /// Look up using the given rel tag and LSN. This differs from a plain
    /// key-value lookup in that if there is any snapshot file that covers the
    /// given LSN, it is returned.
    ///
    fn get(&self, tag: RelTag, lsn: Lsn) -> Option<Arc<SnapshotFile>> {
        let startkey = (tag, Lsn(0));
        let endkey = (tag, lsn);

        if let Some((_k, v)) = self.0.range((Included(startkey), Included(endkey))).next_back() {
            Some(Arc::clone(v))
        } else {
            None
        }
    }

    fn insert(&mut self, snapfile: SnapshotFile) -> Arc<SnapshotFile> {
        let tag = snapfile.tag;
        let start_lsn = snapfile.start_lsn;
        let snapfile_rc = Arc::new(snapfile);

        self.0.insert((tag, start_lsn), Arc::clone(&snapfile_rc));

        snapfile_rc
    }
}

impl Default for SnapshotFileMap {
    fn default() -> Self {
        SnapshotFileMap(BTreeMap::new())
    }
}


/// Metadata stored on disk for each timeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineMetadata {
    last_valid_lsn: Lsn,
    last_record_lsn: Lsn,
    ancestor_timeline: Option<ZTimelineId>,
    ancestor_lsn: Lsn,
}

pub struct InMemoryTimeline {
    conf: &'static PageServerConf,

    timelineid: ZTimelineId,

    snapshot_files: Mutex<SnapshotFileMap>,

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

    // Parent timeline that this timeline was branched from, and the LSN
    // of the branch point.
    ancestor_timeline: Option<Arc<InMemoryTimeline>>,
    ancestor_lsn: Lsn,
}

/// Public interface functions
impl Timeline for InMemoryTimeline {
    /// Look up given page in the cache.
    fn get_page_at_lsn(&self, tag: BufferTag, lsn: Lsn) -> Result<Bytes> {
        info!("get_page_at_lsn: {:?} at {}", tag, lsn);
        let lsn = self.wait_lsn(lsn)?;

        if let Some(snapfile) = self.get_snapshot_file_for_read(tag.rel, lsn)? {
            snapfile.get_page_at_lsn(&*self.walredo_mgr, tag.blknum, lsn)
        } else {
            bail!("relation {} not found at {}", tag.rel, lsn);
        }
    }

    fn get_rel_size(&self, rel: RelTag, lsn: Lsn) -> Result<u32> {
        let lsn = self.wait_lsn(lsn)?;

        if let Some(snapfile) = self.get_snapshot_file_for_read(rel, lsn)? {
            let result = snapfile.get_relsize(lsn);
            info!("get_relsize: {:?} at {} -> {:?}", rel, lsn, result);
            result
        } else {
            info!("get_relsize: {:?} at {} -> not found", rel, lsn);
            bail!("relation {} not found at {}", rel, lsn);
        }
    }

    fn get_rel_exists(&self, rel: RelTag, lsn: Lsn) -> Result<bool> {
        let lsn = self.wait_lsn(lsn)?;

        let result;
        if let Some(snapfile) = self.get_snapshot_file_for_read(rel, lsn)? {
            result = snapfile.exists(lsn)?;
        } else {
            result = false;
        }

        info!("get_relsize_exists: {:?} at {} -> {:?}", rel, lsn, result);
        Ok(result)
    }

    fn list_rels(&self, spcnode: u32, dbnode: u32, _lsn: Lsn) -> Result<HashSet<RelTag>> {
        // SnapshotFile::list_rels works by scanning the directory on disk. Make sure
        // we have a file on disk for each relation.
        self.checkpoint()?;

        // List all rels in this timeline, and all its ancestors.
        let mut all_rels = HashSet::new();
        let mut timeline = self;
        loop {
            let rels = SnapshotFile::list_rels(self.conf, timeline.timelineid, spcnode, dbnode)?;

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

    fn history<'a>(&'a self) -> Result<Box<dyn History + 'a>> {
        // TODO
        todo!();
    }

    fn gc_iteration(&self, _horizon: u64) -> Result<GcResult> {
        //TODO
        Ok(Default::default())
    }

    fn put_wal_record(&self, tag: BufferTag, rec: WALRecord) -> Result<()> {
        debug!("put_wal_record: {:?} at {}", tag, rec.lsn);

        let snapfile = self.get_snapshot_file_for_write(tag.rel, rec.lsn)?;
        snapfile.put_wal_record(tag.blknum, rec)
    }

    fn put_truncation(&self, rel: RelTag, lsn: Lsn, relsize: u32) -> anyhow::Result<()> {
        debug!("put_truncation: {:?} at {}", relsize, lsn);

        let snapfile = self.get_snapshot_file_for_write(rel, lsn)?;
        snapfile.put_truncation(lsn, relsize)
    }

    fn put_page_image(&self, tag: BufferTag, lsn: Lsn, img: Bytes) -> Result<()> {
        debug!("put_page_image: {:?} at {}", tag, lsn);

        let snapfile = self.get_snapshot_file_for_write(tag.rel, lsn)?;
        snapfile.put_page_image(tag.blknum, lsn, img)
    }

    fn put_unlink(&self, _tag: RelTag, _lsn: Lsn) -> Result<()> {
        // TODO
        Ok(())
    }

    ///
    /// Flush to disk all data that was written with the put_* functions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL. We don't
    /// know anything about them here in the repository.
    fn checkpoint(&self) -> Result<()> {

        let last_valid_lsn = self.last_valid_lsn.load();

        let mut snapfiles = self.snapshot_files.lock().unwrap();

        // Walk through each SnapshotFile in memory, and write any
        // dirty ones to disk.
        //
        // Note: We release all the in-memory SnapshotFile entries, and
        // start fresh with an empty map. This keeps memory usage in check,
        // but is perhaps too aggressive.
        //
        let snapfiles = std::mem::take(&mut *snapfiles);
        for snapfile in snapfiles.0.values() {
            if !snapfile.frozen {
                let frozen_file = snapfile.freeze(last_valid_lsn + 1);
                frozen_file.save()?;
            }
        }

        // Also save the metadata, with updated last_valid_lsn and last_record_lsn, to a
        // file in the timeline dir
        let ancestor_timelineid =
            if let Some(x) = &self.ancestor_timeline {
                Some(x.timelineid)
            } else {
                None
            };

        let metadata = TimelineMetadata {
            last_valid_lsn: self.last_valid_lsn.load(),
            last_record_lsn: self.last_record_lsn.load(),
            ancestor_timeline: ancestor_timelineid,
            ancestor_lsn: self.ancestor_lsn,
        };
        InMemoryRepository::save_metadata(self.conf, self.timelineid, &metadata)?;

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
}

impl InMemoryTimeline {
    /// Open a Timeline handle.
    ///
    /// Loads the metadata for the timeline into memory.
    fn new(conf: &'static PageServerConf, metadata: TimelineMetadata, ancestor: Option<Arc<InMemoryTimeline>>, timelineid: ZTimelineId, walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>) -> Result<InMemoryTimeline> {

        let timeline = InMemoryTimeline {
            conf,
            timelineid,
            snapshot_files: Mutex::new(SnapshotFileMap::default()),

            walredo_mgr,

            last_valid_lsn: SeqWait::new(metadata.last_valid_lsn),
            last_record_lsn: AtomicLsn::new(metadata.last_record_lsn.0),

            ancestor_timeline: ancestor,
            ancestor_lsn: metadata.ancestor_lsn,
        };
        Ok(timeline)
    }

    ///
    /// Get a handle to a SnapshotFile for reading.
    ///
    /// The returned SnapshotFile might be from an ancestor timeline, if the
    /// relation hasn't been updated on this timeline yet.
    ///
    fn get_snapshot_file_for_read(&self, tag: RelTag, lsn: Lsn) -> Result<Option<Arc<SnapshotFile>>> {
        // First dig the right ancestor timeline
        let mut timeline = self;
        let mut lsn = lsn;
        while lsn < timeline.ancestor_lsn {
            timeline = &timeline.ancestor_timeline.as_ref().unwrap();
        }
        loop {
            // Then look up the snapshot file
            let mut snapfiles = timeline.snapshot_files.lock().unwrap();

            // FIXME: If there is an entry in memory for an older snapshot file,
            // but there is a newere snapshot file on disk, this will incorrectly
            // return the older entry from memory.
            if let Some(snapfile) = snapfiles.get(tag, lsn) {
                return Ok(Some(snapfile.clone()))
            } else {
                // No SnaphotFile in memory for this relation yet. Read it from disk.
                if let Some(snapfile) = SnapshotFile::load(timeline.conf, timeline.timelineid, tag, lsn)? {
                    let snapfile_rc = snapfiles.insert(snapfile);

                    return Ok(Some(snapfile_rc))
                } else {
                    // No snapshot files for this relation on this timeline. But there might still
                    // be one on the ancestor timeline
                    if let Some(ancestor) = &timeline.ancestor_timeline {
                        lsn = timeline.ancestor_lsn;
                        timeline = &ancestor.as_ref();
                        continue;
                    }
                    return Ok(None);
                }
            }
        }
    }

    ///
    /// Get a handle to the latest SnapshotFile for appending.
    ///
    fn get_snapshot_file_for_write(&self, tag: RelTag, lsn: Lsn) -> Result<Arc<SnapshotFile>> {

        if lsn < self.last_valid_lsn.load() {
            bail!("cannot modify relation after advancing last_valid_lsn");
        }

        // Look up the snapshot file
        let snapfiles = self.snapshot_files.lock().unwrap();
        if let Some(snapfile) = snapfiles.get(tag, lsn) {
            assert!(!snapfile.frozen);
            Ok(Arc::clone(&snapfile))
        } else {
            // No SnapshotFile for this relation yet. Create one.
            //
            // Is this a completely new relation? Or the first modification after branching?
            //
            let snapfile;

            // FIXME: race condition, if another thread creates the SnapshotFile while
            // we're busy looking up the previous one. We should hold the mutex throughout
            // this operation, but for that we'll need a versio of get_snapshot_file_for_read()
            // that doesn't try to also grab the mutex.
            drop(snapfiles);

            if let Some(prev_snapfile) = self.get_snapshot_file_for_read(tag, lsn)? {
                // Create new entry after the previous one.
                let lsn;
                if prev_snapfile.timelineid != self.timelineid {
                    // First modification on this timeline
                    lsn = self.ancestor_lsn;
                } else {
                    lsn = prev_snapfile.end_lsn;
                }
                snapfile = SnapshotFile::copy_snapshot(self.conf, &*self.walredo_mgr, &prev_snapfile, self.timelineid, lsn)?;
            } else {
                // New relation.
                snapfile = SnapshotFile::create(self.conf, self.timelineid, tag, lsn)?;
            }

            let mut snapfiles = self.snapshot_files.lock().unwrap();
            let snapfile_rc = snapfiles.insert(snapfile);

            Ok(snapfile_rc)
        }
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
