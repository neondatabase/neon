//!
//! Zenith repository implementation that stores all the page versions in memory.
//!

use anyhow::{Context, Result};
use bytes::Bytes;
use log::*;
use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::repository::{BufferTag, GcResult, History, RelTag, Repository, Timeline, WALRecord};
use crate::restore_local_repo::import_timeline_wal;
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::ZTimelineId;

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::{AtomicLsn, Lsn};
use zenith_utils::seqwait::SeqWait;

mod relfile;

use relfile::RelFileEntry;

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
        // just to check the source timeline exists XXX
        let src_timeline = self.get_timeline(src)?;
        src_timeline.checkpoint()?;

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

    relfiles: Mutex<HashMap<RelTag, Arc<RelFileEntry>>>,

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

    ancestor_timeline: Option<Arc<InMemoryTimeline>>,
    ancestor_lsn: Lsn,

    // Counters, for metrics collection.
    pub num_entries: AtomicU64,
    pub num_page_images: AtomicU64,
    pub num_wal_records: AtomicU64,
    pub num_getpage_requests: AtomicU64,
}

impl Timeline for InMemoryTimeline {
    /// Look up given page in the cache.
    fn get_page_at_lsn(&self, tag: BufferTag, lsn: Lsn) -> Result<Bytes> {
        debug!("get_page_at_lsn: {:?} at {}", tag, lsn);
        let lsn = self.wait_lsn(lsn)?;

        self.get_relfile(tag.rel)?
            .get_page_at_lsn(&*self.walredo_mgr, tag.blknum, lsn)
    }

    fn get_rel_size(&self, rel: RelTag, lsn: Lsn) -> Result<u32> {
        let lsn = self.wait_lsn(lsn)?;
        let result = self.get_relfile(rel)?.get_relsize(lsn);
        debug!("get_relsize: {:?} at {} -> {:?}", rel, lsn, result);
        result
    }
    fn get_rel_exists(&self, rel: RelTag, lsn: Lsn) -> Result<bool> {
        let lsn = self.wait_lsn(lsn)?;
        let result = self.get_relfile(rel)?.exists(lsn);

        debug!("get_relsize_exists: {:?} at {} -> {:?}", rel, lsn, result);
        result
    }

    fn list_rels(&self, spcnode: u32, dbnode: u32, _lsn: Lsn) -> Result<HashSet<RelTag>> {
        // RelFileEntry::list_rels works by scanning the directory on disk. Make sure
        // we have a file on disk for each relation.
        self.checkpoint()?;

        // List all rels in this timeline, and all its ancestors.
        let mut all_rels = HashSet::new();
        let mut timeline = self;
        loop {
            let rels = RelFileEntry::list_rels(self.conf, timeline.timelineid, spcnode, dbnode)?;

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
        self.get_relfile(tag.rel)?.put_wal_record(tag.blknum, rec)
    }

    fn put_truncation(&self, rel: RelTag, lsn: Lsn, relsize: u32) -> anyhow::Result<()> {
        debug!("put_truncation: {:?} at {}", relsize, lsn);
        self.get_relfile(rel)?.put_truncation(lsn, relsize)
    }

    fn put_page_image(&self, tag: BufferTag, lsn: Lsn, img: Bytes) -> Result<()> {
        debug!("put_page_image: {:?} at {}", tag, lsn);
        self.get_relfile(tag.rel)?
            .put_page_image(tag.blknum, lsn, img)
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

        let relfiles = self.relfiles.lock().unwrap();

        for relentry in relfiles.values() {
            relentry.save()?;
        }

        // Also save last_valid_lsn and last_record_lsn to file in the timeline dir
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
            relfiles: Mutex::new(HashMap::new()),

            walredo_mgr,

            last_valid_lsn: SeqWait::new(metadata.last_valid_lsn),
            last_record_lsn: AtomicLsn::new(metadata.last_record_lsn.0),

            ancestor_timeline: ancestor,
            ancestor_lsn: metadata.ancestor_lsn,

            num_entries: AtomicU64::new(0),
            num_page_images: AtomicU64::new(0),
            num_wal_records: AtomicU64::new(0),
            num_getpage_requests: AtomicU64::new(0),
        };
        Ok(timeline)
    }

    ///
    /// Get a handle to a RelFileEntry
    ///
    fn get_relfile(&self, tag: RelTag) -> Result<Arc<RelFileEntry>> {
        // First, look up the relfile
        let mut relfiles = self.relfiles.lock().unwrap();
        if let Some(relentry) = relfiles.get(&tag) {
            Ok(relentry.clone())
        } else {
            // No RelFileEntry for this relation yet. Create one.
            let relentry = RelFileEntry::load_or_create(self.conf, self.timelineid, tag, self.ancestor_timeline.clone(), self.ancestor_lsn)?;

            let relentry = Arc::new(relentry);

            relfiles.insert(tag, relentry.clone());

            Ok(relentry)
        }
    }

    fn get_relfile_at(&self, tag: RelTag, lsn: Lsn) -> Result<Arc<RelFileEntry>> {
        // Dig the right ancestor timeline
        let mut timeline = self;
        while lsn <= timeline.ancestor_lsn {
            timeline = &self.ancestor_timeline.as_ref().unwrap();
        }
        return timeline.get_relfile(tag);
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
