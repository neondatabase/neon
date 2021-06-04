//!
//! Implementation of the Repository/Timeline traits, using a key-value store
//! (ObjectStore) to for the actual storage.
//!
//! This maps the relation-oriented operations in the Timeline interface into
//! objects stored in an ObjectStore. Relation size is stored as a separate object
//! in the key-value store. If a page is written beyond the current end-of-file,
//! we also insert the new size as a new "page version" in the key-value store.
//!
//! Also, this implements Copy-on-Write forking of timelines. For each timeline,
//! we store the parent timeline in the object store, in a little metadata blob.
//! When we need to find a version of a page, we walk the timeline history backwards
//! until we find the page we're looking for, making a separate lookup into the
//! key-value store for each timeline.

use crate::object_store::{ObjectKey, ObjectStore};
use crate::repository::*;
use crate::restore_local_repo::import_timeline_wal;
use crate::walredo::WalRedoManager;
use crate::{PageServerConf, ZTimelineId};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use log::*;
use postgres_ffi::pg_constants;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryInto;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::{AtomicLsn, Lsn};
use zenith_utils::seqwait::SeqWait;

///
/// A repository corresponds to one .zenith directory. One repository holds multiple
/// timelines, forked off from the same initial call to 'initdb'.
///
pub struct ObjectRepository {
    obj_store: Arc<dyn ObjectStore>,
    conf: &'static PageServerConf,
    timelines: Mutex<HashMap<ZTimelineId, Arc<ObjectTimeline>>>,
    walredo_mgr: Arc<dyn WalRedoManager>,
}

// Timeout when waiting or WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(600);

impl ObjectRepository {
    pub fn new(
        conf: &'static PageServerConf,
        obj_store: Arc<dyn ObjectStore>,
        walredo_mgr: Arc<dyn WalRedoManager>,
    ) -> ObjectRepository {
        ObjectRepository {
            conf,
            obj_store,
            timelines: Mutex::new(HashMap::new()),
            walredo_mgr,
        }
    }
}

impl Repository for ObjectRepository {
    /// Get Timeline handle for given zenith timeline ID.
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        match timelines.get(&timelineid) {
            Some(timeline) => Ok(timeline.clone()),
            None => {
                let timeline = ObjectTimeline::open(
                    Arc::clone(&self.obj_store),
                    timelineid,
                    self.walredo_mgr.clone(),
                )?;

                // Load any new WAL after the last checkpoint into the repository.
                info!(
                    "Loading WAL for timeline {} starting at {}",
                    timelineid,
                    timeline.get_last_record_lsn()
                );
                let wal_dir = self.conf.timeline_path(timelineid).join("wal");
                import_timeline_wal(&wal_dir, &timeline, timeline.get_last_record_lsn())?;

                let timeline_rc = Arc::new(timeline);

                if self.conf.gc_horizon != 0 {
                    ObjectTimeline::launch_gc_thread(self.conf, timeline_rc.clone());
                }

                timelines.insert(timelineid, timeline_rc.clone());

                Ok(timeline_rc)
            }
        }
    }

    /// Create a new, empty timeline. The caller is responsible for loading data into it
    fn create_empty_timeline(
        &self,
        timelineid: ZTimelineId,
        start_lsn: Lsn,
    ) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        // Write metadata key
        let metadata = MetadataEntry {
            last_valid_lsn: start_lsn,
            last_record_lsn: start_lsn,
            ancestor_timeline: None,
            ancestor_lsn: start_lsn,
        };
        self.obj_store.put(
            &timeline_metadata_key(timelineid),
            Lsn(0),
            &MetadataEntry::ser(&metadata)?,
        )?;

        info!("Created empty timeline {}", timelineid);

        let timeline = ObjectTimeline::open(
            Arc::clone(&self.obj_store),
            timelineid,
            self.walredo_mgr.clone(),
        )?;

        let timeline_rc = Arc::new(timeline);
        let r = timelines.insert(timelineid, timeline_rc.clone());
        assert!(r.is_none());

        // don't start the garbage collector for unit tests, either.

        Ok(timeline_rc)
    }

    /// Branch a timeline
    fn branch_timeline(&self, src: ZTimelineId, dst: ZTimelineId, at_lsn: Lsn) -> Result<()> {
        // just to check the source timeline exists
        let _ = self.get_timeline(src)?;

        // Write a metadata key, noting the ancestor of th new timeline. There is initially
        // no data in it, but all the read-calls know to look into the ancestor.
        let metadata = MetadataEntry {
            last_valid_lsn: at_lsn,
            last_record_lsn: at_lsn,
            ancestor_timeline: Some(src),
            ancestor_lsn: at_lsn,
        };
        self.obj_store.put(
            &timeline_metadata_key(dst),
            Lsn(0),
            &MetadataEntry::ser(&metadata)?,
        )?;

        Ok(())
    }
}

///
/// A handle to a specific timeline in the repository. This is the API
/// that's exposed to the rest of the system.
///
pub struct ObjectTimeline {
    timelineid: ZTimelineId,

    // Backing key-value store
    obj_store: Arc<dyn ObjectStore>,

    // WAL redo manager, for reconstructing page versions from WAL records.
    walredo_mgr: Arc<dyn WalRedoManager>,

    // What page versions do we hold in the cache? If we get a request > last_valid_lsn,
    // we need to wait until we receive all the WAL up to the request. The SeqWait
    // provides functions for that. TODO: If we get a request for an old LSN, such that
    // the versions have already been garbage collected away, we should throw an error,
    // but we don't track that currently.
    //
    // last_record_lsn points to the end of last processed WAL record.
    // It can lag behind last_valid_lsn, if the WAL receiver has received some WAL
    // after the end of last record, but not the whole next record yet. In the
    // page cache, we care about last_valid_lsn, but if the WAL receiver needs to
    // restart the streaming, it needs to restart at the end of last record, so
    // we track them separately. last_record_lsn should perhaps be in
    // walreceiver.rs instead of here, but it seems convenient to keep all three
    // values together.
    //
    last_valid_lsn: SeqWait<Lsn>,
    last_record_lsn: AtomicLsn,

    ancestor_timeline: Option<ZTimelineId>,
    ancestor_lsn: Lsn,
}

impl ObjectTimeline {
    /// Open a Timeline handle.
    ///
    /// Loads the metadata for the timeline into memory.
    fn open(
        obj_store: Arc<dyn ObjectStore>,
        timelineid: ZTimelineId,
        walredo_mgr: Arc<dyn WalRedoManager>,
    ) -> Result<ObjectTimeline> {
        // Load metadata into memory
        let v = obj_store
            .get(&timeline_metadata_key(timelineid), Lsn(0))
            .with_context(|| "timeline not found in repository")?;
        let metadata = MetadataEntry::des(&v)?;

        let timeline = ObjectTimeline {
            timelineid,
            obj_store,
            walredo_mgr,
            last_valid_lsn: SeqWait::new(metadata.last_valid_lsn),
            last_record_lsn: AtomicLsn::new(metadata.last_record_lsn.0),
            ancestor_timeline: metadata.ancestor_timeline,
            ancestor_lsn: metadata.ancestor_lsn,
        };
        Ok(timeline)
    }
}

impl Timeline for ObjectTimeline {
    //------------------------------------------------------------------------------
    // Public GET functions
    //------------------------------------------------------------------------------

    /// Look up given page in the cache.
    fn get_page_at_lsn(&self, tag: BufferTag, req_lsn: Lsn) -> Result<Bytes> {
        let lsn = self.wait_lsn(req_lsn)?;

        self.get_page_at_lsn_nowait(tag, lsn)
    }

    /// Get size of relation
    fn get_rel_size(&self, rel: RelTag, lsn: Lsn) -> Result<u32> {
        let lsn = self.wait_lsn(lsn)?;

        match self.relsize_get_nowait(rel, lsn)? {
            Some(nblocks) => Ok(nblocks),
            None => bail!("relation {} not found at {}", rel, lsn),
        }
    }

    /// Does relation exist at given LSN?
    fn get_rel_exists(&self, rel: RelTag, req_lsn: Lsn) -> Result<bool> {
        let lsn = self.wait_lsn(req_lsn)?;
        let key = relation_size_key(self.timelineid, rel);
        let mut iter = self.object_versions(&*self.obj_store, &key, lsn)?;
        if let Some((_key, _val)) = iter.next().transpose()? {
            debug!("Relation {} exists at {}", rel, lsn);
            return Ok(true);
        }
        debug!("Relation {} doesn't exist at {}", rel, lsn);
        Ok(false)
    }

    /// Get a list of all distinct relations in given tablespace and database.
    fn list_rels(&self, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelTag>> {
        // List all relations in this timeline.
        let mut all_rels = self
            .obj_store
            .list_rels(self.timelineid, spcnode, dbnode, lsn)?;

        // Also list all relations in ancestor timelines. If a relation hasn't been modified
        // after the fork, there will be no trace of it in the object store with the current
        // timeline id.
        let mut prev_timeline: Option<ZTimelineId> = self.ancestor_timeline;
        let mut lsn = self.ancestor_lsn;
        while let Some(timeline) = prev_timeline {
            let this_rels = self.obj_store.list_rels(timeline, spcnode, dbnode, lsn)?;

            for rel in this_rels {
                all_rels.insert(rel);
            }

            // Load ancestor metadata.
            let v = self
                .obj_store
                .get(&timeline_metadata_key(timeline), Lsn(0))
                .with_context(|| "timeline not found in repository")?;
            let metadata = MetadataEntry::des(&v)?;

            prev_timeline = metadata.ancestor_timeline;
            lsn = metadata.ancestor_lsn;
        }

        Ok(all_rels)
    }

    //------------------------------------------------------------------------------
    // Public PUT functions, to update the repository with new page versions.
    //
    // These are called by the WAL receiver to digest WAL records.
    //------------------------------------------------------------------------------

    /// Put a new page version that can be constructed from a WAL record
    ///
    /// This will implicitly extend the relation, if the page is beyond the
    /// current end-of-file.
    fn put_wal_record(&self, tag: BufferTag, rec: WALRecord) -> Result<()> {
        let lsn = rec.lsn;
        let key = ObjectKey {
            timeline: self.timelineid,
            buf_tag: tag,
        };
        let val = PageEntry::WALRecord(rec);

        self.obj_store.put(&key, lsn, &PageEntry::ser(&val)?)?;
        debug!(
            "put_wal_record rel {} blk {} at {}",
            tag.rel, tag.blknum, lsn
        );

        // Also check if this created or extended the file
        let old_nblocks = self.relsize_get_nowait(tag.rel, lsn)?.unwrap_or(0);

        if tag.blknum >= old_nblocks {
            let new_nblocks = tag.blknum + 1;
            let key = relation_size_key(self.timelineid, tag.rel);
            let val = RelationSizeEntry::Size(new_nblocks);

            trace!(
                "Extended relation {} from {} to {} blocks at {}",
                tag.rel,
                old_nblocks,
                new_nblocks,
                lsn
            );

            self.obj_store
                .put(&key, lsn, &RelationSizeEntry::ser(&val)?)?;
        }

        Ok(())
    }

    ///
    /// Memorize a full image of a page version
    ///
    fn put_page_image(&self, tag: BufferTag, lsn: Lsn, img: Bytes) -> Result<()> {
        let key = ObjectKey {
            timeline: self.timelineid,
            buf_tag: tag,
        };
        let val = PageEntry::Page(img);

        self.obj_store.put(&key, lsn, &PageEntry::ser(&val)?)?;

        debug!(
            "put_page_image rel {} blk {} at {}",
            tag.rel, tag.blknum, lsn
        );

        // Also check if this created or extended the file
        let old_nblocks = self.relsize_get_nowait(tag.rel, lsn)?.unwrap_or(0);

        if tag.blknum >= old_nblocks {
            let new_nblocks = tag.blknum + 1;
            let key = relation_size_key(self.timelineid, tag.rel);
            let val = RelationSizeEntry::Size(new_nblocks);

            trace!(
                "Extended relation {} from {} to {} blocks at {}",
                tag.rel,
                old_nblocks,
                new_nblocks,
                lsn
            );

            self.obj_store
                .put(&key, lsn, &RelationSizeEntry::ser(&val)?)?;
        }

        Ok(())
    }

    ///
    /// Adds a relation-wide WAL record (like truncate) to the repository,
    /// associating it with all pages started with specified block number
    ///
    fn put_truncation(&self, rel: RelTag, lsn: Lsn, nblocks: u32) -> Result<()> {
        let key = relation_size_key(self.timelineid, rel);
        let val = RelationSizeEntry::Size(nblocks);

        info!("Truncate relation {} to {} blocks at {}", rel, nblocks, lsn);

        self.obj_store
            .put(&key, lsn, &RelationSizeEntry::ser(&val)?)?;

        Ok(())
    }

    /// Remember the all WAL before the given LSN has been processed.
    ///
    /// The WAL receiver calls this after the put_* functions, to indicate that
    /// all WAL before this point has been digested. Before that, if you call
    /// GET on an earlier LSN, it will block.
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

    fn get_last_valid_lsn(&self) -> Lsn {
        self.last_valid_lsn.load()
    }

    fn init_valid_lsn(&self, lsn: Lsn) {
        let old = self.last_valid_lsn.advance(lsn);
        assert!(old == Lsn(0));
        let old = self.last_record_lsn.fetch_max(lsn);
        assert!(old == Lsn(0));
    }

    /// Like `advance_last_valid_lsn`, but this always points to the end of
    /// a WAL record, not in the middle of one.
    ///
    /// This must be <= last valid LSN. This is tracked separately from last
    /// valid LSN, so that the WAL receiver knows where to restart streaming.
    ///
    /// NOTE: this updates last_valid_lsn as well.
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

    ///
    /// Flush to disk all data that was written with the put_* functions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL. We don't
    /// know anything about them here in the repository.

    // Flush all the changes written so far with PUT functions to disk.
    // RocksDB writes out things as we go (?), so we don't need to do much here. We just
    // write out the last valid and record LSNs.
    fn checkpoint(&self) -> Result<()> {
        let metadata = MetadataEntry {
            last_valid_lsn: self.last_valid_lsn.load(),
            last_record_lsn: self.last_record_lsn.load(),
            ancestor_timeline: self.ancestor_timeline,
            ancestor_lsn: self.ancestor_lsn,
        };
        self.obj_store.put(
            &timeline_metadata_key(self.timelineid),
            Lsn(0),
            &MetadataEntry::ser(&metadata)?,
        )?;

        trace!("checkpoint at {}", metadata.last_valid_lsn);

        Ok(())
    }

    fn history<'a>(&'a self) -> Result<Box<dyn History + 'a>> {
        let lsn = self.last_valid_lsn.load();
        let iter = self.obj_store.objects(self.timelineid, lsn)?;
        Ok(Box::new(ObjectHistory {
            lsn,
            iter,
            last_relation_size: None,
        }))
    }
}

impl ObjectTimeline {
    fn get_page_at_lsn_nowait(&self, tag: BufferTag, lsn: Lsn) -> Result<Bytes> {
        // Look up the page entry. If it's a page image, return that. If it's a WAL record,
        // ask the WAL redo service to reconstruct the page image from the WAL records.
        let searchkey = ObjectKey {
            timeline: self.timelineid,
            buf_tag: tag,
        };
        let mut iter = self.object_versions(&*self.obj_store, &searchkey, lsn)?;

        if let Some((version_lsn, value)) = iter.next().transpose()? {
            let page_img: Bytes;

            match PageEntry::des(&value)? {
                PageEntry::Page(img) => {
                    page_img = img;
                }
                PageEntry::WALRecord(_rec) => {
                    // Request the WAL redo manager to apply the WAL records for us.
                    let (base_img, records) = self.collect_records_for_apply(tag, lsn)?;
                    page_img = self.walredo_mgr.request_redo(tag, lsn, base_img, records)?;

                    self.put_page_image(tag, lsn, page_img.clone())?;
                }
            }
            // FIXME: assumes little-endian. Only used for the debugging log though
            let page_lsn_hi = u32::from_le_bytes(page_img.get(0..4).unwrap().try_into().unwrap());
            let page_lsn_lo = u32::from_le_bytes(page_img.get(4..8).unwrap().try_into().unwrap());
            trace!(
                "Returning page with LSN {:X}/{:X} for {} blk {} from {} (request {})",
                page_lsn_hi,
                page_lsn_lo,
                tag.rel,
                tag.blknum,
                version_lsn,
                lsn
            );
            return Ok(page_img);
        }
        static ZERO_PAGE: [u8; 8192] = [0u8; 8192];
        trace!("page {} blk {} at {} not found", tag.rel, tag.blknum, lsn);
        Ok(Bytes::from_static(&ZERO_PAGE))
        /* return Err("could not find page image")?; */
    }

    ///
    /// Internal function to get relation size at given LSN.
    ///
    /// The caller must ensure that WAL has been received up to 'lsn'.
    ///
    fn relsize_get_nowait(&self, rel: RelTag, lsn: Lsn) -> Result<Option<u32>> {
        let key = relation_size_key(self.timelineid, rel);
        let mut iter = self.object_versions(&*self.obj_store, &key, lsn)?;

        if let Some((version_lsn, value)) = iter.next().transpose()? {
            match RelationSizeEntry::des(&value)? {
                RelationSizeEntry::Size(nblocks) => {
                    trace!(
                        "relation {} has size {} at {} (request {})",
                        rel,
                        nblocks,
                        version_lsn,
                        lsn
                    );
                    Ok(Some(nblocks))
                }
                RelationSizeEntry::Unlink => {
                    trace!(
                        "relation {} not found; it was dropped at lsn {}",
                        rel,
                        version_lsn
                    );
                    Ok(None)
                }
            }
        } else {
            info!("relation {} not found at {}", rel, lsn);
            Ok(None)
        }
    }

    ///
    /// Collect all the WAL records that are needed to reconstruct a page
    /// image for the given cache entry.
    ///
    /// Returns an old page image (if any), and a vector of WAL records to apply
    /// over it.
    ///
    fn collect_records_for_apply(
        &self,
        tag: BufferTag,
        lsn: Lsn,
    ) -> Result<(Option<Bytes>, Vec<WALRecord>)> {
        let mut base_img: Option<Bytes> = None;
        let mut records: Vec<WALRecord> = Vec::new();

        // Scan backwards, collecting the WAL records, until we hit an
        // old page image.
        let searchkey = ObjectKey {
            timeline: self.timelineid,
            buf_tag: tag,
        };
        let mut iter = self.object_versions(&*self.obj_store, &searchkey, lsn)?;
        while let Some((_key, value)) = iter.next().transpose()? {
            match PageEntry::des(&value)? {
                PageEntry::Page(img) => {
                    // We have a base image. No need to dig deeper into the list of
                    // records
                    base_img = Some(img);
                    break;
                }
                PageEntry::WALRecord(rec) => {
                    records.push(rec.clone());
                    // If this WAL record initializes the page, no need to dig deeper.
                    if rec.will_init {
                        break;
                    }
                }
            }
        }
        records.reverse();
        Ok((base_img, records))
    }

    fn launch_gc_thread(conf: &'static PageServerConf, timeline_rc: Arc<ObjectTimeline>) {
        let _gc_thread = thread::Builder::new()
            .name("Garbage collection thread".into())
            .spawn(move || {
                // FIXME
                timeline_rc.do_gc(conf).expect("GC thread died");
            })
            .unwrap();
    }

    fn do_gc(&self, conf: &'static PageServerConf) -> Result<()> {
        loop {
            thread::sleep(conf.gc_period);
            let last_lsn = self.get_last_valid_lsn();

            // checked_sub() returns None on overflow.
            if let Some(horizon) = last_lsn.checked_sub(conf.gc_horizon) {
                // WAL is large enough to perform GC
                let now = Instant::now();
                let mut truncated = 0u64;
                let mut deleted = 0u64;

                // Iterate through all relations
                for rels in &self.obj_store.list_rels(self.timelineid, 0, 0, last_lsn)? {
                    let mut last_version = true;
                    let mut key = relation_size_key(self.timelineid, *rels);
                    let mut max_size = 0u32;
                    let mut relation_dropped = false;

                    // Process relation metadata versions
                    for vers in self.obj_store.object_versions(&key, horizon)? {
                        let lsn = vers.0;
                        let rel_meta = RelationSizeEntry::des(&vers.1)?;
                        // If relation is dropped at the horizon,
                        // we can remove all its versions including last (Unlink)
                        match rel_meta {
                            RelationSizeEntry::Size(size) => max_size = max(max_size, size),
                            RelationSizeEntry::Unlink => {
                                if last_version {
                                    relation_dropped = true;
                                    info!("Relation {:?} dropped", rels);
                                }
                            }
                        }
                        if last_version {
                            last_version = false;
                            if !relation_dropped {
                                // preserve last version
                                continue;
                            }
                        }
                        self.obj_store.unlink(&key, lsn)?;
                        deleted += 1;
                    }
                    // Now process all relation blocks
                    for blknum in 0..max_size {
                        key.buf_tag.blknum = blknum;
                        last_version = true;
                        for vers in self.obj_store.object_versions(&key, horizon)? {
                            let lsn = vers.0;
                            if last_version {
                                last_version = false;
                                truncated += 1;
                                if !relation_dropped {
                                    // preserve and materialize last version before deleting all preceeding
                                    self.get_page_at_lsn_nowait(key.buf_tag, lsn)?;
                                    continue;
                                }
                            }
                            self.obj_store.unlink(&key, lsn)?;
                            deleted += 1;
                        }
                    }
                }
                info!("Garbage collection completed in {:?}: {} version histories truncated, {} versions deleted",
					  now.elapsed(), truncated, deleted);
            }
        }
    }

    //
    // Wait until WAL has been received up to the given LSN.
    //
    fn wait_lsn(&self, mut lsn: Lsn) -> Result<Lsn> {
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
        trace!(
            "Start waiting for LSN {}, valid LSN is {}",
            lsn,
            self.last_valid_lsn.load()
        );
        self.last_valid_lsn
            .wait_for_timeout(lsn, TIMEOUT)
            .with_context(|| {
                format!(
                    "Timed out while waiting for WAL record at LSN {} to arrive. valid LSN in {}",
                    lsn,
                    self.last_valid_lsn.load(),
                )
            })?;
        //trace!("Stop waiting for LSN {}, valid LSN is {}", lsn,  self.last_valid_lsn.load());

        Ok(lsn)
    }

    ///
    /// Iterate through object versions with given key, in reverse LSN order.
    ///
    /// This implements following the timeline history over the plain
    /// ObjectStore::object_versions function, which doesn't know
    /// about the relationships between timeline.
    ///
    fn object_versions<'a>(
        &self,
        obj_store: &'a dyn ObjectStore,
        key: &ObjectKey,
        lsn: Lsn,
    ) -> Result<ObjectVersionIter<'a>> {
        let current_iter = obj_store.object_versions(key, lsn)?;

        Ok(ObjectVersionIter {
            obj_store,
            buf_tag: key.buf_tag,
            current_iter,
            ancestor_timeline: self.ancestor_timeline,
            ancestor_lsn: self.ancestor_lsn,
        })
    }
}

struct ObjectHistory<'a> {
    iter: Box<dyn Iterator<Item = Result<(BufferTag, Lsn, Vec<u8>)>> + 'a>,
    lsn: Lsn,
    last_relation_size: Option<(BufferTag, u32)>,
}

impl<'a> Iterator for ObjectHistory<'a> {
    type Item = Result<RelationUpdate>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_result().transpose()
    }
}

impl<'a> History for ObjectHistory<'a> {
    fn lsn(&self) -> Lsn {
        self.lsn
    }
}

impl<'a> ObjectHistory<'a> {
    fn handle_relation_size(
        &mut self,
        buf_tag: BufferTag,
        entry: RelationSizeEntry,
    ) -> Option<Update> {
        match entry {
            RelationSizeEntry::Size(size) => {
                // we only want to output truncations, expansions are filtered out
                let last_relation_size = self.last_relation_size.replace((buf_tag, size));

                match last_relation_size {
                    Some((last_buf, last_size)) if last_buf != buf_tag || size < last_size => {
                        Some(Update::Truncate { n_blocks: size })
                    }
                    _ => None,
                }
            }
            RelationSizeEntry::Unlink => Some(Update::Unlink),
        }
    }

    fn handle_page(&mut self, buf_tag: BufferTag, entry: PageEntry) -> Update {
        match entry {
            PageEntry::Page(img) => Update::Page {
                blknum: buf_tag.blknum,
                img,
            },
            PageEntry::WALRecord(rec) => Update::WALRecord {
                blknum: buf_tag.blknum,
                rec,
            },
        }
    }

    fn next_result(&mut self) -> Result<Option<RelationUpdate>> {
        while let Some((buf_tag, lsn, value)) = self.iter.next().transpose()? {
            if buf_tag.rel.forknum == pg_constants::ROCKSDB_SPECIAL_FORKNUM {
                continue;
            }

            let update = if buf_tag.blknum == RELATION_SIZE_BLKNUM {
                let entry = RelationSizeEntry::des(&value)?;
                match self.handle_relation_size(buf_tag, entry) {
                    Some(relation_update) => relation_update,
                    None => continue,
                }
            } else {
                let entry = PageEntry::des(&value)?;
                self.handle_page(buf_tag, entry)
            };

            return Ok(Some(RelationUpdate {
                rel: buf_tag.rel,
                lsn,
                update,
            }));
        }

        Ok(None)
    }
}

///
/// We store two kinds of page versions in the repository:
///
/// 1. Ready-made images of the block
/// 2. WAL records, to be applied on top of the "previous" entry
///
/// Some WAL records will initialize the page from scratch. For such records,
/// the 'will_init' flag is set. They don't need the previous page image before
/// applying. The 'will_init' flag is set for records containing a full-page image,
/// and for records with the BKPBLOCK_WILL_INIT flag. These differ from PageImages
/// stored directly in the cache entry in that you still need to run the WAL redo
/// routine to generate the page image.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
enum PageEntry {
    Page(Bytes),
    WALRecord(WALRecord),
}

///
/// In addition to page versions, we store relation size as a separate, versioned,
/// object.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RelationSizeEntry {
    Size(u32),

    /// Tombstone for a dropped relation.
    //
    // TODO: Not used. Currently, we never drop relations. The parsing
    // of relation drops in COMMIT/ABORT records has not been
    // implemented. We should also have a mechanism to remove
    // "orphaned" relfiles, if the compute node crashes before writing
    // the COMMIT/ABORT record.
    Unlink,
}

// No real block in PostgreSQL will have block number u32::MAX
// See vendor/postgres/src/include/storage/block.h
const RELATION_SIZE_BLKNUM: u32 = u32::MAX;

const fn relation_size_key(timelineid: ZTimelineId, rel: RelTag) -> ObjectKey {
    ObjectKey {
        timeline: timelineid,
        buf_tag: BufferTag {
            rel,
            blknum: RELATION_SIZE_BLKNUM,
        },
    }
}

///
/// In addition to those per-page and per-relation entries, we also
/// store a little metadata blob for each timeline. It is stored using
/// STORAGE_SPECIAL_FORKNUM.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataEntry {
    last_valid_lsn: Lsn,
    last_record_lsn: Lsn,
    ancestor_timeline: Option<ZTimelineId>,
    ancestor_lsn: Lsn,
}

const fn timeline_metadata_key(timelineid: ZTimelineId) -> ObjectKey {
    ObjectKey {
        timeline: timelineid,
        buf_tag: BufferTag {
            rel: RelTag {
                forknum: pg_constants::ROCKSDB_SPECIAL_FORKNUM,
                spcnode: 0,
                dbnode: 0,
                relnode: 0,
            },
            blknum: 0,
        },
    }
}

///
/// Iterator for `object_versions`. Returns all page versions of a given block, in
/// reverse LSN order. This implements the traversal of ancestor timelines. If
/// a page isn't found in the most recent timeline, this iterates to the parent,
/// until a page version is found.
///
struct ObjectVersionIter<'a> {
    obj_store: &'a dyn ObjectStore,

    buf_tag: BufferTag,

    /// Iterator on the current timeline.
    current_iter: Box<dyn Iterator<Item = (Lsn, Vec<u8>)> + 'a>,

    /// Ancestor of the current timeline being iterated.
    ancestor_timeline: Option<ZTimelineId>,
    ancestor_lsn: Lsn,
}

impl<'a> Iterator for ObjectVersionIter<'a> {
    type Item = Result<(Lsn, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_result().transpose()
    }
}

impl<'a> ObjectVersionIter<'a> {
    ///
    /// "transposed" version of the standard Iterator::next function.
    ///
    /// The rust standard Iterator::next function returns an
    /// Option of a Result, but it's more convenient to work with
    /// Result of a Option so that you can use ? to check for errors.
    ///
    fn next_result(&mut self) -> Result<Option<(Lsn, Vec<u8>)>> {
        loop {
            // If there is another entry on the current timeline, return it.
            if let Some(result) = self.current_iter.next() {
                return Ok(Some(result));
            }

            // Out of entries on this timeline. Move to the ancestor, if any.
            if let Some(ancestor_timeline) = self.ancestor_timeline {
                let searchkey = ObjectKey {
                    timeline: ancestor_timeline,
                    buf_tag: self.buf_tag,
                };
                let ancestor_iter = self
                    .obj_store
                    .object_versions(&searchkey, self.ancestor_lsn)?;

                // Load the parent timeline's metadata. (We don't
                // actually need it yet, only if we need to follow to
                // the grandparent timeline)
                let v = self
                    .obj_store
                    .get(&timeline_metadata_key(ancestor_timeline), Lsn(0))
                    .with_context(|| "timeline not found in repository")?;
                let ancestor_metadata = MetadataEntry::des(&v)?;

                self.ancestor_timeline = ancestor_metadata.ancestor_timeline;
                self.ancestor_lsn = ancestor_metadata.ancestor_lsn;
                self.current_iter = ancestor_iter;
            } else {
                return Ok(None);
            }
        }
    }
}
