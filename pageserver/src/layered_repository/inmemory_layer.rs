//!
//! An in-memory layer stores recently received page versions in memory. The page versions
//! are held in a BTreeMap, and there's another BTreeMap to track the size of the relation.
//!
use crate::layered_repository::filename::DeltaFileName;
use crate::layered_repository::storage_layer::{
    Layer, PageReconstructData, PageReconstructResult, PageVersion, SegmentTag, RELISH_SEG_SIZE,
};
use crate::layered_repository::LayeredTimeline;
use crate::layered_repository::ZERO_PAGE;
use crate::layered_repository::{DeltaLayer, ImageLayer};
use crate::relish::RelishTag;
use crate::repository::WALRecord;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, Result};
use bytes::Bytes;
use log::*;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use zenith_utils::lsn::Lsn;

pub struct InMemoryLayer {
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    seg: SegmentTag,

    ///
    /// This layer contains all the changes from 'start_lsn'. The
    /// start is inclusive. There is no end LSN; we only use in-memory
    /// layer at the end of a timeline.
    ///
    start_lsn: Lsn,

    /// LSN of the oldest page version stored in this layer
    oldest_pending_lsn: Lsn,

    /// The above fields never change. The parts that do change are in 'inner',
    /// and protected by mutex.
    inner: Mutex<InMemoryLayerInner>,

    /// Predecessor layer
    predecessor: Option<Arc<dyn Layer>>,
}

pub struct InMemoryLayerInner {
    /// If this relation was dropped, remember when that happened.
    drop_lsn: Option<Lsn>,

    ///
    /// All versions of all pages in the layer are are kept here.
    /// Indexed by block number and LSN.
    ///
    page_versions: BTreeMap<(u32, Lsn), PageVersion>,

    ///
    /// `segsizes` tracks the size of the segment at different points in time.
    ///
    segsizes: BTreeMap<Lsn, u32>,

    /// True if freeze() has been called on the layer, indicating it no longer
    /// accepts writes.
    frozen: bool,
}

impl InMemoryLayerInner {
    /// Assert that the layer is not frozen
    fn assert_writeable(&self) {
        // TODO current this can happen when a walreceiver thread's
        // `get_layer_for_write` and InMemoryLayer write calls interleave with
        // a checkpoint operation, however timing should make this rare.
        // Assert only so we can identify when the bug triggers more easily.
        assert!(!self.frozen);
    }

    fn get_seg_size(&self, lsn: Lsn) -> u32 {
        // Scan the BTreeMap backwards, starting from the given entry.
        let mut iter = self.segsizes.range((Included(&Lsn(0)), Included(&lsn)));

        if let Some((_entry_lsn, entry)) = iter.next_back() {
            *entry
        } else {
            0
        }
    }
}

impl Layer for InMemoryLayer {
    // An in-memory layer doesn't really have a filename as it's not stored on disk,
    // but we construct a filename as if it was a delta layer
    fn filename(&self) -> PathBuf {
        let inner = self.inner.lock().unwrap();

        let end_lsn;
        let dropped;
        if let Some(drop_lsn) = inner.drop_lsn {
            end_lsn = drop_lsn;
            dropped = true;
        } else {
            end_lsn = Lsn(u64::MAX);
            dropped = false;
        }

        let delta_filename = DeltaFileName {
            seg: self.seg,
            start_lsn: self.start_lsn,
            end_lsn,
            dropped,
        }
        .to_string();

        PathBuf::from(format!("inmem-{}", delta_filename))
    }

    fn get_timeline_id(&self) -> ZTimelineId {
        self.timelineid
    }

    fn get_seg_tag(&self) -> SegmentTag {
        self.seg
    }

    fn get_start_lsn(&self) -> Lsn {
        self.start_lsn
    }

    fn get_end_lsn(&self) -> Lsn {
        let inner = self.inner.lock().unwrap();

        if let Some(drop_lsn) = inner.drop_lsn {
            drop_lsn
        } else {
            Lsn(u64::MAX)
        }
    }

    fn is_dropped(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.drop_lsn.is_some()
    }

    /// Look up given page in the cache.
    fn get_page_reconstruct_data(
        &self,
        blknum: u32,
        lsn: Lsn,
        reconstruct_data: &mut PageReconstructData,
    ) -> Result<PageReconstructResult> {
        let mut need_image = true;

        assert!(self.seg.blknum_in_seg(blknum));

        {
            let inner = self.inner.lock().unwrap();

            // Scan the BTreeMap backwards, starting from reconstruct_data.lsn.
            let minkey = (blknum, Lsn(0));
            let maxkey = (blknum, lsn);
            let mut iter = inner
                .page_versions
                .range((Included(&minkey), Included(&maxkey)));
            while let Some(((_blknum, _entry_lsn), entry)) = iter.next_back() {
                if let Some(img) = &entry.page_image {
                    reconstruct_data.page_img = Some(img.clone());
                    need_image = false;
                    break;
                } else if let Some(rec) = &entry.record {
                    reconstruct_data.records.push(rec.clone());
                    if rec.will_init {
                        // This WAL record initializes the page, so no need to go further back
                        need_image = false;
                        break;
                    }
                } else {
                    // No base image, and no WAL record. Huh?
                    bail!("no page image or WAL record for requested page");
                }
            }

            // release lock on 'inner'
        }

        // If an older page image is needed to reconstruct the page, let the
        // caller know about the predecessor layer.
        if need_image {
            if let Some(cont_layer) = &self.predecessor {
                Ok(PageReconstructResult::Continue(
                    self.start_lsn,
                    Arc::clone(cont_layer),
                ))
            } else {
                Ok(PageReconstructResult::Missing(self.start_lsn))
            }
        } else {
            Ok(PageReconstructResult::Complete)
        }
    }

    /// Get size of the relation at given LSN
    fn get_seg_size(&self, lsn: Lsn) -> Result<u32> {
        assert!(lsn >= self.start_lsn);

        let inner = self.inner.lock().unwrap();
        Ok(inner.get_seg_size(lsn))
    }

    /// Does this segment exist at given LSN?
    fn get_seg_exists(&self, lsn: Lsn) -> Result<bool> {
        let inner = self.inner.lock().unwrap();

        // If the segment created after requested LSN,
        // it doesn't exist in the layer. But we shouldn't
        // have requested it in the first place.
        assert!(lsn >= self.start_lsn);

        // Is the requested LSN after the segment was dropped?
        if let Some(drop_lsn) = inner.drop_lsn {
            if lsn >= drop_lsn {
                return Ok(false);
            }
        }

        // Otherwise, it exists
        Ok(true)
    }

    /// Cannot unload anything in an in-memory layer, since there's no backing
    /// store. To release memory used by an in-memory layer, use 'freeze' to turn
    /// it into an on-disk layer.
    fn unload(&self) -> Result<()> {
        Ok(())
    }

    /// Nothing to do here. When you drop the last reference to the layer, it will
    /// be deallocated.
    fn delete(&self) -> Result<()> {
        Ok(())
    }

    fn is_incremental(&self) -> bool {
        self.predecessor.is_some()
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self) -> Result<()> {
        let inner = self.inner.lock().unwrap();

        let end_str = inner
            .drop_lsn
            .as_ref()
            .map(|drop_lsn| drop_lsn.to_string())
            .unwrap_or_default();

        println!(
            "----- in-memory layer for tli {} seg {} {}-{} ----",
            self.timelineid, self.seg, self.start_lsn, end_str
        );

        for (k, v) in inner.segsizes.iter() {
            println!("segsizes {}: {}", k, v);
        }

        for (k, v) in inner.page_versions.iter() {
            println!(
                "blk {} at {}: {}/{}\n",
                k.0,
                k.1,
                v.page_image.is_some(),
                v.record.is_some()
            );
        }

        Ok(())
    }
}

// Type alias to simplify InMemoryLayer::freeze signature
//
type SuccessorLayers = (Vec<Arc<dyn Layer>>, Option<Arc<InMemoryLayer>>);

impl InMemoryLayer {
    /// Return the oldest page version that's stored in this layer
    pub fn get_oldest_pending_lsn(&self) -> Lsn {
        self.oldest_pending_lsn
    }

    ///
    /// Create a new, empty, in-memory layer
    ///
    pub fn create(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        seg: SegmentTag,
        start_lsn: Lsn,
        oldest_pending_lsn: Lsn,
    ) -> Result<InMemoryLayer> {
        trace!(
            "initializing new empty InMemoryLayer for writing {} on timeline {} at {}",
            seg,
            timelineid,
            start_lsn
        );

        Ok(InMemoryLayer {
            conf,
            timelineid,
            tenantid,
            seg,
            start_lsn,
            oldest_pending_lsn,
            inner: Mutex::new(InMemoryLayerInner {
                drop_lsn: None,
                page_versions: BTreeMap::new(),
                segsizes: BTreeMap::new(),
                frozen: false,
            }),
            predecessor: None,
        })
    }

    // Write operations

    /// Remember new page version, as a WAL record over previous version
    pub fn put_wal_record(&self, blknum: u32, rec: WALRecord) -> Result<u32> {
        self.put_page_version(
            blknum,
            rec.lsn,
            PageVersion {
                page_image: None,
                record: Some(rec),
            },
        )
    }

    /// Remember new page version, as a full page image
    pub fn put_page_image(&self, blknum: u32, lsn: Lsn, img: Bytes) -> Result<u32> {
        self.put_page_version(
            blknum,
            lsn,
            PageVersion {
                page_image: Some(img),
                record: None,
            },
        )
    }

    /// Common subroutine of the public put_wal_record() and put_page_image() functions.
    /// Adds the page version to the in-memory tree
    pub fn put_page_version(&self, blknum: u32, lsn: Lsn, pv: PageVersion) -> Result<u32> {
        assert!(self.seg.blknum_in_seg(blknum));

        trace!(
            "put_page_version blk {} of {} at {}/{}",
            blknum,
            self.seg.rel,
            self.timelineid,
            lsn
        );
        let mut inner = self.inner.lock().unwrap();

        inner.assert_writeable();

        let old = inner.page_versions.insert((blknum, lsn), pv);

        // We already had an entry for this LSN. That's odd..
        // Though, that is fine for checkpoint record that we update
        // at the end of timeline WAL import.
        if old.is_some() && !matches!(self.seg.rel, RelishTag::Checkpoint) {
            warn!(
                "Page version of rel {} blk {} at {} already exists",
                self.seg.rel, blknum, lsn
            );
        }

        // Also update the relation size, if this extended the relation.
        if self.seg.rel.is_blocky() {
            let newsize = blknum - self.seg.segno * RELISH_SEG_SIZE + 1;

            // use inner get_seg_size, since calling self.get_seg_size will try to acquire self.inner.lock
            // which we've just acquired above
            let oldsize = inner.get_seg_size(lsn);
            if newsize > oldsize {
                trace!(
                    "enlarging segment {} from {} to {} blocks at {}",
                    self.seg,
                    oldsize,
                    newsize,
                    lsn
                );

                // If we are extending the relation by more than one page, initialize the "gap"
                // with zeros
                //
                // XXX: What if the caller initializes the gap with subsequent call with same LSN?
                // I don't think that can happen currently, but that is highly dependent on how
                // PostgreSQL writes its WAL records and there's no guarantee of it. If it does
                // happen, we would hit the "page version already exists" warning above on the
                // subsequent call to initialize the gap page.
                let gapstart = self.seg.segno * RELISH_SEG_SIZE + oldsize;
                for gapblknum in gapstart..blknum {
                    let zeropv = PageVersion {
                        page_image: Some(ZERO_PAGE.clone()),
                        record: None,
                    };
                    println!(
                        "filling gap blk {} with zeros for write of {}",
                        gapblknum, blknum
                    );
                    let old = inner.page_versions.insert((gapblknum, lsn), zeropv);
                    // We already had an entry for this LSN. That's odd..

                    if old.is_some() {
                        warn!(
                            "Page version of rel {} blk {} at {} already exists",
                            self.seg.rel, blknum, lsn
                        );
                    }
                }

                inner.segsizes.insert(lsn, newsize);
                return Ok(newsize - oldsize);
            }
        }
        Ok(0)
    }

    /// Remember that the relation was truncated at given LSN
    pub fn put_truncation(&self, lsn: Lsn, segsize: u32) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().unwrap();

        inner.assert_writeable();

        // check that this we truncate to a smaller size than segment was before the truncation
        let oldsize = inner.get_seg_size(lsn);
        assert!(segsize < oldsize);

        let old = inner.segsizes.insert(lsn, segsize);

        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!("Inserting truncation, but had an entry for the LSN already");
        }

        Ok(())
    }

    /// Remember that the segment was dropped at given LSN
    pub fn drop_segment(&self, lsn: Lsn) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().unwrap();

        inner.assert_writeable();

        assert!(inner.drop_lsn.is_none());
        inner.drop_lsn = Some(lsn);

        info!("dropped segment {} at {}", self.seg, lsn);

        Ok(())
    }

    ///
    /// Initialize a new InMemoryLayer for, by copying the state at the given
    /// point in time from given existing layer.
    ///
    pub fn create_successor_layer(
        conf: &'static PageServerConf,
        src: Arc<dyn Layer>,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        start_lsn: Lsn,
        oldest_pending_lsn: Lsn,
    ) -> Result<InMemoryLayer> {
        let seg = src.get_seg_tag();

        trace!(
            "initializing new InMemoryLayer for writing {} on timeline {} at {}",
            seg,
            timelineid,
            start_lsn,
        );

        // For convenience, copy the segment size from the predecessor layer
        let mut segsizes = BTreeMap::new();
        if seg.rel.is_blocky() {
            let size = src.get_seg_size(start_lsn)?;
            segsizes.insert(start_lsn, size);
        }

        Ok(InMemoryLayer {
            conf,
            timelineid,
            tenantid,
            seg,
            start_lsn,
            oldest_pending_lsn,
            inner: Mutex::new(InMemoryLayerInner {
                drop_lsn: None,
                page_versions: BTreeMap::new(),
                segsizes,
                frozen: false,
            }),
            predecessor: Some(src),
        })
    }

    ///
    /// Write the this in-memory layer to disk.
    ///
    /// The cutoff point for the layer that's written to disk is 'end_lsn'.
    ///
    /// Returns new layers that replace this one. Always returns a new image
    /// layer containing the page versions at the cutoff LSN, that were written
    /// to disk, and usually also a DeltaLayer that includes all the WAL records
    /// between start LSN and the cutoff. (The delta layer is not needed when
    /// a new relish is created with a single LSN, so that the start and end LSN
    /// are the same.) If there were page versions newer than 'end_lsn', also
    /// returns a new in-memory layer containing those page versions. The caller
    /// replaces this layer with the returned layers in the layer map.
    ///
    pub fn freeze(
        &self,
        cutoff_lsn: Lsn,
        // This is needed just to call materialize_page()
        timeline: &LayeredTimeline,
    ) -> Result<SuccessorLayers> {
        info!(
            "freezing in memory layer for {} on timeline {} at {}",
            self.seg, self.timelineid, cutoff_lsn
        );

        let mut inner = self.inner.lock().unwrap();
        inner.assert_writeable();
        inner.frozen = true;

        // Normally, use the cutoff LSN as the end of the frozen layer.
        // But if the relation was dropped, we know that there are no
        // more changes coming in for it, and in particular we know that
        // there are no changes "in flight" for the LSN anymore, so we use
        // the drop LSN instead. The drop-LSN could be ahead of the
        // caller-specified LSN!
        let dropped = inner.drop_lsn.is_some();
        let end_lsn = if dropped {
            inner.drop_lsn.unwrap()
        } else {
            cutoff_lsn
        };

        // Divide all the page versions into old and new at the 'end_lsn' cutoff point.
        let mut before_page_versions;
        let mut before_segsizes;
        let mut after_page_versions;
        let mut after_segsizes;
        if !dropped {
            before_segsizes = BTreeMap::new();
            after_segsizes = BTreeMap::new();
            for (lsn, size) in inner.segsizes.iter() {
                if *lsn > end_lsn {
                    after_segsizes.insert(*lsn, *size);
                } else {
                    before_segsizes.insert(*lsn, *size);
                }
            }

            before_page_versions = BTreeMap::new();
            after_page_versions = BTreeMap::new();
            for ((blknum, lsn), pv) in inner.page_versions.iter() {
                match lsn.cmp(&end_lsn) {
                    Ordering::Less => {
                        before_page_versions.insert((*blknum, *lsn), pv.clone());
                    }
                    Ordering::Equal => {
                        // Page versions at the cutoff LSN will be stored in the
                        // materialized image layer.
                    }
                    Ordering::Greater => {
                        after_page_versions.insert((*blknum, *lsn), pv.clone());
                    }
                }
            }
        } else {
            before_page_versions = inner.page_versions.clone();
            before_segsizes = inner.segsizes.clone();
            after_segsizes = BTreeMap::new();
            after_page_versions = BTreeMap::new();
        }

        // we can release the lock now.
        drop(inner);

        let mut frozen_layers: Vec<Arc<dyn Layer>> = Vec::new();

        if self.start_lsn != end_lsn {
            // Write the page versions before the cutoff to disk.
            let delta_layer = DeltaLayer::create(
                self.conf,
                self.timelineid,
                self.tenantid,
                self.seg,
                self.start_lsn,
                end_lsn,
                dropped,
                self.predecessor.clone(),
                before_page_versions,
                before_segsizes,
            )?;
            let delta_layer_rc: Arc<dyn Layer> = Arc::new(delta_layer);
            frozen_layers.push(delta_layer_rc);
            trace!(
                "freeze: created delta layer {} {}-{}",
                self.seg,
                self.start_lsn,
                end_lsn
            );
        } else {
            assert!(before_page_versions.is_empty());
        }

        let mut new_open_rc = None;
        if !dropped {
            // Write a new base image layer at the cutoff point
            let imgfile = ImageLayer::create_from_src(self.conf, timeline, self, end_lsn)?;
            let imgfile_rc: Arc<dyn Layer> = Arc::new(imgfile);
            frozen_layers.push(Arc::clone(&imgfile_rc));
            trace!("freeze: created image layer {} at {}", self.seg, end_lsn);

            // If there were any page versions newer than the cutoff, initialize a new in-memory
            // layer to hold them
            if !after_segsizes.is_empty() || !after_page_versions.is_empty() {
                let new_open = Self::create_successor_layer(
                    self.conf,
                    imgfile_rc,
                    self.timelineid,
                    self.tenantid,
                    end_lsn,
                    end_lsn,
                )?;
                let mut new_inner = new_open.inner.lock().unwrap();
                new_inner.page_versions.append(&mut after_page_versions);
                new_inner.segsizes.append(&mut after_segsizes);
                drop(new_inner);
                trace!("freeze: created new in-mem layer {} {}-", self.seg, end_lsn);

                new_open_rc = Some(Arc::new(new_open))
            }
        }

        Ok((frozen_layers, new_open_rc))
    }
}
