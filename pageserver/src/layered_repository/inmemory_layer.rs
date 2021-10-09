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
use crate::repository::WALRecord;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, ensure, Result};
use bytes::Bytes;
use log::*;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use zenith_utils::vec_map::VecMap;

use zenith_utils::lsn::Lsn;

use super::page_versions::PageVersions;

pub struct InMemoryLayer {
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    seg: SegmentTag,

    ///
    /// This layer contains all the changes from 'start_lsn'. The
    /// start is inclusive.
    ///
    start_lsn: Lsn,

    /// LSN of the oldest page version stored in this layer
    oldest_pending_lsn: Lsn,

    /// The above fields never change. The parts that do change are in 'inner',
    /// and protected by mutex.
    inner: RwLock<InMemoryLayerInner>,

    /// Predecessor layer might be needed?
    incremental: bool,
}

pub struct InMemoryLayerInner {
    /// If this relation was dropped
    dropped: bool,

    /// Frozen or dropped in-memory layers have an end LSN.
    end_lsn: Option<Lsn>,

    ///
    /// All versions of all pages in the layer are are kept here.
    /// Indexed by block number and LSN.
    ///
    page_versions: PageVersions,

    ///
    /// `segsizes` tracks the size of the segment at different points in time.
    ///
    /// For a blocky rel, there is always one entry, at the layer's start_lsn,
    /// so that determining the size never depends on the predecessor layer. For
    /// a non-blocky rel, 'segsizes' is not used and is always empty.
    ///
    segsizes: VecMap<Lsn, u32>,
}

impl InMemoryLayerInner {
    fn assert_writeable(&self) {
        assert!(self.end_lsn.is_none());
    }

    fn get_seg_size(&self, lsn: Lsn) -> u32 {
        // Scan the BTreeMap backwards, starting from the given entry.
        let slice = self.segsizes.slice_range(..=lsn);

        // We make sure there is always at least one entry
        if let Some((_entry_lsn, entry)) = slice.last() {
            *entry
        } else {
            panic!("could not find seg size in in-memory layer");
        }
    }
}

impl Layer for InMemoryLayer {
    // An in-memory layer doesn't really have a filename as it's not stored on disk,
    // but we construct a filename as if it was a delta layer
    fn filename(&self) -> PathBuf {
        let inner = self.inner.read().unwrap();

        self.filename_locked(&inner)
    }

    fn path(&self) -> Option<PathBuf> {
        None
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
        let inner = self.inner.read().unwrap();

        if let Some(end_lsn) = inner.end_lsn {
            end_lsn
        } else {
            Lsn(u64::MAX)
        }
    }

    fn is_dropped(&self) -> bool {
        let inner = self.inner.read().unwrap();
        inner.dropped
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
            let inner = self.inner.read().unwrap();

            // Scan the page versions backwards, starting from `lsn`.
            let iter = inner
                .page_versions
                .get_block_lsn_range(blknum, ..=lsn)
                .iter()
                .rev();
            for (_entry_lsn, entry) in iter {
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
        // caller know
        if need_image {
            if self.incremental {
                Ok(PageReconstructResult::Continue(Lsn(self.start_lsn.0 - 1)))
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
        ensure!(
            self.seg.rel.is_blocky(),
            "get_seg_size() called on a non-blocky rel"
        );

        let inner = self.inner.read().unwrap();
        Ok(inner.get_seg_size(lsn))
    }

    /// Does this segment exist at given LSN?
    fn get_seg_exists(&self, lsn: Lsn) -> Result<bool> {
        let inner = self.inner.read().unwrap();

        // If the segment created after requested LSN,
        // it doesn't exist in the layer. But we shouldn't
        // have requested it in the first place.
        assert!(lsn >= self.start_lsn);

        // Is the requested LSN after the segment was dropped?
        if inner.dropped && lsn >= inner.end_lsn.unwrap() {
            return Ok(false);
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
        self.incremental
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self) -> Result<()> {
        let inner = self.inner.read().unwrap();

        let end_str = inner
            .end_lsn
            .as_ref()
            .map(|end_lsn| end_lsn.to_string())
            .unwrap_or_default();

        println!(
            "----- in-memory layer for tli {} seg {} {}-{} ----",
            self.timelineid, self.seg, self.start_lsn, end_str
        );

        for (k, v) in inner.segsizes.as_slice() {
            println!("segsizes {}: {}", k, v);
        }

        for (blknum, lsn, pv) in inner.page_versions.ordered_page_version_iter(None) {
            println!(
                "blk {} at {}: {}/{}\n",
                blknum,
                lsn,
                pv.page_image.is_some(),
                pv.record.is_some()
            );
        }

        Ok(())
    }
}

impl InMemoryLayer {
    /// Return the oldest page version that's stored in this layer
    pub fn get_oldest_pending_lsn(&self) -> Lsn {
        self.oldest_pending_lsn
    }

    fn filename_locked(&self, inner: &InMemoryLayerInner) -> PathBuf {
        let end_lsn;
        let dropped;
        if inner.dropped {
            end_lsn = inner.end_lsn.unwrap();
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

        // The segment is initially empty, so initialize 'segsizes' with 0.
        let mut segsizes = VecMap::default();
        if seg.rel.is_blocky() {
            segsizes.append(start_lsn, 0).unwrap();
        }

        Ok(InMemoryLayer {
            conf,
            timelineid,
            tenantid,
            seg,
            start_lsn,
            oldest_pending_lsn,
            incremental: false,
            inner: RwLock::new(InMemoryLayerInner {
                end_lsn: None,
                dropped: false,
                page_versions: PageVersions::default(),
                segsizes,
            }),
        })
    }

    // Write operations

    /// Remember new page version, as a WAL record over previous version
    pub fn put_wal_record(&self, blknum: u32, rec: WALRecord) -> u32 {
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
    pub fn put_page_image(&self, blknum: u32, lsn: Lsn, img: Bytes) -> u32 {
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
    pub fn put_page_version(&self, blknum: u32, lsn: Lsn, pv: PageVersion) -> u32 {
        assert!(self.seg.blknum_in_seg(blknum));

        trace!(
            "put_page_version blk {} of {} at {}/{}",
            blknum,
            self.seg.rel,
            self.timelineid,
            lsn
        );
        let mut inner = self.inner.write().unwrap();
        inner.assert_writeable();

        let old = inner.page_versions.append_or_update_last(blknum, lsn, pv);

        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!(
                "Page version of rel {} blk {} at {} already exists",
                self.seg.rel, blknum, lsn
            );
        }

        // Also update the relation size, if this extended the relation.
        if self.seg.rel.is_blocky() {
            let newsize = blknum - self.seg.segno * RELISH_SEG_SIZE + 1;

            // use inner get_seg_size, since calling self.get_seg_size will try to acquire the lock,
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
                    trace!(
                        "filling gap blk {} with zeros for write of {}",
                        gapblknum,
                        blknum
                    );
                    let old = inner
                        .page_versions
                        .append_or_update_last(gapblknum, lsn, zeropv);
                    // We already had an entry for this LSN. That's odd..

                    if old.is_some() {
                        warn!(
                            "Page version of rel {} blk {} at {} already exists",
                            self.seg.rel, blknum, lsn
                        );
                    }
                }

                inner.segsizes.append_or_update_last(lsn, newsize).unwrap();
                return newsize - oldsize;
            }
        }
        0
    }

    /// Remember that the relation was truncated at given LSN
    pub fn put_truncation(&self, lsn: Lsn, segsize: u32) {
        assert!(
            self.seg.rel.is_blocky(),
            "put_truncation() called on a non-blocky rel"
        );

        let mut inner = self.inner.write().unwrap();
        inner.assert_writeable();

        // check that this we truncate to a smaller size than segment was before the truncation
        let oldsize = inner.get_seg_size(lsn);
        assert!(segsize < oldsize);

        let old = inner.segsizes.append_or_update_last(lsn, segsize).unwrap();

        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!("Inserting truncation, but had an entry for the LSN already");
        }
    }

    /// Remember that the segment was dropped at given LSN
    pub fn drop_segment(&self, lsn: Lsn) {
        let mut inner = self.inner.write().unwrap();
        inner.assert_writeable();

        assert!(!inner.dropped);
        assert!(inner.end_lsn.is_none());

        inner.end_lsn = Some(Lsn(lsn.0));
        inner.dropped = true;

        trace!("dropped segment {} at {}", self.seg, lsn);
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

        assert!(oldest_pending_lsn.is_aligned());
        assert!(oldest_pending_lsn >= start_lsn);

        trace!(
            "initializing new InMemoryLayer for writing {} on timeline {} at {}",
            seg,
            timelineid,
            start_lsn,
        );

        // Copy the segment size at the start LSN from the predecessor layer.
        let mut segsizes = VecMap::default();
        if seg.rel.is_blocky() {
            let size = src.get_seg_size(start_lsn)?;
            segsizes.append(start_lsn, size).unwrap();
        }

        Ok(InMemoryLayer {
            conf,
            timelineid,
            tenantid,
            seg,
            start_lsn,
            oldest_pending_lsn,
            incremental: true,
            inner: RwLock::new(InMemoryLayerInner {
                end_lsn: None,
                dropped: false,
                page_versions: PageVersions::default(),
                segsizes,
            }),
        })
    }

    pub fn is_writeable(&self) -> bool {
        let inner = self.inner.read().unwrap();
        inner.end_lsn.is_none()
    }

    /// Splits `self` into two InMemoryLayers: `frozen` and `open`.
    /// All data up to and including `cutoff_lsn`
    /// is copied to `frozen`, while the remaining data is copied to `open`.
    /// After completion, self is non-writeable, but not frozen.
    pub fn freeze(&self, cutoff_lsn: Lsn) {
        let mut inner = self.inner.write().unwrap();

        // Dropped layers have their end-lsn already set, and are effectively
        // frozen already.
        if inner.dropped {
            info!(
                "freezing in memory layer for {} on timeline {} is dropped at {}",
                self.seg,
                self.timelineid,
                inner.end_lsn.unwrap()
            );
        } else {
            inner.assert_writeable();

            // cutoff_lsn is the last record to be included, while end_lsn is exclusive.
            inner.end_lsn = Some(cutoff_lsn + 1);

            info!(
                "freezing in memory layer {} on timeline {} at {} (oldest {})",
                self.filename_locked(&inner).display(),
                self.timelineid,
                cutoff_lsn,
                self.oldest_pending_lsn
            );
        }
    }

    /// Write the this frozen in-memory layer to disk.
    ///
    /// Returns new layers that replace this one.
    /// If not dropped, returns a new image layer containing the page versions
    /// at the `end_lsn`. Can also return a DeltaLayer that includes all the
    /// WAL records between start and end LSN. (The delta layer is not needed
    /// when a new relish is created with a single LSN, so that the start and
    /// end LSN are the same.)
    pub fn write_to_disk(&self, timeline: &LayeredTimeline) -> Result<Vec<Arc<dyn Layer>>> {
        // Grab the lock in read-mode. We hold it over the I/O, but because this
        // layer is not writeable anymore, no one should be trying to aquire the
        // write lock on it, so we shouldn't block anyone.
        let inner = self.inner.read().unwrap();
        let end_lsn = inner.end_lsn.unwrap();

        trace!(
            "write_to_disk {} end_lsn is {}",
            self.filename_locked(&inner).display(),
            end_lsn,
        );

        let img_lsn = Lsn(end_lsn.0 - 1);

        if inner.dropped {
            let delta_layer = DeltaLayer::create(
                self.conf,
                self.timelineid,
                self.tenantid,
                self.seg,
                self.start_lsn,
                end_lsn,
                true,
                inner.page_versions.ordered_page_version_iter(None),
                inner.segsizes.clone(),
            )?;
            info!(
                "freeze: created delta layer for dropped segment {} {}-{}",
                self.seg, self.start_lsn, end_lsn
            );
            return Ok(vec![Arc::new(delta_layer)]);
        }

        let mut frozen_layers: Vec<Arc<dyn Layer>> = Vec::new();

        if self.start_lsn != img_lsn {
            let before_page_versions = inner.page_versions.ordered_page_version_iter(Some(img_lsn));
            let (before_segsizes, _after_segsizes) = inner.segsizes.split_at(&img_lsn);

            // Write the page versions before the cutoff to disk.
            let delta_layer = DeltaLayer::create(
                self.conf,
                self.timelineid,
                self.tenantid,
                self.seg,
                self.start_lsn,
                img_lsn,
                false,
                before_page_versions,
                before_segsizes,
            )?;
            frozen_layers.push(Arc::new(delta_layer));
            info!(
                "freeze: created delta layer {} {}-{}",
                self.seg, self.start_lsn, img_lsn
            );
        }

        drop(inner);

        // Write a new base image layer at the cutoff point
        let image_layer = ImageLayer::create_from_src(self.conf, timeline, self, img_lsn)?;
        frozen_layers.push(Arc::new(image_layer));
        info!("freeze: created image layer {} at {}", self.seg, img_lsn);

        Ok(frozen_layers)
    }
}
