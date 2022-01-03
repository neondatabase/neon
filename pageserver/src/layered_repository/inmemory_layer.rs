//! An in-memory layer stores recently received PageVersions.
//! The page versions are held in a BTreeMap. To avoid OOM errors, the map size is limited
//! and layers can be spilled to disk into ephemeral files.
//!
//! And there's another BTreeMap to track the size of the relation.
//!
use crate::layered_repository::ephemeral_file::EphemeralFile;
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
use anyhow::{ensure, Result};
use bytes::Bytes;
use log::*;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use zenith_utils::lsn::Lsn;
use zenith_utils::vec_map::VecMap;

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
    /// Frozen layers have an exclusive end LSN.
    /// Writes are only allowed when this is None
    end_lsn: Option<Lsn>,

    /// If this relation was dropped, remember when that happened.
    /// The drop LSN is recorded in [`end_lsn`].
    dropped: bool,

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
    // An in-memory layer can be spilled to disk into ephemeral file,
    // This function is used only for debugging, so we don't need to be very precise.
    // Construct a filename as if it was a delta layer.
    fn filename(&self) -> PathBuf {
        let inner = self.inner.read().unwrap();

        let end_lsn;
        if let Some(drop_lsn) = inner.end_lsn {
            end_lsn = drop_lsn;
        } else {
            end_lsn = Lsn(u64::MAX);
        }

        let delta_filename = DeltaFileName {
            start_seg: self.seg,
            end_seg: SegmentTag {
                rel: self.seg.rel,
                segno: self.seg.segno + 1,
            },
            start_lsn: self.start_lsn,
            end_lsn,
            dropped: inner.dropped,
        }
        .to_string();

        PathBuf::from(format!("inmem-{}", delta_filename))
    }

    fn get_tenant_id(&self) -> ZTenantId {
        self.tenantid
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
        seg: SegmentTag,
        blknum: u32,
        lsn: Lsn,
        cached_img_lsn: Option<Lsn>,
        reconstruct_data: &mut PageReconstructData,
    ) -> Result<PageReconstructResult> {
        assert_eq!(self.seg, seg); // TODO

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
            for (entry_lsn, pos) in iter {
                match &cached_img_lsn {
                    Some(cached_lsn) if entry_lsn <= cached_lsn => {
                        return Ok(PageReconstructResult::Cached)
                    }
                    _ => {}
                }

                let pv = inner.page_versions.get_page_version(*pos)?;
                match pv {
                    PageVersion::Page(img) => {
                        reconstruct_data.page_img = Some(img);
                        need_image = false;
                        break;
                    }
                    PageVersion::Wal(rec) => {
                        reconstruct_data.records.push((*entry_lsn, rec.clone()));
                        if rec.will_init {
                            // This WAL record initializes the page, so no need to go further back
                            need_image = false;
                            break;
                        }
                    }
                }
            }

            // If we didn't find any records for this, check if the request is beyond EOF
            if need_image
                && reconstruct_data.records.is_empty()
                && self.seg.rel.is_blocky()
                && blknum - self.seg.segno * RELISH_SEG_SIZE >= self.get_seg_size(seg, lsn)?
            {
                return Ok(PageReconstructResult::Missing(self.start_lsn));
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
    fn get_seg_size(&self, seg: SegmentTag, lsn: Lsn) -> Result<u32> {
        assert_eq!(self.seg, seg);

        assert!(lsn >= self.start_lsn);
        ensure!(
            self.seg.rel.is_blocky(),
            "get_seg_size() called on a non-blocky rel"
        );

        let inner = self.inner.read().unwrap();
        Ok(inner.get_seg_size(lsn))
    }

    /// Does this segment exist at given LSN?
    fn get_seg_exists(&self, seg: SegmentTag, lsn: Lsn) -> Result<bool> {
        assert_eq!(self.seg, seg);

        let inner = self.inner.read().unwrap();

        // If the segment created after requested LSN,
        // it doesn't exist in the layer. But we shouldn't
        // have requested it in the first place.
        assert!(lsn >= self.start_lsn);

        // Is the requested LSN after the segment was dropped?
        if inner.dropped {
            if let Some(end_lsn) = inner.end_lsn {
                if lsn >= end_lsn {
                    return Ok(false);
                }
            } else {
                panic!("dropped in-memory layer with no end LSN");
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
        panic!("can't delete an InMemoryLayer")
    }

    fn is_incremental(&self) -> bool {
        self.incremental
    }

    fn is_in_memory(&self) -> bool {
        true
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self) -> Result<()> {
        let inner = self.inner.read().unwrap();

        let end_str = inner
            .end_lsn
            .as_ref()
            .map(Lsn::to_string)
            .unwrap_or_default();

        println!(
            "----- in-memory layer for tli {} seg {} {}-{} {} ----",
            self.timelineid, self.seg, self.start_lsn, end_str, inner.dropped,
        );

        for (k, v) in inner.segsizes.as_slice() {
            println!("segsizes {}: {}", k, v);
        }

        for (blknum, lsn, pos) in inner.page_versions.ordered_page_version_iter(None) {
            let pv = inner.page_versions.get_page_version(pos)?;
            let pv_description = match pv {
                PageVersion::Page(_img) => "page",
                PageVersion::Wal(_rec) => "wal",
            };

            println!("blk {} at {}: {}\n", blknum, lsn, pv_description);
        }

        Ok(())
    }
}

/// A result of an inmemory layer data being written to disk.
pub struct LayersOnDisk {
    pub delta_layers: Vec<DeltaLayer>,
    pub image_layers: Vec<ImageLayer>,
}

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

        // The segment is initially empty, so initialize 'segsizes' with 0.
        let mut segsizes = VecMap::default();
        if seg.rel.is_blocky() {
            segsizes.append(start_lsn, 0).unwrap();
        }

        let file = EphemeralFile::create(conf, tenantid, timelineid)?;

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
                page_versions: PageVersions::new(file),
                segsizes,
            }),
        })
    }

    // Write operations

    /// Remember new page version, as a WAL record over previous version
    pub fn put_wal_record(&self, lsn: Lsn, blknum: u32, rec: WALRecord) -> Result<u32> {
        self.put_page_version(blknum, lsn, PageVersion::Wal(rec))
    }

    /// Remember new page version, as a full page image
    pub fn put_page_image(&self, blknum: u32, lsn: Lsn, img: Bytes) -> Result<u32> {
        self.put_page_version(blknum, lsn, PageVersion::Page(img))
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
        let mut inner = self.inner.write().unwrap();

        inner.assert_writeable();

        let old = inner.page_versions.append_or_update_last(blknum, lsn, pv)?;

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
                    let zeropv = PageVersion::Page(ZERO_PAGE.clone());
                    trace!(
                        "filling gap blk {} with zeros for write of {}",
                        gapblknum,
                        blknum
                    );
                    let old = inner
                        .page_versions
                        .append_or_update_last(gapblknum, lsn, zeropv)?;
                    // We already had an entry for this LSN. That's odd..

                    if old.is_some() {
                        warn!(
                            "Page version of rel {} blk {} at {} already exists",
                            self.seg.rel, blknum, lsn
                        );
                    }
                }

                inner.segsizes.append_or_update_last(lsn, newsize).unwrap();
                return Ok(newsize - oldsize);
            }
        }

        Ok(0)
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

        let (old, _delta_size) = inner.segsizes.append_or_update_last(lsn, segsize).unwrap();

        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!("Inserting truncation, but had an entry for the LSN already");
        }
    }

    /// Remember that the segment was dropped at given LSN
    pub fn drop_segment(&self, lsn: Lsn) {
        let mut inner = self.inner.write().unwrap();

        assert!(inner.end_lsn.is_none());
        assert!(!inner.dropped);
        inner.dropped = true;
        assert!(self.start_lsn < lsn);
        inner.end_lsn = Some(lsn);

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
            let size = src.get_seg_size(seg, start_lsn)?;
            segsizes.append(start_lsn, size).unwrap();
        }

        let file = EphemeralFile::create(conf, tenantid, timelineid)?;

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
                page_versions: PageVersions::new(file),
                segsizes,
            }),
        })
    }

    pub fn is_writeable(&self) -> bool {
        let inner = self.inner.read().unwrap();
        inner.end_lsn.is_none()
    }

    /// Make the layer non-writeable. Only call once.
    /// Records the end_lsn for non-dropped layers.
    /// `end_lsn` is inclusive
    pub fn freeze(&self, end_lsn: Lsn) {
        let mut inner = self.inner.write().unwrap();

        if inner.end_lsn.is_some() {
            assert!(inner.dropped);
        } else {
            assert!(!inner.dropped);
            assert!(self.start_lsn < end_lsn + 1);
            inner.end_lsn = Some(Lsn(end_lsn.0 + 1));

            if let Some((lsn, _)) = inner.segsizes.as_slice().last() {
                assert!(lsn <= &end_lsn, "{:?} {:?}", lsn, end_lsn);
            }

            for (_blk, lsn, _pv) in inner.page_versions.ordered_page_version_iter(None) {
                assert!(lsn <= end_lsn);
            }
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
    pub fn write_to_disk(&self, timeline: &LayeredTimeline) -> Result<LayersOnDisk> {
        trace!(
            "write_to_disk {} get_end_lsn is {}",
            self.filename().display(),
            self.get_end_lsn()
        );

        // Grab the lock in read-mode. We hold it over the I/O, but because this
        // layer is not writeable anymore, no one should be trying to acquire the
        // write lock on it, so we shouldn't block anyone. There's one exception
        // though: another thread might have grabbed a reference to this layer
        // in `get_layer_for_write' just before the checkpointer called
        // `freeze`, and then `write_to_disk` on it. When the thread gets the
        // lock, it will see that it's not writeable anymore and retry, but it
        // would have to wait until we release it. That race condition is very
        // rare though, so we just accept the potential latency hit for now.
        let inner = self.inner.read().unwrap();
        let end_lsn_exclusive = inner.end_lsn.unwrap();

        if inner.dropped {
            let delta_layer = DeltaLayer::create(
                self.conf,
                self.timelineid,
                self.tenantid,
                self.seg,
                self.start_lsn,
                end_lsn_exclusive,
                true,
                &inner.page_versions,
                None,
                inner.segsizes.as_slice(),
            )?;
            trace!(
                "freeze: created delta layer for dropped segment {} {}-{}",
                self.seg,
                self.start_lsn,
                end_lsn_exclusive
            );
            return Ok(LayersOnDisk {
                delta_layers: vec![delta_layer],
                image_layers: Vec::new(),
            });
        }

        // Since `end_lsn` is inclusive, subtract 1.
        // We want to make an ImageLayer for the last included LSN,
        // so the DeltaLayer should exclude that LSN.
        let end_lsn_inclusive = Lsn(end_lsn_exclusive.0 - 1);

        let mut delta_layers = Vec::new();

        if self.start_lsn != end_lsn_inclusive {
            let (segsizes, _) = inner.segsizes.split_at(&end_lsn_exclusive);
            // Write the page versions before the cutoff to disk.
            let delta_layer = DeltaLayer::create(
                self.conf,
                self.timelineid,
                self.tenantid,
                self.seg,
                self.start_lsn,
                end_lsn_inclusive,
                false,
                &inner.page_versions,
                Some(end_lsn_inclusive),
                segsizes.as_slice(), // TODO avoid copy above
            )?;
            delta_layers.push(delta_layer);
            trace!(
                "freeze: created delta layer {} {}-{}",
                self.seg,
                self.start_lsn,
                end_lsn_inclusive
            );
        } else {
            assert!(inner
                .page_versions
                .ordered_page_version_iter(None)
                .next()
                .is_none());
        }

        drop(inner);

        // Write a new base image layer at the cutoff point
        let image_layer =
            ImageLayer::create_from_src(self.conf, timeline, self, end_lsn_inclusive)?;
        trace!(
            "freeze: created image layer {} at {}",
            self.seg,
            end_lsn_inclusive
        );

        Ok(LayersOnDisk {
            delta_layers,
            image_layers: vec![image_layer],
        })
    }
}
