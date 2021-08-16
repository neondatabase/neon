//!
//! An in-memory layer stores recently received page versions in memory. The page versions
//! are held in a BTreeMap, and there's another BTreeMap to track the size of the relation.
//!
use crate::layered_repository::storage_layer::{
    Layer, PageReconstructData, PageVersion, SegmentTag, RELISH_SEG_SIZE,
};
use crate::layered_repository::LayeredTimeline;
use crate::layered_repository::SnapshotLayer;
use crate::repository::WALRecord;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, Result};
use bytes::Bytes;
use log::*;
use std::collections::BTreeMap;
use std::ops::Bound::Included;
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

    oldest_pending_lsn: Lsn,

    /// The above fields never change. The parts that do change are in 'inner',
    /// and protected by mutex.
    inner: Mutex<InMemoryLayerInner>,
}

pub struct InMemoryLayerInner {
    /// If this relation was dropped, remember when that happened.
    drop_lsn: Option<Lsn>,

    base_images: Vec<Bytes>,

    ///
    /// All versions of all pages in the layer are are kept here.
    /// Indexed by block number and LSN.
    ///
    page_versions: BTreeMap<(u32, Lsn), PageVersion>,

    ///
    /// `segsizes` tracks the size of the segment at different points in time.
    ///
    segsizes: BTreeMap<Lsn, u32>,

    ///
    /// Memory usage
    ///
    mem_used: usize,
}

impl Layer for InMemoryLayer {
    fn get_timeline_id(&self) -> ZTimelineId {
        return self.timelineid;
    }

    fn get_seg_tag(&self) -> SegmentTag {
        return self.seg;
    }

    fn get_start_lsn(&self) -> Lsn {
        return self.start_lsn;
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
    ) -> Result<Option<Lsn>> {
        // Scan the BTreeMap backwards, starting from reconstruct_data.lsn.
        let mut need_base_image_lsn: Option<Lsn> = Some(lsn);

        assert!(self.seg.blknum_in_seg(blknum));

        {
            let inner = self.inner.lock().unwrap();
            let minkey = (blknum, Lsn(0));
            let maxkey = (blknum, lsn);
            let mut iter = inner
                .page_versions
                .range((Included(&minkey), Included(&maxkey)));
            while let Some(((_blknum, entry_lsn), entry)) = iter.next_back() {
                if let Some(img) = &entry.page_image {
                    reconstruct_data.page_img = Some(img.clone());
                    need_base_image_lsn = None;
                    break;
                } else if let Some(rec) = &entry.record {
                    reconstruct_data.records.push(rec.clone());
                    if rec.will_init {
                        // This WAL record initializes the page, so no need to go further back
                        need_base_image_lsn = None;
                        break;
                    } else {
                        need_base_image_lsn = Some(*entry_lsn);
                    }
                } else {
                    // No base image, and no WAL record. Huh?
                    bail!("no page image or WAL record for requested page");
                }
            }

            // Use the base image, if needed
            if need_base_image_lsn.is_some() {
                let base_blknum: usize = (blknum % RELISH_SEG_SIZE) as usize;
                if let Some(img) = inner.base_images.get(base_blknum) {
                    reconstruct_data.page_img = Some(img.clone());
                    need_base_image_lsn = None;
                } else {
                    bail!("inmem: no base img found for {} at blk {} at LSN {}", self.seg, base_blknum, lsn);
                }
            }

            // release lock on 'inner'
        }

        Ok(need_base_image_lsn)
    }

    /// Get size of the relation at given LSN
    fn get_seg_size(&self, lsn: Lsn) -> Result<u32> {
        assert!(lsn >= self.start_lsn);

        // Scan the BTreeMap backwards, starting from the given entry.
        let inner = self.inner.lock().unwrap();
        let mut iter = inner.segsizes.range((Included(&Lsn(0)), Included(&lsn)));

        let result;
        if let Some((_entry_lsn, entry)) = iter.next_back() {
            result = *entry;
        } else {
            result = inner.base_images.len() as u32;
        }
        trace!("get_seg_size: {} at {} -> {}", self.seg, lsn, result);
        Ok(result)
    }

    /// Does this segment exist at given LSN?
    fn get_seg_exists(&self, lsn: Lsn) -> Result<bool> {
        let inner = self.inner.lock().unwrap();

        // Is the requested LSN after the segment was dropped?
        if let Some(drop_lsn) = inner.drop_lsn {
            if lsn >= drop_lsn {
                return Ok(false);
            }
        }

        // Otherwise, it exists
        Ok(true)
    }
}

impl InMemoryLayer {

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
                base_images: Vec::new(),
                page_versions: BTreeMap::new(),
                segsizes: BTreeMap::new(),
                mem_used: 0,
            }),
        })
    }

    // Write operations

    /// Remember new page version, as a WAL record over previous version
    pub fn put_wal_record(&self, blknum: u32, rec: WALRecord) -> Result<()> {
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
    pub fn put_page_image(&self, blknum: u32, lsn: Lsn, img: Bytes) -> Result<()> {
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
    pub fn put_page_version(&self, blknum: u32, lsn: Lsn, pv: PageVersion) -> Result<()> {
        assert!(self.seg.blknum_in_seg(blknum));

        trace!(
            "put_page_version blk {} of {} at {}/{}",
            blknum,
            self.seg.rel,
            self.timelineid,
            lsn
        );

        let mem_size = pv.get_mem_size();

        let mut inner = self.inner.lock().unwrap();

        let old = inner.page_versions.insert((blknum, lsn), pv);

        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!(
                "Page version of rel {} blk {} at {} already exists",
                self.seg.rel, blknum, lsn
            );
        } else {
            inner.mem_used += mem_size;
        }

        // Also update the relation size, if this extended the relation.
        if self.seg.rel.is_blocky() {
            let newsize = blknum - self.seg.segno * RELISH_SEG_SIZE + 1;

            let mut iter = inner.segsizes.range((Included(&Lsn(0)), Included(&lsn)));

            let oldsize;
            if let Some((_entry_lsn, entry)) = iter.next_back() {
                oldsize = *entry;
            } else {
                oldsize = inner.base_images.len() as u32;
                //bail!("No old size found for {} at {}", self.tag, lsn);
            }
            if newsize > oldsize {
                trace!(
                    "enlarging segment {} from {} to {} blocks at {}",
                    self.seg,
                    oldsize,
                    newsize,
                    lsn
                );
                inner.segsizes.insert(lsn, newsize);
            }
        }

        Ok(())
    }

    /// Remember that the relation was truncated at given LSN
    pub fn put_truncation(&self, lsn: Lsn, segsize: u32) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let old = inner.segsizes.insert(lsn, segsize);

        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!("Inserting truncation, but had an entry for the LSN already");
        }

        Ok(())
    }

    /// Remember that the segment was dropped at given LSN
    pub fn put_unlink(&self, lsn: Lsn) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().unwrap();

        assert!(inner.drop_lsn.is_none());
        inner.drop_lsn = Some(lsn);

        info!("dropped segment {} at {}", self.seg, lsn);

        Ok(())
    }

    ///
    /// Initialize a new InMemoryLayer for, by copying the state at the given
    /// point in time from given existing layer.
    ///
    pub fn copy_snapshot(
        conf: &'static PageServerConf,
        timeline: &LayeredTimeline,
        src: &dyn Layer,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        start_lsn: Lsn,
        oldest_pending_lsn: Lsn,
    ) -> Result<InMemoryLayer> {
        let mut mem_used = 0;

        let seg = src.get_seg_tag();

        let startblk;
        let size;
        if seg.rel.is_blocky() {
            size = src.get_seg_size(start_lsn)?;
            startblk = seg.segno * RELISH_SEG_SIZE;
        } else {
            size = 1;
            startblk = 0;
        }

        trace!(
            "initializing new InMemoryLayer for writing {} on timeline {} at {}, size {}",
            src.get_seg_tag(),
            timelineid,
            start_lsn,
            size,
        );

        let mut base_images: Vec<Bytes> = Vec::new();
        for blknum in startblk..(startblk+size) {
            let img = timeline.materialize_page(seg, blknum, start_lsn, src)?;

            mem_used += img.len();

            base_images.push(img);
        }

        Ok(InMemoryLayer {
            conf,
            timelineid,
            tenantid,
            seg: src.get_seg_tag(),
            start_lsn,
            oldest_pending_lsn,
            inner: Mutex::new(InMemoryLayerInner {
                drop_lsn: None,
                base_images: base_images,
                page_versions: BTreeMap::new(),
                segsizes: BTreeMap::new(),
                mem_used: mem_used,
            }),
        })
    }

    ///
    /// Write the this in-memory layer to disk, as a snapshot layer.
    ///
    /// The cutoff point for the layer that's written to disk is 'end_lsn'.
    ///
    /// Returns new layers that replace this one. Always returns a
    /// SnapshotLayer containing the page versions that were written to disk,
    /// but if there were page versions newer than 'end_lsn', also return a new
    /// in-memory layer containing those page versions. The caller replaces
    /// this layer with the returned layers in the layer map.
    ///
    pub fn freeze(
        &self,
        cutoff_lsn: Lsn,
        // This is needed just to call materialize_page()
        timeline: &LayeredTimeline,
    ) -> Result<(Option<Arc<SnapshotLayer>>, Option<Arc<InMemoryLayer>>)> {
        info!(
            "freezing in memory layer for {} on timeline {} at {}",
            self.seg, self.timelineid, cutoff_lsn
        );

        let inner = self.inner.lock().unwrap();

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
        let before_base_images = inner.base_images.clone();
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
                if *lsn > end_lsn {
                    after_page_versions.insert((*blknum, *lsn), pv.clone());
                } else {
                    before_page_versions.insert((*blknum, *lsn), pv.clone());
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

        // Write the page versions before the cutoff to disk.
        let snapfile = SnapshotLayer::create(
            self.conf,
            self.timelineid,
            self.tenantid,
            self.seg,
            self.start_lsn,
            end_lsn,
            dropped,
            before_base_images,
            before_page_versions,
            before_segsizes,
        )?;

        // If there were any "new" page versions, initialize a new in-memory layer to hold
        // them
        let new_open =
            if !after_segsizes.is_empty() || !after_page_versions.is_empty() {
                info!("created new in-mem layer for {} {}-", self.seg, end_lsn);

                let new_open = Self::copy_snapshot(
                    self.conf,
                    timeline,
                    &snapfile,
                    self.timelineid,
                    self.tenantid,
                    end_lsn,
                    end_lsn,
                )?;
                let mut new_inner = new_open.inner.lock().unwrap();
                new_inner.page_versions.append(&mut after_page_versions);
                new_inner.segsizes.append(&mut after_segsizes);
                drop(new_inner);

                Some(Arc::new(new_open))
            } else {
                None
            };

        let new_historic = Some(Arc::new(snapfile));

        Ok((new_historic, new_open))
    }

    /// debugging function to print out the contents of the layer
    #[allow(unused)]
    pub fn dump(&self) -> String {
        let mut result = format!(
            "----- inmemory layer for {} {}-> ----\n",
            self.seg, self.start_lsn
        );

        let inner = self.inner.lock().unwrap();

        for (k, v) in inner.segsizes.iter() {
            result += &format!("{}: {}\n", k, v);
        }
        for (k, v) in inner.page_versions.iter() {
            result += &format!(
                "blk {} at {}: {}/{}\n",
                k.0,
                k.1,
                v.page_image.is_some(),
                v.record.is_some()
            );
        }

        result
    }
}
