//! An in-memory layer stores recently received PageVersions.
//! The page versions are held in a BTreeMap. To avoid OOM errors, the map size is limited
//! and layers can be spilled to disk into ephemeral files.
//!
//! And there's another BTreeMap to track the size of the relation.
//!
use crate::config::PageServerConf;
use crate::layered_repository::delta_layer::{DeltaLayer, DeltaLayerWriter};
use crate::layered_repository::ephemeral_file::EphemeralFile;
use crate::layered_repository::filename::DeltaFileName;
use crate::layered_repository::image_layer::{ImageLayer, ImageLayerWriter};
use crate::layered_repository::storage_layer::{
    Layer, PageReconstructData, PageReconstructResult, PageVersion, SegmentBlk, SegmentTag,
    RELISH_SEG_SIZE,
};
use crate::layered_repository::LayeredTimeline;
use crate::layered_repository::ZERO_PAGE;
use crate::repository::ZenithWalRecord;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{ensure, Result};
use bytes::Bytes;
use log::*;
use std::collections::HashMap;
use std::io::Seek;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;
use zenith_utils::vec_map::VecMap;

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

    ///
    /// LSN of the oldest page version stored in this layer.
    ///
    /// This is different from 'start_lsn' in that we enforce that the 'start_lsn'
    /// of a layer always matches the 'end_lsn' of its predecessor, even if there
    /// are no page versions until at a later LSN. That way you can detect any
    /// missing layer files more easily. 'oldest_lsn' is the first page version
    /// actually stored in this layer. In the range between 'start_lsn' and
    /// 'oldest_lsn', there are no changes to the segment.
    /// 'oldest_lsn' is used to adjust 'disk_consistent_lsn' and that is why it should
    /// point to the beginning of WAL record. This is the other difference with 'start_lsn'
    /// which points to end of WAL record. This is why 'oldest_lsn' can be smaller than 'start_lsn'.
    ///
    oldest_lsn: Lsn,

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

    /// The PageVersion structs are stored in a serialized format in this file.
    /// Each serialized PageVersion is preceded by a 'u32' length field.
    /// 'page_versions' map stores offsets into this file.
    file: EphemeralFile,

    /// Metadata about all versions of all pages in the layer is kept
    /// here.  Indexed by block number and LSN. The value is an offset
    /// into the ephemeral file where the page version is stored.
    page_versions: HashMap<SegmentBlk, VecMap<Lsn, u64>>,

    ///
    /// `seg_sizes` tracks the size of the segment at different points in time.
    ///
    /// For a blocky rel, there is always one entry, at the layer's start_lsn,
    /// so that determining the size never depends on the predecessor layer. For
    /// a non-blocky rel, 'seg_sizes' is not used and is always empty.
    ///
    seg_sizes: VecMap<Lsn, SegmentBlk>,

    ///
    /// LSN of the newest page version stored in this layer.
    ///
    /// The difference between 'end_lsn' and 'latest_lsn' is the same as between
    /// 'start_lsn' and 'oldest_lsn'. See comments in 'oldest_lsn'.
    ///
    latest_lsn: Lsn,
}

impl InMemoryLayerInner {
    fn assert_writeable(&self) {
        assert!(self.end_lsn.is_none());
    }

    fn get_seg_size(&self, lsn: Lsn) -> SegmentBlk {
        // Scan the BTreeMap backwards, starting from the given entry.
        let slice = self.seg_sizes.slice_range(..=lsn);

        // We make sure there is always at least one entry
        if let Some((_entry_lsn, entry)) = slice.last() {
            *entry
        } else {
            panic!("could not find seg size in in-memory layer");
        }
    }

    ///
    /// Read a page version from the ephemeral file.
    ///
    fn read_pv(&self, off: u64) -> Result<PageVersion> {
        let mut buf = Vec::new();
        self.read_pv_bytes(off, &mut buf)?;
        Ok(PageVersion::des(&buf)?)
    }

    ///
    /// Read a page version from the ephemeral file, as raw bytes, at
    /// the given offset.  The bytes are read into 'buf', which is
    /// expanded if necessary. Returns the size of the page version.
    ///
    fn read_pv_bytes(&self, off: u64, buf: &mut Vec<u8>) -> Result<usize> {
        // read length
        let mut lenbuf = [0u8; 4];
        self.file.read_exact_at(&mut lenbuf, off)?;
        let len = u32::from_ne_bytes(lenbuf) as usize;

        if buf.len() < len {
            buf.resize(len, 0);
        }
        self.file.read_exact_at(&mut buf[0..len], off + 4)?;
        Ok(len)
    }

    fn write_pv(&mut self, pv: &PageVersion) -> Result<u64> {
        // remember starting position
        let pos = self.file.stream_position()?;

        // make room for the 'length' field by writing zeros as a placeholder.
        self.file.seek(std::io::SeekFrom::Start(pos + 4)).unwrap();

        pv.ser_into(&mut self.file).unwrap();

        // write the 'length' field.
        let len = self.file.stream_position()? - pos - 4;
        let lenbuf = u32::to_ne_bytes(len as u32);
        self.file.write_all_at(&lenbuf, pos)?;

        Ok(pos)
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
            seg: self.seg,
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
        blknum: SegmentBlk,
        lsn: Lsn,
        cached_img_lsn: Option<Lsn>,
        reconstruct_data: &mut PageReconstructData,
    ) -> Result<PageReconstructResult> {
        let mut need_image = true;

        assert!((0..RELISH_SEG_SIZE).contains(&blknum));

        {
            let inner = self.inner.read().unwrap();

            // Scan the page versions backwards, starting from `lsn`.
            if let Some(vec_map) = inner.page_versions.get(&blknum) {
                let slice = vec_map.slice_range(..=lsn);
                for (entry_lsn, pos) in slice.iter().rev() {
                    match &cached_img_lsn {
                        Some(cached_lsn) if entry_lsn <= cached_lsn => {
                            return Ok(PageReconstructResult::Cached)
                        }
                        _ => {}
                    }

                    let pv = inner.read_pv(*pos)?;
                    match pv {
                        PageVersion::Page(img) => {
                            reconstruct_data.page_img = Some(img);
                            need_image = false;
                            break;
                        }
                        PageVersion::Wal(rec) => {
                            reconstruct_data.records.push((*entry_lsn, rec.clone()));
                            if rec.will_init() {
                                // This WAL record initializes the page, so no need to go further back
                                need_image = false;
                                break;
                            }
                        }
                    }
                }
            }

            // If we didn't find any records for this, check if the request is beyond EOF
            if need_image
                && reconstruct_data.records.is_empty()
                && self.seg.rel.is_blocky()
                && blknum >= self.get_seg_size(lsn)?
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
    fn get_seg_size(&self, lsn: Lsn) -> Result<SegmentBlk> {
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

        for (k, v) in inner.seg_sizes.as_slice() {
            println!("seg_sizes {}: {}", k, v);
        }

        // List the blocks in order
        let mut page_versions: Vec<(&SegmentBlk, &VecMap<Lsn, u64>)> =
            inner.page_versions.iter().collect();
        page_versions.sort_by_key(|k| k.0);

        for (blknum, versions) in page_versions {
            for (lsn, off) in versions.as_slice() {
                let pv = inner.read_pv(*off);
                let pv_description = match pv {
                    Ok(PageVersion::Page(_img)) => "page",
                    Ok(PageVersion::Wal(_rec)) => "wal",
                    Err(_err) => "INVALID",
                };

                println!("blk {} at {}: {}\n", blknum, lsn, pv_description);
            }
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
    pub fn get_oldest_lsn(&self) -> Lsn {
        self.oldest_lsn
    }

    pub fn get_latest_lsn(&self) -> Lsn {
        let inner = self.inner.read().unwrap();
        inner.latest_lsn
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
        oldest_lsn: Lsn,
    ) -> Result<InMemoryLayer> {
        trace!(
            "initializing new empty InMemoryLayer for writing {} on timeline {} at {}",
            seg,
            timelineid,
            start_lsn
        );

        // The segment is initially empty, so initialize 'seg_sizes' with 0.
        let mut seg_sizes = VecMap::default();
        if seg.rel.is_blocky() {
            seg_sizes.append(start_lsn, 0).unwrap();
        }

        let file = EphemeralFile::create(conf, tenantid, timelineid)?;

        Ok(InMemoryLayer {
            conf,
            timelineid,
            tenantid,
            seg,
            start_lsn,
            oldest_lsn,
            incremental: false,
            inner: RwLock::new(InMemoryLayerInner {
                end_lsn: None,
                dropped: false,
                file,
                page_versions: HashMap::new(),
                seg_sizes,
                latest_lsn: oldest_lsn,
            }),
        })
    }

    // Write operations

    /// Remember new page version, as a WAL record over previous version
    pub fn put_wal_record(
        &self,
        lsn: Lsn,
        blknum: SegmentBlk,
        rec: ZenithWalRecord,
    ) -> Result<u32> {
        self.put_page_version(blknum, lsn, PageVersion::Wal(rec))
    }

    /// Remember new page version, as a full page image
    pub fn put_page_image(&self, blknum: SegmentBlk, lsn: Lsn, img: Bytes) -> Result<u32> {
        self.put_page_version(blknum, lsn, PageVersion::Page(img))
    }

    /// Common subroutine of the public put_wal_record() and put_page_image() functions.
    /// Adds the page version to the in-memory tree
    pub fn put_page_version(&self, blknum: SegmentBlk, lsn: Lsn, pv: PageVersion) -> Result<u32> {
        assert!((0..RELISH_SEG_SIZE).contains(&blknum));

        trace!(
            "put_page_version blk {} of {} at {}/{}",
            blknum,
            self.seg.rel,
            self.timelineid,
            lsn
        );
        let mut inner = self.inner.write().unwrap();

        inner.assert_writeable();
        assert!(lsn >= inner.latest_lsn);
        inner.latest_lsn = lsn;

        // Write the page version to the file, and remember its offset in 'page_versions'
        {
            let off = inner.write_pv(&pv)?;
            let vec_map = inner.page_versions.entry(blknum).or_default();
            let old = vec_map.append_or_update_last(lsn, off).unwrap().0;
            if old.is_some() {
                // We already had an entry for this LSN. That's odd..
                warn!(
                    "Page version of rel {} blk {} at {} already exists",
                    self.seg.rel, blknum, lsn
                );
            }
        }

        // Also update the relation size, if this extended the relation.
        if self.seg.rel.is_blocky() {
            let newsize = blknum + 1;

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
                for gapblknum in oldsize..blknum {
                    let zeropv = PageVersion::Page(ZERO_PAGE.clone());
                    trace!(
                        "filling gap blk {} with zeros for write of {}",
                        gapblknum,
                        blknum
                    );

                    // Write the page version to the file, and remember its offset in
                    // 'page_versions'
                    {
                        let off = inner.write_pv(&zeropv)?;
                        let vec_map = inner.page_versions.entry(gapblknum).or_default();
                        let old = vec_map.append_or_update_last(lsn, off).unwrap().0;
                        if old.is_some() {
                            warn!(
                                "Page version of seg {} blk {} at {} already exists",
                                self.seg, gapblknum, lsn
                            );
                        }
                    }
                }

                inner.seg_sizes.append_or_update_last(lsn, newsize).unwrap();
                return Ok(newsize - oldsize);
            }
        }

        Ok(0)
    }

    /// Remember that the relation was truncated at given LSN
    pub fn put_truncation(&self, lsn: Lsn, new_size: SegmentBlk) {
        assert!(
            self.seg.rel.is_blocky(),
            "put_truncation() called on a non-blocky rel"
        );

        let mut inner = self.inner.write().unwrap();
        inner.assert_writeable();

        // check that this we truncate to a smaller size than segment was before the truncation
        let old_size = inner.get_seg_size(lsn);
        assert!(new_size < old_size);

        let (old, _delta_size) = inner
            .seg_sizes
            .append_or_update_last(lsn, new_size)
            .unwrap();

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
        oldest_lsn: Lsn,
    ) -> Result<InMemoryLayer> {
        let seg = src.get_seg_tag();

        assert!(oldest_lsn.is_aligned());

        trace!(
            "initializing new InMemoryLayer for writing {} on timeline {} at {}",
            seg,
            timelineid,
            start_lsn,
        );

        // Copy the segment size at the start LSN from the predecessor layer.
        let mut seg_sizes = VecMap::default();
        if seg.rel.is_blocky() {
            let size = src.get_seg_size(start_lsn)?;
            seg_sizes.append(start_lsn, size).unwrap();
        }

        let file = EphemeralFile::create(conf, tenantid, timelineid)?;

        Ok(InMemoryLayer {
            conf,
            timelineid,
            tenantid,
            seg,
            start_lsn,
            oldest_lsn,
            incremental: true,
            inner: RwLock::new(InMemoryLayerInner {
                end_lsn: None,
                dropped: false,
                file,
                page_versions: HashMap::new(),
                seg_sizes,
                latest_lsn: oldest_lsn,
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

            if let Some((lsn, _)) = inner.seg_sizes.as_slice().last() {
                assert!(lsn <= &end_lsn, "{:?} {:?}", lsn, end_lsn);
            }

            for (_blk, vec_map) in inner.page_versions.iter() {
                for (lsn, _pos) in vec_map.as_slice() {
                    assert!(*lsn <= end_lsn);
                }
            }
        }
    }

    /// Write the this frozen in-memory layer to disk.
    ///
    /// Returns new layers that replace this one.
    /// If not dropped and reconstruct_pages is true, returns a new image layer containing the page versions
    /// at the `end_lsn`. Can also return a DeltaLayer that includes all the
    /// WAL records between start and end LSN. (The delta layer is not needed
    /// when a new relish is created with a single LSN, so that the start and
    /// end LSN are the same.)
    pub fn write_to_disk(
        &self,
        timeline: &LayeredTimeline,
        reconstruct_pages: bool,
    ) -> Result<LayersOnDisk> {
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

        // Since `end_lsn` is exclusive, subtract 1 to calculate the last LSN
        // that is included.
        let end_lsn_exclusive = inner.end_lsn.unwrap();
        let end_lsn_inclusive = Lsn(end_lsn_exclusive.0 - 1);

        // Figure out if we should create a delta layer, image layer, or both.
        let image_lsn: Option<Lsn>;
        let delta_end_lsn: Option<Lsn>;
        if self.is_dropped() || !reconstruct_pages {
            // The segment was dropped. Create just a delta layer containing all the
            // changes up to and including the drop.
            delta_end_lsn = Some(end_lsn_exclusive);
            image_lsn = None;
        } else if self.start_lsn == end_lsn_inclusive {
            // The layer contains exactly one LSN. It's enough to write an image
            // layer at that LSN.
            delta_end_lsn = None;
            image_lsn = Some(end_lsn_inclusive);
        } else {
            // Create a delta layer with all the changes up to the end LSN,
            // and an image layer at the end LSN.
            //
            // Note that we the delta layer does *not* include the page versions
            // at the end LSN. They are included in the image layer, and there's
            // no need to store them twice.
            delta_end_lsn = Some(end_lsn_inclusive);
            image_lsn = Some(end_lsn_inclusive);
        }

        let mut delta_layers = Vec::new();
        let mut image_layers = Vec::new();

        if let Some(delta_end_lsn) = delta_end_lsn {
            let mut delta_layer_writer = DeltaLayerWriter::new(
                self.conf,
                self.timelineid,
                self.tenantid,
                self.seg,
                self.start_lsn,
                delta_end_lsn,
                self.is_dropped(),
            )?;

            // Write all page versions, in block + LSN order
            let mut buf: Vec<u8> = Vec::new();

            let pv_iter = inner.page_versions.iter();
            let mut pages: Vec<(&SegmentBlk, &VecMap<Lsn, u64>)> = pv_iter.collect();
            pages.sort_by_key(|(blknum, _vec_map)| *blknum);
            for (blknum, vec_map) in pages {
                for (lsn, pos) in vec_map.as_slice() {
                    if *lsn < delta_end_lsn {
                        let len = inner.read_pv_bytes(*pos, &mut buf)?;
                        delta_layer_writer.put_page_version(*blknum, *lsn, &buf[..len])?;
                    }
                }
            }

            // Create seg_sizes
            let seg_sizes = if delta_end_lsn == end_lsn_exclusive {
                inner.seg_sizes.clone()
            } else {
                inner.seg_sizes.split_at(&end_lsn_exclusive).0
            };

            let delta_layer = delta_layer_writer.finish(seg_sizes)?;
            delta_layers.push(delta_layer);
        }

        drop(inner);

        // Write a new base image layer at the cutoff point
        if let Some(image_lsn) = image_lsn {
            let size = if self.seg.rel.is_blocky() {
                self.get_seg_size(image_lsn)?
            } else {
                1
            };
            let mut image_layer_writer = ImageLayerWriter::new(
                self.conf,
                self.timelineid,
                self.tenantid,
                self.seg,
                image_lsn,
                size,
            )?;

            for blknum in 0..size {
                let img = timeline.materialize_page(self.seg, blknum, image_lsn, &*self)?;

                image_layer_writer.put_page_image(&img)?;
            }
            let image_layer = image_layer_writer.finish()?;
            image_layers.push(image_layer);
        }

        Ok(LayersOnDisk {
            delta_layers,
            image_layers,
        })
    }
}
