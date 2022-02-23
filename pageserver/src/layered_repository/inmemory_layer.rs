//! An in-memory layer stores recently received PageVersions.
//!
//! The key space of all relishes is split into multiple Segments. As of this
//! writing, Delta- and ImageLayers contain data about a single Segment, whereas
//! an InMemoryLayer contains data about *all* modified segments.
//!
//! Before InMemoryLayer can store any information about a segment, whether that's
//! a page modification or the creation, truncation or dropping of a segment,
//! the segment must first be registered by a call to 'register_seg'. After that, the
//! segment is said to be "covered" by the layer. For every covered segment, we store
//! the old size of the segment at the beginning of the layer's LSN range, and all
//! modifications to the layer in the LSN range. If a segment is not covered by the
//! layer, there has been no modifications to it in the layer's LSN range, and you
//! should check the predecessor layer. You can check if a segment is covered by
//! calling 'covers_seg'.
//!
//! The "in-memory" part of the name is a bit misleading: the actual page versions are
//! held in an ephemeral file, not in memory. The metadata for each page version, i.e.
//! its position in the file, is kept in memory, though.
//!
use crate::config::PageServerConf;
use crate::layered_repository::delta_layer::{DeltaLayer, DeltaLayerWriter};
use crate::layered_repository::ephemeral_file::EphemeralFile;
use crate::layered_repository::image_layer::{ImageLayer, ImageLayerWriter};
use crate::layered_repository::storage_layer::{
    Layer, PageReconstructData, PageReconstructResult, PageVersion, SegmentBlk, SegmentRange,
    SegmentTag, ALL_SEG_RANGE, RELISH_SEG_SIZE,
};
use crate::layered_repository::LayeredTimeline;
use crate::{ZTenantId, ZTimelineId};
use anyhow::Result;
use log::*;
use std::collections::{HashMap, HashSet};
use std::io::Seek;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::RwLock;
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;
use zenith_utils::vec_map::VecMap;

pub struct InMemoryLayer {
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    timelineid: ZTimelineId,

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
}

pub struct InMemoryLayerInner {
    /// Frozen layers have an exclusive end LSN.
    /// Writes are only allowed when this is None
    end_lsn: Option<Lsn>,

    /// Segments covered by this layer
    segs: HashMap<SegmentTag, PerSeg>,

    /// The PageVersion structs are stored in a serialized format in this file.
    /// Each serialized PageVersion is preceded by a 'u32' length field.
    /// PerSeg::page_versions map stores offsets into this file.
    file: EphemeralFile,
}

///
/// 'PerSeg' holds information about one segment that the in-memory
/// layer covers.
///
#[derive(Default)]
struct PerSeg {
    ///
    /// All versions of all pages in the layer are kept here.  Indexed
    /// by block number and LSN. The value is an offset into the
    /// ephemeral file where the page version is stored.
    ///
    page_versions: HashMap<SegmentBlk, VecMap<Lsn, u64>>,

    /// Stores the old size of the segment, at 'start_lsn', or None if
    /// it did not exist at that point, i.e. it was created later. (We
    /// carry over the old size for convenience; you could dig into
    /// the predecessor layer for it, too.)
    old_size: Option<SegmentBlk>,

    ///
    /// `seg_sizes` tracks the size changes of the segment that's
    /// covered in this layer at different points in time.
    ///
    seg_sizes: VecMap<Lsn, SegSizeEntry>,
}

///
/// Whenever the size of a segment changes, we store a SegSizeEntry to record
/// the change in the 'seg_sizes' list.
///
#[derive(Copy, Clone, Debug)]
enum SegSizeEntry {
    Create(SegmentBlk),
    Size(SegmentBlk),
    Drop,
}

impl InMemoryLayerInner {
    fn assert_writeable(&self) {
        assert!(self.end_lsn.is_none());
    }

    fn get_perseg_mut(&mut self, seg: &SegmentTag) -> &mut PerSeg {
        self.segs
            .get_mut(seg)
            .unwrap_or_else(|| panic!("segment {} is not covered by the in-memory layer", seg))
    }

    fn get_seg_size(&self, seg: SegmentTag, lsn: Lsn) -> Option<SegSizeEntry> {
        // Scan the VecMap backwards, starting from the given entry.
        let perseg = self.segs.get(&seg)?;
        let slice = perseg.seg_sizes.slice_range(..=lsn);

        // We make sure there is always at least one entry
        if let Some((_entry_lsn, entry)) = slice.last() {
            Some(*entry)
        } else {
            None
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

    fn append_size(&mut self, seg: SegmentTag, lsn: Lsn, size_entry: SegSizeEntry) {
        let perseg = self.get_perseg_mut(&seg);
        let old = perseg
            .seg_sizes
            .append_or_update_last(lsn, size_entry)
            .unwrap()
            .0;
        if old.is_some() {
            // We already had an entry for this LSN. That's normal currently:
            // if one WAL record extends the relation by two blocks, append_size
            // gets called twice. Also happens during bootstrapping, when we load
            // the whole freshly initdb'd cluster into the repository with a single
            // LSN. FIXME: would be nice to change the repository API to make that
            // more explicit, so that we could enable this warning.
            //warn!("Inserting seg size, but had an entry for the same LSN already");
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
        if let Some(freeze_lsn) = inner.end_lsn {
            end_lsn = freeze_lsn;
        } else {
            end_lsn = Lsn(u64::MAX);
        }

        PathBuf::from(format!(
            "inmem-{:016X}-{:016X}",
            self.start_lsn.0, end_lsn.0
        ))
    }

    fn get_tenant_id(&self) -> ZTenantId {
        self.tenantid
    }

    fn get_timeline_id(&self) -> ZTimelineId {
        self.timelineid
    }

    fn get_seg_range(&self) -> SegmentRange {
        ALL_SEG_RANGE
    }

    fn covers_seg(&self, seg: SegmentTag) -> bool {
        let inner = self.inner.read().unwrap();

        inner.segs.get(&seg).is_some()
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

    /// Look up given page in the cache.
    fn get_page_reconstruct_data(
        &self,
        seg: SegmentTag,
        blknum: SegmentBlk,
        lsn: Lsn,
        reconstruct_data: &mut PageReconstructData,
    ) -> Result<PageReconstructResult> {
        let mut need_image = true;

        assert!((0..RELISH_SEG_SIZE).contains(&blknum));

        {
            let inner = self.inner.read().unwrap();

            // Scan the page versions backwards, starting from `lsn`.
            let perseg = inner.segs.get(&seg).unwrap();
            if let Some(vec_map) = perseg.page_versions.get(&blknum) {
                let slice = vec_map.slice_range(..=lsn);
                for (entry_lsn, pos) in slice.iter().rev() {
                    match &reconstruct_data.page_img {
                        Some((cached_lsn, _)) if entry_lsn <= cached_lsn => {
                            return Ok(PageReconstructResult::Complete)
                        }
                        _ => {}
                    }

                    let pv = inner.read_pv(*pos)?;
                    match pv {
                        PageVersion::Page(img) => {
                            reconstruct_data.page_img = Some((*entry_lsn, img));
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
            //
            // FIXME: unwrap_or(0) is a bit iffy here. If we have a 'perseg' entry,
            // we should always have a size
            if need_image
                && reconstruct_data.records.is_empty()
                && blknum >= self.get_seg_size(seg, lsn)?.unwrap_or(0)
            {
                return Ok(PageReconstructResult::Missing(self.start_lsn));
            }

            // release lock on 'inner'
        }

        // If an older page image is needed to reconstruct the page, let the
        // caller know
        if need_image {
            Ok(PageReconstructResult::Continue(Lsn(self.start_lsn.0 - 1)))
        } else {
            Ok(PageReconstructResult::Complete)
        }
    }

    /// Get size of the relation at given LSN
    fn get_seg_size(&self, seg: SegmentTag, lsn: Lsn) -> Result<Option<SegmentBlk>> {
        assert!(lsn >= self.start_lsn);

        let inner = self.inner.read().unwrap();

        match inner.get_seg_size(seg, lsn) {
            None => {
                // Does not exist. Maybe it was created later?
                if let Some(perseg) = inner.segs.get(&seg) {
                    Ok(perseg.old_size)
                } else {
                    assert!(self.covers_seg(seg));
                    Ok(None)
                }
            }
            Some(SegSizeEntry::Create(size)) => Ok(Some(size)),
            Some(SegSizeEntry::Size(size)) => Ok(Some(size)),
            Some(SegSizeEntry::Drop) => Ok(None),
        }
    }

    /// Does this segment exist at given LSN?
    fn get_seg_exists(&self, seg: SegmentTag, lsn: Lsn) -> Result<bool> {
        assert!(lsn >= self.start_lsn);

        let inner = self.inner.read().unwrap();

        match inner.get_seg_size(seg, lsn) {
            None => {
                // Does not exist. Maybe it was created later?
                if let Some(perseg) = inner.segs.get(&seg) {
                    Ok(perseg.old_size.is_some())
                } else {
                    assert!(self.covers_seg(seg));
                    Ok(false)
                }
            }
            Some(SegSizeEntry::Create(_size)) => Ok(true),
            Some(SegSizeEntry::Size(_size)) => Ok(true),
            Some(SegSizeEntry::Drop) => Ok(false),
        }
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
        // in-memory layer is always considered incremental.
        true
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
            "----- in-memory layer for tli {} LSNs {}-{} ----",
            self.timelineid,
            self.start_lsn,
            end_str,
            //inner.dropped,
        );

        for (seg, perseg) in inner.segs.iter() {
            println!("--- segment {} old size {:?} ---", seg, perseg.old_size);

            for (lsn, size_entry) in perseg.seg_sizes.as_slice() {
                println!("  {}: {:?}", lsn, size_entry);
            }

            let mut page_versions: Vec<(&SegmentBlk, &VecMap<Lsn, u64>)> =
                perseg.page_versions.iter().collect();
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

    ///
    /// Create a new, empty, in-memory layer
    ///
    pub fn create(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        start_lsn: Lsn,
        oldest_lsn: Lsn,
    ) -> Result<InMemoryLayer> {
        trace!(
            "initializing new empty InMemoryLayer for writing on timeline {} at {}",
            timelineid,
            start_lsn
        );

        let file = EphemeralFile::create(conf, tenantid, timelineid)?;

        Ok(InMemoryLayer {
            conf,
            timelineid,
            tenantid,
            start_lsn,
            oldest_lsn,
            inner: RwLock::new(InMemoryLayerInner {
                end_lsn: None,
                segs: HashMap::new(),
                file,
            }),
        })
    }

    // Register a segment for modifications.
    //
    // 'size' is the size of the segment at 'start_lsn', or None if it did not exist.
    pub fn register_seg(&self, seg: SegmentTag, size: Option<SegmentBlk>) {
        let mut inner = self.inner.write().unwrap();

        inner.assert_writeable();

        let perseg = PerSeg {
            old_size: size,
            ..Default::default()
        };

        let old_perseg = inner.segs.insert(seg, perseg);
        assert!(
            old_perseg.is_none(),
            "register_seg called on a segment that is already covered by the layer"
        );
    }

    // Write operations

    /// Common subroutine of the public put_wal_record() and put_page_image() functions.
    /// Adds the page version to the in-memory tree
    pub fn put_page_version(
        &self,
        seg: SegmentTag,
        blknum: SegmentBlk,
        lsn: Lsn,
        pv: PageVersion,
    ) -> Result<()> {
        assert!((0..RELISH_SEG_SIZE).contains(&blknum));

        assert!(self.covers_seg(seg));

        trace!(
            "put_page_version blk {} of {} at {}/{}",
            blknum,
            seg.rel,
            self.timelineid,
            lsn
        );
        let mut inner = self.inner.write().unwrap();

        inner.assert_writeable();

        let off = inner.write_pv(&pv)?;

        let perseg = inner.get_perseg_mut(&seg);

        // Check that we have a size for it already
        assert!(perseg.old_size.is_some() || !perseg.seg_sizes.is_empty());

        let vec_map = perseg.page_versions.entry(blknum).or_default();
        let old = vec_map.append_or_update_last(lsn, off).unwrap().0;
        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!(
                "Page version of rel {} blk {} at {} already exists",
                seg.rel, blknum, lsn
            );
        }

        Ok(())
    }

    /// Remember that the segment was truncated at given LSN
    pub fn put_seg_size(&self, seg: SegmentTag, lsn: Lsn, new_size: SegmentBlk) {
        assert!(self.covers_seg(seg));
        let mut inner = self.inner.write().unwrap();
        inner.assert_writeable();

        // Check that we have a size for it already
        if lsn > self.start_lsn {
            let perseg = inner.get_perseg_mut(&seg);
            assert!(perseg.old_size.is_some() || !perseg.seg_sizes.is_empty());
        }

        inner.append_size(seg, lsn, SegSizeEntry::Size(new_size));
    }

    /// Remember that the segment was created at given LSN
    pub fn put_creation(&self, seg: SegmentTag, lsn: Lsn, size: SegmentBlk) {
        assert!(self.covers_seg(seg));
        let mut inner = self.inner.write().unwrap();
        inner.assert_writeable();

        inner.append_size(seg, lsn, SegSizeEntry::Create(size));
    }

    /// Remember that the segment was dropped at given LSN
    pub fn drop_segment(&self, seg: SegmentTag, lsn: Lsn) {
        assert!(self.covers_seg(seg));
        let mut inner = self.inner.write().unwrap();

        // Check that we have a size for it already
        if lsn > self.start_lsn {
            let perseg = inner.get_perseg_mut(&seg);
            assert!(perseg.old_size.is_some() || !perseg.seg_sizes.is_empty());
        }

        assert!(inner.end_lsn.is_none());
        assert!(self.start_lsn <= lsn);
        inner.append_size(seg, lsn, SegSizeEntry::Drop);
    }

    /// Make the layer non-writeable. Only call once.
    /// Records the end_lsn for non-dropped layers.
    /// `end_lsn` is exclusive
    pub fn freeze(&self, end_lsn: Lsn) {
        let mut inner = self.inner.write().unwrap();

        assert!(self.start_lsn < end_lsn);
        inner.end_lsn = Some(end_lsn);

        for perseg in inner.segs.values() {
            if let Some((lsn, _)) = perseg.seg_sizes.as_slice().last() {
                assert!(lsn < &end_lsn, "{:?} {:?}", lsn, end_lsn);
            }

            for (_blk, vec_map) in perseg.page_versions.iter() {
                for (lsn, _pos) in vec_map.as_slice() {
                    assert!(*lsn < end_lsn);
                }
            }
        }
    }

    pub fn list_covered_segs(&self) -> Result<HashSet<SegmentTag>> {
        // Collect list of distinct segments modified
        let inner = self.inner.read().unwrap();
        let mut segs: HashSet<SegmentTag> = HashSet::new();
        for seg in inner.segs.keys() {
            segs.insert(*seg);
        }
        Ok(segs)
    }

    /// Write this frozen in-memory layer to disk.
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
            "write_to_disk {} start {} get_end_lsn is {}",
            self.filename().display(),
            self.get_start_lsn(),
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

        let mut delta_layers = Vec::new();
        let mut image_layers = Vec::new();

        for (seg, perseg) in inner.segs.iter() {
            let mut seg_sizes: VecMap<Lsn, SegmentBlk> = VecMap::default();
            let mut last_size: Option<SegmentBlk> = None;
            let mut created_at: Option<Lsn> = None;
            let mut dropped_at: Option<Lsn> = None;

            // Since `end_lsn` is exclusive, subtract 1 to calculate the last LSN
            // that is included.
            let end_lsn_exclusive = inner.end_lsn.unwrap();
            let end_lsn_inclusive = Lsn(end_lsn_exclusive.0 - 1);

            if let Some(old_size) = perseg.old_size {
                seg_sizes.append(self.start_lsn, old_size).unwrap();
                last_size = Some(old_size);
            }

            for (lsn, size_entry) in perseg.seg_sizes.as_slice() {
                // FIXME: A segment could be dropped, and later recreated, within the same inmemory
                // layer. We don't handle that currently.
                assert!(*lsn < end_lsn_exclusive);
                match size_entry {
                    SegSizeEntry::Create(size) => {
                        seg_sizes.append(*lsn, *size).unwrap();
                        last_size = Some(*size);
                        created_at = Some(*lsn);
                    }
                    SegSizeEntry::Size(size) => {
                        seg_sizes.append(*lsn, *size).unwrap();
                        last_size = Some(*size);
                    }
                    SegSizeEntry::Drop => {
                        dropped_at = Some(*lsn);
                    }
                }
            }

            let start_lsn = created_at.unwrap_or(self.start_lsn);

            // Figure out if we should create a delta layer, image layer, or both.
            let image_lsn: Option<Lsn>;
            let delta_end_lsn: Option<Lsn>;
            if let Some(dropped_at) = dropped_at {
                // The segment was dropped. Create just a delta layer containing all the
                // changes up to and including the drop.
                delta_end_lsn = Some(Lsn(dropped_at.0));
                image_lsn = None;
            } else if !reconstruct_pages {
                // The caller requested to not reconstruct any pages. Just write out
                // all the data to a new delta layer.
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

            if let Some(delta_end_lsn) = delta_end_lsn {
                let mut delta_layer_writer = DeltaLayerWriter::new(
                    self.conf,
                    self.timelineid,
                    self.tenantid,
                    *seg,
                    start_lsn,
                    delta_end_lsn,
                    dropped_at.is_some(),
                )?;

                // Write all page versions
                let mut buf: Vec<u8> = Vec::new();

                let pv_iter = perseg.page_versions.iter();
                let mut sorted_pages: Vec<(&SegmentBlk, &VecMap<Lsn, u64>)> = pv_iter.collect();
                sorted_pages.sort_by_key(|(blknum, _vec_map)| *blknum);
                for (blknum, vec_map) in sorted_pages {
                    for (lsn, pos) in vec_map.as_slice() {
                        if *lsn < delta_end_lsn {
                            let len = inner.read_pv_bytes(*pos, &mut buf)?;
                            delta_layer_writer.put_page_version(*blknum, *lsn, &buf[..len])?;
                        }
                    }
                }

                let delta_layer = delta_layer_writer.finish(seg_sizes)?;
                delta_layers.push(delta_layer);
            }

            // Write a new base image layer at the cutoff point
            if let Some(image_lsn) = image_lsn {
                let size = last_size.unwrap();

                let mut image_layer_writer = ImageLayerWriter::new(
                    self.conf,
                    self.timelineid,
                    self.tenantid,
                    *seg,
                    image_lsn,
                    size,
                )?;

                for blknum in 0..size {
                    let img = timeline.materialize_page(*seg, blknum, image_lsn, &*self)?;

                    image_layer_writer.put_page_image(&img)?;
                }
                let image_layer = image_layer_writer.finish()?;
                image_layers.push(image_layer);
            }
        }

        Ok(LayersOnDisk {
            delta_layers,
            image_layers,
        })
    }
}
