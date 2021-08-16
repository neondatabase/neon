//!
//! A SnapshotLayer represents one snapshot file on disk. One file holds all page
//! version and size information of one relation, in a range of LSN.
//! The name "snapshot file" is a bit of a misnomer because a snapshot file doesn't
//! contain a snapshot at a specific LSN, but rather all the page versions in a range
//! of LSNs.
//!
//! Currently, a snapshot file contains full information needed to reconstruct any
//! page version in the LSN range, without consulting any other snapshot files. When
//! a new snapshot file is created for writing, the full contents of relation are
//! materialized as it is at the beginning of the LSN range. That can be very expensive,
//! we should find a way to store differential files. But this keeps the read-side
//! of things simple. You can find the correct snapshot file based on RelishTag and
//! timeline+LSN, and once you've located it, you have all the data you need to in that
//! file.
//!
//! When a snapshot file needs to be accessed, we slurp the whole file into memory, into
//! the SnapshotLayer struct. See load() and unload() functions.
//!
//! On disk, the snapshot files are stored in timelines/<timelineid> directory.
//! Currently, there are no subdirectories, and each snapshot file is named like this:
//!
//!    <spcnode>_<dbnode>_<relnode>_<forknum>_<start LSN>_<end LSN>
//!
//! For example:
//!
//!    1663_13990_2609_0_000000000169C348_000000000169C349
//!
//! If a relation is dropped, we add a '_DROPPED' to the end of the filename to indicate that.
//! So the above example would become:
//!
//!    1663_13990_2609_0_000000000169C348_000000000169C349_DROPPED
//!
//! The end LSN indicates when it was dropped in that case, we don't store it in the
//! file contents in any way.
//!
//! A snapshot file is constructed using the 'bookfile' crate. Each file consists of two
//! parts: the page versions and the relation sizes. They are stored as separate chapters.
//! FIXME
//!
use crate::layered_repository::storage_layer::{
    Layer, PageReconstructData, PageVersion, SegmentTag,
};
use crate::layered_repository::filename::{SnapshotFileName};
use crate::layered_repository::RELISH_SEG_SIZE;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, Result};
use bytes::Bytes;
use log::*;
use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::ops::Bound::Included;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

use bookfile::{Book, BookWriter};

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

// Magic constant to identify a Zenith snapshot file
static SNAPSHOT_FILE_MAGIC: u32 = 0x5A616E01;

static BASE_IMAGES_CHAPTER: u64 = 1;
static PAGE_VERSIONS_CHAPTER: u64 = 2;
static REL_SIZES_CHAPTER: u64 = 3;

///
/// SnapshotLayer is the in-memory data structure associated with an
/// on-disk snapshot file.  We keep a SnapshotLayer in memory for each
/// file, in the LayerMap. If a layer is in "loaded" state, we have a
/// copy of the file in memory, in 'inner'. Otherwise the struct is
/// just a placeholder for a file that exists on disk, and it needs to
/// be loaded before using it in queries.
///
pub struct SnapshotLayer {
    conf: &'static PageServerConf,
    pub tenantid: ZTenantId,
    pub timelineid: ZTimelineId,
    pub seg: SegmentTag,

    //
    // This entry contains all the changes from 'start_lsn' to 'end_lsn'. The
    // start is inclusive, and end is exclusive.
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,

    dropped: bool,

    inner: Mutex<SnapshotLayerInner>,
}

pub struct SnapshotLayerInner {
    /// If false, the 'page_versions' and 'relsizes' have not been
    /// loaded into memory yet.
    loaded: bool,

    // indexed by block number (within segment)
    base_images: Vec<Bytes>,

    /// All versions of all pages in the file are are kept here.
    /// Indexed by block number and LSN.
    page_versions: BTreeMap<(u32, Lsn), PageVersion>,

    /// `relsizes` tracks the size of the relation at different points in time.
    relsizes: BTreeMap<Lsn, u32>,
}

impl Layer for SnapshotLayer {
    fn get_timeline_id(&self) -> ZTimelineId {
        return self.timelineid;
    }

    fn get_seg_tag(&self) -> SegmentTag {
        return self.seg;
    }

    fn is_dropped(&self) -> bool {
        return self.dropped;
    }

    fn get_start_lsn(&self) -> Lsn {
        return self.start_lsn;
    }

    fn get_end_lsn(&self) -> Lsn {
        return self.end_lsn;
    }

    /// Look up given page in the cache.
    fn get_page_reconstruct_data(
        &self,
        blknum: u32,
        lsn: Lsn,
        reconstruct_data: &mut PageReconstructData,
    ) -> Result<Option<Lsn>> {
        // Scan the BTreeMap backwards, starting from the given entry.
        let mut need_base_image_lsn: Option<Lsn> = Some(lsn);
        {
            let inner = self.load()?;
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
                    bail!("no base img found for {} at blk {} at LSN {}", self.seg, base_blknum, lsn);
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
        let inner = self.load()?;
        let mut iter = inner.relsizes.range((Included(&Lsn(0)), Included(&lsn)));

        let result;
        if let Some((_entry_lsn, entry)) = iter.next_back() {
            result = *entry;
        } else {
            result = inner.base_images.len() as u32;
        }
        info!("get_seg_size: {} at {} -> {}", self.seg, lsn, result);
        Ok(result)
    }

    /// Does this segment exist at given LSN?
    fn get_seg_exists(&self, lsn: Lsn) -> Result<bool> {
        // Is the requested LSN after the rel was dropped?
        if self.dropped && lsn >= self.end_lsn {
            return Ok(false);
        }

        // Otherwise, it exists.
        Ok(true)
    }
}

impl SnapshotLayer {
    fn path(&self) -> PathBuf {
        Self::path_for(
            self.conf,
            self.timelineid,
            self.tenantid,
            &SnapshotFileName {
                seg: self.seg,
                start_lsn: self.start_lsn,
                end_lsn: self.end_lsn,
                dropped: self.dropped,
            },
        )
    }

    fn path_for(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        fname: &SnapshotFileName,
    ) -> PathBuf {
        conf.timeline_path(&timelineid, &tenantid)
            .join(fname.to_string())
    }

    /// Create a new snapshot file, using the given btreemaps containing the page versions and
    /// relsizes.
    ///
    /// This is used to write the in-memory layer to disk. The in-memory layer uses the same
    /// data structure with two btreemaps as we do, so passing the btreemaps is currently
    /// expedient.
    pub fn create(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        seg: SegmentTag,
        start_lsn: Lsn,
        end_lsn: Lsn,
        dropped: bool,
        base_images: Vec<Bytes>,
        page_versions: BTreeMap<(u32, Lsn), PageVersion>,
        relsizes: BTreeMap<Lsn, u32>,
    ) -> Result<SnapshotLayer> {

        let snapfile = SnapshotLayer {
            conf: conf,
            timelineid: timelineid,
            tenantid: tenantid,
            seg: seg,
            start_lsn: start_lsn,
            end_lsn,
            dropped,
            inner: Mutex::new(SnapshotLayerInner {
                loaded: true,
                base_images: base_images,
                page_versions: page_versions,
                relsizes: relsizes,
            }),
        };
        let inner = snapfile.inner.lock().unwrap();

        // Write the in-memory btreemaps into a file
        let path = snapfile.path();

        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let file = File::create(&path)?;
        let book = BookWriter::new(file, SNAPSHOT_FILE_MAGIC)?;

        // Write out the base images
        let mut chapter = book.new_chapter(BASE_IMAGES_CHAPTER);
        let buf = Vec::ser(&inner.base_images)?;

        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        // Write out the other page versions
        let mut chapter = book.new_chapter(PAGE_VERSIONS_CHAPTER);
        let buf = BTreeMap::ser(&inner.page_versions)?;
        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        // and relsizes to separate chapter
        let mut chapter = book.new_chapter(REL_SIZES_CHAPTER);
        let buf = BTreeMap::ser(&inner.relsizes)?;
        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        book.close()?;

        trace!("saved {}", &path.display());

        drop(inner);

        Ok(snapfile)
    }

    ///
    /// Load the contents of the file into memory
    ///
    fn load(&self) -> Result<MutexGuard<SnapshotLayerInner>> {
        // quick exit if already loaded
        let mut inner = self.inner.lock().unwrap();

        if inner.loaded {
            return Ok(inner);
        }

        let path = Self::path_for(
            self.conf,
            self.timelineid,
            self.tenantid,
            &SnapshotFileName {
                seg: self.seg,
                start_lsn: self.start_lsn,
                end_lsn: self.end_lsn,
                dropped: self.dropped,
            },
        );

        let file = File::open(&path)?;
        let book = Book::new(file)?;

        let chapter = book.read_chapter(BASE_IMAGES_CHAPTER)?;
        let base_images = Vec::des(&chapter)?;

        let chapter = book.read_chapter(PAGE_VERSIONS_CHAPTER)?;
        let page_versions = BTreeMap::des(&chapter)?;

        let chapter = book.read_chapter(REL_SIZES_CHAPTER)?;
        let relsizes = BTreeMap::des(&chapter)?;

        debug!("loaded from {}", &path.display());

        *inner = SnapshotLayerInner {
            loaded: true,
            base_images,
            page_versions,
            relsizes,
        };

        Ok(inner)
    }

    /// Create SnapshotLayers representing all files on disk
    ///
    // TODO: returning an Iterator would be more idiomatic
    pub fn load_snapshot_layer(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        filename: &SnapshotFileName,
    ) -> Result<SnapshotLayer> {
        let snapfile = SnapshotLayer {
            conf,
            timelineid,
            tenantid,
            seg: filename.seg,
            start_lsn: filename.start_lsn,
            end_lsn: filename.end_lsn,
            dropped: filename.dropped,
            inner: Mutex::new(SnapshotLayerInner {
                loaded: false,
                base_images: Vec::new(),
                page_versions: BTreeMap::new(),
                relsizes: BTreeMap::new(),
            }),
        };

        Ok(snapfile)
    }

    pub fn delete(&self) -> Result<()> {
        // delete underlying file
        fs::remove_file(self.path())?;
        Ok(())
    }

    ///
    /// Release most of the memory used by this layer. If it's accessed again later,
    /// it will need to be loaded back.
    ///
    pub fn unload(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.base_images = Vec::new();
        inner.page_versions = BTreeMap::new();
        inner.relsizes = BTreeMap::new();
        inner.loaded = false;
        Ok(())
    }

    /// debugging function to print out the contents of the layer
    #[allow(unused)]
    pub fn dump(&self) -> String {
        let mut result = format!(
            "----- snapshot layer for {} {}-{} ----\n",
            self.seg, self.start_lsn, self.end_lsn
        );

        let inner = self.inner.lock().unwrap();
        for (k, v) in inner.relsizes.iter() {
            result += &format!("{}: {}\n", k, v);
        }
        //for (k, v) in inner.page_versions.iter() {
        //    result += &format!("blk {} at {}: {}/{}\n", k.0, k.1, v.page_image.is_some(), v.record.is_some());
        //}

        result
    }
}
