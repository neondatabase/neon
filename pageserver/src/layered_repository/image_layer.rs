//! FIXME
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
use crate::layered_repository::storage_layer::{Layer, PageReconstructData, SegmentTag};
use crate::layered_repository::LayeredTimeline;
use crate::layered_repository::filename::{ImageFileName};
use crate::layered_repository::RELISH_SEG_SIZE;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, Result};
use bytes::Bytes;
use lazy_static::lazy_static;
use log::*;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

use bookfile::{Book, BookWriter};

use zenith_metrics::{register_histogram, Histogram};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

// Magic constant to identify a Zenith segment image file
static IMAGE_FILE_MAGIC: u32 = 0x5A616E01 + 1;

static BASE_IMAGES_CHAPTER: u64 = 1;


// Metrics collected on operations on the storage repository.
lazy_static! {
    static ref RECONSTRUCT_TIME: Histogram = register_histogram!(
        "pageserver_image_reconstruct_time",
        "FIXME Time spent on storage operations"
    )
    .expect("failed to define a metric");
}

///
/// SnapshotLayer is the in-memory data structure associated with an
/// on-disk snapshot file.  We keep a SnapshotLayer in memory for each
/// file, in the LayerMap. If a layer is in "loaded" state, we have a
/// copy of the file in memory, in 'inner'. Otherwise the struct is
/// just a placeholder for a file that exists on disk, and it needs to
/// be loaded before using it in queries.
///
pub struct ImageLayer {
    conf: &'static PageServerConf,
    pub tenantid: ZTenantId,
    pub timelineid: ZTimelineId,
    pub seg: SegmentTag,

    // This entry contains an image of all pages as of this LSN
    pub lsn: Lsn,

    inner: Mutex<ImageLayerInner>,
}

pub struct ImageLayerInner {
    /// If false, the 'page_versions' and 'relsizes' have not been
    /// loaded into memory yet.
    loaded: bool,

    // indexed by block number (within segment)
    base_images: Vec<Bytes>,
}

impl Layer for ImageLayer {
    fn get_timeline_id(&self) -> ZTimelineId {
        return self.timelineid;
    }

    fn get_seg_tag(&self) -> SegmentTag {
        return self.seg;
    }

    fn is_dropped(&self) -> bool {
        return false;
    }

    fn get_start_lsn(&self) -> Lsn {
        return self.lsn;
    }

    fn get_end_lsn(&self) -> Lsn {
        return self.lsn;
    }

    /// Look up given page in the cache.
    fn get_page_reconstruct_data(
        &self,
        blknum: u32,
        lsn: Lsn,
        reconstruct_data: &mut PageReconstructData,
    ) -> Result<Option<Lsn>> {
        let need_base_image_lsn: Option<Lsn>;

        assert!(lsn >= self.lsn);

        {
            let inner = self.load()?;

            let base_blknum: usize = (blknum % RELISH_SEG_SIZE) as usize;
            if let Some(img) = inner.base_images.get(base_blknum) {
                reconstruct_data.page_img = Some(img.clone());
                need_base_image_lsn = None;
            } else {
                bail!("no base img found for {} at blk {} at LSN {}", self.seg, base_blknum, lsn);
            }
            // release lock on 'inner'
        }

        Ok(need_base_image_lsn)
    }

    /// Get size of the relation at given LSN
    fn get_seg_size(&self, _lsn: Lsn) -> Result<u32> {

        let inner = self.load()?;
        let result = inner.base_images.len() as u32;

        Ok(result)
    }

    /// Does this segment exist at given LSN?
    fn get_seg_exists(&self, _lsn: Lsn) -> Result<bool> {
        Ok(true)
    }


    ///
    /// Release most of the memory used by this layer. If it's accessed again later,
    /// it will need to be loaded back.
    ///
    fn unload(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.base_images = Vec::new();
        inner.loaded = false;
        Ok(())
    }

    fn delete(&self) -> Result<()> {
        // delete underlying file
        fs::remove_file(self.path())?;
        Ok(())
    }

    fn is_incremental(&self) -> bool {
        false
    }
}

impl ImageLayer {
    fn path(&self) -> PathBuf {
        Self::path_for(
            self.conf,
            self.timelineid,
            self.tenantid,
            &ImageFileName {
                seg: self.seg,
                lsn: self.lsn,
            },
        )
    }

    fn path_for(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        fname: &ImageFileName,
    ) -> PathBuf {
        conf.timeline_path(&timelineid, &tenantid)
            .join(fname.to_string())
    }

    /// Create a new snapshot file, using the given btreemaps containing the page versions and
    /// relsizes.
    /// FIXME comment
    /// This is used to write the in-memory layer to disk. The in-memory layer uses the same
    /// data structure with two btreemaps as we do, so passing the btreemaps is currently
    /// expedient.
    pub fn create(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        seg: SegmentTag,
        lsn: Lsn,
        base_images: Vec<Bytes>,
    ) -> Result<ImageLayer> {

        let layer = ImageLayer {
            conf: conf,
            timelineid: timelineid,
            tenantid: tenantid,
            seg: seg,
            lsn: lsn,
            inner: Mutex::new(ImageLayerInner {
                loaded: true,
                base_images: base_images,
            }),
        };
        let inner = layer.inner.lock().unwrap();

        // Write the images into a file
        let path = layer.path();

        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let file = File::create(&path)?;
        let book = BookWriter::new(file, IMAGE_FILE_MAGIC)?;

        // Write out the base images
        let mut chapter = book.new_chapter(BASE_IMAGES_CHAPTER);
        let buf = Vec::ser(&inner.base_images)?;

        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        book.close()?;

        trace!("saved {}", &path.display());

        drop(inner);

        Ok(layer)
    }

    pub fn create_from_src(
        conf: &'static PageServerConf,
        timeline: &LayeredTimeline,
        src: &dyn Layer,
        lsn: Lsn,
    ) -> Result<ImageLayer> {
        let seg = src.get_seg_tag();
        let timelineid = timeline.timelineid;

        let startblk;
        let size;
        if seg.rel.is_blocky() {
            size = src.get_seg_size(lsn)?;
            startblk = seg.segno * RELISH_SEG_SIZE;
        } else {
            size = 1;
            startblk = 0;
        }

        trace!(
            "creating new ImageLayer for {} on timeline {} at {}",
            seg,
            timelineid,
            lsn,
        );
 
        let mut base_images: Vec<Bytes> = Vec::new();
        for blknum in startblk..(startblk+size) {
            let img = 
            RECONSTRUCT_TIME
                .observe_closure_duration(|| {
                    timeline.materialize_page(seg, blknum, lsn, &*src)
                })?;

            base_images.push(img);
        }

        Self::create(conf, timelineid, timeline.tenantid, seg, lsn,
                     base_images)
    }


    ///
    /// Load the contents of the file into memory
    ///
    fn load(&self) -> Result<MutexGuard<ImageLayerInner>> {
        // quick exit if already loaded
        let mut inner = self.inner.lock().unwrap();

        if inner.loaded {
            return Ok(inner);
        }

        let path = Self::path_for(
            self.conf,
            self.timelineid,
            self.tenantid,
            &ImageFileName {
                seg: self.seg,
                lsn: self.lsn,
            },
        );

        let file = File::open(&path)?;
        let book = Book::new(file)?;

        let chapter = book.read_chapter(BASE_IMAGES_CHAPTER)?;
        let base_images = Vec::des(&chapter)?;

        debug!("loaded from {}", &path.display());

        *inner = ImageLayerInner {
            loaded: true,
            base_images,
        };

        Ok(inner)
    }

    /// Create an ImageLayer represent a file on disk
    pub fn load_image_layer(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        filename: &ImageFileName,
    ) -> Result<ImageLayer> {
        let layer = ImageLayer {
            conf,
            timelineid,
            tenantid,
            seg: filename.seg,
            lsn: filename.lsn,
            inner: Mutex::new(ImageLayerInner {
                loaded: false,
                base_images: Vec::new(),
            }),
        };

        Ok(layer)
    }

    /// debugging function to print out the contents of the layer
    #[allow(unused)]
    pub fn dump(&self) -> String {
        let mut result = format!(
            "----- image layer for {} at {} ----\n",
            self.seg, self.lsn,
        );

        //let inner = self.inner.lock().unwrap();

        //for (k, v) in inner.page_versions.iter() {
        //    result += &format!("blk {} at {}: {}/{}\n", k.0, k.1, v.page_image.is_some(), v.record.is_some());
        //}

        result
    }
}
