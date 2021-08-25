//! An ImageLayer represents an image or a snapshot of a segment at one particular LSN.
//! It is stored in a file on disk.
//!
//! On disk, the image files are stored in timelines/<timelineid> directory.
//! Currently, there are no subdirectories, and each snapshot file is named like this:
//!
//!    <spcnode>_<dbnode>_<relnode>_<forknum>_<segno>_<LSN>
//!
//! For example:
//!
//!    1663_13990_2609_0_5_000000000169C348
//!
//! An image file is constructed using the 'bookfile' crate.
//!
//! When a snapshot file needs to be accessed, we slurp the whole file into memory,
//! into the ImageLayerInner struct. See load() and unload() functions.
//! TODO: That's very inefficient, we should be smarter.
//!
use crate::layered_repository::filename::ImageFileName;
use crate::layered_repository::storage_layer::{Layer, PageReconstructData, SegmentTag};
use crate::layered_repository::LayeredTimeline;
use crate::layered_repository::RELISH_SEG_SIZE;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, Result};
use bytes::Bytes;
use log::*;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

use bookfile::{Book, BookWriter};

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

// Magic constant to identify a Zenith segment image file
static IMAGE_FILE_MAGIC: u32 = 0x5A616E01 + 1;

static BASE_IMAGES_CHAPTER: u64 = 1;

///
/// ImageLayer is the in-memory data structure associated with an on-disk image
/// file.  We keep an ImageLayer in memory for each file, in the LayerMap. If a
/// layer is in "loaded" state, we have a copy of the file in memory, in 'inner'.
/// Otherwise the struct is just a placeholder for a file that exists on disk,
/// and it needs to be loaded before using it in queries.
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

    /// The data is held in this vector of Bytes buffers, with one
    /// Bytes for each block. It's indexed by block number (counted from
    /// the beginning of the segment)
    base_images: Vec<Bytes>,
}

impl Layer for ImageLayer {
    fn filename(&self) -> PathBuf {
        PathBuf::from(
            ImageFileName {
                seg: self.seg,
                lsn: self.lsn,
            }
            .to_string(),
        )
    }

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

    /// Look up given page in the file
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
                bail!(
                    "no base img found for {} at blk {} at LSN {}",
                    self.seg,
                    base_blknum,
                    lsn
                );
            }
            // release lock on 'inner'
        }

        Ok(need_base_image_lsn)
    }

    /// Get size of the segment
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

    /// Create a new image file, using the given array of pages.
    fn create(
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

        let mut chapter = book.new_chapter(BASE_IMAGES_CHAPTER);
        let buf = Vec::ser(&inner.base_images)?;

        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        book.close()?;

        trace!("saved {}", &path.display());

        drop(inner);

        Ok(layer)
    }

    // Create a new image file by materializing every page in a source layer
    // at given LSN.
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
        for blknum in startblk..(startblk + size) {
            let img = timeline.materialize_page(seg, blknum, lsn, &*src)?;

            base_images.push(img);
        }

        Self::create(conf, timelineid, timeline.tenantid, seg, lsn, base_images)
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

    /// Create an ImageLayer struct representing an existing file on disk
    pub fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        filename: &ImageFileName,
    ) -> ImageLayer {
        ImageLayer {
            conf,
            timelineid,
            tenantid,
            seg: filename.seg,
            lsn: filename.lsn,
            inner: Mutex::new(ImageLayerInner {
                loaded: false,
                base_images: Vec::new(),
            }),
        }
    }

    /// debugging function to print out the contents of the layer
    #[allow(unused)]
    pub fn dump(&self) -> String {
        let mut result = format!("----- image layer for {} at {} ----\n", self.seg, self.lsn);

        //let inner = self.inner.lock().unwrap();

        //for (k, v) in inner.page_versions.iter() {
        //    result += &format!("blk {} at {}: {}/{}\n", k.0, k.1, v.page_image.is_some(), v.record.is_some());
        //}

        result
    }
}
