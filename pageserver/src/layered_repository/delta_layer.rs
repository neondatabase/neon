//!
//! A DeltaLayer represents a collection of WAL records or page images in a range of
//! LSNs, for one segment. It is stored on a file on disk.
//!
//! Usually a delta layer only contains differences - in the form of WAL records against
//! a base LSN. However, if a segment is newly created, by creating a new relation or
//! extending an old one, there might be no base image. In that case, all the entries in
//! the delta layer must be page images or WAL records with the 'will_init' flag set, so
//! that they can be replayed without referring to an older page version. Also in some
//! circumstances, the predecessor layer might actually be another delta layer. That
//! can happen when you create a new branch in the middle of a delta layer, and the WAL
//! records on the new branch are put in a new delta layer.
//!
//! When a delta file needs to be accessed, we slurp the metadata and segsize chapters
//! into memory, into the DeltaLayerInner struct. See load() and unload() functions.
//! To access a page/WAL record, we search `page_version_metas` for the block # and LSN.
//! The byte ranges in the metadata can be used to find the page/WAL record in
//! PAGE_VERSIONS_CHAPTER.
//!
//! On disk, the delta files are stored in timelines/<timelineid> directory.
//! Currently, there are no subdirectories, and each delta file is named like this:
//!
//!    <spcnode>_<dbnode>_<relnode>_<forknum>_<segno>_<start LSN>_<end LSN>
//!
//! For example:
//!
//!    1663_13990_2609_0_5_000000000169C348_000000000169C349
//!
//! If a relation is dropped, we add a '_DROPPED' to the end of the filename to indicate that.
//! So the above example would become:
//!
//!    1663_13990_2609_0_5_000000000169C348_000000000169C349_DROPPED
//!
//! The end LSN indicates when it was dropped in that case, we don't store it in the
//! file contents in any way.
//!
//! A detlta file is constructed using the 'bookfile' crate. Each file consists of two
//! parts: the page versions and the segment sizes. They are stored as separate chapters.
//!
use crate::config::PageServerConf;
use crate::layered_repository::filename::{DeltaFileName, PathOrConf};
use crate::layered_repository::storage_layer::{
    Layer, PageReconstructData, PageReconstructResult, PageVersion, SegmentBlk, SegmentTag,
    RELISH_SEG_SIZE,
};
use crate::virtual_file::VirtualFile;
use crate::walrecord;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, ensure, Result};
use log::*;
use serde::{Deserialize, Serialize};
use zenith_utils::vec_map::VecMap;
// avoid binding to Write (conflicts with std::io::Write)
// while being able to use std::fmt::Write's methods
use std::fmt::Write as _;
use std::fs;
use std::io::{BufWriter, Write};
use std::ops::Bound::Included;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use bookfile::{Book, BookWriter, BoundedReader, ChapterWriter};

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

// Magic constant to identify a Zenith delta file
pub const DELTA_FILE_MAGIC: u32 = 0x5A616E01;

/// Mapping from (block #, lsn) -> page/WAL record
/// byte ranges in PAGE_VERSIONS_CHAPTER
static PAGE_VERSION_METAS_CHAPTER: u64 = 1;
/// Page/WAL bytes - cannot be interpreted
/// without PAGE_VERSION_METAS_CHAPTER
static PAGE_VERSIONS_CHAPTER: u64 = 2;
static SEG_SIZES_CHAPTER: u64 = 3;

/// Contains the [`Summary`] struct
static SUMMARY_CHAPTER: u64 = 4;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Summary {
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    seg: SegmentTag,

    start_lsn: Lsn,
    end_lsn: Lsn,

    dropped: bool,
}

impl From<&DeltaLayer> for Summary {
    fn from(layer: &DeltaLayer) -> Self {
        Self {
            tenantid: layer.tenantid,
            timelineid: layer.timelineid,
            seg: layer.seg,

            start_lsn: layer.start_lsn,
            end_lsn: layer.end_lsn,

            dropped: layer.dropped,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct BlobRange {
    offset: u64,
    size: usize,
}

fn read_blob<F: FileExt>(reader: &BoundedReader<&'_ F>, range: &BlobRange) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; range.size];
    reader.read_exact_at(&mut buf, range.offset)?;
    Ok(buf)
}

///
/// DeltaLayer is the in-memory data structure associated with an
/// on-disk delta file.  We keep a DeltaLayer in memory for each
/// file, in the LayerMap. If a layer is in "loaded" state, we have a
/// copy of the file in memory, in 'inner'. Otherwise the struct is
/// just a placeholder for a file that exists on disk, and it needs to
/// be loaded before using it in queries.
///
pub struct DeltaLayer {
    path_or_conf: PathOrConf,

    pub tenantid: ZTenantId,
    pub timelineid: ZTimelineId,
    pub seg: SegmentTag,

    //
    // This entry contains all the changes from 'start_lsn' to 'end_lsn'. The
    // start is inclusive, and end is exclusive.
    //
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,

    dropped: bool,

    inner: Mutex<DeltaLayerInner>,
}

pub struct DeltaLayerInner {
    /// If false, the 'page_version_metas' and 'seg_sizes' have not been
    /// loaded into memory yet.
    loaded: bool,

    book: Option<Book<VirtualFile>>,

    /// All versions of all pages in the file are are kept here.
    /// Indexed by block number and LSN.
    page_version_metas: VecMap<(SegmentBlk, Lsn), BlobRange>,

    /// `seg_sizes` tracks the size of the segment at different points in time.
    seg_sizes: VecMap<Lsn, SegmentBlk>,
}

impl DeltaLayerInner {
    fn get_seg_size(&self, lsn: Lsn) -> Result<SegmentBlk> {
        // Scan the VecMap backwards, starting from the given entry.
        let slice = self
            .seg_sizes
            .slice_range((Included(&Lsn(0)), Included(&lsn)));
        if let Some((_entry_lsn, entry)) = slice.last() {
            Ok(*entry)
        } else {
            Err(anyhow::anyhow!("could not find seg size in delta layer"))
        }
    }
}

impl Layer for DeltaLayer {
    fn get_tenant_id(&self) -> ZTenantId {
        self.tenantid
    }

    fn get_timeline_id(&self) -> ZTimelineId {
        self.timelineid
    }

    fn get_seg_tag(&self) -> SegmentTag {
        self.seg
    }

    fn is_dropped(&self) -> bool {
        self.dropped
    }

    fn get_start_lsn(&self) -> Lsn {
        self.start_lsn
    }

    fn get_end_lsn(&self) -> Lsn {
        self.end_lsn
    }

    fn filename(&self) -> PathBuf {
        PathBuf::from(self.layer_name().to_string())
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

        match &cached_img_lsn {
            Some(cached_lsn) if &self.end_lsn <= cached_lsn => {
                return Ok(PageReconstructResult::Cached)
            }
            _ => {}
        }

        {
            // Open the file and lock the metadata in memory
            let inner = self.load()?;
            let page_version_reader = inner
                .book
                .as_ref()
                .expect("should be loaded in load call above")
                .chapter_reader(PAGE_VERSIONS_CHAPTER)?;

            // Scan the metadata VecMap backwards, starting from the given entry.
            let minkey = (blknum, Lsn(0));
            let maxkey = (blknum, lsn);
            let iter = inner
                .page_version_metas
                .slice_range((Included(&minkey), Included(&maxkey)))
                .iter()
                .rev();
            for ((_blknum, pv_lsn), blob_range) in iter {
                match &cached_img_lsn {
                    Some(cached_lsn) if pv_lsn <= cached_lsn => {
                        return Ok(PageReconstructResult::Cached)
                    }
                    _ => {}
                }

                let pv = PageVersion::des(&read_blob(&page_version_reader, blob_range)?)?;

                match pv {
                    PageVersion::Page(img) => {
                        // Found a page image, return it
                        reconstruct_data.page_img = Some(img);
                        need_image = false;
                        break;
                    }
                    PageVersion::Wal(rec) => {
                        let will_init = rec.will_init();
                        reconstruct_data.records.push((*pv_lsn, rec));
                        if will_init {
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
                && blknum >= inner.get_seg_size(lsn)?
            {
                return Ok(PageReconstructResult::Missing(self.start_lsn));
            }

            // release metadata lock and close the file
        }

        // If an older page image is needed to reconstruct the page, let the
        // caller know.
        if need_image {
            Ok(PageReconstructResult::Continue(Lsn(self.start_lsn.0 - 1)))
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

        let inner = self.load()?;
        inner.get_seg_size(lsn)
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

    ///
    /// Release most of the memory used by this layer. If it's accessed again later,
    /// it will need to be loaded back.
    ///
    fn unload(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.page_version_metas = VecMap::default();
        inner.seg_sizes = VecMap::default();
        inner.loaded = false;

        // Note: we keep the Book open. Is that a good idea? The virtual file
        // machinery has its own rules for closing the file descriptor if it's not
        // needed, but the Book struct uses up some memory, too.

        Ok(())
    }

    fn delete(&self) -> Result<()> {
        // delete underlying file
        fs::remove_file(self.path())?;
        Ok(())
    }

    fn is_incremental(&self) -> bool {
        true
    }

    fn is_in_memory(&self) -> bool {
        false
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self) -> Result<()> {
        println!(
            "----- delta layer for ten {} tli {} seg {} {}-{} ----",
            self.tenantid, self.timelineid, self.seg, self.start_lsn, self.end_lsn
        );

        println!("--- seg sizes ---");
        let inner = self.load()?;
        for (k, v) in inner.seg_sizes.as_slice() {
            println!("  {}: {}", k, v);
        }
        println!("--- page versions ---");

        let path = self.path();
        let file = std::fs::File::open(&path)?;
        let book = Book::new(file)?;

        let chapter = book.chapter_reader(PAGE_VERSIONS_CHAPTER)?;
        for ((blk, lsn), blob_range) in inner.page_version_metas.as_slice() {
            let mut desc = String::new();

            let buf = read_blob(&chapter, blob_range)?;
            let pv = PageVersion::des(&buf)?;

            match pv {
                PageVersion::Page(img) => {
                    write!(&mut desc, " img {} bytes", img.len())?;
                }
                PageVersion::Wal(rec) => {
                    let wal_desc = walrecord::describe_wal_record(&rec);
                    write!(
                        &mut desc,
                        " rec {} bytes will_init: {} {}",
                        blob_range.size,
                        rec.will_init(),
                        wal_desc
                    )?;
                }
            }

            println!("  blk {} at {}: {}", blk, lsn, desc);
        }

        Ok(())
    }
}

impl DeltaLayer {
    fn path_for(
        path_or_conf: &PathOrConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        fname: &DeltaFileName,
    ) -> PathBuf {
        match path_or_conf {
            PathOrConf::Path(path) => path.clone(),
            PathOrConf::Conf(conf) => conf
                .timeline_path(&timelineid, &tenantid)
                .join(fname.to_string()),
        }
    }

    ///
    /// Load the contents of the file into memory
    ///
    fn load(&self) -> Result<MutexGuard<DeltaLayerInner>> {
        // quick exit if already loaded
        let mut inner = self.inner.lock().unwrap();

        if inner.loaded {
            return Ok(inner);
        }

        let path = self.path();

        // Open the file if it's not open already.
        if inner.book.is_none() {
            let file = VirtualFile::open(&path)?;
            inner.book = Some(Book::new(file)?);
        }
        let book = inner.book.as_ref().unwrap();

        match &self.path_or_conf {
            PathOrConf::Conf(_) => {
                let chapter = book.read_chapter(SUMMARY_CHAPTER)?;
                let actual_summary = Summary::des(&chapter)?;

                let expected_summary = Summary::from(self);

                if actual_summary != expected_summary {
                    bail!("in-file summary does not match expected summary. actual = {:?} expected = {:?}", actual_summary, expected_summary);
                }
            }
            PathOrConf::Path(path) => {
                let actual_filename = Path::new(path.file_name().unwrap());
                let expected_filename = self.filename();

                if actual_filename != expected_filename {
                    println!(
                        "warning: filename does not match what is expected from in-file summary"
                    );
                    println!("actual: {:?}", actual_filename);
                    println!("expected: {:?}", expected_filename);
                }
            }
        }

        let chapter = book.read_chapter(PAGE_VERSION_METAS_CHAPTER)?;
        let page_version_metas = VecMap::des(&chapter)?;

        let chapter = book.read_chapter(SEG_SIZES_CHAPTER)?;
        let seg_sizes = VecMap::des(&chapter)?;

        debug!("loaded from {}", &path.display());

        inner.page_version_metas = page_version_metas;
        inner.seg_sizes = seg_sizes;
        inner.loaded = true;

        Ok(inner)
    }

    /// Create a DeltaLayer struct representing an existing file on disk.
    pub fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        filename: &DeltaFileName,
    ) -> DeltaLayer {
        DeltaLayer {
            path_or_conf: PathOrConf::Conf(conf),
            timelineid,
            tenantid,
            seg: filename.seg,
            start_lsn: filename.start_lsn,
            end_lsn: filename.end_lsn,
            dropped: filename.dropped,
            inner: Mutex::new(DeltaLayerInner {
                loaded: false,
                book: None,
                page_version_metas: VecMap::default(),
                seg_sizes: VecMap::default(),
            }),
        }
    }

    /// Create a DeltaLayer struct representing an existing file on disk.
    ///
    /// This variant is only used for debugging purposes, by the 'dump_layerfile' binary.
    pub fn new_for_path<F>(path: &Path, book: &Book<F>) -> Result<Self>
    where
        F: std::os::unix::prelude::FileExt,
    {
        let chapter = book.read_chapter(SUMMARY_CHAPTER)?;
        let summary = Summary::des(&chapter)?;

        Ok(DeltaLayer {
            path_or_conf: PathOrConf::Path(path.to_path_buf()),
            timelineid: summary.timelineid,
            tenantid: summary.tenantid,
            seg: summary.seg,
            start_lsn: summary.start_lsn,
            end_lsn: summary.end_lsn,
            dropped: summary.dropped,
            inner: Mutex::new(DeltaLayerInner {
                loaded: false,
                book: None,
                page_version_metas: VecMap::default(),
                seg_sizes: VecMap::default(),
            }),
        })
    }

    fn layer_name(&self) -> DeltaFileName {
        DeltaFileName {
            seg: self.seg,
            start_lsn: self.start_lsn,
            end_lsn: self.end_lsn,
            dropped: self.dropped,
        }
    }

    /// Path to the layer file in pageserver workdir.
    pub fn path(&self) -> PathBuf {
        Self::path_for(
            &self.path_or_conf,
            self.timelineid,
            self.tenantid,
            &self.layer_name(),
        )
    }
}

/// A builder object for constructing a new delta layer.
///
/// Usage:
///
/// 1. Create the DeltaLayerWriter by calling DeltaLayerWriter::new(...)
///
/// 2. Write the contents by calling `put_page_version` for every page
///    version to store in the layer.
///
/// 3. Call `finish`.
///
pub struct DeltaLayerWriter {
    conf: &'static PageServerConf,
    timelineid: ZTimelineId,
    tenantid: ZTenantId,
    seg: SegmentTag,
    start_lsn: Lsn,
    end_lsn: Lsn,
    dropped: bool,

    page_version_writer: ChapterWriter<BufWriter<VirtualFile>>,
    pv_offset: u64,

    page_version_metas: VecMap<(SegmentBlk, Lsn), BlobRange>,
}

impl DeltaLayerWriter {
    ///
    /// Start building a new delta layer.
    ///
    pub fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        seg: SegmentTag,
        start_lsn: Lsn,
        end_lsn: Lsn,
        dropped: bool,
    ) -> Result<DeltaLayerWriter> {
        // Create the file
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let path = DeltaLayer::path_for(
            &PathOrConf::Conf(conf),
            timelineid,
            tenantid,
            &DeltaFileName {
                seg,
                start_lsn,
                end_lsn,
                dropped,
            },
        );
        let file = VirtualFile::create(&path)?;
        let buf_writer = BufWriter::new(file);
        let book = BookWriter::new(buf_writer, DELTA_FILE_MAGIC)?;

        // Open the page-versions chapter for writing. The calls to
        // `put_page_version` will use this to write the contents.
        let page_version_writer = book.new_chapter(PAGE_VERSIONS_CHAPTER);

        Ok(DeltaLayerWriter {
            conf,
            timelineid,
            tenantid,
            seg,
            start_lsn,
            end_lsn,
            dropped,
            page_version_writer,
            page_version_metas: VecMap::default(),
            pv_offset: 0,
        })
    }

    ///
    /// Append a page version to the file.
    ///
    /// 'buf' is a serialized PageVersion.
    /// The page versions must be appended in blknum, lsn order.
    ///
    pub fn put_page_version(&mut self, blknum: SegmentBlk, lsn: Lsn, buf: &[u8]) -> Result<()> {
        // Remember the offset and size metadata. The metadata is written
        // to a separate chapter, in `finish`.
        let blob_range = BlobRange {
            offset: self.pv_offset,
            size: buf.len(),
        };
        self.page_version_metas
            .append((blknum, lsn), blob_range)
            .unwrap();

        // write the page version
        self.page_version_writer.write_all(buf)?;
        self.pv_offset += buf.len() as u64;

        Ok(())
    }

    ///
    /// Finish writing the delta layer.
    ///
    /// 'seg_sizes' is a list of size changes to store with the actual data.
    ///
    pub fn finish(self, seg_sizes: VecMap<Lsn, SegmentBlk>) -> Result<DeltaLayer> {
        // Close the page-versions chapter
        let book = self.page_version_writer.close()?;

        // Write out page versions metadata
        let mut chapter = book.new_chapter(PAGE_VERSION_METAS_CHAPTER);
        let buf = VecMap::ser(&self.page_version_metas)?;
        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        if self.seg.rel.is_blocky() {
            assert!(!seg_sizes.is_empty());
        }

        // and seg_sizes to separate chapter
        let mut chapter = book.new_chapter(SEG_SIZES_CHAPTER);
        let buf = VecMap::ser(&seg_sizes)?;
        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        let mut chapter = book.new_chapter(SUMMARY_CHAPTER);
        let summary = Summary {
            tenantid: self.tenantid,
            timelineid: self.timelineid,
            seg: self.seg,

            start_lsn: self.start_lsn,
            end_lsn: self.end_lsn,

            dropped: self.dropped,
        };
        Summary::ser_into(&summary, &mut chapter)?;
        let book = chapter.close()?;

        // This flushes the underlying 'buf_writer'.
        book.close()?;

        // Note: Because we opened the file in write-only mode, we cannot
        // reuse the same VirtualFile for reading later. That's why we don't
        // set inner.book here. The first read will have to re-open it.
        let layer = DeltaLayer {
            path_or_conf: PathOrConf::Conf(self.conf),
            tenantid: self.tenantid,
            timelineid: self.timelineid,
            seg: self.seg,
            start_lsn: self.start_lsn,
            end_lsn: self.end_lsn,
            dropped: self.dropped,
            inner: Mutex::new(DeltaLayerInner {
                loaded: false,
                book: None,
                page_version_metas: VecMap::default(),
                seg_sizes: VecMap::default(),
            }),
        };

        trace!("created delta layer {}", &layer.path().display());

        Ok(layer)
    }
}
