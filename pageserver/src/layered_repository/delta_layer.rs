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
//! When a delta file needs to be accessed, we slurp the metadata and relsize chapters
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
//! parts: the page versions and the relation sizes. They are stored as separate chapters.
//!
use crate::layered_repository::blob::BlobWriter;
use crate::layered_repository::filename::{DeltaFileName, PathOrConf};
use crate::layered_repository::page_versions::PageVersions;
use crate::layered_repository::storage_layer::{
    Layer, PageReconstructData, PageReconstructResult, PageVersion, SegmentTag,
};
use crate::virtual_file::VirtualFile;
use crate::waldecoder;
use crate::PageServerConf;
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
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use bookfile::{Book, BookWriter};

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

use super::blob::{read_blob, BlobRange};

// Magic constant to identify a Zenith delta file
pub const DELTA_FILE_MAGIC: u32 = 0x5A616E01;

/// Mapping from (block #, lsn) -> page/WAL record
/// byte ranges in PAGE_VERSIONS_CHAPTER
static PAGE_VERSION_METAS_CHAPTER: u64 = 1;
/// Page/WAL bytes - cannot be interpreted
/// without PAGE_VERSION_METAS_CHAPTER
static PAGE_VERSIONS_CHAPTER: u64 = 2;
static REL_SIZES_CHAPTER: u64 = 3;

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
    /// If false, the 'page_version_metas' and 'relsizes' have not been
    /// loaded into memory yet.
    loaded: bool,

    book: Option<Book<VirtualFile>>,

    /// All versions of all pages in the file are are kept here.
    /// Indexed by block number and LSN.
    page_version_metas: VecMap<(u32, Lsn), BlobRange>,

    /// `relsizes` tracks the size of the relation at different points in time.
    relsizes: VecMap<Lsn, u32>,
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
        blknum: u32,
        lsn: Lsn,
        cached_img_lsn: Option<Lsn>,
        reconstruct_data: &mut PageReconstructData,
    ) -> Result<PageReconstructResult> {
        let mut need_image = true;

        assert!(self.seg.blknum_in_seg(blknum));

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

            // Scan the metadata BTreeMap backwards, starting from the given entry.
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
                        let will_init = rec.will_init;
                        reconstruct_data.records.push((*pv_lsn, rec));
                        if will_init {
                            // This WAL record initializes the page, so no need to go further back
                            need_image = false;
                            break;
                        }
                    }
                }
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
    fn get_seg_size(&self, lsn: Lsn) -> Result<u32> {
        assert!(lsn >= self.start_lsn);
        ensure!(
            self.seg.rel.is_blocky(),
            "get_seg_size() called on a non-blocky rel"
        );

        // Scan the BTreeMap backwards, starting from the given entry.
        let inner = self.load()?;
        let slice = inner
            .relsizes
            .slice_range((Included(&Lsn(0)), Included(&lsn)));

        if let Some((_entry_lsn, entry)) = slice.last() {
            Ok(*entry)
        } else {
            Err(anyhow::anyhow!("could not find seg size in delta layer"))
        }
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
        inner.relsizes = VecMap::default();
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

        println!("--- relsizes ---");
        let inner = self.load()?;
        for (k, v) in inner.relsizes.as_slice() {
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
                    let wal_desc = waldecoder::describe_wal_record(&rec.rec);
                    write!(
                        &mut desc,
                        " rec {} bytes will_init: {} {}",
                        rec.rec.len(),
                        rec.will_init,
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

    /// Create a new delta file, using the given page versions and relsizes.
    /// The page versions are passed in a PageVersions struct. If 'cutoff' is
    /// given, only page versions with LSN < cutoff are included.
    ///
    /// This is used to write the in-memory layer to disk. The page_versions and
    /// relsizes are thus passed in the same format as they are in the in-memory
    /// layer, as that's expedient.
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        seg: SegmentTag,
        start_lsn: Lsn,
        end_lsn: Lsn,
        dropped: bool,
        page_versions: &PageVersions,
        cutoff: Option<Lsn>,
        relsizes: VecMap<Lsn, u32>,
    ) -> Result<DeltaLayer> {
        if seg.rel.is_blocky() {
            assert!(!relsizes.is_empty());
        }

        let delta_layer = DeltaLayer {
            path_or_conf: PathOrConf::Conf(conf),
            timelineid,
            tenantid,
            seg,
            start_lsn,
            end_lsn,
            dropped,
            inner: Mutex::new(DeltaLayerInner {
                loaded: false,
                book: None,
                page_version_metas: VecMap::default(),
                relsizes,
            }),
        };
        let mut inner = delta_layer.inner.lock().unwrap();

        // Write the data into a file
        //
        // Note: Because we open the file in write-only mode, we cannot
        // reuse the same VirtualFile for reading later. That's why we don't
        // set inner.book here. The first read will have to re-open it.
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let path = delta_layer.path();
        let file = VirtualFile::create(&path)?;
        let buf_writer = BufWriter::new(file);
        let book = BookWriter::new(buf_writer, DELTA_FILE_MAGIC)?;

        let mut page_version_writer = BlobWriter::new(book, PAGE_VERSIONS_CHAPTER);

        let page_versions_iter = page_versions.ordered_page_version_iter(cutoff);
        for (blknum, lsn, pos) in page_versions_iter {
            let blob_range =
                page_version_writer.write_blob_from_reader(&mut page_versions.reader(pos)?)?;

            inner
                .page_version_metas
                .append((blknum, lsn), blob_range)
                .unwrap();
        }

        let book = page_version_writer.close()?;

        // Write out page versions
        let mut chapter = book.new_chapter(PAGE_VERSION_METAS_CHAPTER);
        let buf = VecMap::ser(&inner.page_version_metas)?;
        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        // and relsizes to separate chapter
        let mut chapter = book.new_chapter(REL_SIZES_CHAPTER);
        let buf = VecMap::ser(&inner.relsizes)?;
        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        let mut chapter = book.new_chapter(SUMMARY_CHAPTER);
        let summary = Summary {
            tenantid,
            timelineid,
            seg,

            start_lsn,
            end_lsn,

            dropped,
        };
        Summary::ser_into(&summary, &mut chapter)?;
        let book = chapter.close()?;

        // This flushes the underlying 'buf_writer'.
        let writer = book.close()?;
        writer.get_ref().sync_all()?;

        trace!("saved {}", &path.display());

        drop(inner);

        Ok(delta_layer)
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

        let chapter = book.read_chapter(REL_SIZES_CHAPTER)?;
        let relsizes = VecMap::des(&chapter)?;

        debug!("loaded from {}", &path.display());

        inner.page_version_metas = page_version_metas;
        inner.relsizes = relsizes;
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
                relsizes: VecMap::default(),
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
                relsizes: VecMap::default(),
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
