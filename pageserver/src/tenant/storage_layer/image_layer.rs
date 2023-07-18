//! An ImageLayer represents an image or a snapshot of a key-range at
//! one particular LSN. It contains an image of all key-value pairs
//! in its key-range. Any key that falls into the image layer's range
//! but does not exist in the layer, does not exist.
//!
//! An image layer is stored in a file on disk. The file is stored in
//! timelines/<timeline_id> directory.  Currently, there are no
//! subdirectories, and each image layer file is named like this:
//!
//! ```text
//!    <key start>-<key end>__<LSN>
//! ```
//!
//! For example:
//!
//! ```text
//!    000000067F000032BE0000400000000070B6-000000067F000032BE0000400000000080B6__00000000346BC568
//! ```
//!
//! Every image layer file consists of three parts: "summary",
//! "index", and "values".  The summary is a fixed size header at the
//! beginning of the file, and it contains basic information about the
//! layer, and offsets to the other parts. The "index" is a B-tree,
//! mapping from Key to an offset in the "values" part.  The
//! actual page images are stored in the "values" part.
use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::page_cache::PAGE_SZ;
use crate::repository::{Key, KEY_SIZE};
use crate::tenant::blob_io::{BlobCursor, BlobWriter, WriteBlobWriter};
use crate::tenant::block_io::{BlockBuf, BlockReader, FileBlockReader};
use crate::tenant::disk_btree::{DiskBtreeBuilder, DiskBtreeReader, VisitDirection};
use crate::tenant::storage_layer::{
    LayerAccessStats, PersistentLayer, ValueReconstructResult, ValueReconstructState,
};
use crate::virtual_file::VirtualFile;
use crate::{IMAGE_FILE_MAGIC, STORAGE_FORMAT_VERSION, TEMP_FILE_SUFFIX};
use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use hex;
use pageserver_api::models::{HistoricLayerInfo, LayerAccessKind};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::io::{Seek, SeekFrom};
use std::ops::Range;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{RwLock, RwLockReadGuard};
use tracing::*;

use utils::{
    bin_ser::BeSer,
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use super::filename::ImageFileName;
use super::{
    AsLayerDesc, Layer, LayerAccessStatsReset, LayerIter, PathOrConf, PersistentLayerDesc,
};

///
/// Header stored in the beginning of the file
///
/// After this comes the 'values' part, starting on block 1. After that,
/// the 'index' starts at the block indicated by 'index_start_blk'
///
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Summary {
    /// Magic value to identify this as a neon image file. Always IMAGE_FILE_MAGIC.
    magic: u16,
    format_version: u16,

    tenant_id: TenantId,
    timeline_id: TimelineId,
    key_range: Range<Key>,
    lsn: Lsn,

    /// Block number where the 'index' part of the file begins.
    index_start_blk: u32,
    /// Block within the 'index', where the B-tree root page is stored
    index_root_blk: u32,
    // the 'values' part starts after the summary header, on block 1.
}

impl From<&ImageLayer> for Summary {
    fn from(layer: &ImageLayer) -> Self {
        Self {
            magic: IMAGE_FILE_MAGIC,
            format_version: STORAGE_FORMAT_VERSION,
            tenant_id: layer.desc.tenant_id,
            timeline_id: layer.desc.timeline_id,
            key_range: layer.desc.key_range.clone(),
            lsn: layer.lsn,

            index_start_blk: 0,
            index_root_blk: 0,
        }
    }
}

/// ImageLayer is the in-memory data structure associated with an on-disk image
/// file.
///
/// We keep an ImageLayer in memory for each file, in the LayerMap. If a layer
/// is in "loaded" state, we have a copy of the index in memory, in 'inner'.
/// Otherwise the struct is just a placeholder for a file that exists on disk,
/// and it needs to be loaded before using it in queries.
pub struct ImageLayer {
    path_or_conf: PathOrConf,

    pub desc: PersistentLayerDesc,
    // This entry contains an image of all pages as of this LSN, should be the same as desc.lsn
    pub lsn: Lsn,

    access_stats: LayerAccessStats,

    inner: RwLock<ImageLayerInner>,
}

impl std::fmt::Debug for ImageLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use super::RangeDisplayDebug;

        f.debug_struct("ImageLayer")
            .field("key_range", &RangeDisplayDebug(&self.desc.key_range))
            .field("file_size", &self.desc.file_size)
            .field("lsn", &self.lsn)
            .field("inner", &self.inner)
            .finish()
    }
}

pub struct ImageLayerInner {
    /// If false, the 'index' has not been loaded into memory yet.
    loaded: bool,

    // values copied from summary
    index_start_blk: u32,
    index_root_blk: u32,

    /// Reader object for reading blocks from the file. (None if not loaded yet)
    file: Option<FileBlockReader<VirtualFile>>,
}

impl std::fmt::Debug for ImageLayerInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImageLayerInner")
            .field("loaded", &self.loaded)
            .field("index_start_blk", &self.index_start_blk)
            .field("index_root_blk", &self.index_root_blk)
            .finish()
    }
}

impl Layer for ImageLayer {
    /// debugging function to print out the contents of the layer
    fn dump(&self, verbose: bool, ctx: &RequestContext) -> Result<()> {
        println!(
            "----- image layer for ten {} tli {} key {}-{} at {} is_incremental {} size {} ----",
            self.desc.tenant_id,
            self.desc.timeline_id,
            self.desc.key_range.start,
            self.desc.key_range.end,
            self.lsn,
            self.desc.is_incremental,
            self.desc.file_size
        );

        if !verbose {
            return Ok(());
        }

        let inner = self.load(LayerAccessKind::Dump, ctx)?;
        let file = inner.file.as_ref().unwrap();
        let tree_reader =
            DiskBtreeReader::<_, KEY_SIZE>::new(inner.index_start_blk, inner.index_root_blk, file);

        tree_reader.dump()?;

        tree_reader.visit(&[0u8; KEY_SIZE], VisitDirection::Forwards, |key, value| {
            println!("key: {} offset {}", hex::encode(key), value);
            true
        })?;

        Ok(())
    }

    /// Look up given page in the file
    fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValueReconstructState,
        ctx: &RequestContext,
    ) -> anyhow::Result<ValueReconstructResult> {
        assert!(self.desc.key_range.contains(&key));
        assert!(lsn_range.start >= self.lsn);
        assert!(lsn_range.end >= self.lsn);

        let inner = self.load(LayerAccessKind::GetValueReconstructData, ctx)?;

        let file = inner.file.as_ref().unwrap();
        let tree_reader = DiskBtreeReader::new(inner.index_start_blk, inner.index_root_blk, file);

        let mut keybuf: [u8; KEY_SIZE] = [0u8; KEY_SIZE];
        key.write_to_byte_slice(&mut keybuf);
        if let Some(offset) = tree_reader.get(&keybuf)? {
            let blob = file.block_cursor().read_blob(offset).with_context(|| {
                format!(
                    "failed to read value from data file {} at offset {}",
                    self.path().display(),
                    offset
                )
            })?;
            let value = Bytes::from(blob);

            reconstruct_state.img = Some((self.lsn, value));
            Ok(ValueReconstructResult::Complete)
        } else {
            Ok(ValueReconstructResult::Missing)
        }
    }

    /// Boilerplate to implement the Layer trait, always use layer_desc for persistent layers.
    fn get_key_range(&self) -> Range<Key> {
        self.layer_desc().key_range.clone()
    }

    /// Boilerplate to implement the Layer trait, always use layer_desc for persistent layers.
    fn get_lsn_range(&self) -> Range<Lsn> {
        self.layer_desc().lsn_range.clone()
    }

    /// Boilerplate to implement the Layer trait, always use layer_desc for persistent layers.
    fn is_incremental(&self) -> bool {
        self.layer_desc().is_incremental
    }
}

/// Boilerplate to implement the Layer trait, always use layer_desc for persistent layers.
impl std::fmt::Display for ImageLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.layer_desc().short_id())
    }
}

impl AsLayerDesc for ImageLayer {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        &self.desc
    }
}

impl PersistentLayer for ImageLayer {
    fn local_path(&self) -> Option<PathBuf> {
        Some(self.path())
    }

    fn iter(&self, _ctx: &RequestContext) -> Result<LayerIter<'_>> {
        unimplemented!();
    }

    fn delete_resident_layer_file(&self) -> Result<()> {
        // delete underlying file
        fs::remove_file(self.path())?;
        Ok(())
    }

    fn info(&self, reset: LayerAccessStatsReset) -> HistoricLayerInfo {
        let layer_file_name = self.filename().file_name();
        let lsn_range = self.get_lsn_range();

        HistoricLayerInfo::Image {
            layer_file_name,
            layer_file_size: self.desc.file_size,
            lsn_start: lsn_range.start,
            remote: false,
            access_stats: self.access_stats.as_api_model(reset),
        }
    }

    fn access_stats(&self) -> &LayerAccessStats {
        &self.access_stats
    }
}

impl ImageLayer {
    fn path_for(
        path_or_conf: &PathOrConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        fname: &ImageFileName,
    ) -> PathBuf {
        match path_or_conf {
            PathOrConf::Path(path) => path.to_path_buf(),
            PathOrConf::Conf(conf) => conf
                .timeline_path(&tenant_id, &timeline_id)
                .join(fname.to_string()),
        }
    }

    fn temp_path_for(
        conf: &PageServerConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        fname: &ImageFileName,
    ) -> PathBuf {
        let rand_string: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();

        conf.timeline_path(&tenant_id, &timeline_id)
            .join(format!("{fname}.{rand_string}.{TEMP_FILE_SUFFIX}"))
    }

    ///
    /// Open the underlying file and read the metadata into memory, if it's
    /// not loaded already.
    ///
    fn load(
        &self,
        access_kind: LayerAccessKind,
        ctx: &RequestContext,
    ) -> Result<RwLockReadGuard<ImageLayerInner>> {
        self.access_stats
            .record_access(access_kind, ctx.task_kind());
        loop {
            // Quick exit if already loaded
            let inner = self.inner.read().unwrap();
            if inner.loaded {
                return Ok(inner);
            }

            // Need to open the file and load the metadata. Upgrade our lock to
            // a write lock. (Or rather, release and re-lock in write mode.)
            drop(inner);
            let mut inner = self.inner.write().unwrap();
            if !inner.loaded {
                self.load_inner(&mut inner).with_context(|| {
                    format!("Failed to load image layer {}", self.path().display())
                })?
            } else {
                // Another thread loaded it while we were not holding the lock.
            }

            // We now have the file open and loaded. There's no function to do
            // that in the std library RwLock, so we have to release and re-lock
            // in read mode. (To be precise, the lock guard was moved in the
            // above call to `load_inner`, so it's already been released). And
            // while we do that, another thread could unload again, so we have
            // to re-check and retry if that happens.
            drop(inner);
        }
    }

    fn load_inner(&self, inner: &mut ImageLayerInner) -> Result<()> {
        let path = self.path();

        // Open the file if it's not open already.
        if inner.file.is_none() {
            let file = VirtualFile::open(&path)
                .with_context(|| format!("Failed to open file '{}'", path.display()))?;
            inner.file = Some(FileBlockReader::new(file));
        }
        let file = inner.file.as_mut().unwrap();
        let summary_blk = file.read_blk(0)?;
        let actual_summary = Summary::des_prefix(summary_blk.as_ref())?;

        match &self.path_or_conf {
            PathOrConf::Conf(_) => {
                let mut expected_summary = Summary::from(self);
                expected_summary.index_start_blk = actual_summary.index_start_blk;
                expected_summary.index_root_blk = actual_summary.index_root_blk;

                if actual_summary != expected_summary {
                    bail!("in-file summary does not match expected summary. actual = {:?} expected = {:?}", actual_summary, expected_summary);
                }
            }
            PathOrConf::Path(path) => {
                let actual_filename = path.file_name().unwrap().to_str().unwrap().to_owned();
                let expected_filename = self.filename().file_name();

                if actual_filename != expected_filename {
                    println!(
                        "warning: filename does not match what is expected from in-file summary"
                    );
                    println!("actual: {:?}", actual_filename);
                    println!("expected: {:?}", expected_filename);
                }
            }
        }

        inner.index_start_blk = actual_summary.index_start_blk;
        inner.index_root_blk = actual_summary.index_root_blk;
        inner.loaded = true;
        Ok(())
    }

    /// Create an ImageLayer struct representing an existing file on disk
    pub fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        filename: &ImageFileName,
        file_size: u64,
        access_stats: LayerAccessStats,
    ) -> ImageLayer {
        ImageLayer {
            path_or_conf: PathOrConf::Conf(conf),
            desc: PersistentLayerDesc::new_img(
                tenant_id,
                timeline_id,
                filename.key_range.clone(),
                filename.lsn,
                false,
                file_size,
            ), // Now we assume image layer ALWAYS covers the full range. This may change in the future.
            lsn: filename.lsn,
            access_stats,
            inner: RwLock::new(ImageLayerInner {
                loaded: false,
                file: None,
                index_start_blk: 0,
                index_root_blk: 0,
            }),
        }
    }

    /// Create an ImageLayer struct representing an existing file on disk.
    ///
    /// This variant is only used for debugging purposes, by the 'pagectl' binary.
    pub fn new_for_path(path: &Path, file: File) -> Result<ImageLayer> {
        let mut summary_buf = Vec::new();
        summary_buf.resize(PAGE_SZ, 0);
        file.read_exact_at(&mut summary_buf, 0)?;
        let summary = Summary::des_prefix(&summary_buf)?;
        let metadata = file
            .metadata()
            .context("get file metadata to determine size")?;
        Ok(ImageLayer {
            path_or_conf: PathOrConf::Path(path.to_path_buf()),
            desc: PersistentLayerDesc::new_img(
                summary.tenant_id,
                summary.timeline_id,
                summary.key_range,
                summary.lsn,
                false,
                metadata.len(),
            ), // Now we assume image layer ALWAYS covers the full range. This may change in the future.
            lsn: summary.lsn,
            access_stats: LayerAccessStats::empty_will_record_residence_event_later(),
            inner: RwLock::new(ImageLayerInner {
                file: None,
                loaded: false,
                index_start_blk: 0,
                index_root_blk: 0,
            }),
        })
    }

    fn layer_name(&self) -> ImageFileName {
        self.desc.image_file_name()
    }

    /// Path to the layer file in pageserver workdir.
    pub fn path(&self) -> PathBuf {
        Self::path_for(
            &self.path_or_conf,
            self.desc.timeline_id,
            self.desc.tenant_id,
            &self.layer_name(),
        )
    }
}

/// A builder object for constructing a new image layer.
///
/// Usage:
///
/// 1. Create the ImageLayerWriter by calling ImageLayerWriter::new(...)
///
/// 2. Write the contents by calling `put_page_image` for every key-value
///    pair in the key range.
///
/// 3. Call `finish`.
///
struct ImageLayerWriterInner {
    conf: &'static PageServerConf,
    path: PathBuf,
    timeline_id: TimelineId,
    tenant_id: TenantId,
    key_range: Range<Key>,
    lsn: Lsn,
    is_incremental: bool,

    blob_writer: WriteBlobWriter<VirtualFile>,
    tree: DiskBtreeBuilder<BlockBuf, KEY_SIZE>,
}

impl ImageLayerWriterInner {
    ///
    /// Start building a new image layer.
    ///
    fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        key_range: &Range<Key>,
        lsn: Lsn,
        is_incremental: bool,
    ) -> anyhow::Result<Self> {
        // Create the file initially with a temporary filename.
        // We'll atomically rename it to the final name when we're done.
        let path = ImageLayer::temp_path_for(
            conf,
            timeline_id,
            tenant_id,
            &ImageFileName {
                key_range: key_range.clone(),
                lsn,
            },
        );
        info!("new image layer {}", path.display());
        let mut file = VirtualFile::open_with_options(
            &path,
            std::fs::OpenOptions::new().write(true).create_new(true),
        )?;
        // make room for the header block
        file.seek(SeekFrom::Start(PAGE_SZ as u64))?;
        let blob_writer = WriteBlobWriter::new(file, PAGE_SZ as u64);

        // Initialize the b-tree index builder
        let block_buf = BlockBuf::new();
        let tree_builder = DiskBtreeBuilder::new(block_buf);

        let writer = Self {
            conf,
            path,
            timeline_id,
            tenant_id,
            key_range: key_range.clone(),
            lsn,
            tree: tree_builder,
            blob_writer,
            is_incremental,
        };

        Ok(writer)
    }

    ///
    /// Write next value to the file.
    ///
    /// The page versions must be appended in blknum order.
    ///
    fn put_image(&mut self, key: Key, img: &[u8]) -> anyhow::Result<()> {
        ensure!(self.key_range.contains(&key));
        let off = self.blob_writer.write_blob(img)?;

        let mut keybuf: [u8; KEY_SIZE] = [0u8; KEY_SIZE];
        key.write_to_byte_slice(&mut keybuf);
        self.tree.append(&keybuf, off)?;

        Ok(())
    }

    ///
    /// Finish writing the image layer.
    ///
    fn finish(self) -> anyhow::Result<ImageLayer> {
        let index_start_blk =
            ((self.blob_writer.size() + PAGE_SZ as u64 - 1) / PAGE_SZ as u64) as u32;

        let mut file = self.blob_writer.into_inner();

        // Write out the index
        file.seek(SeekFrom::Start(index_start_blk as u64 * PAGE_SZ as u64))?;
        let (index_root_blk, block_buf) = self.tree.finish()?;
        for buf in block_buf.blocks {
            file.write_all(buf.as_ref())?;
        }

        // Fill in the summary on blk 0
        let summary = Summary {
            magic: IMAGE_FILE_MAGIC,
            format_version: STORAGE_FORMAT_VERSION,
            tenant_id: self.tenant_id,
            timeline_id: self.timeline_id,
            key_range: self.key_range.clone(),
            lsn: self.lsn,
            index_start_blk,
            index_root_blk,
        };
        file.seek(SeekFrom::Start(0))?;
        Summary::ser_into(&summary, &mut file)?;

        let metadata = file
            .metadata()
            .context("get metadata to determine file size")?;

        let desc = PersistentLayerDesc::new_img(
            self.tenant_id,
            self.timeline_id,
            self.key_range.clone(),
            self.lsn,
            self.is_incremental, // for now, image layer ALWAYS covers the full range
            metadata.len(),
        );

        // Note: Because we open the file in write-only mode, we cannot
        // reuse the same VirtualFile for reading later. That's why we don't
        // set inner.file here. The first read will have to re-open it.
        let layer = ImageLayer {
            path_or_conf: PathOrConf::Conf(self.conf),
            desc,
            lsn: self.lsn,
            access_stats: LayerAccessStats::empty_will_record_residence_event_later(),
            inner: RwLock::new(ImageLayerInner {
                loaded: false,
                file: None,
                index_start_blk,
                index_root_blk,
            }),
        };

        // fsync the file
        file.sync_all()?;

        // Rename the file to its final name
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let final_path = ImageLayer::path_for(
            &PathOrConf::Conf(self.conf),
            self.timeline_id,
            self.tenant_id,
            &ImageFileName {
                key_range: self.key_range.clone(),
                lsn: self.lsn,
            },
        );
        std::fs::rename(self.path, final_path)?;

        trace!("created image layer {}", layer.path().display());

        Ok(layer)
    }
}

/// A builder object for constructing a new image layer.
///
/// Usage:
///
/// 1. Create the ImageLayerWriter by calling ImageLayerWriter::new(...)
///
/// 2. Write the contents by calling `put_page_image` for every key-value
///    pair in the key range.
///
/// 3. Call `finish`.
///
/// # Note
///
/// As described in <https://github.com/neondatabase/neon/issues/2650>, it's
/// possible for the writer to drop before `finish` is actually called. So this
/// could lead to odd temporary files in the directory, exhausting file system.
/// This structure wraps `ImageLayerWriterInner` and also contains `Drop`
/// implementation that cleans up the temporary file in failure. It's not
/// possible to do this directly in `ImageLayerWriterInner` since `finish` moves
/// out some fields, making it impossible to implement `Drop`.
///
#[must_use]
pub struct ImageLayerWriter {
    inner: Option<ImageLayerWriterInner>,
}

impl ImageLayerWriter {
    ///
    /// Start building a new image layer.
    ///
    pub fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        key_range: &Range<Key>,
        lsn: Lsn,
        is_incremental: bool,
    ) -> anyhow::Result<ImageLayerWriter> {
        Ok(Self {
            inner: Some(ImageLayerWriterInner::new(
                conf,
                timeline_id,
                tenant_id,
                key_range,
                lsn,
                is_incremental,
            )?),
        })
    }

    ///
    /// Write next value to the file.
    ///
    /// The page versions must be appended in blknum order.
    ///
    pub fn put_image(&mut self, key: Key, img: &[u8]) -> anyhow::Result<()> {
        self.inner.as_mut().unwrap().put_image(key, img)
    }

    ///
    /// Finish writing the image layer.
    ///
    pub fn finish(mut self) -> anyhow::Result<ImageLayer> {
        self.inner.take().unwrap().finish()
    }
}

impl Drop for ImageLayerWriter {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.blob_writer.into_inner().remove();
        }
    }
}
