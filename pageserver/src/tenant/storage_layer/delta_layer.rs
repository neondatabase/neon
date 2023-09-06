//! A DeltaLayer represents a collection of WAL records or page images in a range of
//! LSNs, and in a range of Keys. It is stored on a file on disk.
//!
//! Usually a delta layer only contains differences, in the form of WAL records
//! against a base LSN. However, if a relation extended or a whole new relation
//! is created, there would be no base for the new pages. The entries for them
//! must be page images or WAL records with the 'will_init' flag set, so that
//! they can be replayed without referring to an older page version.
//!
//! The delta files are stored in `timelines/<timeline_id>` directory.  Currently,
//! there are no subdirectories, and each delta file is named like this:
//!
//! ```text
//!    <key start>-<key end>__<start LSN>-<end LSN>
//! ```
//!
//! For example:
//!
//! ```text
//!    000000067F000032BE0000400000000020B6-000000067F000032BE0000400000000030B6__000000578C6B29-0000000057A50051
//! ```
//!
//! Every delta file consists of three parts: "summary", "index", and
//! "values". The summary is a fixed size header at the beginning of the file,
//! and it contains basic information about the layer, and offsets to the other
//! parts. The "index" is a B-tree, mapping from Key and LSN to an offset in the
//! "values" part.  The actual page images and WAL records are stored in the
//! "values" part.
//!
use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::page_cache::PAGE_SZ;
use crate::repository::{Key, Value, KEY_SIZE};
use crate::tenant::blob_io::BlobWriter;
use crate::tenant::block_io::{BlockBuf, BlockCursor, BlockLease, BlockReader, FileBlockReader};
use crate::tenant::disk_btree::{DiskBtreeBuilder, DiskBtreeReader, VisitDirection};
use crate::tenant::storage_layer::{
    PersistentLayer, ValueReconstructResult, ValueReconstructState,
};
use crate::virtual_file::VirtualFile;
use crate::{walrecord, TEMP_FILE_SUFFIX};
use crate::{DELTA_FILE_MAGIC, STORAGE_FORMAT_VERSION};
use anyhow::{bail, ensure, Context, Result};
use pageserver_api::models::{HistoricLayerInfo, LayerAccessKind};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::SeekFrom;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::OnceCell;
use tracing::*;

use utils::{
    bin_ser::BeSer,
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use super::{
    AsLayerDesc, DeltaFileName, Layer, LayerAccessStats, LayerAccessStatsReset, PathOrConf,
    PersistentLayerDesc,
};

///
/// Header stored in the beginning of the file
///
/// After this comes the 'values' part, starting on block 1. After that,
/// the 'index' starts at the block indicated by 'index_start_blk'
///
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Summary {
    /// Magic value to identify this as a neon delta file. Always DELTA_FILE_MAGIC.
    magic: u16,
    format_version: u16,

    tenant_id: TenantId,
    timeline_id: TimelineId,
    key_range: Range<Key>,
    lsn_range: Range<Lsn>,

    /// Block number where the 'index' part of the file begins.
    pub index_start_blk: u32,
    /// Block within the 'index', where the B-tree root page is stored
    pub index_root_blk: u32,
}

impl From<&DeltaLayer> for Summary {
    fn from(layer: &DeltaLayer) -> Self {
        Self::expected(
            layer.desc.tenant_id,
            layer.desc.timeline_id,
            layer.desc.key_range.clone(),
            layer.desc.lsn_range.clone(),
        )
    }
}

impl Summary {
    pub(super) fn expected(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        keys: Range<Key>,
        lsns: Range<Lsn>,
    ) -> Self {
        Self {
            magic: DELTA_FILE_MAGIC,
            format_version: STORAGE_FORMAT_VERSION,

            tenant_id,
            timeline_id,
            key_range: keys,
            lsn_range: lsns,

            index_start_blk: 0,
            index_root_blk: 0,
        }
    }
}

// Flag indicating that this version initialize the page
const WILL_INIT: u64 = 1;

/// Struct representing reference to BLOB in layers. Reference contains BLOB
/// offset, and for WAL records it also contains `will_init` flag. The flag
/// helps to determine the range of records that needs to be applied, without
/// reading/deserializing records themselves.
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct BlobRef(pub u64);

impl BlobRef {
    pub fn will_init(&self) -> bool {
        (self.0 & WILL_INIT) != 0
    }

    pub fn pos(&self) -> u64 {
        self.0 >> 1
    }

    pub fn new(pos: u64, will_init: bool) -> BlobRef {
        let mut blob_ref = pos << 1;
        if will_init {
            blob_ref |= WILL_INIT;
        }
        BlobRef(blob_ref)
    }
}

pub const DELTA_KEY_SIZE: usize = KEY_SIZE + 8;
struct DeltaKey([u8; DELTA_KEY_SIZE]);

/// This is the key of the B-tree index stored in the delta layer. It consists
/// of the serialized representation of a Key and LSN.
impl DeltaKey {
    fn from_slice(buf: &[u8]) -> Self {
        let mut bytes: [u8; DELTA_KEY_SIZE] = [0u8; DELTA_KEY_SIZE];
        bytes.copy_from_slice(buf);
        DeltaKey(bytes)
    }

    fn from_key_lsn(key: &Key, lsn: Lsn) -> Self {
        let mut bytes: [u8; DELTA_KEY_SIZE] = [0u8; DELTA_KEY_SIZE];
        key.write_to_byte_slice(&mut bytes[0..KEY_SIZE]);
        bytes[KEY_SIZE..].copy_from_slice(&u64::to_be_bytes(lsn.0));
        DeltaKey(bytes)
    }

    fn key(&self) -> Key {
        Key::from_slice(&self.0)
    }

    fn lsn(&self) -> Lsn {
        Lsn(u64::from_be_bytes(self.0[KEY_SIZE..].try_into().unwrap()))
    }

    fn extract_lsn_from_buf(buf: &[u8]) -> Lsn {
        let mut lsn_buf = [0u8; 8];
        lsn_buf.copy_from_slice(&buf[KEY_SIZE..]);
        Lsn(u64::from_be_bytes(lsn_buf))
    }
}

/// DeltaLayer is the in-memory data structure associated with an on-disk delta
/// file.
///
/// We keep a DeltaLayer in memory for each file, in the LayerMap. If a layer
/// is in "loaded" state, we have a copy of the index in memory, in 'inner'.
/// Otherwise the struct is just a placeholder for a file that exists on disk,
/// and it needs to be loaded before using it in queries.
pub struct DeltaLayer {
    path_or_conf: PathOrConf,

    pub desc: PersistentLayerDesc,

    access_stats: LayerAccessStats,

    inner: OnceCell<Arc<DeltaLayerInner>>,
}

impl std::fmt::Debug for DeltaLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use super::RangeDisplayDebug;

        f.debug_struct("DeltaLayer")
            .field("key_range", &RangeDisplayDebug(&self.desc.key_range))
            .field("lsn_range", &self.desc.lsn_range)
            .field("file_size", &self.desc.file_size)
            .field("inner", &self.inner)
            .finish()
    }
}

pub struct DeltaLayerInner {
    // values copied from summary
    index_start_blk: u32,
    index_root_blk: u32,

    /// Reader object for reading blocks from the file.
    file: FileBlockReader,
}

impl AsRef<DeltaLayerInner> for DeltaLayerInner {
    fn as_ref(&self) -> &DeltaLayerInner {
        self
    }
}

impl std::fmt::Debug for DeltaLayerInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaLayerInner")
            .field("index_start_blk", &self.index_start_blk)
            .field("index_root_blk", &self.index_root_blk)
            .finish()
    }
}

#[async_trait::async_trait]
impl Layer for DeltaLayer {
    async fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValueReconstructState,
        ctx: &RequestContext,
    ) -> anyhow::Result<ValueReconstructResult> {
        self.get_value_reconstruct_data(key, lsn_range, reconstruct_state, ctx)
            .await
    }
}
/// Boilerplate to implement the Layer trait, always use layer_desc for persistent layers.
impl std::fmt::Display for DeltaLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.layer_desc().short_id())
    }
}

impl AsLayerDesc for DeltaLayer {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        &self.desc
    }
}

impl PersistentLayer for DeltaLayer {
    fn downcast_delta_layer(self: Arc<Self>) -> Option<std::sync::Arc<DeltaLayer>> {
        Some(self)
    }

    fn local_path(&self) -> Option<PathBuf> {
        self.local_path()
    }

    fn delete_resident_layer_file(&self) -> Result<()> {
        self.delete_resident_layer_file()
    }

    fn info(&self, reset: LayerAccessStatsReset) -> HistoricLayerInfo {
        self.info(reset)
    }

    fn access_stats(&self) -> &LayerAccessStats {
        self.access_stats()
    }
}

impl DeltaLayer {
    pub(crate) async fn dump(&self, verbose: bool, ctx: &RequestContext) -> Result<()> {
        println!(
            "----- delta layer for ten {} tli {} keys {}-{} lsn {}-{} size {} ----",
            self.desc.tenant_id,
            self.desc.timeline_id,
            self.desc.key_range.start,
            self.desc.key_range.end,
            self.desc.lsn_range.start,
            self.desc.lsn_range.end,
            self.desc.file_size,
        );

        if !verbose {
            return Ok(());
        }

        let inner = self.load(LayerAccessKind::Dump, ctx).await?;

        println!(
            "index_start_blk: {}, root {}",
            inner.index_start_blk, inner.index_root_blk
        );

        let file = &inner.file;
        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            inner.index_start_blk,
            inner.index_root_blk,
            file,
        );

        tree_reader.dump().await?;

        let keys = DeltaLayerInner::load_keys(&inner).await?;

        // A subroutine to dump a single blob
        async fn dump_blob(val: ValueRef<'_>) -> Result<String> {
            let buf = val.reader.read_blob(val.blob_ref.pos()).await?;
            let val = Value::des(&buf)?;
            let desc = match val {
                Value::Image(img) => {
                    format!(" img {} bytes", img.len())
                }
                Value::WalRecord(rec) => {
                    let wal_desc = walrecord::describe_wal_record(&rec)?;
                    format!(
                        " rec {} bytes will_init: {} {}",
                        buf.len(),
                        rec.will_init(),
                        wal_desc
                    )
                }
            };
            Ok(desc)
        }

        for entry in keys {
            let DeltaEntry { key, lsn, val, .. } = entry;
            let desc = match dump_blob(val).await {
                Ok(desc) => desc,
                Err(err) => {
                    let err: anyhow::Error = err;
                    format!("ERROR: {err}")
                }
            };
            println!("  key {key} at {lsn}: {desc}");
        }

        Ok(())
    }

    pub(crate) async fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValueReconstructState,
        ctx: &RequestContext,
    ) -> anyhow::Result<ValueReconstructResult> {
        ensure!(lsn_range.start >= self.desc.lsn_range.start);

        ensure!(self.desc.key_range.contains(&key));

        let inner = self
            .load(LayerAccessKind::GetValueReconstructData, ctx)
            .await?;
        inner
            .get_value_reconstruct_data(key, lsn_range, reconstruct_state)
            .await
    }

    pub(crate) fn local_path(&self) -> Option<PathBuf> {
        Some(self.path())
    }

    pub(crate) fn delete_resident_layer_file(&self) -> Result<()> {
        // delete underlying file
        fs::remove_file(self.path())?;
        Ok(())
    }

    pub(crate) fn info(&self, reset: LayerAccessStatsReset) -> HistoricLayerInfo {
        let layer_file_name = self.layer_desc().filename().file_name();
        let lsn_range = self.layer_desc().lsn_range.clone();

        let access_stats = self.access_stats.as_api_model(reset);

        HistoricLayerInfo::Delta {
            layer_file_name,
            layer_file_size: self.desc.file_size,
            lsn_start: lsn_range.start,
            lsn_end: lsn_range.end,
            remote: false,
            access_stats,
        }
    }

    pub(crate) fn access_stats(&self) -> &LayerAccessStats {
        &self.access_stats
    }

    fn path_for(
        path_or_conf: &PathOrConf,
        tenant_id: &TenantId,
        timeline_id: &TimelineId,
        fname: &DeltaFileName,
    ) -> PathBuf {
        match path_or_conf {
            PathOrConf::Path(path) => path.clone(),
            PathOrConf::Conf(conf) => conf
                .timeline_path(tenant_id, timeline_id)
                .join(fname.to_string()),
        }
    }

    fn temp_path_for(
        conf: &PageServerConf,
        tenant_id: &TenantId,
        timeline_id: &TimelineId,
        key_start: Key,
        lsn_range: &Range<Lsn>,
    ) -> PathBuf {
        let rand_string: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();

        conf.timeline_path(tenant_id, timeline_id).join(format!(
            "{}-XXX__{:016X}-{:016X}.{}.{}",
            key_start,
            u64::from(lsn_range.start),
            u64::from(lsn_range.end),
            rand_string,
            TEMP_FILE_SUFFIX,
        ))
    }

    ///
    /// Open the underlying file and read the metadata into memory, if it's
    /// not loaded already.
    ///
    async fn load(
        &self,
        access_kind: LayerAccessKind,
        ctx: &RequestContext,
    ) -> Result<&Arc<DeltaLayerInner>> {
        self.access_stats.record_access(access_kind, ctx);
        // Quick exit if already loaded
        self.inner
            .get_or_try_init(|| self.load_inner())
            .await
            .with_context(|| format!("Failed to load delta layer {}", self.path().display()))
    }

    async fn load_inner(&self) -> Result<Arc<DeltaLayerInner>> {
        let path = self.path();

        let summary = match &self.path_or_conf {
            PathOrConf::Conf(_) => Some(Summary::from(self)),
            PathOrConf::Path(_) => None,
        };

        let loaded = DeltaLayerInner::load(&path, summary).await?;

        if let PathOrConf::Path(ref path) = self.path_or_conf {
            // not production code

            let actual_filename = path.file_name().unwrap().to_str().unwrap().to_owned();
            let expected_filename = self.filename().file_name();

            if actual_filename != expected_filename {
                println!("warning: filename does not match what is expected from in-file summary");
                println!("actual: {:?}", actual_filename);
                println!("expected: {:?}", expected_filename);
            }
        }

        Ok(Arc::new(loaded))
    }

    /// Create a DeltaLayer struct representing an existing file on disk.
    pub fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        filename: &DeltaFileName,
        file_size: u64,
        access_stats: LayerAccessStats,
    ) -> DeltaLayer {
        DeltaLayer {
            path_or_conf: PathOrConf::Conf(conf),
            desc: PersistentLayerDesc::new_delta(
                tenant_id,
                timeline_id,
                filename.key_range.clone(),
                filename.lsn_range.clone(),
                file_size,
            ),
            access_stats,
            inner: OnceCell::new(),
        }
    }

    /// Create a DeltaLayer struct representing an existing file on disk.
    ///
    /// This variant is only used for debugging purposes, by the 'pagectl' binary.
    pub fn new_for_path(path: &Path, file: File) -> Result<Self> {
        let mut summary_buf = Vec::new();
        summary_buf.resize(PAGE_SZ, 0);
        file.read_exact_at(&mut summary_buf, 0)?;
        let summary = Summary::des_prefix(&summary_buf)?;

        let metadata = file
            .metadata()
            .context("get file metadata to determine size")?;

        Ok(DeltaLayer {
            path_or_conf: PathOrConf::Path(path.to_path_buf()),
            desc: PersistentLayerDesc::new_delta(
                summary.tenant_id,
                summary.timeline_id,
                summary.key_range,
                summary.lsn_range,
                metadata.len(),
            ),
            access_stats: LayerAccessStats::empty_will_record_residence_event_later(),
            inner: OnceCell::new(),
        })
    }

    fn layer_name(&self) -> DeltaFileName {
        self.desc.delta_file_name()
    }
    /// Path to the layer file in pageserver workdir.
    pub fn path(&self) -> PathBuf {
        Self::path_for(
            &self.path_or_conf,
            &self.desc.tenant_id,
            &self.desc.timeline_id,
            &self.layer_name(),
        )
    }
    /// Loads all keys stored in the layer. Returns key, lsn, value size and value reference.
    ///
    /// The value can be obtained via the [`ValueRef::load`] function.
    pub(crate) async fn load_keys(&self, ctx: &RequestContext) -> Result<Vec<DeltaEntry<'_>>> {
        let inner = self
            .load(LayerAccessKind::KeyIter, ctx)
            .await
            .context("load delta layer keys")?;
        DeltaLayerInner::load_keys(inner)
            .await
            .context("Layer index is corrupted")
    }
}

/// A builder object for constructing a new delta layer.
///
/// Usage:
///
/// 1. Create the DeltaLayerWriter by calling DeltaLayerWriter::new(...)
///
/// 2. Write the contents by calling `put_value` for every page
///    version to store in the layer.
///
/// 3. Call `finish`.
///
struct DeltaLayerWriterInner {
    conf: &'static PageServerConf,
    pub path: PathBuf,
    timeline_id: TimelineId,
    tenant_id: TenantId,

    key_start: Key,
    lsn_range: Range<Lsn>,

    tree: DiskBtreeBuilder<BlockBuf, DELTA_KEY_SIZE>,

    blob_writer: BlobWriter<true>,
}

impl DeltaLayerWriterInner {
    ///
    /// Start building a new delta layer.
    ///
    async fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        key_start: Key,
        lsn_range: Range<Lsn>,
    ) -> anyhow::Result<Self> {
        // Create the file initially with a temporary filename. We don't know
        // the end key yet, so we cannot form the final filename yet. We will
        // rename it when we're done.
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let path = DeltaLayer::temp_path_for(conf, &tenant_id, &timeline_id, key_start, &lsn_range);

        let mut file = VirtualFile::create(&path).await?;
        // make room for the header block
        file.seek(SeekFrom::Start(PAGE_SZ as u64)).await?;
        let blob_writer = BlobWriter::new(file, PAGE_SZ as u64);

        // Initialize the b-tree index builder
        let block_buf = BlockBuf::new();
        let tree_builder = DiskBtreeBuilder::new(block_buf);

        Ok(Self {
            conf,
            path,
            timeline_id,
            tenant_id,
            key_start,
            lsn_range,
            tree: tree_builder,
            blob_writer,
        })
    }

    ///
    /// Append a key-value pair to the file.
    ///
    /// The values must be appended in key, lsn order.
    ///
    async fn put_value(&mut self, key: Key, lsn: Lsn, val: Value) -> anyhow::Result<()> {
        self.put_value_bytes(key, lsn, &Value::ser(&val)?, val.will_init())
            .await
    }

    async fn put_value_bytes(
        &mut self,
        key: Key,
        lsn: Lsn,
        val: &[u8],
        will_init: bool,
    ) -> anyhow::Result<()> {
        assert!(self.lsn_range.start <= lsn);

        let off = self.blob_writer.write_blob(val).await?;

        let blob_ref = BlobRef::new(off, will_init);

        let delta_key = DeltaKey::from_key_lsn(&key, lsn);
        self.tree.append(&delta_key.0, blob_ref.0)?;

        Ok(())
    }

    fn size(&self) -> u64 {
        self.blob_writer.size() + self.tree.borrow_writer().size()
    }

    ///
    /// Finish writing the delta layer.
    ///
    async fn finish(self, key_end: Key) -> anyhow::Result<DeltaLayer> {
        let index_start_blk =
            ((self.blob_writer.size() + PAGE_SZ as u64 - 1) / PAGE_SZ as u64) as u32;

        let mut file = self.blob_writer.into_inner().await?;

        // Write out the index
        let (index_root_blk, block_buf) = self.tree.finish()?;
        file.seek(SeekFrom::Start(index_start_blk as u64 * PAGE_SZ as u64))
            .await?;
        for buf in block_buf.blocks {
            file.write_all(buf.as_ref()).await?;
        }
        assert!(self.lsn_range.start < self.lsn_range.end);
        // Fill in the summary on blk 0
        let summary = Summary {
            magic: DELTA_FILE_MAGIC,
            format_version: STORAGE_FORMAT_VERSION,
            tenant_id: self.tenant_id,
            timeline_id: self.timeline_id,
            key_range: self.key_start..key_end,
            lsn_range: self.lsn_range.clone(),
            index_start_blk,
            index_root_blk,
        };

        let mut buf = smallvec::SmallVec::<[u8; PAGE_SZ]>::new();
        Summary::ser_into(&summary, &mut buf)?;
        if buf.spilled() {
            // This is bad as we only have one free block for the summary
            warn!(
                "Used more than one page size for summary buffer: {}",
                buf.len()
            );
        }
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(&buf).await?;

        let metadata = file
            .metadata()
            .await
            .context("get file metadata to determine size")?;

        // 5GB limit for objects without multipart upload (which we don't want to use)
        // Make it a little bit below to account for differing GB units
        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html
        const S3_UPLOAD_LIMIT: u64 = 4_500_000_000;
        ensure!(
            metadata.len() <= S3_UPLOAD_LIMIT,
            "Created delta layer file at {} of size {} above limit {S3_UPLOAD_LIMIT}!",
            file.path.display(),
            metadata.len()
        );

        // Note: Because we opened the file in write-only mode, we cannot
        // reuse the same VirtualFile for reading later. That's why we don't
        // set inner.file here. The first read will have to re-open it.
        let layer = DeltaLayer {
            path_or_conf: PathOrConf::Conf(self.conf),
            desc: PersistentLayerDesc::new_delta(
                self.tenant_id,
                self.timeline_id,
                self.key_start..key_end,
                self.lsn_range.clone(),
                metadata.len(),
            ),
            access_stats: LayerAccessStats::empty_will_record_residence_event_later(),
            inner: OnceCell::new(),
        };

        // fsync the file
        file.sync_all().await?;
        // Rename the file to its final name
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let final_path = DeltaLayer::path_for(
            &PathOrConf::Conf(self.conf),
            &self.tenant_id,
            &self.timeline_id,
            &DeltaFileName {
                key_range: self.key_start..key_end,
                lsn_range: self.lsn_range,
            },
        );
        std::fs::rename(self.path, &final_path)?;

        trace!("created delta layer {}", final_path.display());

        Ok(layer)
    }
}

/// A builder object for constructing a new delta layer.
///
/// Usage:
///
/// 1. Create the DeltaLayerWriter by calling DeltaLayerWriter::new(...)
///
/// 2. Write the contents by calling `put_value` for every page
///    version to store in the layer.
///
/// 3. Call `finish`.
///
/// # Note
///
/// As described in <https://github.com/neondatabase/neon/issues/2650>, it's
/// possible for the writer to drop before `finish` is actually called. So this
/// could lead to odd temporary files in the directory, exhausting file system.
/// This structure wraps `DeltaLayerWriterInner` and also contains `Drop`
/// implementation that cleans up the temporary file in failure. It's not
/// possible to do this directly in `DeltaLayerWriterInner` since `finish` moves
/// out some fields, making it impossible to implement `Drop`.
///
#[must_use]
pub struct DeltaLayerWriter {
    inner: Option<DeltaLayerWriterInner>,
}

impl DeltaLayerWriter {
    ///
    /// Start building a new delta layer.
    ///
    pub async fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        key_start: Key,
        lsn_range: Range<Lsn>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Some(
                DeltaLayerWriterInner::new(conf, timeline_id, tenant_id, key_start, lsn_range)
                    .await?,
            ),
        })
    }

    ///
    /// Append a key-value pair to the file.
    ///
    /// The values must be appended in key, lsn order.
    ///
    pub async fn put_value(&mut self, key: Key, lsn: Lsn, val: Value) -> anyhow::Result<()> {
        self.inner.as_mut().unwrap().put_value(key, lsn, val).await
    }

    pub async fn put_value_bytes(
        &mut self,
        key: Key,
        lsn: Lsn,
        val: &[u8],
        will_init: bool,
    ) -> anyhow::Result<()> {
        self.inner
            .as_mut()
            .unwrap()
            .put_value_bytes(key, lsn, val, will_init)
            .await
    }

    pub fn size(&self) -> u64 {
        self.inner.as_ref().unwrap().size()
    }

    ///
    /// Finish writing the delta layer.
    ///
    pub async fn finish(mut self, key_end: Key) -> anyhow::Result<DeltaLayer> {
        self.inner.take().unwrap().finish(key_end).await
    }
}

impl Drop for DeltaLayerWriter {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            // We want to remove the virtual file here, so it's fine to not
            // having completely flushed unwritten data.
            let vfile = inner.blob_writer.into_inner_no_flush();
            vfile.remove();
        }
    }
}

impl DeltaLayerInner {
    pub(super) async fn load(
        path: &std::path::Path,
        summary: Option<Summary>,
    ) -> anyhow::Result<Self> {
        let file = VirtualFile::open(path)
            .await
            .with_context(|| format!("Failed to open file '{}'", path.display()))?;
        let file = FileBlockReader::new(file);

        let summary_blk = file.read_blk(0).await?;
        let actual_summary = Summary::des_prefix(summary_blk.as_ref())?;

        if let Some(mut expected_summary) = summary {
            // production code path
            expected_summary.index_start_blk = actual_summary.index_start_blk;
            expected_summary.index_root_blk = actual_summary.index_root_blk;
            if actual_summary != expected_summary {
                bail!(
                    "in-file summary does not match expected summary. actual = {:?} expected = {:?}",
                    actual_summary,
                    expected_summary
                );
            }
        }

        Ok(DeltaLayerInner {
            file,
            index_start_blk: actual_summary.index_start_blk,
            index_root_blk: actual_summary.index_root_blk,
        })
    }

    pub(super) async fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValueReconstructState,
    ) -> anyhow::Result<ValueReconstructResult> {
        let mut need_image = true;
        // Scan the page versions backwards, starting from `lsn`.
        let file = &self.file;
        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            self.index_start_blk,
            self.index_root_blk,
            file,
        );
        let search_key = DeltaKey::from_key_lsn(&key, Lsn(lsn_range.end.0 - 1));

        let mut offsets: Vec<(Lsn, u64)> = Vec::new();

        tree_reader
            .visit(&search_key.0, VisitDirection::Backwards, |key, value| {
                let blob_ref = BlobRef(value);
                if key[..KEY_SIZE] != search_key.0[..KEY_SIZE] {
                    return false;
                }
                let entry_lsn = DeltaKey::extract_lsn_from_buf(key);
                if entry_lsn < lsn_range.start {
                    return false;
                }
                offsets.push((entry_lsn, blob_ref.pos()));

                !blob_ref.will_init()
            })
            .await?;

        // Ok, 'offsets' now contains the offsets of all the entries we need to read
        let cursor = file.block_cursor();
        let mut buf = Vec::new();
        for (entry_lsn, pos) in offsets {
            cursor
                .read_blob_into_buf(pos, &mut buf)
                .await
                .with_context(|| {
                    format!(
                        "Failed to read blob from virtual file {}",
                        file.file.path.display()
                    )
                })?;
            let val = Value::des(&buf).with_context(|| {
                format!(
                    "Failed to deserialize file blob from virtual file {}",
                    file.file.path.display()
                )
            })?;
            match val {
                Value::Image(img) => {
                    reconstruct_state.img = Some((entry_lsn, img));
                    need_image = false;
                    break;
                }
                Value::WalRecord(rec) => {
                    let will_init = rec.will_init();
                    reconstruct_state.records.push((entry_lsn, rec));
                    if will_init {
                        // This WAL record initializes the page, so no need to go further back
                        need_image = false;
                        break;
                    }
                }
            }
        }

        // If an older page image is needed to reconstruct the page, let the
        // caller know.
        if need_image {
            Ok(ValueReconstructResult::Continue)
        } else {
            Ok(ValueReconstructResult::Complete)
        }
    }

    pub(super) async fn load_keys<T: AsRef<DeltaLayerInner> + Clone>(
        this: &T,
    ) -> Result<Vec<DeltaEntry<'_>>> {
        let dl = this.as_ref();
        let file = &dl.file;

        let tree_reader =
            DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(dl.index_start_blk, dl.index_root_blk, file);

        let mut all_keys: Vec<DeltaEntry<'_>> = Vec::new();

        tree_reader
            .visit(
                &[0u8; DELTA_KEY_SIZE],
                VisitDirection::Forwards,
                |key, value| {
                    let delta_key = DeltaKey::from_slice(key);
                    let val_ref = ValueRef {
                        blob_ref: BlobRef(value),
                        reader: BlockCursor::new(crate::tenant::block_io::BlockReaderRef::Adapter(
                            Adapter(dl),
                        )),
                    };
                    let pos = BlobRef(value).pos();
                    if let Some(last) = all_keys.last_mut() {
                        // subtract offset of the current and last entries to get the size
                        // of the value associated with this (key, lsn) tuple
                        let first_pos = last.size;
                        last.size = pos - first_pos;
                    }
                    let entry = DeltaEntry {
                        key: delta_key.key(),
                        lsn: delta_key.lsn(),
                        size: pos,
                        val: val_ref,
                    };
                    all_keys.push(entry);
                    true
                },
            )
            .await?;
        if let Some(last) = all_keys.last_mut() {
            // Last key occupies all space till end of value storage,
            // which corresponds to beginning of the index
            last.size = dl.index_start_blk as u64 * PAGE_SZ as u64 - last.size;
        }
        Ok(all_keys)
    }
}

/// A set of data associated with a delta layer key and its value
pub struct DeltaEntry<'a> {
    pub key: Key,
    pub lsn: Lsn,
    /// Size of the stored value
    pub size: u64,
    /// Reference to the on-disk value
    pub val: ValueRef<'a>,
}

/// Reference to an on-disk value
pub struct ValueRef<'a> {
    blob_ref: BlobRef,
    reader: BlockCursor<'a>,
}

impl<'a> ValueRef<'a> {
    /// Loads the value from disk
    pub async fn load(&self) -> Result<Value> {
        // theoretically we *could* record an access time for each, but it does not really matter
        let buf = self.reader.read_blob(self.blob_ref.pos()).await?;
        let val = Value::des(&buf)?;
        Ok(val)
    }
}

pub(crate) struct Adapter<T>(T);

impl<T: AsRef<DeltaLayerInner>> Adapter<T> {
    pub(crate) async fn read_blk(&self, blknum: u32) -> Result<BlockLease, std::io::Error> {
        self.0.as_ref().file.read_blk(blknum).await
    }
}
