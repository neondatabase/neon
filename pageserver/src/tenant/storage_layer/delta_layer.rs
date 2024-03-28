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
//! Every delta file consists of three parts: "summary", "values", and
//! "index". The summary is a fixed size header at the beginning of the file,
//! and it contains basic information about the layer, and offsets to the other
//! parts. The "index" is a B-tree, mapping from Key and LSN to an offset in the
//! "values" part.  The actual page images and WAL records are stored in the
//! "values" part.
//!
use crate::config::PageServerConf;
use crate::context::{PageContentKind, RequestContext, RequestContextBuilder};
use crate::page_cache::{self, FileId, PAGE_SZ};
use crate::repository::{Key, Value, KEY_SIZE};
use crate::tenant::blob_io::BlobWriter;
use crate::tenant::block_io::{BlockBuf, BlockCursor, BlockLease, BlockReader, FileBlockReader};
use crate::tenant::disk_btree::{DiskBtreeBuilder, DiskBtreeReader, VisitDirection};
use crate::tenant::storage_layer::{Layer, ValueReconstructResult, ValueReconstructState};
use crate::tenant::timeline::GetVectoredError;
use crate::tenant::vectored_blob_io::{
    BlobFlag, MaxVectoredReadBytes, VectoredBlobReader, VectoredRead, VectoredReadPlanner,
};
use crate::tenant::{PageReconstructError, Timeline};
use crate::virtual_file::{self, VirtualFile};
use crate::{walrecord, TEMP_FILE_SUFFIX};
use crate::{DELTA_FILE_MAGIC, STORAGE_FORMAT_VERSION};
use anyhow::{anyhow, bail, ensure, Context, Result};
use bytes::BytesMut;
use camino::{Utf8Path, Utf8PathBuf};
use futures::StreamExt;
use pageserver_api::keyspace::KeySpace;
use pageserver_api::models::LayerAccessKind;
use pageserver_api::shard::TenantShardId;
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::SeekFrom;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tracing::*;

use utils::{
    bin_ser::BeSer,
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use super::{
    AsLayerDesc, LayerAccessStats, PersistentLayerDesc, ResidentLayer, ValuesReconstructState,
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
    pub magic: u16,
    pub format_version: u16,

    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub key_range: Range<Key>,
    pub lsn_range: Range<Lsn>,

    /// Block number where the 'index' part of the file begins.
    pub index_start_blk: u32,
    /// Block within the 'index', where the B-tree root page is stored
    pub index_root_blk: u32,
}

impl From<&DeltaLayer> for Summary {
    fn from(layer: &DeltaLayer) -> Self {
        Self::expected(
            layer.desc.tenant_shard_id.tenant_id,
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

/// This is used only from `pagectl`. Within pageserver, all layers are
/// [`crate::tenant::storage_layer::Layer`], which can hold a [`DeltaLayerInner`].
pub struct DeltaLayer {
    path: Utf8PathBuf,
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

/// `DeltaLayerInner` is the in-memory data structure associated with an on-disk delta
/// file.
pub struct DeltaLayerInner {
    // values copied from summary
    index_start_blk: u32,
    index_root_blk: u32,

    file: VirtualFile,
    file_id: FileId,

    max_vectored_read_bytes: Option<MaxVectoredReadBytes>,
}

impl std::fmt::Debug for DeltaLayerInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaLayerInner")
            .field("index_start_blk", &self.index_start_blk)
            .field("index_root_blk", &self.index_root_blk)
            .finish()
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

impl DeltaLayer {
    pub(crate) async fn dump(&self, verbose: bool, ctx: &RequestContext) -> Result<()> {
        self.desc.dump();

        if !verbose {
            return Ok(());
        }

        let inner = self.load(LayerAccessKind::Dump, ctx).await?;

        inner.dump(ctx).await
    }

    fn temp_path_for(
        conf: &PageServerConf,
        tenant_shard_id: &TenantShardId,
        timeline_id: &TimelineId,
        key_start: Key,
        lsn_range: &Range<Lsn>,
    ) -> Utf8PathBuf {
        let rand_string: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();

        conf.timeline_path(tenant_shard_id, timeline_id)
            .join(format!(
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
            .get_or_try_init(|| self.load_inner(ctx))
            .await
            .with_context(|| format!("Failed to load delta layer {}", self.path()))
    }

    async fn load_inner(&self, ctx: &RequestContext) -> Result<Arc<DeltaLayerInner>> {
        let path = self.path();

        let loaded = DeltaLayerInner::load(&path, None, None, ctx)
            .await
            .and_then(|res| res)?;

        // not production code
        let actual_filename = path.file_name().unwrap().to_owned();
        let expected_filename = self.layer_desc().filename().file_name();

        if actual_filename != expected_filename {
            println!("warning: filename does not match what is expected from in-file summary");
            println!("actual: {:?}", actual_filename);
            println!("expected: {:?}", expected_filename);
        }

        Ok(Arc::new(loaded))
    }

    /// Create a DeltaLayer struct representing an existing file on disk.
    ///
    /// This variant is only used for debugging purposes, by the 'pagectl' binary.
    pub fn new_for_path(path: &Utf8Path, file: File) -> Result<Self> {
        let mut summary_buf = vec![0; PAGE_SZ];
        file.read_exact_at(&mut summary_buf, 0)?;
        let summary = Summary::des_prefix(&summary_buf)?;

        let metadata = file
            .metadata()
            .context("get file metadata to determine size")?;

        // This function is never used for constructing layers in a running pageserver,
        // so it does not need an accurate TenantShardId.
        let tenant_shard_id = TenantShardId::unsharded(summary.tenant_id);

        Ok(DeltaLayer {
            path: path.to_path_buf(),
            desc: PersistentLayerDesc::new_delta(
                tenant_shard_id,
                summary.timeline_id,
                summary.key_range,
                summary.lsn_range,
                metadata.len(),
            ),
            access_stats: LayerAccessStats::empty_will_record_residence_event_later(),
            inner: OnceCell::new(),
        })
    }

    /// Path to the layer file in pageserver workdir.
    fn path(&self) -> Utf8PathBuf {
        self.path.clone()
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
    pub path: Utf8PathBuf,
    timeline_id: TimelineId,
    tenant_shard_id: TenantShardId,

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
        tenant_shard_id: TenantShardId,
        key_start: Key,
        lsn_range: Range<Lsn>,
    ) -> anyhow::Result<Self> {
        // Create the file initially with a temporary filename. We don't know
        // the end key yet, so we cannot form the final filename yet. We will
        // rename it when we're done.
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let path =
            DeltaLayer::temp_path_for(conf, &tenant_shard_id, &timeline_id, key_start, &lsn_range);

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
            tenant_shard_id,
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
        let (_, res) = self
            .put_value_bytes(key, lsn, Value::ser(&val)?, val.will_init())
            .await;
        res
    }

    async fn put_value_bytes(
        &mut self,
        key: Key,
        lsn: Lsn,
        val: Vec<u8>,
        will_init: bool,
    ) -> (Vec<u8>, anyhow::Result<()>) {
        assert!(self.lsn_range.start <= lsn);
        let (val, res) = self.blob_writer.write_blob(val).await;
        let off = match res {
            Ok(off) => off,
            Err(e) => return (val, Err(anyhow::anyhow!(e))),
        };

        let blob_ref = BlobRef::new(off, will_init);

        let delta_key = DeltaKey::from_key_lsn(&key, lsn);
        let res = self.tree.append(&delta_key.0, blob_ref.0);
        (val, res.map_err(|e| anyhow::anyhow!(e)))
    }

    fn size(&self) -> u64 {
        self.blob_writer.size() + self.tree.borrow_writer().size()
    }

    ///
    /// Finish writing the delta layer.
    ///
    async fn finish(self, key_end: Key, timeline: &Arc<Timeline>) -> anyhow::Result<ResidentLayer> {
        let index_start_blk =
            ((self.blob_writer.size() + PAGE_SZ as u64 - 1) / PAGE_SZ as u64) as u32;

        let mut file = self.blob_writer.into_inner().await?;

        // Write out the index
        let (index_root_blk, block_buf) = self.tree.finish()?;
        file.seek(SeekFrom::Start(index_start_blk as u64 * PAGE_SZ as u64))
            .await?;
        for buf in block_buf.blocks {
            let (_buf, res) = file.write_all(buf).await;
            res?;
        }
        assert!(self.lsn_range.start < self.lsn_range.end);
        // Fill in the summary on blk 0
        let summary = Summary {
            magic: DELTA_FILE_MAGIC,
            format_version: STORAGE_FORMAT_VERSION,
            tenant_id: self.tenant_shard_id.tenant_id,
            timeline_id: self.timeline_id,
            key_range: self.key_start..key_end,
            lsn_range: self.lsn_range.clone(),
            index_start_blk,
            index_root_blk,
        };

        let mut buf = Vec::with_capacity(PAGE_SZ);
        // TODO: could use smallvec here but it's a pain with Slice<T>
        Summary::ser_into(&summary, &mut buf)?;
        file.seek(SeekFrom::Start(0)).await?;
        let (_buf, res) = file.write_all(buf).await;
        res?;

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
            file.path,
            metadata.len()
        );

        // Note: Because we opened the file in write-only mode, we cannot
        // reuse the same VirtualFile for reading later. That's why we don't
        // set inner.file here. The first read will have to re-open it.

        let desc = PersistentLayerDesc::new_delta(
            self.tenant_shard_id,
            self.timeline_id,
            self.key_start..key_end,
            self.lsn_range.clone(),
            metadata.len(),
        );

        // fsync the file
        file.sync_all().await?;

        let layer = Layer::finish_creating(self.conf, timeline, desc, &self.path)?;

        trace!("created delta layer {}", layer.local_path());

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
        tenant_shard_id: TenantShardId,
        key_start: Key,
        lsn_range: Range<Lsn>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Some(
                DeltaLayerWriterInner::new(
                    conf,
                    timeline_id,
                    tenant_shard_id,
                    key_start,
                    lsn_range,
                )
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
        val: Vec<u8>,
        will_init: bool,
    ) -> (Vec<u8>, anyhow::Result<()>) {
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
    pub(crate) async fn finish(
        mut self,
        key_end: Key,
        timeline: &Arc<Timeline>,
    ) -> anyhow::Result<ResidentLayer> {
        let inner = self.inner.take().unwrap();
        let temp_path = inner.path.clone();
        let result = inner.finish(key_end, timeline).await;
        // The delta layer files can sometimes be really large. Clean them up.
        if result.is_err() {
            tracing::warn!(
                "Cleaning up temporary delta file {temp_path} after error during writing"
            );
            if let Err(e) = std::fs::remove_file(&temp_path) {
                tracing::warn!("Error cleaning up temporary delta layer file {temp_path}: {e:?}")
            }
        }
        result
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

#[derive(thiserror::Error, Debug)]
pub enum RewriteSummaryError {
    #[error("magic mismatch")]
    MagicMismatch,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<std::io::Error> for RewriteSummaryError {
    fn from(e: std::io::Error) -> Self {
        Self::Other(anyhow::anyhow!(e))
    }
}

impl DeltaLayer {
    pub async fn rewrite_summary<F>(
        path: &Utf8Path,
        rewrite: F,
        ctx: &RequestContext,
    ) -> Result<(), RewriteSummaryError>
    where
        F: Fn(Summary) -> Summary,
    {
        let mut file = VirtualFile::open_with_options(
            path,
            virtual_file::OpenOptions::new().read(true).write(true),
        )
        .await
        .with_context(|| format!("Failed to open file '{}'", path))?;
        let file_id = page_cache::next_file_id();
        let block_reader = FileBlockReader::new(&file, file_id);
        let summary_blk = block_reader.read_blk(0, ctx).await?;
        let actual_summary = Summary::des_prefix(summary_blk.as_ref()).context("deserialize")?;
        if actual_summary.magic != DELTA_FILE_MAGIC {
            return Err(RewriteSummaryError::MagicMismatch);
        }

        let new_summary = rewrite(actual_summary);

        let mut buf = Vec::with_capacity(PAGE_SZ);
        // TODO: could use smallvec here, but it's a pain with Slice<T>
        Summary::ser_into(&new_summary, &mut buf).context("serialize")?;
        file.seek(SeekFrom::Start(0)).await?;
        let (_buf, res) = file.write_all(buf).await;
        res?;
        Ok(())
    }
}

impl DeltaLayerInner {
    /// Returns nested result following Result<Result<_, OpErr>, Critical>:
    /// - inner has the success or transient failure
    /// - outer has the permanent failure
    pub(super) async fn load(
        path: &Utf8Path,
        summary: Option<Summary>,
        max_vectored_read_bytes: Option<MaxVectoredReadBytes>,
        ctx: &RequestContext,
    ) -> Result<Result<Self, anyhow::Error>, anyhow::Error> {
        let file = match VirtualFile::open(path).await {
            Ok(file) => file,
            Err(e) => return Ok(Err(anyhow::Error::new(e).context("open layer file"))),
        };
        let file_id = page_cache::next_file_id();

        let block_reader = FileBlockReader::new(&file, file_id);

        let summary_blk = match block_reader.read_blk(0, ctx).await {
            Ok(blk) => blk,
            Err(e) => return Ok(Err(anyhow::Error::new(e).context("read first block"))),
        };

        // TODO: this should be an assertion instead; see ImageLayerInner::load
        let actual_summary =
            Summary::des_prefix(summary_blk.as_ref()).context("deserialize first block")?;

        if let Some(mut expected_summary) = summary {
            // production code path
            expected_summary.index_start_blk = actual_summary.index_start_blk;
            expected_summary.index_root_blk = actual_summary.index_root_blk;
            // mask out the timeline_id, but still require the layers to be from the same tenant
            expected_summary.timeline_id = actual_summary.timeline_id;

            if actual_summary != expected_summary {
                bail!(
                    "in-file summary does not match expected summary. actual = {:?} expected = {:?}",
                    actual_summary,
                    expected_summary
                );
            }
        }

        Ok(Ok(DeltaLayerInner {
            file,
            file_id,
            index_start_blk: actual_summary.index_start_blk,
            index_root_blk: actual_summary.index_root_blk,
            max_vectored_read_bytes,
        }))
    }

    pub(super) async fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValueReconstructState,
        ctx: &RequestContext,
    ) -> anyhow::Result<ValueReconstructResult> {
        let mut need_image = true;
        // Scan the page versions backwards, starting from `lsn`.
        let block_reader = FileBlockReader::new(&self.file, self.file_id);
        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            self.index_start_blk,
            self.index_root_blk,
            &block_reader,
        );
        let search_key = DeltaKey::from_key_lsn(&key, Lsn(lsn_range.end.0 - 1));

        let mut offsets: Vec<(Lsn, u64)> = Vec::new();

        tree_reader
            .visit(
                &search_key.0,
                VisitDirection::Backwards,
                |key, value| {
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
                },
                &RequestContextBuilder::extend(ctx)
                    .page_content_kind(PageContentKind::DeltaLayerBtreeNode)
                    .build(),
            )
            .await?;

        let ctx = &RequestContextBuilder::extend(ctx)
            .page_content_kind(PageContentKind::DeltaLayerValue)
            .build();

        // Ok, 'offsets' now contains the offsets of all the entries we need to read
        let cursor = block_reader.block_cursor();
        let mut buf = Vec::new();
        for (entry_lsn, pos) in offsets {
            cursor
                .read_blob_into_buf(pos, &mut buf, ctx)
                .await
                .with_context(|| {
                    format!("Failed to read blob from virtual file {}", self.file.path)
                })?;
            let val = Value::des(&buf).with_context(|| {
                format!(
                    "Failed to deserialize file blob from virtual file {}",
                    self.file.path
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

    // Look up the keys in the provided keyspace and update
    // the reconstruct state with whatever is found.
    //
    // If the key is cached, go no further than the cached Lsn.
    //
    // Currently, the index is visited for each range, but this
    // can be further optimised to visit the index only once.
    pub(super) async fn get_values_reconstruct_data(
        &self,
        keyspace: KeySpace,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> Result<(), GetVectoredError> {
        let block_reader = FileBlockReader::new(&self.file, self.file_id);
        let index_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            self.index_start_blk,
            self.index_root_blk,
            block_reader,
        );

        let planner = VectoredReadPlanner::new(
            self.max_vectored_read_bytes
                .expect("Layer is loaded with max vectored bytes config")
                .0
                .into(),
        );

        let data_end_offset = self.index_start_offset();

        let reads = Self::plan_reads(
            keyspace,
            lsn_range,
            data_end_offset,
            index_reader,
            planner,
            reconstruct_state,
            ctx,
        )
        .await
        .map_err(GetVectoredError::Other)?;

        self.do_reads_and_update_state(reads, reconstruct_state)
            .await;

        Ok(())
    }

    async fn plan_reads<Reader>(
        keyspace: KeySpace,
        lsn_range: Range<Lsn>,
        data_end_offset: u64,
        index_reader: DiskBtreeReader<Reader, DELTA_KEY_SIZE>,
        mut planner: VectoredReadPlanner,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<VectoredRead>>
    where
        Reader: BlockReader,
    {
        let ctx = RequestContextBuilder::extend(ctx)
            .page_content_kind(PageContentKind::DeltaLayerBtreeNode)
            .build();

        for range in keyspace.ranges.iter() {
            let mut range_end_handled = false;

            let start_key = DeltaKey::from_key_lsn(&range.start, lsn_range.start);
            let index_stream = index_reader.get_stream_from(&start_key.0, &ctx);
            let mut index_stream = std::pin::pin!(index_stream);

            while let Some(index_entry) = index_stream.next().await {
                let (raw_key, value) = index_entry?;
                let key = Key::from_slice(&raw_key[..KEY_SIZE]);
                let lsn = DeltaKey::extract_lsn_from_buf(&raw_key);
                let blob_ref = BlobRef(value);

                // Lsns are not monotonically increasing across keys, so we don't assert on them.
                assert!(key >= range.start);

                let outside_lsn_range = !lsn_range.contains(&lsn);
                let below_cached_lsn = reconstruct_state.get_cached_lsn(&key) >= Some(lsn);

                let flag = {
                    if outside_lsn_range || below_cached_lsn {
                        BlobFlag::Ignore
                    } else if blob_ref.will_init() {
                        BlobFlag::ReplaceAll
                    } else {
                        // Usual path: add blob to the read
                        BlobFlag::None
                    }
                };

                if key >= range.end || (key.next() == range.end && lsn >= lsn_range.end) {
                    planner.handle_range_end(blob_ref.pos());
                    range_end_handled = true;
                    break;
                } else {
                    planner.handle(key, lsn, blob_ref.pos(), flag);
                }
            }

            if !range_end_handled {
                tracing::info!("Handling range end fallback at {}", data_end_offset);
                planner.handle_range_end(data_end_offset);
            }
        }

        Ok(planner.finish())
    }

    async fn do_reads_and_update_state(
        &self,
        reads: Vec<VectoredRead>,
        reconstruct_state: &mut ValuesReconstructState,
    ) {
        let vectored_blob_reader = VectoredBlobReader::new(&self.file);
        let mut ignore_key_with_err = None;

        let max_vectored_read_bytes = self
            .max_vectored_read_bytes
            .expect("Layer is loaded with max vectored bytes config")
            .0
            .into();
        let mut buf = Some(BytesMut::with_capacity(max_vectored_read_bytes));

        // Note that reads are processed in reverse order (from highest key+lsn).
        // This is the order that `ReconstructState` requires such that it can
        // track when a key is done.
        for read in reads.into_iter().rev() {
            let res = vectored_blob_reader
                .read_blobs(&read, buf.take().expect("Should have a buffer"))
                .await;

            let blobs_buf = match res {
                Ok(blobs_buf) => blobs_buf,
                Err(err) => {
                    let kind = err.kind();
                    for (_, blob_meta) in read.blobs_at.as_slice() {
                        reconstruct_state.on_key_error(
                            blob_meta.key,
                            PageReconstructError::from(anyhow!(
                                "Failed to read blobs from virtual file {}: {}",
                                self.file.path,
                                kind
                            )),
                        );
                    }

                    // We have "lost" the buffer since the lower level IO api
                    // doesn't return the buffer on error. Allocate a new one.
                    buf = Some(BytesMut::with_capacity(max_vectored_read_bytes));

                    continue;
                }
            };

            for meta in blobs_buf.blobs.iter().rev() {
                if Some(meta.meta.key) == ignore_key_with_err {
                    continue;
                }

                let value = Value::des(&blobs_buf.buf[meta.start..meta.end]);
                let value = match value {
                    Ok(v) => v,
                    Err(e) => {
                        reconstruct_state.on_key_error(
                            meta.meta.key,
                            PageReconstructError::from(anyhow!(e).context(format!(
                                "Failed to deserialize blob from virtual file {}",
                                self.file.path,
                            ))),
                        );

                        ignore_key_with_err = Some(meta.meta.key);
                        continue;
                    }
                };

                // Invariant: once a key reaches [`ValueReconstructSituation::Complete`]
                // state, no further updates shall be made to it. The call below will
                // panic if the invariant is violated.
                reconstruct_state.update_key(&meta.meta.key, meta.meta.lsn, value);
            }

            buf = Some(blobs_buf.buf);
        }
    }

    pub(super) async fn load_keys<'a>(
        &'a self,
        ctx: &RequestContext,
    ) -> Result<Vec<DeltaEntry<'a>>> {
        let block_reader = FileBlockReader::new(&self.file, self.file_id);
        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            self.index_start_blk,
            self.index_root_blk,
            block_reader,
        );

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
                            Adapter(self),
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
                &RequestContextBuilder::extend(ctx)
                    .page_content_kind(PageContentKind::DeltaLayerBtreeNode)
                    .build(),
            )
            .await?;
        if let Some(last) = all_keys.last_mut() {
            // Last key occupies all space till end of value storage,
            // which corresponds to beginning of the index
            last.size = self.index_start_offset() - last.size;
        }
        Ok(all_keys)
    }

    /// Using the given writer, write out a truncated version, where LSNs higher than the
    /// truncate_at are missing.
    ///
    /// Return the amount of key value records pushed to the writer.
    pub(super) async fn copy_prefix(
        &self,
        writer: &mut DeltaLayerWriter,
        truncate_at: Lsn,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize> {
        use crate::tenant::vectored_blob_io::{
            BlobMeta, VectoredReadBuilder, VectoredReadExtended,
        };
        use futures::stream::TryStreamExt;

        #[derive(Debug)]
        enum Item {
            Actual(Key, Lsn, BlobRef),
            Sentinel,
        }

        impl From<Item> for Option<(Key, Lsn, BlobRef)> {
            fn from(value: Item) -> Self {
                match value {
                    Item::Actual(key, lsn, blob) => Some((key, lsn, blob)),
                    Item::Sentinel => None,
                }
            }
        }

        impl Item {
            fn offset(&self) -> Option<BlobRef> {
                match self {
                    Item::Actual(_, _, blob) => Some(*blob),
                    Item::Sentinel => None,
                }
            }

            fn is_last(&self) -> bool {
                matches!(self, Item::Sentinel)
            }
        }

        let block_reader = FileBlockReader::new(&self.file, self.file_id);
        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            self.index_start_blk,
            self.index_root_blk,
            block_reader,
        );

        let stream = self.stream_index_forwards(&tree_reader, &[0u8; DELTA_KEY_SIZE], ctx);
        let stream = stream.map_ok(|(key, lsn, pos)| Item::Actual(key, lsn, pos));
        // put in a sentinel value for getting the end offset for last item, and not having to
        // repeat the whole read part
        let stream = stream.chain(futures::stream::once(futures::future::ready(Ok(
            Item::Sentinel,
        ))));
        let mut stream = std::pin::pin!(stream);

        let mut prev: Option<(Key, Lsn, BlobRef)> = None;

        let mut read_builder: Option<VectoredReadBuilder> = None;

        let max_read_size = self
            .max_vectored_read_bytes
            .map(|x| x.0.get())
            .unwrap_or(8192);

        let mut buffer = Some(BytesMut::with_capacity(max_read_size));

        // FIXME: buffering of DeltaLayerWriter
        let mut per_blob_copy = Vec::new();

        let mut records = 0;

        while let Some(item) = stream.try_next().await? {
            tracing::debug!(?item, "popped");
            let offset = item
                .offset()
                .unwrap_or(BlobRef::new(self.index_start_offset(), false));

            let actionable = if let Some((key, lsn, start_offset)) = prev.take() {
                let end_offset = offset;

                Some((BlobMeta { key, lsn }, start_offset..end_offset))
            } else {
                None
            };

            let is_last = item.is_last();

            prev = Option::from(item);

            let actionable = actionable.filter(|x| x.0.lsn < truncate_at);

            let builder = if let Some((meta, offsets)) = actionable {
                // extend or create a new builder
                if read_builder
                    .as_mut()
                    .map(|x| x.extend(offsets.start.pos(), offsets.end.pos(), meta))
                    .unwrap_or(VectoredReadExtended::No)
                    == VectoredReadExtended::Yes
                {
                    None
                } else {
                    read_builder.replace(VectoredReadBuilder::new(
                        offsets.start.pos(),
                        offsets.end.pos(),
                        meta,
                        max_read_size,
                    ))
                }
            } else {
                // nothing to do, except perhaps flush any existing for the last element
                None
            };

            // flush the possible older builder and also the new one if the item was the last one
            let builders = builder.into_iter();
            let builders = if is_last {
                builders.chain(read_builder.take())
            } else {
                builders.chain(None)
            };

            for builder in builders {
                let read = builder.build();

                let reader = VectoredBlobReader::new(&self.file);

                let mut buf = buffer.take().unwrap();

                buf.clear();
                buf.reserve(read.size());
                let res = reader.read_blobs(&read, buf).await?;

                for blob in res.blobs {
                    let key = blob.meta.key;
                    let lsn = blob.meta.lsn;
                    let data = &res.buf[blob.start..blob.end];

                    #[cfg(debug_assertions)]
                    Value::des(data)
                        .with_context(|| {
                            format!(
                                "blob failed to deserialize for {}@{}, {}..{}: {:?}",
                                blob.meta.key,
                                blob.meta.lsn,
                                blob.start,
                                blob.end,
                                utils::Hex(data)
                            )
                        })
                        .unwrap();

                    // is it an image or will_init walrecord?
                    // FIXME: this could be handled by threading the BlobRef to the
                    // VectoredReadBuilder
                    let will_init =
                        data[3] == 0 || (data[3] == 1 && data.len() > 8 && data[8] == 1);

                    per_blob_copy.clear();
                    per_blob_copy.extend_from_slice(data);

                    let (tmp, res) = writer
                        .put_value_bytes(key, lsn, std::mem::take(&mut per_blob_copy), will_init)
                        .await;
                    per_blob_copy = tmp;

                    res?;

                    records += 1;
                }

                buffer = Some(res.buf);
            }
        }

        assert!(
            read_builder.is_none(),
            "with the sentinel above loop should had handled all"
        );

        Ok(records)
    }

    pub(super) async fn dump(&self, ctx: &RequestContext) -> anyhow::Result<()> {
        println!(
            "index_start_blk: {}, root {}",
            self.index_start_blk, self.index_root_blk
        );

        let block_reader = FileBlockReader::new(&self.file, self.file_id);
        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            self.index_start_blk,
            self.index_root_blk,
            block_reader,
        );

        tree_reader.dump().await?;

        let keys = self.load_keys(ctx).await?;

        async fn dump_blob(val: &ValueRef<'_>, ctx: &RequestContext) -> anyhow::Result<String> {
            let buf = val.reader.read_blob(val.blob_ref.pos(), ctx).await?;
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
            let desc = match dump_blob(&val, ctx).await {
                Ok(desc) => desc,
                Err(err) => {
                    format!("ERROR: {err}")
                }
            };
            println!("  key {key} at {lsn}: {desc}");

            // Print more details about CHECKPOINT records. Would be nice to print details
            // of many other record types too, but these are particularly interesting, as
            // have a lot of special processing for them in walingest.rs.
            use pageserver_api::key::CHECKPOINT_KEY;
            use postgres_ffi::CheckPoint;
            if key == CHECKPOINT_KEY {
                let buf = val.reader.read_blob(val.blob_ref.pos(), ctx).await?;
                let val = Value::des(&buf)?;
                match val {
                    Value::Image(img) => {
                        let checkpoint = CheckPoint::decode(&img)?;
                        println!("   CHECKPOINT: {:?}", checkpoint);
                    }
                    Value::WalRecord(_rec) => {
                        println!("   unexpected walrecord value for checkpoint key");
                    }
                }
            }
        }

        Ok(())
    }

    fn stream_index_forwards<'a, R>(
        &'a self,
        reader: &'a DiskBtreeReader<R, DELTA_KEY_SIZE>,
        start: &'a [u8; DELTA_KEY_SIZE],
        ctx: &'a RequestContext,
    ) -> impl futures::stream::Stream<
        Item = Result<(Key, Lsn, BlobRef), crate::tenant::disk_btree::DiskBtreeError>,
    > + 'a
    where
        R: BlockReader,
    {
        use futures::stream::TryStreamExt;
        let stream = reader.get_stream_from(start, ctx);
        stream.map_ok(|(key, value)| {
            let key = DeltaKey::from_slice(&key);
            let (key, lsn) = (key.key(), key.lsn());
            let offset = BlobRef(value);

            (key, lsn, offset)
        })
    }

    /// The file offset to the first block of index.
    ///
    /// The file structure is summary, values, and index. We often need this for the size of last blob.
    fn index_start_offset(&self) -> u64 {
        let offset = self.index_start_blk as u64 * PAGE_SZ as u64;
        let bref = BlobRef(offset);
        tracing::debug!(
            index_start_blk = self.index_start_blk,
            offset,
            pos = bref.pos(),
            "index_start_offset"
        );
        offset
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
    pub async fn load(&self, ctx: &RequestContext) -> Result<Value> {
        // theoretically we *could* record an access time for each, but it does not really matter
        let buf = self.reader.read_blob(self.blob_ref.pos(), ctx).await?;
        let val = Value::des(&buf)?;
        Ok(val)
    }
}

pub(crate) struct Adapter<T>(T);

impl<T: AsRef<DeltaLayerInner>> Adapter<T> {
    pub(crate) async fn read_blk(
        &self,
        blknum: u32,
        ctx: &RequestContext,
    ) -> Result<BlockLease, std::io::Error> {
        let block_reader = FileBlockReader::new(&self.0.as_ref().file, self.0.as_ref().file_id);
        block_reader.read_blk(blknum, ctx).await
    }
}

impl AsRef<DeltaLayerInner> for DeltaLayerInner {
    fn as_ref(&self) -> &DeltaLayerInner {
        self
    }
}

impl<'a> pageserver_compaction::interface::CompactionDeltaEntry<'a, Key> for DeltaEntry<'a> {
    fn key(&self) -> Key {
        self.key
    }
    fn lsn(&self) -> Lsn {
        self.lsn
    }
    fn size(&self) -> u64 {
        self.size
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{
        context::DownloadBehavior, task_mgr::TaskKind, tenant::disk_btree::tests::TestDisk,
    };

    /// Construct an index for a fictional delta layer and and then
    /// traverse in order to plan vectored reads for a query. Finally,
    /// verify that the traversal fed the right index key and value
    /// pairs into the planner.
    #[tokio::test]
    async fn test_delta_layer_index_traversal() {
        let base_key = Key {
            field1: 0,
            field2: 1663,
            field3: 12972,
            field4: 16396,
            field5: 0,
            field6: 246080,
        };

        // Populate the index with some entries
        let entries: BTreeMap<Key, Vec<Lsn>> = BTreeMap::from([
            (base_key, vec![Lsn(1), Lsn(5), Lsn(25), Lsn(26), Lsn(28)]),
            (base_key.add(1), vec![Lsn(2), Lsn(5), Lsn(10), Lsn(50)]),
            (base_key.add(2), vec![Lsn(2), Lsn(5), Lsn(10), Lsn(50)]),
            (base_key.add(5), vec![Lsn(10), Lsn(15), Lsn(16), Lsn(20)]),
        ]);

        let mut disk = TestDisk::default();
        let mut writer = DiskBtreeBuilder::<_, DELTA_KEY_SIZE>::new(&mut disk);

        let mut disk_offset = 0;
        for (key, lsns) in &entries {
            for lsn in lsns {
                let index_key = DeltaKey::from_key_lsn(key, *lsn);
                let blob_ref = BlobRef::new(disk_offset, false);
                writer
                    .append(&index_key.0, blob_ref.0)
                    .expect("In memory disk append should never fail");

                disk_offset += 1;
            }
        }

        // Prepare all the arguments for the call into `plan_reads` below
        let (root_offset, _writer) = writer
            .finish()
            .expect("In memory disk finish should never fail");
        let reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(0, root_offset, disk);
        let planner = VectoredReadPlanner::new(100);
        let mut reconstruct_state = ValuesReconstructState::new();
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        let keyspace = KeySpace {
            ranges: vec![
                base_key..base_key.add(3),
                base_key.add(3)..base_key.add(100),
            ],
        };
        let lsn_range = Lsn(2)..Lsn(40);

        // Plan and validate
        let vectored_reads = DeltaLayerInner::plan_reads(
            keyspace.clone(),
            lsn_range.clone(),
            disk_offset,
            reader,
            planner,
            &mut reconstruct_state,
            &ctx,
        )
        .await
        .expect("Read planning should not fail");

        validate(keyspace, lsn_range, vectored_reads, entries);
    }

    fn validate(
        keyspace: KeySpace,
        lsn_range: Range<Lsn>,
        vectored_reads: Vec<VectoredRead>,
        index_entries: BTreeMap<Key, Vec<Lsn>>,
    ) {
        #[derive(Debug, PartialEq, Eq)]
        struct BlobSpec {
            key: Key,
            lsn: Lsn,
            at: u64,
        }

        let mut planned_blobs = Vec::new();
        for read in vectored_reads {
            for (at, meta) in read.blobs_at.as_slice() {
                planned_blobs.push(BlobSpec {
                    key: meta.key,
                    lsn: meta.lsn,
                    at: *at,
                });
            }
        }

        let mut expected_blobs = Vec::new();
        let mut disk_offset = 0;
        for (key, lsns) in index_entries {
            for lsn in lsns {
                let key_included = keyspace.ranges.iter().any(|range| range.contains(&key));
                let lsn_included = lsn_range.contains(&lsn);

                if key_included && lsn_included {
                    expected_blobs.push(BlobSpec {
                        key,
                        lsn,
                        at: disk_offset,
                    });
                }

                disk_offset += 1;
            }
        }

        assert_eq!(planned_blobs, expected_blobs);
    }

    #[tokio::test]
    async fn copy_delta_prefix_smoke() {
        use crate::walrecord::NeonWalRecord;
        use bytes::Bytes;

        let h = crate::tenant::harness::TenantHarness::create("truncate_delta_smoke").unwrap();
        let (tenant, ctx) = h.load().await;
        let ctx = &ctx;
        let timeline = tenant
            .create_test_timeline(TimelineId::generate(), Lsn(0x10), 14, ctx)
            .await
            .unwrap();

        let initdb_layer = timeline
            .layers
            .read()
            .await
            .likely_resident_layers()
            .next()
            .unwrap();

        let key = Key::from_i128(12);

        {
            let mut writer = timeline.writer().await;

            let data = [
                (0x20, Value::Image(Bytes::from_static(b"foobar"))),
                (
                    0x30,
                    Value::WalRecord(NeonWalRecord::Postgres {
                        will_init: false,
                        rec: Bytes::from_static(b"1"),
                    }),
                ),
                (
                    0x40,
                    Value::WalRecord(NeonWalRecord::Postgres {
                        will_init: true,
                        rec: Bytes::from_static(b"2"),
                    }),
                ),
                // build an oversized value so we cannot extend and existing read over
                // this
                (
                    0x50,
                    Value::WalRecord(NeonWalRecord::Postgres {
                        will_init: true,
                        rec: {
                            let mut buf =
                                vec![0u8; tenant.conf.max_vectored_read_bytes.0.get() + 1024];
                            buf.iter_mut()
                                .enumerate()
                                .for_each(|(i, slot)| *slot = (i % 256) as u8);
                            Bytes::from(buf)
                        },
                    }),
                ),
                // because the oversized read cannot be extended further, we are sure to exercise the
                // builder created on the last round with this:
                (
                    0x60,
                    Value::WalRecord(NeonWalRecord::Postgres {
                        will_init: true,
                        rec: Bytes::from_static(b"3"),
                    }),
                ),
            ];

            let mut last_lsn = None;

            for (lsn, value) in data {
                writer.put(key, Lsn(lsn), &value, ctx).await.unwrap();
                last_lsn = Some(lsn);
            }

            writer.finish_write(Lsn(last_lsn.unwrap()));
        }
        timeline.freeze_and_flush().await.unwrap();

        let new_layer = timeline
            .layers
            .read()
            .await
            .likely_resident_layers()
            .find(|x| x != &initdb_layer)
            .unwrap();

        // create a copy for the timeline, so we don't overwrite the file
        let branch = tenant
            .branch_timeline_test(&timeline, TimelineId::generate(), None, ctx)
            .await
            .unwrap();

        assert_eq!(branch.get_ancestor_lsn(), Lsn(0x60));

        // truncating at 0x61 gives us a full copy, otherwise just go backwards until there's just
        // a single key

        for truncate_at in [0x61, 0x51, 0x41, 0x31, 0x21] {
            let truncate_at = Lsn(truncate_at);

            let mut writer = DeltaLayerWriter::new(
                tenant.conf,
                branch.timeline_id,
                tenant.tenant_shard_id,
                Key::MIN,
                Lsn(0x11)..truncate_at,
            )
            .await
            .unwrap();

            let new_layer = new_layer.download_and_keep_resident().await.unwrap();

            new_layer
                .copy_delta_prefix(&mut writer, truncate_at, ctx)
                .await
                .unwrap();

            let copied_layer = writer.finish(Key::MAX, &branch).await.unwrap();

            copied_layer.as_delta(ctx).await.unwrap();

            assert_keys_and_values_eq(
                new_layer.as_delta(ctx).await.unwrap(),
                copied_layer.as_delta(ctx).await.unwrap(),
                truncate_at,
                ctx,
            )
            .await;
        }
    }

    async fn assert_keys_and_values_eq(
        source: &DeltaLayerInner,
        truncated: &DeltaLayerInner,
        truncated_at: Lsn,
        ctx: &RequestContext,
    ) {
        use futures::future::ready;
        use futures::stream::TryStreamExt;

        let start_key = [0u8; DELTA_KEY_SIZE];

        let source_reader = FileBlockReader::new(&source.file, source.file_id);
        let source_tree = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            source.index_start_blk,
            source.index_root_blk,
            &source_reader,
        );
        let source_stream = source.stream_index_forwards(&source_tree, &start_key, ctx);
        let source_stream = source_stream.filter(|res| match res {
            Ok((_, lsn, _)) => ready(lsn < &truncated_at),
            _ => ready(true),
        });
        let mut source_stream = std::pin::pin!(source_stream);

        let truncated_reader = FileBlockReader::new(&truncated.file, truncated.file_id);
        let truncated_tree = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            truncated.index_start_blk,
            truncated.index_root_blk,
            &truncated_reader,
        );
        let truncated_stream = truncated.stream_index_forwards(&truncated_tree, &start_key, ctx);
        let mut truncated_stream = std::pin::pin!(truncated_stream);

        let mut scratch_left = Vec::new();
        let mut scratch_right = Vec::new();

        loop {
            let (src, truncated) = (source_stream.try_next(), truncated_stream.try_next());
            let (src, truncated) = tokio::try_join!(src, truncated).unwrap();

            if src.is_none() {
                assert!(truncated.is_none());
                break;
            }

            let (src, truncated) = (src.unwrap(), truncated.unwrap());

            // because we've filtered the source with Lsn, we should always have the same keys from both.
            assert_eq!(src.0, truncated.0);
            assert_eq!(src.1, truncated.1);

            // if this is needed for something else, just drop this assert.
            assert!(
                src.2.pos() >= truncated.2.pos(),
                "value position should not go backwards {} vs. {}",
                src.2.pos(),
                truncated.2.pos()
            );

            scratch_left.clear();
            let src_cursor = source_reader.block_cursor();
            let left = src_cursor.read_blob_into_buf(src.2.pos(), &mut scratch_left, ctx);
            scratch_right.clear();
            let trunc_cursor = truncated_reader.block_cursor();
            let right = trunc_cursor.read_blob_into_buf(truncated.2.pos(), &mut scratch_right, ctx);

            tokio::try_join!(left, right).unwrap();

            assert_eq!(utils::Hex(&scratch_left), utils::Hex(&scratch_right));
        }
    }
}
