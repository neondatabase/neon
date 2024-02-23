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
use crate::context::{PageContentKind, RequestContext, RequestContextBuilder};
use crate::page_cache::{self, FileId, PAGE_SZ};
use crate::repository::{Key, Value, KEY_SIZE};
use crate::tenant::blob_io::BlobWriter;
use crate::tenant::block_io::{BlockBuf, BlockCursor, BlockLease, BlockReader, FileBlockReader};
use crate::tenant::disk_btree::{DiskBtreeBuilder, DiskBtreeReader, VisitDirection};
use crate::tenant::storage_layer::{Layer, ValueReconstructResult, ValueReconstructState};
use crate::tenant::timeline::GetVectoredError;
use crate::tenant::vectored_blob_io::{
    BlobFlag, VectoredBlobReader, VectoredRead, VectoredReadPlanner,
};
use crate::tenant::{PageReconstructError, Timeline};
use crate::virtual_file::{self, VirtualFile};
use crate::{walrecord, TEMP_FILE_SUFFIX};
use crate::{DELTA_FILE_MAGIC, STORAGE_FORMAT_VERSION};
use anyhow::{anyhow, bail, ensure, Context, Result};
use bytes::BytesMut;
use camino::{Utf8Path, Utf8PathBuf};
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

    max_vectored_read_bytes: usize,
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

        let inner = self.load(LayerAccessKind::Dump, 0, ctx).await?;

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
        max_vectored_read_bytes: usize,
        ctx: &RequestContext,
    ) -> Result<&Arc<DeltaLayerInner>> {
        self.access_stats.record_access(access_kind, ctx);
        // Quick exit if already loaded
        self.inner
            .get_or_try_init(|| self.load_inner(max_vectored_read_bytes, ctx))
            .await
            .with_context(|| format!("Failed to load delta layer {}", self.path()))
    }

    async fn load_inner(
        &self,
        max_vectored_read_bytes: usize,
        ctx: &RequestContext,
    ) -> Result<Arc<DeltaLayerInner>> {
        let path = self.path();

        let loaded = DeltaLayerInner::load(&path, None, max_vectored_read_bytes, ctx)
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
        max_vectored_read_bytes: usize,
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
        start_lsn: Lsn,
        end_lsn: Lsn,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> Result<(), GetVectoredError> {
        let reads = self
            .plan_reads(keyspace, start_lsn..end_lsn, reconstruct_state, ctx)
            .await
            .map_err(GetVectoredError::Other)?;

        self.do_reads_and_update_state(reads, reconstruct_state)
            .await;

        Ok(())
    }

    async fn plan_reads(
        &self,
        keyspace: KeySpace,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<VectoredRead>> {
        let mut planner = VectoredReadPlanner::new(self.max_vectored_read_bytes);

        let block_reader = FileBlockReader::new(&self.file, self.file_id);
        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            self.index_start_blk,
            self.index_root_blk,
            block_reader,
        );

        for range in keyspace.ranges.iter() {
            let mut range_end_handled = false;

            let start_key = DeltaKey::from_key_lsn(&range.start, lsn_range.start);
            tree_reader
                .visit(
                    &start_key.0,
                    VisitDirection::Forwards,
                    |raw_key, value| {
                        let key = Key::from_slice(&raw_key[..KEY_SIZE]);
                        let lsn = DeltaKey::extract_lsn_from_buf(raw_key);
                        let blob_ref = BlobRef(value);

                        assert!(key >= range.start && lsn >= lsn_range.start);

                        let cached_lsn = reconstruct_state.get_cached_lsn(&key);
                        let flag = {
                            if cached_lsn >= Some(lsn) {
                                BlobFlag::Ignore
                            } else if blob_ref.will_init() {
                                BlobFlag::Replaces
                            } else {
                                BlobFlag::None
                            }
                        };

                        if key >= range.end || (key.next() == range.end && lsn >= lsn_range.end) {
                            planner.handle_range_end(blob_ref.pos());
                            range_end_handled = true;
                            false
                        } else {
                            planner.handle(key, lsn, blob_ref.pos(), flag);
                            true
                        }
                    },
                    &RequestContextBuilder::extend(ctx)
                        .page_content_kind(PageContentKind::DeltaLayerBtreeNode)
                        .build(),
                )
                .await
                .map_err(|err| anyhow!(err))?;

            if !range_end_handled {
                let payload_end = self.index_start_blk as u64 * PAGE_SZ as u64;
                tracing::info!("Handling range end fallback at {}", payload_end);
                planner.handle_range_end(payload_end);
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

        let mut buf = Some(BytesMut::with_capacity(self.max_vectored_read_bytes));

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
                    buf = Some(BytesMut::with_capacity(self.max_vectored_read_bytes));

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
            last.size = self.index_start_blk as u64 * PAGE_SZ as u64 - last.size;
        }
        Ok(all_keys)
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
