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
use crate::page_cache::PAGE_SZ;
use crate::repository::{Key, Value, KEY_SIZE};
use crate::tenant::blob_io::BlobWriter;
use crate::tenant::block_io::{BlockBuf, BlockCursor, BlockLease, BlockReader, FileBlockReader};
use crate::tenant::disk_btree::{DiskBtreeBuilder, DiskBtreeReader, VisitDirection};
use crate::tenant::storage_layer::{Layer, ValueReconstructResult, ValueReconstructState};
use crate::tenant::Timeline;
use crate::virtual_file::VirtualFile;
use crate::{walrecord, TEMP_FILE_SUFFIX};
use crate::{DELTA_FILE_MAGIC, STORAGE_FORMAT_VERSION};
use anyhow::{bail, ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
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

use super::{AsLayerDesc, LayerAccessStats, PersistentLayerDesc, ResidentLayer};

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

    /// Reader object for reading blocks from the file.
    file: FileBlockReader,
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

        let loaded = DeltaLayerInner::load(&path, None, ctx)
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

        // TODO(sharding): we must get the TenantShardId from the path instead of reading the Summary.
        // we should also validate the path against the Summary, as both should contain the same tenant, timeline, key, lsn.
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
    async fn finish(self, key_end: Key, timeline: &Arc<Timeline>) -> anyhow::Result<ResidentLayer> {
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
            tenant_id: self.tenant_shard_id.tenant_id,
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
    pub(crate) async fn finish(
        mut self,
        key_end: Key,
        timeline: &Arc<Timeline>,
    ) -> anyhow::Result<ResidentLayer> {
        self.inner.take().unwrap().finish(key_end, timeline).await
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
        let file = VirtualFile::open_with_options(
            path,
            &*std::fs::OpenOptions::new().read(true).write(true),
        )
        .await
        .with_context(|| format!("Failed to open file '{}'", path))?;
        let file = FileBlockReader::new(file);
        let summary_blk = file.read_blk(0, ctx).await?;
        let actual_summary = Summary::des_prefix(summary_blk.as_ref()).context("deserialize")?;
        let mut file = file.file;
        if actual_summary.magic != DELTA_FILE_MAGIC {
            return Err(RewriteSummaryError::MagicMismatch);
        }

        let new_summary = rewrite(actual_summary);

        let mut buf = smallvec::SmallVec::<[u8; PAGE_SZ]>::new();
        Summary::ser_into(&new_summary, &mut buf).context("serialize")?;
        if buf.spilled() {
            // The code in DeltaLayerWriterInner just warn!()s for this.
            // It should probably error out as well.
            return Err(RewriteSummaryError::Other(anyhow::anyhow!(
                "Used more than one page size for summary buffer: {}",
                buf.len()
            )));
        }
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(&buf).await?;
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
        ctx: &RequestContext,
    ) -> Result<Result<Self, anyhow::Error>, anyhow::Error> {
        let file = match VirtualFile::open(path).await {
            Ok(file) => file,
            Err(e) => return Ok(Err(anyhow::Error::new(e).context("open layer file"))),
        };
        let file = FileBlockReader::new(file);

        let summary_blk = match file.read_blk(0, ctx).await {
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
            index_start_blk: actual_summary.index_start_blk,
            index_root_blk: actual_summary.index_root_blk,
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
        let file = &self.file;
        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            self.index_start_blk,
            self.index_root_blk,
            file,
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
        let cursor = file.block_cursor();
        let mut buf = Vec::new();
        for (entry_lsn, pos) in offsets {
            cursor
                .read_blob_into_buf(pos, &mut buf, ctx)
                .await
                .with_context(|| {
                    format!("Failed to read blob from virtual file {}", file.file.path)
                })?;
            let val = Value::des(&buf).with_context(|| {
                format!(
                    "Failed to deserialize file blob from virtual file {}",
                    file.file.path
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

    pub(super) async fn load_keys<'a>(
        &'a self,
        ctx: &RequestContext,
    ) -> Result<Vec<DeltaEntry<'a>>> {
        let file = &self.file;

        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            self.index_start_blk,
            self.index_root_blk,
            file,
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

        let file = &self.file;
        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            self.index_start_blk,
            self.index_root_blk,
            file,
        );

        tree_reader.dump().await?;

        let keys = self.load_keys(ctx).await?;

        async fn dump_blob(val: ValueRef<'_>, ctx: &RequestContext) -> anyhow::Result<String> {
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
            let desc = match dump_blob(val, ctx).await {
                Ok(desc) => desc,
                Err(err) => {
                    format!("ERROR: {err}")
                }
            };
            println!("  key {key} at {lsn}: {desc}");
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
        self.0.as_ref().file.read_blk(blknum, ctx).await
    }
}

impl AsRef<DeltaLayerInner> for DeltaLayerInner {
    fn as_ref(&self) -> &DeltaLayerInner {
        self
    }
}
