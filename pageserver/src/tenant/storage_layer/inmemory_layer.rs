//! An in-memory layer stores recently received key-value pairs.
//!
//! The "in-memory" part of the name is a bit misleading: the actual page versions are
//! held in an ephemeral file, not in memory. The metadata for each page version, i.e.
//! its position in the file, is kept in memory, though.
//!
use crate::config::PageServerConf;
use crate::context::{PageContentKind, RequestContext, RequestContextBuilder};
use crate::repository::{Key, Value};
use crate::tenant::block_io::BlockReader;
use crate::tenant::ephemeral_file::EphemeralFile;
use crate::tenant::storage_layer::ValueReconstructResult;
use crate::tenant::timeline::GetVectoredError;
use crate::tenant::PageReconstructError;
use crate::{page_cache, walrecord};
use anyhow::{anyhow, ensure, Result};
use camino::Utf8PathBuf;
use pageserver_api::keyspace::KeySpace;
use pageserver_api::models::InMemoryLayerInfo;
use pageserver_api::shard::TenantShardId;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::sync::OnceLock;
use std::time::Instant;
use tracing::*;
use utils::{bin_ser::BeSer, id::TimelineId, lsn::Lsn, vec_map::VecMap};
// avoid binding to Write (conflicts with std::io::Write)
// while being able to use std::fmt::Write's methods
use crate::metrics::TIMELINE_EPHEMERAL_BYTES;
use std::cmp::Ordering;
use std::fmt::Write as _;
use std::ops::Range;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use tokio::sync::{RwLock, RwLockWriteGuard};

use super::{
    DeltaLayerWriter, PersistentLayerDesc, ValueReconstructSituation, ValueReconstructState,
    ValuesReconstructState,
};

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub(crate) struct InMemoryLayerFileId(page_cache::FileId);

pub struct InMemoryLayer {
    conf: &'static PageServerConf,
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    file_id: InMemoryLayerFileId,

    /// This layer contains all the changes from 'start_lsn'. The
    /// start is inclusive.
    start_lsn: Lsn,

    /// Frozen layers have an exclusive end LSN.
    /// Writes are only allowed when this is `None`.
    end_lsn: OnceLock<Lsn>,

    opened_at: Instant,

    /// The above fields never change, except for `end_lsn`, which is only set once.
    /// All other changing parts are in `inner`, and protected by a mutex.
    inner: RwLock<InMemoryLayerInner>,
}

impl std::fmt::Debug for InMemoryLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryLayer")
            .field("start_lsn", &self.start_lsn)
            .field("end_lsn", &self.end_lsn)
            .field("inner", &self.inner)
            .finish()
    }
}

pub struct InMemoryLayerInner {
    /// All versions of all pages in the layer are kept here.  Indexed
    /// by block number and LSN. The value is an offset into the
    /// ephemeral file where the page version is stored.
    index: BTreeMap<Key, VecMap<Lsn, u64>>,

    /// The values are stored in a serialized format in this file.
    /// Each serialized Value is preceded by a 'u32' length field.
    /// PerSeg::page_versions map stores offsets into this file.
    file: EphemeralFile,

    resource_units: GlobalResourceUnits,
}

impl std::fmt::Debug for InMemoryLayerInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryLayerInner").finish()
    }
}

/// State shared by all in-memory (ephemeral) layers.  Updated infrequently during background ticks in Timeline,
/// to minimize contention.
///
/// This global state is used to implement behaviors that require a global view of the system, e.g.
/// rolling layers proactively to limit the total amount of dirty data.
pub(crate) struct GlobalResources {
    // Limit on how high dirty_bytes may grow before we start freezing layers to reduce it.
    // Zero means unlimited.
    pub(crate) max_dirty_bytes: AtomicU64,
    // How many bytes are in all EphemeralFile objects
    dirty_bytes: AtomicU64,
    // How many layers are contributing to dirty_bytes
    dirty_layers: AtomicUsize,
}

// Per-timeline RAII struct for its contribution to [`GlobalResources`]
struct GlobalResourceUnits {
    // How many dirty bytes have I added to the global dirty_bytes: this guard object is responsible
    // for decrementing the global counter by this many bytes when dropped.
    dirty_bytes: u64,
}

impl GlobalResourceUnits {
    // Hint for the layer append path to update us when the layer size differs from the last
    // call to update_size by this much.  If we don't reach this threshold, we'll still get
    // updated when the Timeline "ticks" in the background.
    const MAX_SIZE_DRIFT: u64 = 10 * 1024 * 1024;

    fn new() -> Self {
        GLOBAL_RESOURCES
            .dirty_layers
            .fetch_add(1, AtomicOrdering::Relaxed);
        Self { dirty_bytes: 0 }
    }

    /// Do not call this frequently: all timelines will write to these same global atomics,
    /// so this is a relatively expensive operation.  Wait at least a few seconds between calls.
    ///
    /// Returns the effective layer size limit that should be applied, if any, to keep
    /// the total number of dirty bytes below the configured maximum.
    fn publish_size(&mut self, size: u64) -> Option<u64> {
        let new_global_dirty_bytes = match size.cmp(&self.dirty_bytes) {
            Ordering::Equal => GLOBAL_RESOURCES.dirty_bytes.load(AtomicOrdering::Relaxed),
            Ordering::Greater => {
                let delta = size - self.dirty_bytes;
                let old = GLOBAL_RESOURCES
                    .dirty_bytes
                    .fetch_add(delta, AtomicOrdering::Relaxed);
                old + delta
            }
            Ordering::Less => {
                let delta = self.dirty_bytes - size;
                let old = GLOBAL_RESOURCES
                    .dirty_bytes
                    .fetch_sub(delta, AtomicOrdering::Relaxed);
                old - delta
            }
        };

        // This is a sloppy update: concurrent updates to the counter will race, and the exact
        // value of the metric might not be the exact latest value of GLOBAL_RESOURCES::dirty_bytes.
        // That's okay: as long as the metric contains some recent value, it doesn't have to always
        // be literally the last update.
        TIMELINE_EPHEMERAL_BYTES.set(new_global_dirty_bytes);

        self.dirty_bytes = size;

        let max_dirty_bytes = GLOBAL_RESOURCES
            .max_dirty_bytes
            .load(AtomicOrdering::Relaxed);
        if max_dirty_bytes > 0 && new_global_dirty_bytes > max_dirty_bytes {
            // Set the layer file limit to the average layer size: this implies that all above-average
            // sized layers will be elegible for freezing.  They will be frozen in the order they
            // next enter publish_size.
            Some(
                new_global_dirty_bytes
                    / GLOBAL_RESOURCES.dirty_layers.load(AtomicOrdering::Relaxed) as u64,
            )
        } else {
            None
        }
    }

    // Call publish_size if the input size differs from last published size by more than
    // the drift limit
    fn maybe_publish_size(&mut self, size: u64) {
        let publish = match size.cmp(&self.dirty_bytes) {
            Ordering::Equal => false,
            Ordering::Greater => size - self.dirty_bytes > Self::MAX_SIZE_DRIFT,
            Ordering::Less => self.dirty_bytes - size > Self::MAX_SIZE_DRIFT,
        };

        if publish {
            self.publish_size(size);
        }
    }
}

impl Drop for GlobalResourceUnits {
    fn drop(&mut self) {
        GLOBAL_RESOURCES
            .dirty_layers
            .fetch_sub(1, AtomicOrdering::Relaxed);

        // Subtract our contribution to the global total dirty bytes
        self.publish_size(0);
    }
}

pub(crate) static GLOBAL_RESOURCES: GlobalResources = GlobalResources {
    max_dirty_bytes: AtomicU64::new(0),
    dirty_bytes: AtomicU64::new(0),
    dirty_layers: AtomicUsize::new(0),
};

impl InMemoryLayer {
    pub(crate) fn file_id(&self) -> InMemoryLayerFileId {
        self.file_id
    }

    pub(crate) fn get_timeline_id(&self) -> TimelineId {
        self.timeline_id
    }

    pub(crate) fn info(&self) -> InMemoryLayerInfo {
        let lsn_start = self.start_lsn;

        if let Some(&lsn_end) = self.end_lsn.get() {
            InMemoryLayerInfo::Frozen { lsn_start, lsn_end }
        } else {
            InMemoryLayerInfo::Open { lsn_start }
        }
    }

    pub(crate) fn try_len(&self) -> Option<u64> {
        self.inner.try_read().map(|i| i.file.len()).ok()
    }

    pub(crate) fn assert_writable(&self) {
        assert!(self.end_lsn.get().is_none());
    }

    pub(crate) fn end_lsn_or_max(&self) -> Lsn {
        self.end_lsn.get().copied().unwrap_or(Lsn::MAX)
    }

    pub(crate) fn get_lsn_range(&self) -> Range<Lsn> {
        self.start_lsn..self.end_lsn_or_max()
    }

    /// debugging function to print out the contents of the layer
    ///
    /// this is likely completly unused
    pub async fn dump(&self, verbose: bool, ctx: &RequestContext) -> Result<()> {
        let inner = self.inner.read().await;

        let end_str = self.end_lsn_or_max();

        println!(
            "----- in-memory layer for tli {} LSNs {}-{} ----",
            self.timeline_id, self.start_lsn, end_str,
        );

        if !verbose {
            return Ok(());
        }

        let cursor = inner.file.block_cursor();
        let mut buf = Vec::new();
        for (key, vec_map) in inner.index.iter() {
            for (lsn, pos) in vec_map.as_slice() {
                let mut desc = String::new();
                cursor.read_blob_into_buf(*pos, &mut buf, ctx).await?;
                let val = Value::des(&buf);
                match val {
                    Ok(Value::Image(img)) => {
                        write!(&mut desc, " img {} bytes", img.len())?;
                    }
                    Ok(Value::WalRecord(rec)) => {
                        let wal_desc = walrecord::describe_wal_record(&rec).unwrap();
                        write!(
                            &mut desc,
                            " rec {} bytes will_init: {} {}",
                            buf.len(),
                            rec.will_init(),
                            wal_desc
                        )?;
                    }
                    Err(err) => {
                        write!(&mut desc, " DESERIALIZATION ERROR: {}", err)?;
                    }
                }
                println!("  key {} at {}: {}", key, lsn, desc);
            }
        }

        Ok(())
    }

    /// Look up given value in the layer.
    pub(crate) async fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValueReconstructState,
        ctx: &RequestContext,
    ) -> anyhow::Result<ValueReconstructResult> {
        ensure!(lsn_range.start >= self.start_lsn);
        let mut need_image = true;

        let ctx = RequestContextBuilder::extend(ctx)
            .page_content_kind(PageContentKind::InMemoryLayer)
            .build();

        let inner = self.inner.read().await;

        let reader = inner.file.block_cursor();

        // Scan the page versions backwards, starting from `lsn`.
        if let Some(vec_map) = inner.index.get(&key) {
            let slice = vec_map.slice_range(lsn_range);
            for (entry_lsn, pos) in slice.iter().rev() {
                let buf = reader.read_blob(*pos, &ctx).await?;
                let value = Value::des(&buf)?;
                match value {
                    Value::Image(img) => {
                        reconstruct_state.img = Some((*entry_lsn, img));
                        return Ok(ValueReconstructResult::Complete);
                    }
                    Value::WalRecord(rec) => {
                        let will_init = rec.will_init();
                        reconstruct_state.records.push((*entry_lsn, rec));
                        if will_init {
                            // This WAL record initializes the page, so no need to go further back
                            need_image = false;
                            break;
                        }
                    }
                }
            }
        }

        // release lock on 'inner'

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
    pub(crate) async fn get_values_reconstruct_data(
        &self,
        keyspace: KeySpace,
        end_lsn: Lsn,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> Result<(), GetVectoredError> {
        let ctx = RequestContextBuilder::extend(ctx)
            .page_content_kind(PageContentKind::InMemoryLayer)
            .build();

        let inner = self.inner.read().await;
        let reader = inner.file.block_cursor();

        #[derive(Eq, PartialEq, Ord, PartialOrd)]
        struct BlockRead {
            key: Key,
            lsn: Lsn,
            block_offset: u64,
        }

        let mut planned_block_reads = BinaryHeap::new();

        for range in keyspace.ranges.iter() {
            let mut key = range.start;
            while key < range.end {
                if let Some(vec_map) = inner.index.get(&key) {
                    let lsn_range = match reconstruct_state.get_cached_lsn(&key) {
                        Some(cached_lsn) => (cached_lsn + 1)..end_lsn,
                        None => self.start_lsn..end_lsn,
                    };

                    let slice = vec_map.slice_range(lsn_range);
                    for (entry_lsn, pos) in slice.iter().rev() {
                        planned_block_reads.push(BlockRead {
                            key,
                            lsn: *entry_lsn,
                            block_offset: *pos,
                        });
                    }
                }

                key = key.next();
            }
        }

        let keyspace_size = keyspace.total_size();

        let mut completed_keys = HashSet::new();
        while completed_keys.len() < keyspace_size && !planned_block_reads.is_empty() {
            let block_read = planned_block_reads.pop().unwrap();
            if completed_keys.contains(&block_read.key) {
                continue;
            }

            let buf = reader.read_blob(block_read.block_offset, &ctx).await;
            if let Err(e) = buf {
                reconstruct_state
                    .on_key_error(block_read.key, PageReconstructError::from(anyhow!(e)));
                completed_keys.insert(block_read.key);
                continue;
            }

            let value = Value::des(&buf.unwrap());
            if let Err(e) = value {
                reconstruct_state
                    .on_key_error(block_read.key, PageReconstructError::from(anyhow!(e)));
                completed_keys.insert(block_read.key);
                continue;
            }

            let key_situation =
                reconstruct_state.update_key(&block_read.key, block_read.lsn, value.unwrap());
            if key_situation == ValueReconstructSituation::Complete {
                completed_keys.insert(block_read.key);
            }
        }

        Ok(())
    }
}

impl std::fmt::Display for InMemoryLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let end_lsn = self.end_lsn_or_max();
        write!(f, "inmem-{:016X}-{:016X}", self.start_lsn.0, end_lsn.0)
    }
}

impl InMemoryLayer {
    /// Get layer size.
    pub async fn size(&self) -> Result<u64> {
        let inner = self.inner.read().await;
        Ok(inner.file.len())
    }

    /// Create a new, empty, in-memory layer
    pub async fn create(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        start_lsn: Lsn,
    ) -> Result<InMemoryLayer> {
        trace!("initializing new empty InMemoryLayer for writing on timeline {timeline_id} at {start_lsn}");

        let file = EphemeralFile::create(conf, tenant_shard_id, timeline_id).await?;
        let key = InMemoryLayerFileId(file.id());

        Ok(InMemoryLayer {
            file_id: key,
            conf,
            timeline_id,
            tenant_shard_id,
            start_lsn,
            end_lsn: OnceLock::new(),
            opened_at: Instant::now(),
            inner: RwLock::new(InMemoryLayerInner {
                index: BTreeMap::new(),
                file,
                resource_units: GlobalResourceUnits::new(),
            }),
        })
    }

    // Write operations

    /// Common subroutine of the public put_wal_record() and put_page_image() functions.
    /// Adds the page version to the in-memory tree
    pub async fn put_value(
        &self,
        key: Key,
        lsn: Lsn,
        buf: &[u8],
        ctx: &RequestContext,
    ) -> Result<()> {
        let mut inner = self.inner.write().await;
        self.assert_writable();
        self.put_value_locked(&mut inner, key, lsn, buf, ctx).await
    }

    async fn put_value_locked(
        &self,
        locked_inner: &mut RwLockWriteGuard<'_, InMemoryLayerInner>,
        key: Key,
        lsn: Lsn,
        buf: &[u8],
        ctx: &RequestContext,
    ) -> Result<()> {
        trace!("put_value key {} at {}/{}", key, self.timeline_id, lsn);

        let off = {
            locked_inner
                .file
                .write_blob(
                    buf,
                    &RequestContextBuilder::extend(ctx)
                        .page_content_kind(PageContentKind::InMemoryLayer)
                        .build(),
                )
                .await?
        };

        let vec_map = locked_inner.index.entry(key).or_default();
        let old = vec_map.append_or_update_last(lsn, off).unwrap().0;
        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!("Key {} at {} already exists", key, lsn);
        }

        let size = locked_inner.file.len();
        locked_inner.resource_units.maybe_publish_size(size);

        Ok(())
    }

    pub(crate) fn get_opened_at(&self) -> Instant {
        self.opened_at
    }

    pub(crate) async fn tick(&self) -> Option<u64> {
        let mut inner = self.inner.write().await;
        let size = inner.file.len();
        inner.resource_units.publish_size(size)
    }

    pub(crate) async fn put_tombstones(&self, _key_ranges: &[(Range<Key>, Lsn)]) -> Result<()> {
        // TODO: Currently, we just leak the storage for any deleted keys
        Ok(())
    }

    /// Records the end_lsn for non-dropped layers.
    /// `end_lsn` is exclusive
    pub async fn freeze(&self, end_lsn: Lsn) {
        assert!(
            self.start_lsn < end_lsn,
            "{} >= {}",
            self.start_lsn,
            end_lsn
        );
        self.end_lsn.set(end_lsn).expect("end_lsn set only once");

        #[cfg(debug_assertions)]
        {
            let inner = self.inner.write().await;
            for vec_map in inner.index.values() {
                for (lsn, _pos) in vec_map.as_slice() {
                    debug_assert!(*lsn < end_lsn);
                }
            }
        }
    }

    /// Write this frozen in-memory layer to disk.
    ///
    /// Returns a new delta layer with all the same data as this in-memory layer
    pub async fn write_to_disk(
        &self,
        ctx: &RequestContext,
    ) -> Result<(PersistentLayerDesc, Utf8PathBuf)> {
        // Grab the lock in read-mode. We hold it over the I/O, but because this
        // layer is not writeable anymore, no one should be trying to acquire the
        // write lock on it, so we shouldn't block anyone. There's one exception
        // though: another thread might have grabbed a reference to this layer
        // in `get_layer_for_write' just before the checkpointer called
        // `freeze`, and then `write_to_disk` on it. When the thread gets the
        // lock, it will see that it's not writeable anymore and retry, but it
        // would have to wait until we release it. That race condition is very
        // rare though, so we just accept the potential latency hit for now.
        let inner = self.inner.read().await;

        let end_lsn = *self.end_lsn.get().unwrap();

        let mut delta_layer_writer = DeltaLayerWriter::new(
            self.conf,
            self.timeline_id,
            self.tenant_shard_id,
            Key::MIN,
            self.start_lsn..end_lsn,
        )
        .await?;

        let mut buf = Vec::new();

        let cursor = inner.file.block_cursor();

        // Sort the keys because delta layer writer expects them sorted.
        //
        // NOTE: this sort can take up significant time if the layer has millions of
        //       keys. To speed up all the comparisons we convert the key to i128 and
        //       keep the value as a reference.
        let mut keys: Vec<_> = inner.index.iter().map(|(k, m)| (k.to_i128(), m)).collect();
        keys.sort_unstable_by_key(|k| k.0);

        let ctx = RequestContextBuilder::extend(ctx)
            .page_content_kind(PageContentKind::InMemoryLayer)
            .build();
        for (key, vec_map) in keys.iter() {
            let key = Key::from_i128(*key);
            // Write all page versions
            for (lsn, pos) in vec_map.as_slice() {
                cursor.read_blob_into_buf(*pos, &mut buf, &ctx).await?;
                let will_init = Value::des(&buf)?.will_init();
                let res;
                (buf, res) = delta_layer_writer
                    .put_value_bytes(key, *lsn, buf, will_init)
                    .await;
                res?;
            }
        }

        // MAX is used here because we identify L0 layers by full key range
        delta_layer_writer.finish(Key::MAX).await
    }
}
