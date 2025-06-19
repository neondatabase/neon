//! An in-memory layer stores recently received key-value pairs.
//!
//! The "in-memory" part of the name is a bit misleading: the actual page versions are
//! held in an ephemeral file, not in memory. The metadata for each page version, i.e.
//! its position in the file, is kept in memory, though.
//!
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use anyhow::Result;
use camino::Utf8PathBuf;
use pageserver_api::key::{CompactKey, Key};
use pageserver_api::keyspace::KeySpace;
use pageserver_api::models::InMemoryLayerInfo;
use pageserver_api::shard::TenantShardId;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::id::TimelineId;
use utils::lsn::Lsn;
use utils::vec_map::VecMap;
use wal_decoder::serialized_batch::{SerializedValueBatch, SerializedValueMeta, ValueMeta};

use super::{DeltaLayerWriter, PersistentLayerDesc, ValuesReconstructState};
use crate::assert_u64_eq_usize::{U64IsUsize, UsizeIsU64, u64_to_usize};
use crate::config::PageServerConf;
use crate::context::{PageContentKind, RequestContext, RequestContextBuilder};
// avoid binding to Write (conflicts with std::io::Write)
// while being able to use std::fmt::Write's methods
use crate::metrics::TIMELINE_EPHEMERAL_BYTES;
use crate::tenant::ephemeral_file::EphemeralFile;
use crate::tenant::storage_layer::{OnDiskValue, OnDiskValueIo};
use crate::tenant::timeline::GetVectoredError;
use crate::virtual_file::owned_buffers_io::io_buf_ext::IoBufExt;
use crate::{l0_flush, page_cache};

pub(crate) mod vectored_dio_read;

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
    pub(crate) end_lsn: OnceLock<Lsn>,

    /// Used for traversal path. Cached representation of the in-memory layer after frozen.
    frozen_local_path_str: OnceLock<Arc<str>>,

    opened_at: Instant,

    /// All versions of all pages in the layer are kept here. Indexed
    /// by block number and LSN. The [`IndexEntry`] is an offset into the
    /// ephemeral file where the page version is stored.
    ///
    /// We use a separate lock for the index to reduce the critical section
    /// during which reads cannot be planned.
    ///
    /// Note that the file backing [`InMemoryLayer::file`] is append-only,
    /// so it is not necessary to hold a lock on the index while reading or writing from the file.
    /// In particular:
    /// 1. It is safe to read and release [`InMemoryLayer::index`] before reading from [`InMemoryLayer::file`].
    /// 2. It is safe to write to [`InMemoryLayer::file`] before locking and updating [`InMemoryLayer::index`].
    index: RwLock<BTreeMap<CompactKey, VecMap<Lsn, IndexEntry>>>,

    /// Wrapper for the actual on-disk file. Uses interior mutability for concurrent reads/writes.
    file: EphemeralFile,

    estimated_in_mem_size: AtomicU64,
}

impl std::fmt::Debug for InMemoryLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryLayer")
            .field("start_lsn", &self.start_lsn)
            .field("end_lsn", &self.end_lsn)
            .finish()
    }
}

/// Support the same max blob length as blob_io, because ultimately
/// all the InMemoryLayer contents end up being written into a delta layer,
/// using the [`crate::tenant::blob_io`].
const MAX_SUPPORTED_BLOB_LEN: usize = crate::tenant::blob_io::MAX_SUPPORTED_BLOB_LEN;
const MAX_SUPPORTED_BLOB_LEN_BITS: usize = {
    let trailing_ones = MAX_SUPPORTED_BLOB_LEN.trailing_ones() as usize;
    let leading_zeroes = MAX_SUPPORTED_BLOB_LEN.leading_zeros() as usize;
    assert!(trailing_ones + leading_zeroes == std::mem::size_of::<usize>() * 8);
    trailing_ones
};

/// See [`InMemoryLayer::index`].
///
/// For memory efficiency, the data is packed into a u64.
///
/// Layout:
/// - 1 bit: `will_init`
/// - [`MAX_SUPPORTED_BLOB_LEN_BITS`][]: `len`
/// - [`MAX_SUPPORTED_POS_BITS`](IndexEntry::MAX_SUPPORTED_POS_BITS): `pos`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry(u64);

impl IndexEntry {
    /// See [`Self::MAX_SUPPORTED_POS`].
    const MAX_SUPPORTED_POS_BITS: usize = {
        let remainder = 64 - 1 - MAX_SUPPORTED_BLOB_LEN_BITS;
        if remainder < 32 {
            panic!("pos can be u32 as per type system, support that");
        }
        remainder
    };
    /// The maximum supported blob offset that can be represented by [`Self`].
    /// See also [`Self::validate_checkpoint_distance`].
    const MAX_SUPPORTED_POS: usize = (1 << Self::MAX_SUPPORTED_POS_BITS) - 1;

    // Layout
    const WILL_INIT_RANGE: Range<usize> = 0..1;
    const LEN_RANGE: Range<usize> =
        Self::WILL_INIT_RANGE.end..Self::WILL_INIT_RANGE.end + MAX_SUPPORTED_BLOB_LEN_BITS;
    const POS_RANGE: Range<usize> =
        Self::LEN_RANGE.end..Self::LEN_RANGE.end + Self::MAX_SUPPORTED_POS_BITS;
    const _ASSERT: () = {
        if Self::POS_RANGE.end != 64 {
            panic!("we don't want undefined bits for our own sanity")
        }
    };

    /// Fails if and only if the offset or length encoded in `arg` is too large to be represented by [`Self`].
    ///
    /// The only reason why that can happen in the system is if the [`InMemoryLayer`] grows too long.
    /// The [`InMemoryLayer`] size is determined by the checkpoint distance, enforced by [`crate::tenant::Timeline::should_roll`].
    ///
    /// Thus, to avoid failure of this function, whenever we start up and/or change checkpoint distance,
    /// call [`Self::validate_checkpoint_distance`] with the new checkpoint distance value.
    ///
    /// TODO: this check should happen ideally at config parsing time (and in the request handler when a change to checkpoint distance is requested)
    /// When cleaning this up, also look into the s3 max file size check that is performed in delta layer writer.
    #[inline(always)]
    fn new(arg: IndexEntryNewArgs) -> anyhow::Result<Self> {
        let IndexEntryNewArgs {
            base_offset,
            batch_offset,
            len,
            will_init,
        } = arg;

        let pos = base_offset
            .checked_add(batch_offset)
            .ok_or_else(|| anyhow::anyhow!("base_offset + batch_offset overflows u64: base_offset={base_offset} batch_offset={batch_offset}"))?;

        if pos.into_usize() > Self::MAX_SUPPORTED_POS {
            anyhow::bail!(
                "base_offset+batch_offset exceeds the maximum supported value: base_offset={base_offset} batch_offset={batch_offset} (+)={pos} max={max}",
                max = Self::MAX_SUPPORTED_POS
            );
        }

        if len > MAX_SUPPORTED_BLOB_LEN {
            anyhow::bail!(
                "len exceeds the maximum supported length: len={len} max={MAX_SUPPORTED_BLOB_LEN}",
            );
        }

        let mut data: u64 = 0;
        use bit_field::BitField;
        data.set_bits(Self::WILL_INIT_RANGE, if will_init { 1 } else { 0 });
        data.set_bits(Self::LEN_RANGE, len.into_u64());
        data.set_bits(Self::POS_RANGE, pos);

        Ok(Self(data))
    }

    #[inline(always)]
    fn unpack(&self) -> IndexEntryUnpacked {
        use bit_field::BitField;
        IndexEntryUnpacked {
            will_init: self.0.get_bits(Self::WILL_INIT_RANGE) != 0,
            len: self.0.get_bits(Self::LEN_RANGE),
            pos: self.0.get_bits(Self::POS_RANGE),
        }
    }

    /// See [`Self::new`].
    pub(crate) const fn validate_checkpoint_distance(
        checkpoint_distance: u64,
    ) -> Result<(), &'static str> {
        if checkpoint_distance > Self::MAX_SUPPORTED_POS as u64 {
            return Err("exceeds the maximum supported value");
        }
        let res = u64_to_usize(checkpoint_distance).checked_add(MAX_SUPPORTED_BLOB_LEN);
        if res.is_none() {
            return Err(
                "checkpoint distance + max supported blob len overflows in-memory addition",
            );
        }

        // NB: it is ok for the result of the addition to be larger than MAX_SUPPORTED_POS

        Ok(())
    }

    const _ASSERT_DEFAULT_CHECKPOINT_DISTANCE_IS_VALID: () = {
        let res = Self::validate_checkpoint_distance(
            pageserver_api::config::tenant_conf_defaults::DEFAULT_CHECKPOINT_DISTANCE,
        );
        if res.is_err() {
            panic!("default checkpoint distance is valid")
        }
    };
}

/// Args to [`IndexEntry::new`].
#[derive(Clone, Copy)]
struct IndexEntryNewArgs {
    base_offset: u64,
    batch_offset: u64,
    len: usize,
    will_init: bool,
}

/// Unpacked representation of the bitfielded [`IndexEntry`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct IndexEntryUnpacked {
    will_init: bool,
    len: u64,
    pos: u64,
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
pub(crate) struct GlobalResourceUnits {
    // How many dirty bytes have I added to the global dirty_bytes: this guard object is responsible
    // for decrementing the global counter by this many bytes when dropped.
    dirty_bytes: u64,
}

impl GlobalResourceUnits {
    // Hint for the layer append path to update us when the layer size differs from the last
    // call to update_size by this much.  If we don't reach this threshold, we'll still get
    // updated when the Timeline "ticks" in the background.
    const MAX_SIZE_DRIFT: u64 = 10 * 1024 * 1024;

    pub(crate) fn new() -> Self {
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
    pub(crate) fn publish_size(&mut self, size: u64) -> Option<u64> {
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
    pub(crate) fn maybe_publish_size(&mut self, size: u64) {
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

    pub(crate) fn len(&self) -> u64 {
        self.file.len()
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
    pub async fn dump(&self, _verbose: bool, _ctx: &RequestContext) -> Result<()> {
        let end_str = self.end_lsn_or_max();

        println!(
            "----- in-memory layer for tli {} LSNs {}-{} ----",
            self.timeline_id, self.start_lsn, end_str,
        );

        Ok(())
    }

    // Look up the keys in the provided keyspace and update
    // the reconstruct state with whatever is found.
    pub async fn get_values_reconstruct_data(
        self: &Arc<InMemoryLayer>,
        keyspace: KeySpace,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> Result<(), GetVectoredError> {
        let ctx = RequestContextBuilder::from(ctx)
            .page_content_kind(PageContentKind::InMemoryLayer)
            .attached_child();

        let index = self.index.read().await;

        struct ValueRead {
            entry_lsn: Lsn,
            read: vectored_dio_read::LogicalRead<Vec<u8>>,
        }
        let mut reads: HashMap<Key, Vec<ValueRead>> = HashMap::new();
        let mut ios: HashMap<(Key, Lsn), OnDiskValueIo> = Default::default();

        for range in keyspace.ranges.iter() {
            for (key, vec_map) in index.range(range.start.to_compact()..range.end.to_compact()) {
                let key = Key::from_compact(*key);
                let slice = vec_map.slice_range(lsn_range.clone());

                for (entry_lsn, index_entry) in slice.iter().rev() {
                    let IndexEntryUnpacked {
                        pos,
                        len,
                        will_init,
                    } = index_entry.unpack();

                    reads.entry(key).or_default().push(ValueRead {
                        entry_lsn: *entry_lsn,
                        read: vectored_dio_read::LogicalRead::new(
                            pos,
                            Vec::with_capacity(len as usize),
                        ),
                    });

                    let io = reconstruct_state.update_key(&key, *entry_lsn, will_init);
                    ios.insert((key, *entry_lsn), io);

                    if will_init {
                        break;
                    }
                }
            }
        }
        drop(index); // release the lock before we spawn the IO
        let read_from = Arc::clone(self);
        let read_ctx = ctx.attached_child();
        reconstruct_state
            .spawn_io(async move {
                let f = vectored_dio_read::execute(
                    &read_from.file,
                    reads
                        .iter()
                        .flat_map(|(_, value_reads)| value_reads.iter().map(|v| &v.read)),
                    &read_ctx,
                );
                send_future::SendFuture::send(f) // https://github.com/rust-lang/rust/issues/96865
                    .await;

                for (key, value_reads) in reads {
                    for ValueRead { entry_lsn, read } in value_reads {
                        let io = ios.remove(&(key, entry_lsn)).expect("sender must exist");
                        match read.into_result().expect("we run execute() above") {
                            Err(e) => {
                                io.complete(Err(std::io::Error::new(
                                    e.kind(),
                                    "dio vec read failed",
                                )));
                            }
                            Ok(value_buf) => {
                                io.complete(Ok(OnDiskValue::WalRecordOrImage(value_buf.into())));
                            }
                        }
                    }
                }

                assert!(ios.is_empty());

                // Keep layer existent until this IO is done;
                // This is kinda forced for InMemoryLayer because we need to inner.read() anyway,
                // but it's less obvious for DeltaLayer and ImageLayer. So, keep this explicit
                // drop for consistency among all three layer types.
                drop(read_from);
            })
            .await;

        Ok(())
    }
}

fn inmem_layer_display(mut f: impl Write, start_lsn: Lsn, end_lsn: Lsn) -> std::fmt::Result {
    write!(f, "inmem-{:016X}-{:016X}", start_lsn.0, end_lsn.0)
}

fn inmem_layer_log_display(
    mut f: impl Write,
    timeline: TimelineId,
    start_lsn: Lsn,
    end_lsn: Lsn,
) -> std::fmt::Result {
    write!(f, "timeline {} in-memory ", timeline)?;
    inmem_layer_display(f, start_lsn, end_lsn)
}

impl std::fmt::Display for InMemoryLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let end_lsn = self.end_lsn_or_max();
        inmem_layer_display(f, self.start_lsn, end_lsn)
    }
}

impl InMemoryLayer {
    pub fn estimated_in_mem_size(&self) -> u64 {
        self.estimated_in_mem_size.load(AtomicOrdering::Relaxed)
    }

    /// Create a new, empty, in-memory layer
    pub async fn create(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        start_lsn: Lsn,
        gate: &utils::sync::gate::Gate,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<InMemoryLayer> {
        trace!(
            "initializing new empty InMemoryLayer for writing on timeline {timeline_id} at {start_lsn}"
        );

        let file =
            EphemeralFile::create(conf, tenant_shard_id, timeline_id, gate, cancel, ctx).await?;
        let key = InMemoryLayerFileId(file.page_cache_file_id());

        Ok(InMemoryLayer {
            file_id: key,
            frozen_local_path_str: OnceLock::new(),
            conf,
            timeline_id,
            tenant_shard_id,
            start_lsn,
            end_lsn: OnceLock::new(),
            opened_at: Instant::now(),
            index: RwLock::new(BTreeMap::new()),
            file,
            estimated_in_mem_size: AtomicU64::new(0),
        })
    }

    /// Write path.
    ///
    /// Errors are not retryable, the [`InMemoryLayer`] must be discarded, and not be read from.
    /// The reason why it's not retryable is that the [`EphemeralFile`] writes are not retryable.
    ///
    /// This method shall not be called concurrently. We enforce this property via [`crate::tenant::Timeline::write_lock`].
    ///
    /// TODO: it can be made retryable if we aborted the process on EphemeralFile write errors.
    pub async fn put_batch(
        &self,
        serialized_batch: SerializedValueBatch,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        self.assert_writable();

        let base_offset = self.file.len();

        let SerializedValueBatch {
            raw,
            metadata,
            max_lsn: _,
            len: _,
        } = serialized_batch;

        // Write the batch to the file
        self.file.write_raw(&raw, ctx).await?;
        let new_size = self.file.len();

        let expected_new_len = base_offset
            .checked_add(raw.len().into_u64())
            // write_raw would error if we were to overflow u64.
            // also IndexEntry and higher levels in
            //the code don't allow the file to grow that large
            .unwrap();
        assert_eq!(new_size, expected_new_len);

        // Update the index with the new entries
        let mut index = self.index.write().await;

        for meta in metadata {
            let SerializedValueMeta {
                key,
                lsn,
                batch_offset,
                len,
                will_init,
            } = match meta {
                ValueMeta::Serialized(ser) => ser,
                ValueMeta::Observed(_) => {
                    continue;
                }
            };

            // Add the base_offset to the batch's index entries which are relative to the batch start.
            let index_entry = IndexEntry::new(IndexEntryNewArgs {
                base_offset,
                batch_offset,
                len,
                will_init,
            })?;

            let vec_map = index.entry(key).or_default();
            let old = vec_map.append_or_update_last(lsn, index_entry).unwrap().0;
            if old.is_some() {
                // This should not break anything, but is unexpected: ingestion code aims to filter out
                // multiple writes to the same key at the same LSN.  This happens in cases where our
                // ingenstion code generates some write like an empty page, and we see a write from postgres
                // to the same key in the same wal record.  If one such write makes it through, we
                // index the most recent write, implicitly ignoring the earlier write.  We log a warning
                // because this case is unexpected, and we would like tests to fail if this happens.
                warn!("Key {} at {} written twice at same LSN", key, lsn);
            }
            self.estimated_in_mem_size.fetch_add(
                (std::mem::size_of::<CompactKey>()
                    + std::mem::size_of::<Lsn>()
                    + std::mem::size_of::<IndexEntry>()) as u64,
                AtomicOrdering::Relaxed,
            );
        }

        Ok(())
    }

    pub(crate) fn get_opened_at(&self) -> Instant {
        self.opened_at
    }

    pub(crate) fn tick(&self) -> Option<u64> {
        self.file.tick()
    }

    pub(crate) async fn put_tombstones(&self, _key_ranges: &[(Range<Key>, Lsn)]) -> Result<()> {
        // TODO: Currently, we just leak the storage for any deleted keys
        Ok(())
    }

    /// Records the end_lsn for non-dropped layers.
    /// `end_lsn` is exclusive
    ///
    /// A note on locking:
    /// The current API of [`InMemoryLayer`] does not ensure that there's no ongoing
    /// writes while freezing the layer. This is enforced at a higher level via
    /// [`crate::tenant::Timeline::write_lock`]. Freeze might be called via two code paths:
    /// 1. Via the active [`crate::tenant::timeline::TimelineWriter`]. This holds the
    ///    Timeline::write_lock for its lifetime. The rolling is handled in
    ///    [`crate::tenant::timeline::TimelineWriter::put_batch`]. It's a &mut self function
    ///    so can't be called from different threads.
    /// 2. In the background via [`crate::tenant::Timeline::maybe_freeze_ephemeral_layer`].
    ///    This only proceeds if try_lock on Timeline::write_lock succeeds (i.e. there's no active writer),
    ///    hence there can be no concurrent writes
    pub async fn freeze(&self, end_lsn: Lsn) {
        assert!(
            self.start_lsn < end_lsn,
            "{} >= {}",
            self.start_lsn,
            end_lsn
        );
        self.end_lsn.set(end_lsn).expect("end_lsn set only once");

        self.frozen_local_path_str
            .set({
                let mut buf = String::new();
                inmem_layer_log_display(&mut buf, self.get_timeline_id(), self.start_lsn, end_lsn)
                    .unwrap();
                buf.into()
            })
            .expect("frozen_local_path_str set only once");

        #[cfg(debug_assertions)]
        {
            let index = self.index.read().await;
            for vec_map in index.values() {
                for (lsn, _) in vec_map.as_slice() {
                    assert!(*lsn < end_lsn);
                }
            }
        }
    }

    /// Write this frozen in-memory layer to disk. If `key_range` is set, the delta
    /// layer will only contain the key range the user specifies, and may return `None`
    /// if there are no matching keys.
    ///
    /// Returns a new delta layer with all the same data as this in-memory layer
    pub async fn write_to_disk(
        &self,
        ctx: &RequestContext,
        key_range: Option<Range<Key>>,
        l0_flush_global_state: &l0_flush::Inner,
        gate: &utils::sync::gate::Gate,
        cancel: CancellationToken,
    ) -> Result<Option<(PersistentLayerDesc, Utf8PathBuf)>> {
        let index = self.index.read().await;

        use l0_flush::Inner;
        let _concurrency_permit = match l0_flush_global_state {
            Inner::Direct { semaphore, .. } => Some(semaphore.acquire().await),
        };

        let end_lsn = *self.end_lsn.get().unwrap();

        let key_count = if let Some(key_range) = key_range {
            let key_range = key_range.start.to_compact()..key_range.end.to_compact();

            index.iter().filter(|(k, _)| key_range.contains(k)).count()
        } else {
            index.len()
        };
        if key_count == 0 {
            return Ok(None);
        }

        let mut delta_layer_writer = DeltaLayerWriter::new(
            self.conf,
            self.timeline_id,
            self.tenant_shard_id,
            Key::MIN,
            self.start_lsn..end_lsn,
            gate,
            cancel,
            ctx,
        )
        .await?;

        match l0_flush_global_state {
            l0_flush::Inner::Direct { .. } => {
                let file_contents = self.file.load_to_io_buf(ctx).await?;
                let file_contents = file_contents.freeze();

                for (key, vec_map) in index.iter() {
                    // Write all page versions
                    for (lsn, entry) in vec_map
                        .as_slice()
                        .iter()
                        .map(|(lsn, entry)| (lsn, entry.unpack()))
                    {
                        let IndexEntryUnpacked {
                            pos,
                            len,
                            will_init,
                        } = entry;
                        let buf = file_contents.slice(pos as usize..(pos + len) as usize);
                        let (_buf, res) = delta_layer_writer
                            .put_value_bytes(
                                Key::from_compact(*key),
                                *lsn,
                                buf.slice_len(),
                                will_init,
                                ctx,
                            )
                            .await;
                        res?;
                    }
                }
            }
        }

        // MAX is used here because we identify L0 layers by full key range
        let (desc, path) = delta_layer_writer.finish(Key::MAX, ctx).await?;

        // Hold the permit until all the IO is done, including the fsync in `delta_layer_writer.finish()``.
        //
        // If we didn't and our caller drops this future, tokio-epoll-uring would extend the lifetime of
        // the `file_contents: Vec<u8>` until the IO is done, but not the permit's lifetime.
        // Thus, we'd have more concurrenct `Vec<u8>` in existence than the semaphore allows.
        //
        // We hold across the fsync so that on ext4 mounted with data=ordered, all the kernel page cache pages
        // we dirtied when writing to the filesystem have been flushed and marked !dirty.
        drop(_concurrency_permit);

        Ok(Some((desc, path)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_entry() {
        const MAX_SUPPORTED_POS: usize = IndexEntry::MAX_SUPPORTED_POS;
        use {IndexEntryNewArgs as Args, IndexEntryUnpacked as Unpacked};

        let roundtrip = |args, expect: Unpacked| {
            let res = IndexEntry::new(args).expect("this tests expects no errors");
            let IndexEntryUnpacked {
                will_init,
                len,
                pos,
            } = res.unpack();
            assert_eq!(will_init, expect.will_init);
            assert_eq!(len, expect.len);
            assert_eq!(pos, expect.pos);
        };

        // basic roundtrip
        for pos in [0, MAX_SUPPORTED_POS] {
            for len in [0, MAX_SUPPORTED_BLOB_LEN] {
                for will_init in [true, false] {
                    let expect = Unpacked {
                        will_init,
                        len: len.into_u64(),
                        pos: pos.into_u64(),
                    };
                    roundtrip(
                        Args {
                            will_init,
                            base_offset: pos.into_u64(),
                            batch_offset: 0,
                            len,
                        },
                        expect,
                    );
                    roundtrip(
                        Args {
                            will_init,
                            base_offset: 0,
                            batch_offset: pos.into_u64(),
                            len,
                        },
                        expect,
                    );
                }
            }
        }

        // too-large len
        let too_large = Args {
            will_init: false,
            len: MAX_SUPPORTED_BLOB_LEN + 1,
            base_offset: 0,
            batch_offset: 0,
        };
        assert!(IndexEntry::new(too_large).is_err());

        // too-large pos
        {
            let too_large = Args {
                will_init: false,
                len: 0,
                base_offset: MAX_SUPPORTED_POS.into_u64() + 1,
                batch_offset: 0,
            };
            assert!(IndexEntry::new(too_large).is_err());
            let too_large = Args {
                will_init: false,
                len: 0,
                base_offset: 0,
                batch_offset: MAX_SUPPORTED_POS.into_u64() + 1,
            };
            assert!(IndexEntry::new(too_large).is_err());
        }

        // too large (base_offset + batch_offset)
        {
            let too_large = Args {
                will_init: false,
                len: 0,
                base_offset: MAX_SUPPORTED_POS.into_u64(),
                batch_offset: 1,
            };
            assert!(IndexEntry::new(too_large).is_err());
            let too_large = Args {
                will_init: false,
                len: 0,
                base_offset: MAX_SUPPORTED_POS.into_u64() - 1,
                batch_offset: MAX_SUPPORTED_POS.into_u64() - 1,
            };
            assert!(IndexEntry::new(too_large).is_err());
        }

        // valid special cases
        // - area past the max supported pos that is accessible by len
        for len in [1, MAX_SUPPORTED_BLOB_LEN] {
            roundtrip(
                Args {
                    will_init: false,
                    len,
                    base_offset: MAX_SUPPORTED_POS.into_u64(),
                    batch_offset: 0,
                },
                Unpacked {
                    will_init: false,
                    len: len as u64,
                    pos: MAX_SUPPORTED_POS.into_u64(),
                },
            );
            roundtrip(
                Args {
                    will_init: false,
                    len,
                    base_offset: 0,
                    batch_offset: MAX_SUPPORTED_POS.into_u64(),
                },
                Unpacked {
                    will_init: false,
                    len: len as u64,
                    pos: MAX_SUPPORTED_POS.into_u64(),
                },
            );
        }
    }
}
