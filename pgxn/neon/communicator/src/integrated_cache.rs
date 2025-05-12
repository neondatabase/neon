//! Integrated communicator cache
//!
//! It tracks:
//! - Relation sizes and existence
//! - Last-written LSN
//! - Block cache (also known as LFC)
//!
//! TODO: limit the size
//! TODO: concurrency
//!
//! Note: This deals with "relations" which is really just one "relation fork" in Postgres
//! terms. RelFileLocator + ForkNumber is the key.

//
// TODO: Thoughts on eviction:
//
// There are two things we need to track, and evict if we run out of space:
// - blocks in the file cache's file. If the file grows too large, need to evict something.
//   Also if the cache is resized
//
// - entries in the cache tree. If we run out of memory in the shmem area, need to evict
//   something
//

use std::mem::MaybeUninit;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use utils::lsn::{AtomicLsn, Lsn};
use zerocopy::FromBytes;

use crate::file_cache::INVALID_CACHE_BLOCK;
use crate::file_cache::{CacheBlock, FileCache};
use pageserver_page_api::model::RelTag;

use metrics::{IntCounter, IntGauge, IntGaugeVec};

use neonart;
use neonart::TreeInitStruct;
use neonart::TreeIterator;
use neonart::UpdateAction;

const CACHE_AREA_SIZE: usize = 10 * 1024 * 1024;

type IntegratedCacheTreeInitStruct<'t> =
    TreeInitStruct<'t, TreeKey, TreeEntry, neonart::ArtMultiSlabAllocator<'t, TreeEntry>>;

/// This struct is initialized at postmaster startup, and passed to all the processes via fork().
pub struct IntegratedCacheInitStruct<'t> {
    allocator: &'t neonart::ArtMultiSlabAllocator<'t, TreeEntry>,
    handle: IntegratedCacheTreeInitStruct<'t>,
}

/// Represents write-access to the integrated cache. This is used by the communicator process.
pub struct IntegratedCacheWriteAccess<'t> {
    cache_tree: neonart::TreeWriteAccess<
        't,
        TreeKey,
        TreeEntry,
        neonart::ArtMultiSlabAllocator<'t, TreeEntry>,
    >,

    global_lw_lsn: AtomicU64,

    pub(crate) file_cache: Option<FileCache>,

    // Fields for eviction
    clock_hand: std::sync::Mutex<TreeIterator<TreeKey>>,

    // Metrics
    page_evictions_counter: IntCounter,
    clock_iterations_counter: IntCounter,

    nodes_total: IntGaugeVec,
    nodes_leaf_total: IntGauge,
    nodes_internal4_total: IntGauge,
    nodes_internal16_total: IntGauge,
    nodes_internal48_total: IntGauge,
    nodes_internal256_total: IntGauge,

    nodes_memory_bytes: IntGaugeVec,
    nodes_memory_leaf_bytes: IntGauge,
    nodes_memory_internal4_bytes: IntGauge,
    nodes_memory_internal16_bytes: IntGauge,
    nodes_memory_internal48_bytes: IntGauge,
    nodes_memory_internal256_bytes: IntGauge,

    // metrics from the art tree
    cache_memory_size_bytes: IntGauge,
    cache_memory_used_bytes: IntGauge,
    cache_tree_epoch: IntGauge,
    cache_tree_oldest_epoch: IntGauge,
    cache_tree_garbage_total: IntGauge,
}

/// Represents read-only access to the integrated cache. Backend processes have this.
pub struct IntegratedCacheReadAccess<'t> {
    cache_tree: neonart::TreeReadAccess<'t, TreeKey, TreeEntry>,
}

impl<'t> IntegratedCacheInitStruct<'t> {
    /// Return the desired size in bytes of the shared memory area to reserve for the integrated
    /// cache.
    pub fn shmem_size(_max_procs: u32) -> usize {
        CACHE_AREA_SIZE
    }

    /// Initialize the shared memory segment. This runs once in postmaster. Returns a struct which
    /// will be inherited by all processes through fork.
    pub fn shmem_init(
        _max_procs: u32,
        shmem_area: &'t mut [MaybeUninit<u8>],
    ) -> IntegratedCacheInitStruct<'t> {
        let allocator = neonart::ArtMultiSlabAllocator::new(shmem_area);

        let handle = IntegratedCacheTreeInitStruct::new(allocator);

        // Initialize the shared memory area
        IntegratedCacheInitStruct { allocator, handle }
    }

    pub fn worker_process_init(
        self,
        lsn: Lsn,
        file_cache: Option<FileCache>,
    ) -> IntegratedCacheWriteAccess<'t> {
        let IntegratedCacheInitStruct {
            allocator: _allocator,
            handle,
        } = self;
        let tree_writer = handle.attach_writer();

        let nodes_total = IntGaugeVec::new(
            metrics::core::Opts::new("nodes_total", "Number of nodes in cache tree."),
            &["node_kind"],
        )
        .unwrap();
        let nodes_leaf_total = nodes_total.with_label_values(&["leaf"]);
        let nodes_internal4_total = nodes_total.with_label_values(&["internal4"]);
        let nodes_internal16_total = nodes_total.with_label_values(&["internal16"]);
        let nodes_internal48_total = nodes_total.with_label_values(&["internal48"]);
        let nodes_internal256_total = nodes_total.with_label_values(&["internal256"]);

        let nodes_memory_bytes = IntGaugeVec::new(
            metrics::core::Opts::new(
                "nodes_memory_bytes",
                "Memory reserved for nodes in cache tree.",
            ),
            &["node_kind"],
        )
        .unwrap();
        let nodes_memory_leaf_bytes = nodes_memory_bytes.with_label_values(&["leaf"]);
        let nodes_memory_internal4_bytes = nodes_memory_bytes.with_label_values(&["internal4"]);
        let nodes_memory_internal16_bytes = nodes_memory_bytes.with_label_values(&["internal16"]);
        let nodes_memory_internal48_bytes = nodes_memory_bytes.with_label_values(&["internal48"]);
        let nodes_memory_internal256_bytes = nodes_memory_bytes.with_label_values(&["internal256"]);

        IntegratedCacheWriteAccess {
            cache_tree: tree_writer,
            global_lw_lsn: AtomicU64::new(lsn.0),
            file_cache,
            clock_hand: std::sync::Mutex::new(TreeIterator::new_wrapping()),

            page_evictions_counter: metrics::IntCounter::new(
                "integrated_cache_evictions",
                "Page evictions from the Local File Cache",
            )
            .unwrap(),

            clock_iterations_counter: metrics::IntCounter::new(
                "clock_iterations",
                "Number of times the clock hand has moved",
            )
            .unwrap(),

            nodes_total,
            nodes_leaf_total,
            nodes_internal4_total,
            nodes_internal16_total,
            nodes_internal48_total,
            nodes_internal256_total,

            nodes_memory_bytes,
            nodes_memory_leaf_bytes,
            nodes_memory_internal4_bytes,
            nodes_memory_internal16_bytes,
            nodes_memory_internal48_bytes,
            nodes_memory_internal256_bytes,

            cache_memory_size_bytes: metrics::IntGauge::new(
                "cache_memory_size_bytes",
                "Memory reserved for cache metadata",
            )
            .unwrap(),
            cache_memory_used_bytes: metrics::IntGauge::new(
                "cache_memory_size_bytes",
                "Memory used for cache metadata",
            )
            .unwrap(),

            cache_tree_epoch: metrics::IntGauge::new(
                "cache_tree_epoch",
                "Current epoch of the cache tree",
            )
            .unwrap(),
            cache_tree_oldest_epoch: metrics::IntGauge::new(
                "cache_tree_oldest_epoch",
                "Oldest active epoch of the cache tree",
            )
            .unwrap(),
            cache_tree_garbage_total: metrics::IntGauge::new(
                "cache_tree_garbage_total",
                "Number of obsoleted nodes in cache tree pending GC",
            )
            .unwrap(),
        }
    }

    pub fn backend_init(self) -> IntegratedCacheReadAccess<'t> {
        let IntegratedCacheInitStruct {
            allocator: _allocator,
            handle,
        } = self;

        let tree_reader = handle.attach_reader();

        IntegratedCacheReadAccess {
            cache_tree: tree_reader,
        }
    }
}

enum TreeEntry {
    Rel(RelEntry),
    Block(BlockEntry),
}

struct BlockEntry {
    lw_lsn: AtomicLsn,
    cache_block: AtomicU64,

    pinned: AtomicBool,

    // 'referenced' bit for the clock algorithm
    referenced: AtomicBool,
}

struct RelEntry {
    /// cached size of the relation
    /// u32::MAX means 'not known' (that's InvalidBlockNumber in Postgres)
    nblocks: AtomicU32,
}

impl std::fmt::Debug for TreeEntry {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            TreeEntry::Rel(e) => fmt
                .debug_struct("Rel")
                .field("nblocks", &e.nblocks.load(Ordering::Relaxed))
                .finish(),
            TreeEntry::Block(e) => fmt
                .debug_struct("Block")
                .field("lw_lsn", &e.lw_lsn.load())
                .field("cache_block", &e.cache_block.load(Ordering::Relaxed))
                .field("pinned", &e.pinned.load(Ordering::Relaxed))
                .field("referenced", &e.referenced.load(Ordering::Relaxed))
                .finish(),
        }
    }
}

#[derive(
    Clone,
    Debug,
    PartialEq,
    PartialOrd,
    Eq,
    Ord,
    zerocopy_derive::IntoBytes,
    zerocopy_derive::Immutable,
    zerocopy_derive::FromBytes,
)]
#[repr(packed)]
// Note: the fields are stored in big-endian order, to make the radix tree more
// efficient, and to make scans over ranges of blocks work correctly.
struct TreeKey {
    spc_oid_be: u32,
    db_oid_be: u32,
    rel_number_be: u32,
    fork_number: u8,
    block_number_be: u32,
}
impl<'a> From<&'a [u8]> for TreeKey {
    fn from(bytes: &'a [u8]) -> Self {
        Self::read_from_bytes(bytes).expect("invalid key length")
    }
}

fn key_range_for_rel_blocks(rel: &RelTag) -> Range<TreeKey> {
    Range {
        start: TreeKey::from((rel, 0)),
        end: TreeKey::from((rel, u32::MAX)),
    }
}

impl From<&RelTag> for TreeKey {
    fn from(val: &RelTag) -> TreeKey {
        TreeKey {
            spc_oid_be: val.spc_oid.to_be(),
            db_oid_be: val.db_oid.to_be(),
            rel_number_be: val.rel_number.to_be(),
            fork_number: val.fork_number.to_be(),
            block_number_be: u32::MAX.to_be(),
        }
    }
}

impl From<(&RelTag, u32)> for TreeKey {
    fn from(val: (&RelTag, u32)) -> TreeKey {
        TreeKey {
            spc_oid_be: val.0.spc_oid.to_be(),
            db_oid_be: val.0.db_oid.to_be(),
            rel_number_be: val.0.rel_number.to_be(),
            fork_number: val.0.fork_number.to_be(),
            block_number_be: val.1.to_be(),
        }
    }
}

impl neonart::Key for TreeKey {
    const KEY_LEN: usize = 4 + 4 + 4 + 1 + 4;

    fn as_bytes(&self) -> &[u8] {
        zerocopy::IntoBytes::as_bytes(self)
    }
}

impl neonart::Value for TreeEntry {}

/// Return type used in the cache's get_*() functions. 'Found' means that the page, or other
/// information that was enqueried, exists in the cache. '
pub enum CacheResult<V> {
    /// The enqueried page or other information existed in the cache.
    Found(V),

    /// The cache doesn't contain the page (or other enqueried information, like relation size). The
    /// Lsn is the 'not_modified_since' LSN that should be used in the request to the pageserver to
    /// read the page.
    NotFound(Lsn),
}

impl<'t> IntegratedCacheWriteAccess<'t> {
    pub fn get_rel_size(&'t self, rel: &RelTag) -> CacheResult<u32> {
        let r = self.cache_tree.start_read();
        if let Some(nblocks) = get_rel_size(&r, rel) {
            CacheResult::Found(nblocks)
        } else {
            let lsn = Lsn(self.global_lw_lsn.load(Ordering::Relaxed));
            CacheResult::NotFound(lsn)
        }
    }

    pub async fn get_page(
        &'t self,
        rel: &RelTag,
        block_number: u32,
        dst: impl uring_common::buf::IoBufMut + Send + Sync,
    ) -> Result<CacheResult<()>, std::io::Error> {
        let r = self.cache_tree.start_read();
        if let Some(block_tree_entry) = r.get(&TreeKey::from((rel, block_number))) {
            let block_entry = if let TreeEntry::Block(e) = block_tree_entry {
                e
            } else {
                panic!("unexpected tree entry type for block key");
            };
            block_entry.referenced.store(true, Ordering::Relaxed);

            let cache_block = block_entry.cache_block.load(Ordering::Relaxed);
            if cache_block != INVALID_CACHE_BLOCK {
                self.file_cache
                    .as_ref()
                    .unwrap()
                    .read_block(cache_block, dst)
                    .await?;

                Ok(CacheResult::Found(()))
            } else {
                Ok(CacheResult::NotFound(block_entry.lw_lsn.load()))
            }
        } else {
            let lsn = Lsn(self.global_lw_lsn.load(Ordering::Relaxed));
            Ok(CacheResult::NotFound(lsn))
        }
    }

    pub async fn page_is_cached(
        &'t self,
        rel: &RelTag,
        block_number: u32,
    ) -> Result<CacheResult<()>, std::io::Error> {
        let r = self.cache_tree.start_read();
        if let Some(block_tree_entry) = r.get(&TreeKey::from((rel, block_number))) {
            let block_entry = if let TreeEntry::Block(e) = block_tree_entry {
                e
            } else {
                panic!("unexpected tree entry type for block key");
            };

            // This is used for prefetch requests. Treat the probe as an 'access', to keep it
            // in cache.
            block_entry.referenced.store(true, Ordering::Relaxed);

            let cache_block = block_entry.cache_block.load(Ordering::Relaxed);

            if cache_block != INVALID_CACHE_BLOCK {
                Ok(CacheResult::Found(()))
            } else {
                Ok(CacheResult::NotFound(block_entry.lw_lsn.load()))
            }
        } else {
            let lsn = Lsn(self.global_lw_lsn.load(Ordering::Relaxed));
            Ok(CacheResult::NotFound(lsn))
        }
    }

    /// Does the relation exists? CacheResult::NotFound means that the cache doesn't contain that
    /// information, i.e. we don't know if the relation exists or not.
    pub fn get_rel_exists(&'t self, rel: &RelTag) -> CacheResult<bool> {
        // we don't currently cache negative entries, so if the relation is in the cache, it exists
        let r = self.cache_tree.start_read();
        if let Some(_rel_entry) = r.get(&TreeKey::from(rel)) {
            CacheResult::Found(true)
        } else {
            let lsn = Lsn(self.global_lw_lsn.load(Ordering::Relaxed));
            CacheResult::NotFound(lsn)
        }
    }

    pub fn get_db_size(&'t self, _db_oid: u32) -> CacheResult<u64> {
        // TODO: it would be nice to cache database sizes too. Getting the database size
        // is not a very common operation, but when you do it, it's often interactive, with
        // e.g. psql \l+ command, so the user will feel the latency.

        // fixme: is this right lsn?
        let lsn = Lsn(self.global_lw_lsn.load(Ordering::Relaxed));
        CacheResult::NotFound(lsn)
    }

    pub fn remember_rel_size(&'t self, rel: &RelTag, nblocks: u32) {
        let w = self.cache_tree.start_write();
        w.update_with_fn(&TreeKey::from(rel), |existing| match existing {
            None => {
                tracing::info!("inserting rel entry for {rel:?}, {nblocks} blocks");
                UpdateAction::Insert(TreeEntry::Rel(RelEntry {
                    nblocks: AtomicU32::new(nblocks),
                }))
            }
            Some(TreeEntry::Block(_)) => panic!("unexpected tree entry type for rel key"),
            Some(TreeEntry::Rel(e)) => {
                tracing::info!("updating rel entry for {rel:?}, {nblocks} blocks");
                e.nblocks.store(nblocks, Ordering::Relaxed);
                UpdateAction::Nothing
            }
        });
    }

    /// Remember the given page contents in the cache.
    pub async fn remember_page(
        &'t self,
        rel: &RelTag,
        block_number: u32,
        src: impl uring_common::buf::IoBuf + Send + Sync,
        lw_lsn: Lsn,
        is_write: bool,
    ) {
        let key = TreeKey::from((rel, block_number));

        // FIXME: make this work when file cache is disabled. Or make it mandatory
        let file_cache = self.file_cache.as_ref().unwrap();

        if is_write {
            // there should be no concurrent IOs. If a backend tries to read the page
            // at the same time, they may get a torn write. That's the same as with
            // regular POSIX filesystem read() and write()

            // First check if we have a block in cache already
            let w = self.cache_tree.start_write();

            let mut old_cache_block = None;
            let mut found_existing = false;

            w.update_with_fn(&key, |existing| {
                if let Some(existing) = existing {
                    let block_entry = if let TreeEntry::Block(e) = existing {
                        e
                    } else {
                        panic!("unexpected tree entry type for block key");
                    };

                    found_existing = true;

                    // Prevent this entry from being evicted
                    let was_pinned = block_entry.pinned.swap(true, Ordering::Relaxed);
                    if was_pinned {
                        // this is unexpected, because the caller has obtained the io-in-progress lock,
                        // so no one else should try to modify the page at the same time.
                        panic!("block entry was unexpectedly pinned");
                    }

                    let cache_block = block_entry.cache_block.load(Ordering::Relaxed);
                    old_cache_block = if cache_block != INVALID_CACHE_BLOCK {
                        Some(cache_block)
                    } else {
                        None
                    };
                }
                // if there was no existing entry, we will insert one, but not yet
                UpdateAction::Nothing
            });

            // Allocate a new block if required
            let cache_block = old_cache_block.unwrap_or_else(|| {
                loop {
                    if let Some(x) = file_cache.alloc_block() {
                        break x;
                    }
                    if let Some(x) = self.try_evict_one_cache_block() {
                        break x;
                    }
                }
            });

            // Write the page to the cache file
            file_cache
                .write_block(cache_block, src)
                .await
                .expect("error writing to cache");
            // FIXME: handle errors gracefully.
            // FIXME: unpin the block entry on error

            // Update the block entry
            let w = self.cache_tree.start_write();
            w.update_with_fn(&key, |existing| {
                assert_eq!(found_existing, existing.is_some());
                if let Some(existing) = existing {
                    let block_entry = if let TreeEntry::Block(e) = existing {
                        e
                    } else {
                        panic!("unexpected tree entry type for block key");
                    };

                    // Update the cache block
                    let old_blk = block_entry.cache_block.compare_exchange(
                        INVALID_CACHE_BLOCK,
                        cache_block,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    );
                    assert!(old_blk == Ok(INVALID_CACHE_BLOCK) || old_blk == Err(cache_block));

                    block_entry.lw_lsn.store(lw_lsn);

                    block_entry.referenced.store(true, Ordering::Relaxed);

                    let was_pinned = block_entry.pinned.swap(false, Ordering::Relaxed);
                    assert!(was_pinned);
                    UpdateAction::Nothing
                } else {
                    UpdateAction::Insert(TreeEntry::Block(BlockEntry {
                        lw_lsn: AtomicLsn::new(lw_lsn.0),
                        cache_block: AtomicU64::new(cache_block),
                        pinned: AtomicBool::new(false),
                        referenced: AtomicBool::new(true),
                    }))
                }
            });
        } else {
            // !is_write
            //
            // We can assume that it doesn't already exist, because the
            // caller is assumed to have already checked it, and holds
            // the io-in-progress lock. (The BlockEntry might exist, but no cache block)

            // Allocate a new block first
            let cache_block = {
                loop {
                    if let Some(x) = file_cache.alloc_block() {
                        break x;
                    }
                    if let Some(x) = self.try_evict_one_cache_block() {
                        break x;
                    }
                }
            };

            // Write the page to the cache file
            file_cache
                .write_block(cache_block, src)
                .await
                .expect("error writing to cache");
            // FIXME: handle errors gracefully.

            let w = self.cache_tree.start_write();

            w.update_with_fn(&key, |existing| {
                if let Some(existing) = existing {
                    let block_entry = if let TreeEntry::Block(e) = existing {
                        e
                    } else {
                        panic!("unexpected tree entry type for block key");
                    };

                    assert!(!block_entry.pinned.load(Ordering::Relaxed));

                    let old_cache_block = block_entry.cache_block.swap(cache_block, Ordering::Relaxed);
                    if old_cache_block != INVALID_CACHE_BLOCK {
                        panic!("remember_page called in !is_write mode, but page is already cached at blk {}", old_cache_block);
                    }
                    UpdateAction::Nothing
                } else {
                    UpdateAction::Insert(TreeEntry::Block(BlockEntry {
                        lw_lsn: AtomicLsn::new(lw_lsn.0),
                        cache_block: AtomicU64::new(cache_block),
                        pinned: AtomicBool::new(false),
                        referenced: AtomicBool::new(true),
                    }))
                }
            });
        }
    }

    /// Forget information about given relation in the cache. (For DROP TABLE and such)
    pub fn forget_rel(&'t self, rel: &RelTag) {
        tracing::info!("forgetting rel entry for {rel:?}");
        let w = self.cache_tree.start_write();
        w.remove(&TreeKey::from(rel));

        // also forget all cached blocks for the relation
        let mut iter = TreeIterator::new(&key_range_for_rel_blocks(rel));
        let r = self.cache_tree.start_read();
        while let Some((k, _v)) = iter.next(&r) {
            let w = self.cache_tree.start_write();

            let mut evicted_cache_block = None;

            w.update_with_fn(&k, |e| {
                if let Some(e) = e {
                    let block_entry = if let TreeEntry::Block(e) = e {
                        e
                    } else {
                        panic!("unexpected tree entry type for block key");
                    };
                    let cache_block = block_entry
                        .cache_block
                        .swap(INVALID_CACHE_BLOCK, Ordering::Relaxed);
                    if cache_block != INVALID_CACHE_BLOCK {
                        evicted_cache_block = Some(cache_block);
                    }
                    UpdateAction::Remove
                } else {
                    UpdateAction::Nothing
                }
            });

            if let Some(evicted_cache_block) = evicted_cache_block {
                self.file_cache
                    .as_ref()
                    .unwrap()
                    .dealloc_block(evicted_cache_block);
            }
        }
    }

    // Maintenance routines

    /// Evict one block from the file cache. This is used when the file cache fills up
    /// Returns the evicted block. It's not put to the free list, so it's available for the
    /// caller to use immediately.
    pub fn try_evict_one_cache_block(&self) -> Option<CacheBlock> {
        let mut clock_hand = self.clock_hand.lock().unwrap();
        for _ in 0..100 {
            let r = self.cache_tree.start_read();

            self.clock_iterations_counter.inc();

            match clock_hand.next(&r) {
                None => {
                    // The cache is completely empty. Pretty unexpected that this function
                    // was called then..
                    break;
                }
                Some((_k, TreeEntry::Rel(_))) => {
                    // ignore rel entries for now.
                    // TODO: They stick in the cache forever
                }
                Some((k, TreeEntry::Block(blk_entry))) => {
                    if !blk_entry.referenced.swap(false, Ordering::Relaxed) {
                        // Evict this. Maybe.
                        let w = self.cache_tree.start_write();

                        let mut evicted_cache_block = None;
                        w.update_with_fn(&k, |old| {
                            match old {
                                None => UpdateAction::Nothing,
                                Some(TreeEntry::Rel(_)) => panic!("unexpected Rel entry"),
                                Some(TreeEntry::Block(old)) => {
                                    // note: all the accesses to 'pinned' currently happen
                                    // within update_with_fn(), which protects from concurrent
                                    // updates. Otherwise, another thread could set the 'pinned'
                                    // flag just after we have checked it here.
                                    if blk_entry.pinned.load(Ordering::Relaxed) {
                                        return UpdateAction::Nothing;
                                    }

                                    let _ = self
                                        .global_lw_lsn
                                        .fetch_max(old.lw_lsn.load().0, Ordering::Relaxed);
                                    let cache_block = old
                                        .cache_block
                                        .swap(INVALID_CACHE_BLOCK, Ordering::Relaxed);
                                    if cache_block != INVALID_CACHE_BLOCK {
                                        evicted_cache_block = Some(cache_block);
                                    }
                                    // TODO: we don't evict the entry, just the block. Does it make
                                    // sense to keep the entry?
                                    UpdateAction::Nothing
                                }
                            }
                        });
                        if evicted_cache_block.is_some() {
                            self.page_evictions_counter.inc();
                            return evicted_cache_block;
                        }
                    }
                }
            }
        }
        // Give up if we didn't find anything
        None
    }

    pub fn dump_tree(&self, dst: &mut dyn std::io::Write) {
        self.cache_tree.start_read().dump(dst);
    }
}

impl metrics::core::Collector for IntegratedCacheWriteAccess<'_> {
    fn desc(&self) -> Vec<&metrics::core::Desc> {
        let mut descs = Vec::new();
        descs.append(&mut self.nodes_total.desc());
        descs.append(&mut self.nodes_memory_bytes.desc());
        descs.append(&mut self.page_evictions_counter.desc());
        descs.append(&mut self.clock_iterations_counter.desc());

        descs.append(&mut self.cache_memory_size_bytes.desc());
        descs.append(&mut self.cache_memory_used_bytes.desc());

        descs.append(&mut self.cache_tree_epoch.desc());
        descs.append(&mut self.cache_tree_oldest_epoch.desc());
        descs.append(&mut self.cache_tree_garbage_total.desc());

        descs
    }
    fn collect(&self) -> Vec<metrics::proto::MetricFamily> {
        const ALLOC_BLOCK_SIZE: i64 = neonart::allocator::block::BLOCK_SIZE as i64;

        // Update gauges
        let art_statistics = self.cache_tree.get_statistics();
        self.nodes_leaf_total
            .set(art_statistics.slabs.num_leaf as i64);
        self.nodes_internal4_total
            .set(art_statistics.slabs.num_internal4 as i64);
        self.nodes_internal16_total
            .set(art_statistics.slabs.num_internal16 as i64);
        self.nodes_internal48_total
            .set(art_statistics.slabs.num_internal48 as i64);
        self.nodes_internal256_total
            .set(art_statistics.slabs.num_internal256 as i64);

        self.nodes_memory_leaf_bytes
            .set(art_statistics.slabs.num_blocks_leaf as i64 * ALLOC_BLOCK_SIZE);
        self.nodes_memory_internal4_bytes
            .set(art_statistics.slabs.num_blocks_internal4 as i64 * ALLOC_BLOCK_SIZE);
        self.nodes_memory_internal16_bytes
            .set(art_statistics.slabs.num_blocks_internal16 as i64 * ALLOC_BLOCK_SIZE);
        self.nodes_memory_internal48_bytes
            .set(art_statistics.slabs.num_blocks_internal48 as i64 * ALLOC_BLOCK_SIZE);
        self.nodes_memory_internal256_bytes
            .set(art_statistics.slabs.num_blocks_internal256 as i64 * ALLOC_BLOCK_SIZE);

        let block_statistics = &art_statistics.blocks;
        self.cache_memory_size_bytes
            .set(block_statistics.num_blocks as i64 * ALLOC_BLOCK_SIZE as i64);
        self.cache_memory_used_bytes.set(
            (block_statistics.num_initialized as i64 - block_statistics.num_free_blocks as i64)
                * ALLOC_BLOCK_SIZE as i64,
        );

        self.cache_tree_epoch.set(art_statistics.epoch as i64);
        self.cache_tree_oldest_epoch
            .set(art_statistics.oldest_epoch as i64);
        self.cache_tree_garbage_total
            .set(art_statistics.num_garbage as i64);

        let mut values = Vec::new();
        values.append(&mut self.nodes_total.collect());
        values.append(&mut self.nodes_memory_bytes.collect());
        values.append(&mut self.page_evictions_counter.collect());
        values.append(&mut self.clock_iterations_counter.collect());

        values.append(&mut self.cache_memory_size_bytes.collect());
        values.append(&mut self.cache_memory_used_bytes.collect());

        values.append(&mut self.cache_tree_epoch.collect());
        values.append(&mut self.cache_tree_oldest_epoch.collect());
        values.append(&mut self.cache_tree_garbage_total.collect());

        values
    }
}

/// Read relation size from the cache.
///
/// This is in a separate function so that it can be shared by
/// IntegratedCacheReadAccess::get_rel_size() and IntegratedCacheWriteAccess::get_rel_size()
fn get_rel_size<'t>(r: &neonart::TreeReadGuard<TreeKey, TreeEntry>, rel: &RelTag) -> Option<u32> {
    if let Some(existing) = r.get(&TreeKey::from(rel)) {
        let rel_entry = if let TreeEntry::Rel(e) = existing {
            e
        } else {
            panic!("unexpected tree entry type for rel key");
        };

        let nblocks = rel_entry.nblocks.load(Ordering::Relaxed);
        if nblocks != u32::MAX {
            Some(nblocks)
        } else {
            None
        }
    } else {
        None
    }
}

/// Accessor for other backends
///
/// This allows backends to read pages from the cache directly, on their own, without making a
/// request to the communicator process.
impl<'t> IntegratedCacheReadAccess<'t> {
    pub fn get_rel_size(&'t self, rel: &RelTag) -> Option<u32> {
        get_rel_size(&self.cache_tree.start_read(), rel)
    }

    pub fn start_read_op(&'t self) -> BackendCacheReadOp<'t> {
        let r = self.cache_tree.start_read();
        BackendCacheReadOp { read_guard: r }
    }
}

pub struct BackendCacheReadOp<'t> {
    read_guard: neonart::TreeReadGuard<'t, TreeKey, TreeEntry>,
}

impl<'e> BackendCacheReadOp<'e> {
    /// Initiate a read of the page from the cache.
    ///
    /// This returns the "cache block number", i.e. the block number within the cache file, where
    /// the page's contents is stored. To get the page contents, the caller needs to read that block
    /// from the cache file. This returns a guard object that you must hold while it performs the
    /// read. It's possible that while you are performing the read, the cache block is invalidated.
    /// After you have completed the read, call BackendCacheReadResult::finish() to check if the
    /// read was in fact valid or not. If it was concurrently invalidated, you need to retry.
    pub fn get_page(&self, rel: &RelTag, block_number: u32) -> Option<u64> {
        if let Some(block_tree_entry) = self.read_guard.get(&TreeKey::from((rel, block_number))) {
            let block_entry = if let TreeEntry::Block(e) = block_tree_entry {
                e
            } else {
                panic!("unexpected tree entry type for block key");
            };
            block_entry.referenced.store(true, Ordering::Relaxed);

            let cache_block = block_entry.cache_block.load(Ordering::Relaxed);
            if cache_block != INVALID_CACHE_BLOCK {
                Some(cache_block)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn finish(self) -> bool {
        // TODO: currently, we use a spinlock to protect the in-memory tree, so concurrent
        // invalidations are not possible. But the plan is to switch to optimistic locking,
        // and once we do that, this would return 'false' if the optimistic locking failed and
        // you need to retry.
        true
    }
}
