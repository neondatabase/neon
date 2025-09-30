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
// - entries in the cache map. If we run out of memory in the shmem area, need to evict
//   something
//

use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};

use utils::lsn::{AtomicLsn, Lsn};

use crate::file_cache::INVALID_CACHE_BLOCK;
use crate::file_cache::{CacheBlock, FileCache};
use crate::init::alloc_from_slice;
use pageserver_page_api::RelTag;

use measured::metric;
use measured::metric::MetricEncoding;
use measured::metric::counter::CounterState;
use measured::metric::gauge::GaugeState;
use measured::{Counter, Gauge, MetricGroup};

use neon_shmem::hash::{HashMapInit, entry::Entry};
use neon_shmem::shmem::ShmemHandle;

// in # of entries
const RELSIZE_CACHE_SIZE: u32 = 64 * 1024;

/// This struct is initialized at postmaster startup, and passed to all the processes via fork().
pub struct IntegratedCacheInitStruct<'t> {
    shared: &'t IntegratedCacheShared,
    relsize_cache_handle: HashMapInit<'t, RelKey, RelEntry>,
    block_map_handle: HashMapInit<'t, BlockKey, BlockEntry>,
}

/// This struct is allocated in the (fixed-size) shared memory area at postmaster startup.
/// It is accessible by all the backends and the communicator process.
#[derive(Debug)]
pub struct IntegratedCacheShared {
    global_lw_lsn: AtomicU64,
}

/// Represents write-access to the integrated cache. This is used by the communicator process.
pub struct IntegratedCacheWriteAccess<'t> {
    shared: &'t IntegratedCacheShared,
    relsize_cache: neon_shmem::hash::HashMapAccess<'t, RelKey, RelEntry>,
    block_map: neon_shmem::hash::HashMapAccess<'t, BlockKey, BlockEntry>,

    pub(crate) file_cache: Option<FileCache>,

    // Fields for eviction
    clock_hand: AtomicUsize,

    metrics: IntegratedCacheMetricGroup,
}

#[derive(MetricGroup)]
#[metric(new())]
struct IntegratedCacheMetricGroup {
    /// Page evictions from the Local File Cache
    cache_page_evictions_counter: Counter,

    /// Block entry evictions from the integrated cache
    block_entry_evictions_counter: Counter,

    /// Number of times the clock hand has moved
    clock_iterations_counter: Counter,

    // metrics from the hash map
    /// Allocated size of the block cache hash map
    block_map_num_buckets: Gauge,

    /// Number of buckets in use in the block cache hash map
    block_map_num_buckets_in_use: Gauge,

    /// Allocated size of the relsize cache hash map
    relsize_cache_num_buckets: Gauge,

    /// Number of buckets in use in the relsize cache hash map
    relsize_cache_num_buckets_in_use: Gauge,
}

/// Represents read-only access to the integrated cache. Backend processes have this.
pub struct IntegratedCacheReadAccess<'t> {
    shared: &'t IntegratedCacheShared,
    relsize_cache: neon_shmem::hash::HashMapAccess<'t, RelKey, RelEntry>,
    block_map: neon_shmem::hash::HashMapAccess<'t, BlockKey, BlockEntry>,
}

impl<'t> IntegratedCacheInitStruct<'t> {
    /// Return the desired size in bytes of the fixed-size shared memory area to reserve for the
    /// integrated cache.
    pub fn shmem_size() -> usize {
        // The relsize cache is fixed-size. The block map is allocated in a separate resizable
        // area.
        let mut sz = 0;
        sz += std::mem::size_of::<IntegratedCacheShared>();
        sz += HashMapInit::<RelKey, RelEntry>::estimate_size(RELSIZE_CACHE_SIZE);

        sz
    }

    /// Initialize the shared memory segment. This runs once in postmaster. Returns a struct which
    /// will be inherited by all processes through fork.
    pub fn shmem_init(
        shmem_area: &'t mut [MaybeUninit<u8>],
        initial_file_cache_size: u64,
        max_file_cache_size: u64,
    ) -> IntegratedCacheInitStruct<'t> {
        // Initialize the shared struct
        let (shared, remain_shmem_area) = alloc_from_slice::<IntegratedCacheShared>(shmem_area);
        let shared = shared.write(IntegratedCacheShared {
            global_lw_lsn: AtomicU64::new(0),
        });

        // Use the remaining part of the fixed-size area for the relsize cache
        let relsize_cache_handle =
            neon_shmem::hash::HashMapInit::with_fixed(RELSIZE_CACHE_SIZE, remain_shmem_area);

        let max_bytes =
            HashMapInit::<BlockKey, BlockEntry>::estimate_size(max_file_cache_size as u32);

        // Initialize the block map in a separate resizable shared memory area
        let shmem_handle = ShmemHandle::new("block mapping", 0, max_bytes).unwrap();

        let block_map_handle =
            neon_shmem::hash::HashMapInit::with_shmem(initial_file_cache_size as u32, shmem_handle);
        IntegratedCacheInitStruct {
            shared,
            relsize_cache_handle,
            block_map_handle,
        }
    }

    /// Initialize access to the integrated cache for the communicator worker process
    pub fn worker_process_init(
        self,
        lsn: Lsn,
        file_cache: Option<FileCache>,
    ) -> IntegratedCacheWriteAccess<'t> {
        let IntegratedCacheInitStruct {
            shared,
            relsize_cache_handle,
            block_map_handle,
        } = self;

        shared.global_lw_lsn.store(lsn.0, Ordering::Relaxed);

        IntegratedCacheWriteAccess {
            shared,
            relsize_cache: relsize_cache_handle.attach_writer(),
            block_map: block_map_handle.attach_writer(),
            file_cache,
            clock_hand: AtomicUsize::new(0),
            metrics: IntegratedCacheMetricGroup::new(),
        }
    }

    /// Initialize access to the integrated cache for a backend process
    pub fn backend_init(self) -> IntegratedCacheReadAccess<'t> {
        let IntegratedCacheInitStruct {
            shared,
            relsize_cache_handle,
            block_map_handle,
        } = self;

        IntegratedCacheReadAccess {
            shared,
            relsize_cache: relsize_cache_handle.attach_reader(),
            block_map: block_map_handle.attach_reader(),
        }
    }
}

/// Value stored in the cache mapping hash table.
struct BlockEntry {
    lw_lsn: AtomicLsn,
    cache_block: AtomicU64,

    pinned: AtomicU64,

    // 'referenced' bit for the clock algorithm
    referenced: AtomicBool,
}

/// Value stored in the relsize cache hash table.
struct RelEntry {
    /// cached size of the relation
    /// u32::MAX means 'not known' (that's InvalidBlockNumber in Postgres)
    nblocks: AtomicU32,

    /// This is the last time the "metadata" of this relation changed, not
    /// the contents of the blocks. That is, the size of the relation.
    lw_lsn: AtomicLsn,
}

impl std::fmt::Debug for RelEntry {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        fmt.debug_struct("Rel")
            .field("nblocks", &self.nblocks.load(Ordering::Relaxed))
            .finish()
    }
}
impl std::fmt::Debug for BlockEntry {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        fmt.debug_struct("Block")
            .field("lw_lsn", &self.lw_lsn.load())
            .field("cache_block", &self.cache_block.load(Ordering::Relaxed))
            .field("pinned", &self.pinned.load(Ordering::Relaxed))
            .field("referenced", &self.referenced.load(Ordering::Relaxed))
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Hash, Ord)]
struct RelKey(RelTag);

impl From<&RelTag> for RelKey {
    fn from(val: &RelTag) -> RelKey {
        RelKey(*val)
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Hash, Ord)]
struct BlockKey {
    rel: RelTag,
    block_number: u32,
}

impl From<(&RelTag, u32)> for BlockKey {
    fn from(val: (&RelTag, u32)) -> BlockKey {
        BlockKey {
            rel: *val.0,
            block_number: val.1,
        }
    }
}

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

/// Return type of [try_evict_entry]
enum EvictResult {
    /// Could not evict page because it was pinned
    Pinned,

    /// The victim bucket was already vacant
    Vacant,

    /// Evicted an entry. If it had a cache block associated with it, it's returned
    /// here, otherwise None
    Evicted(Option<CacheBlock>),
}

impl<'t> IntegratedCacheWriteAccess<'t> {
    pub fn get_rel_size(&'t self, rel: &RelTag) -> CacheResult<u32> {
        if let Some(nblocks) = get_rel_size(&self.relsize_cache, rel) {
            CacheResult::Found(nblocks)
        } else {
            let lsn = Lsn(self.shared.global_lw_lsn.load(Ordering::Relaxed));
            CacheResult::NotFound(lsn)
        }
    }

    pub async fn get_page(
        &'t self,
        rel: &RelTag,
        block_number: u32,
        dst: impl uring_common::buf::IoBufMut + Send + Sync,
    ) -> Result<CacheResult<()>, std::io::Error> {
        let x = if let Some(block_entry) = self.block_map.get(&BlockKey::from((rel, block_number)))
        {
            block_entry.referenced.store(true, Ordering::Relaxed);

            let cache_block = block_entry.cache_block.load(Ordering::Relaxed);
            if cache_block != INVALID_CACHE_BLOCK {
                // pin it and release lock
                block_entry.pinned.fetch_add(1, Ordering::Relaxed);

                (cache_block, DeferredUnpin(block_entry.pinned.as_ptr()))
            } else {
                return Ok(CacheResult::NotFound(block_entry.lw_lsn.load()));
            }
        } else {
            let lsn = Lsn(self.shared.global_lw_lsn.load(Ordering::Relaxed));
            return Ok(CacheResult::NotFound(lsn));
        };

        let (cache_block, _deferred_pin) = x;
        self.file_cache
            .as_ref()
            .unwrap()
            .read_block(cache_block, dst)
            .await?;

        // unpin the entry (by implicitly dropping deferred_pin)
        Ok(CacheResult::Found(()))
    }

    pub async fn page_is_cached(
        &'t self,
        rel: &RelTag,
        block_number: u32,
    ) -> Result<CacheResult<()>, std::io::Error> {
        if let Some(block_entry) = self.block_map.get(&BlockKey::from((rel, block_number))) {
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
            let lsn = Lsn(self.shared.global_lw_lsn.load(Ordering::Relaxed));
            Ok(CacheResult::NotFound(lsn))
        }
    }

    /// Does the relation exists? CacheResult::NotFound means that the cache doesn't contain that
    /// information, i.e. we don't know if the relation exists or not.
    pub fn get_rel_exists(&'t self, rel: &RelTag) -> CacheResult<bool> {
        // we don't currently cache negative entries, so if the relation is in the cache, it exists
        if let Some(_rel_entry) = self.relsize_cache.get(&RelKey::from(rel)) {
            CacheResult::Found(true)
        } else {
            let lsn = Lsn(self.shared.global_lw_lsn.load(Ordering::Relaxed));
            CacheResult::NotFound(lsn)
        }
    }

    pub fn get_db_size(&'t self, _db_oid: u32) -> CacheResult<u64> {
        // TODO: it would be nice to cache database sizes too. Getting the database size
        // is not a very common operation, but when you do it, it's often interactive, with
        // e.g. psql \l+ command, so the user will feel the latency.

        // fixme: is this right lsn?
        let lsn = Lsn(self.shared.global_lw_lsn.load(Ordering::Relaxed));
        CacheResult::NotFound(lsn)
    }

    pub fn remember_rel_size(&'t self, rel: &RelTag, nblocks: u32, lsn: Lsn) {
        match self.relsize_cache.entry(RelKey::from(rel)) {
            Entry::Vacant(e) => {
                tracing::trace!("inserting rel entry for {rel:?}, {nblocks} blocks");
                // FIXME: what to do if we run out of memory? Evict other relation entries?
                _ = e
                    .insert(RelEntry {
                        nblocks: AtomicU32::new(nblocks),
                        lw_lsn: AtomicLsn::new(lsn.0),
                    })
                    .expect("out of memory");
            }
            Entry::Occupied(e) => {
                tracing::trace!("updating rel entry for {rel:?}, {nblocks} blocks");
                e.get().nblocks.store(nblocks, Ordering::Relaxed);
                e.get().lw_lsn.store(lsn);
            }
        };
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
        let key = BlockKey::from((rel, block_number));

        // FIXME: make this work when file cache is disabled. Or make it mandatory
        let file_cache = self.file_cache.as_ref().unwrap();

        if is_write {
            // there should be no concurrent IOs. If a backend tries to read the page
            // at the same time, they may get a torn write. That's the same as with
            // regular POSIX filesystem read() and write()

            // First check if we have a block in cache already
            let mut old_cache_block = None;
            let mut found_existing = false;

            // NOTE(quantumish): honoring original semantics here (used to be update_with_fn)
            // but I don't see any reason why this has to take a write lock.
            if let Entry::Occupied(e) = self.block_map.entry(key.clone()) {
                let block_entry = e.get();
                found_existing = true;

                // Prevent this entry from being evicted
                let pin_count = block_entry.pinned.fetch_add(1, Ordering::Relaxed);
                if pin_count > 0 {
                    // this is unexpected, because the caller has obtained the io-in-progress lock,
                    // so no one else should try to modify the page at the same time.
                    // XXX: and I think a read should not be happening either, because the postgres
                    // buffer is held locked. TODO: check these conditions and tidy this up a little. Seems fragile to just panic.
                    panic!("block entry was unexpectedly pinned");
                }

                let cache_block = block_entry.cache_block.load(Ordering::Relaxed);
                old_cache_block = if cache_block != INVALID_CACHE_BLOCK {
                    Some(cache_block)
                } else {
                    None
                };
            }

            // Allocate a new block if required
            let cache_block = old_cache_block.unwrap_or_else(|| {
                loop {
                    if let Some(x) = file_cache.alloc_block() {
                        break x;
                    }
                    if let Some(x) = self.try_evict_cache_block() {
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
            loop {
                let entry = self.block_map.entry(key.clone());
                assert_eq!(found_existing, matches!(entry, Entry::Occupied(_)));
                match entry {
                    Entry::Occupied(e) => {
                        let block_entry = e.get();
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

                        let pin_count = block_entry.pinned.fetch_sub(1, Ordering::Relaxed);
                        assert!(pin_count > 0);
                        break;
                    }
                    Entry::Vacant(e) => {
                        if e.insert(BlockEntry {
                            lw_lsn: AtomicLsn::new(lw_lsn.0),
                            cache_block: AtomicU64::new(cache_block),
                            pinned: AtomicU64::new(0),
                            referenced: AtomicBool::new(true),
                        })
                        .is_ok()
                        {
                            break;
                        } else {
                            // The hash map was full. Evict an entry and retry.
                        }
                    }
                }
                self.try_evict_block_entry();
            }
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
                    if let Some(x) = self.try_evict_cache_block() {
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

            loop {
                match self.block_map.entry(key.clone()) {
                    Entry::Occupied(e) => {
                        let block_entry = e.get();
                        // FIXME: could there be concurrent readers?
                        assert!(block_entry.pinned.load(Ordering::Relaxed) == 0);

                        let old_cache_block =
                            block_entry.cache_block.swap(cache_block, Ordering::Relaxed);
                        if old_cache_block != INVALID_CACHE_BLOCK {
                            panic!(
                                "remember_page called in !is_write mode, but page is already cached at blk {old_cache_block}"
                            );
                        }
                        break;
                    }
                    Entry::Vacant(e) => {
                        if e.insert(BlockEntry {
                            lw_lsn: AtomicLsn::new(lw_lsn.0),
                            cache_block: AtomicU64::new(cache_block),
                            pinned: AtomicU64::new(0),
                            referenced: AtomicBool::new(true),
                        })
                        .is_ok()
                        {
                            break;
                        } else {
                            // The hash map was full. Evict an entry and retry.
                        }
                    }
                };

                self.try_evict_block_entry();
            }
        }
    }

    /// Forget information about given relation in the cache. (For DROP TABLE and such)
    pub fn forget_rel(&'t self, rel: &RelTag, _nblocks: Option<u32>, flush_lsn: Lsn) {
        tracing::trace!("forgetting rel entry for {rel:?}");
        self.relsize_cache.remove(&RelKey::from(rel));

        // update with flush LSN
        let _ = self
            .shared
            .global_lw_lsn
            .fetch_max(flush_lsn.0, Ordering::Relaxed);

        // also forget all cached blocks for the relation
        // FIXME
        /*
            let mut iter = MapIterator::new(&key_range_for_rel_blocks(rel));
            let r = self.cache_tree.start_read();
            while let Some((k, _v)) = iter.next(&r) {
                let w = self.cache_tree.start_write();

                let mut evicted_cache_block = None;

                let res = w.update_with_fn(&k, |e| {
                    if let Some(e) = e {
                        let block_entry = if let MapEntry::Block(e) = e {
                            e
                        } else {
                            panic!("unexpected map entry type for block key");
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

                // FIXME: It's pretty surprising to run out of memory while removing. But
                // maybe it can happen because of trying to shrink a node?
                res.expect("out of memory");

                if let Some(evicted_cache_block) = evicted_cache_block {
                    self.file_cache
                        .as_ref()
                        .unwrap()
                        .dealloc_block(evicted_cache_block);
                }
        }

            */
    }

    // Maintenance routines

    /// Evict one block entry from the cache.
    ///
    /// This is called when the hash map is full, to make an entry available for a new
    /// insertion. There's no guarantee that the entry is free by the time this function
    /// returns anymore; it can taken by a concurrent thread at any time. So you need to
    /// call this and retry repeatedly until you succeed.
    fn try_evict_block_entry(&self) {
        let num_buckets = self.block_map.get_num_buckets();
        loop {
            self.metrics.clock_iterations_counter.inc();
            let victim_bucket = self.clock_hand.fetch_add(1, Ordering::Relaxed) % num_buckets;

            let evict_this = match self.block_map.get_at_bucket(victim_bucket).as_deref() {
                None => {
                    // The caller wants to have a free bucket. If there's one already, we're good.
                    return;
                }
                Some((_, blk_entry)) => {
                    // Clear the 'referenced' flag. If it was already clear,
                    // release the lock (by exiting this scope), and try to
                    // evict it.
                    !blk_entry.referenced.swap(false, Ordering::Relaxed)
                }
            };
            if evict_this {
                match self.try_evict_entry(victim_bucket) {
                    EvictResult::Pinned => {
                        // keep looping
                    }
                    EvictResult::Vacant => {
                        // This was released by someone else. Return so that
                        // the caller will try to use it. (Chances are that it
                        // will be reused by someone else, but let's try.)
                        return;
                    }
                    EvictResult::Evicted(None) => {
                        // This is now free.
                        return;
                    }
                    EvictResult::Evicted(Some(cache_block)) => {
                        // This is now free. We must not leak the cache block, so put it to the freelist
                        self.file_cache.as_ref().unwrap().dealloc_block(cache_block);
                        return;
                    }
                }
            }
            // TODO: add some kind of a backstop to error out if we loop
            // too many times without finding any unpinned entries
        }
    }

    /// Evict one block from the file cache. This is called when the file cache fills up,
    /// to release a cache block.
    ///
    /// Returns the evicted block. It's not put to the free list, so it's available for
    /// the caller to use immediately.
    fn try_evict_cache_block(&self) -> Option<CacheBlock> {
        let num_buckets = self.block_map.get_num_buckets();
        let mut iterations = 0;
        while iterations < 100 {
            self.metrics.clock_iterations_counter.inc();
            let victim_bucket = self.clock_hand.fetch_add(1, Ordering::Relaxed) % num_buckets;

            let evict_this = match self.block_map.get_at_bucket(victim_bucket).as_deref() {
                None => {
                    // This bucket was unused. It's no use for finding a free cache block
                    continue;
                }
                Some((_, blk_entry)) => {
                    // Clear the 'referenced' flag. If it was already clear,
                    // release the lock (by exiting this scope), and try to
                    // evict it.
                    !blk_entry.referenced.swap(false, Ordering::Relaxed)
                }
            };

            if evict_this {
                match self.try_evict_entry(victim_bucket) {
                    EvictResult::Pinned => {
                        // keep looping
                    }
                    EvictResult::Vacant => {
                        // This was released by someone else. Keep looping.
                    }
                    EvictResult::Evicted(None) => {
                        // This is now free, but it didn't have a cache block
                        // associated with it. Keep looping.
                    }
                    EvictResult::Evicted(Some(cache_block)) => {
                        // Reuse this
                        return Some(cache_block);
                    }
                }
            }

            iterations += 1;
        }

        // Reached the max iteration count without finding an entry. Return
        // to give the caller a chance to do other things
        None
    }

    /// Returns Err, if the page could not be evicted because it was pinned
    fn try_evict_entry(&self, victim: usize) -> EvictResult {
        // grab the write lock
        if let Some(e) = self.block_map.entry_at_bucket(victim) {
            let old = e.get();
            // note: all the accesses to 'pinned' currently happen
            // within update_with_fn(), or while holding ValueReadGuard, which protects from concurrent
            // updates. Otherwise, another thread could set the 'pinned'
            // flag just after we have checked it here.
            //
            // FIXME: ^^ outdated comment, update_with_fn() is no more

            if old.pinned.load(Ordering::Relaxed) == 0 {
                let old_val = e.remove();
                let _ = self
                    .shared
                    .global_lw_lsn
                    .fetch_max(old_val.lw_lsn.into_inner().0, Ordering::Relaxed);
                let evicted_cache_block = match old_val.cache_block.into_inner() {
                    INVALID_CACHE_BLOCK => None,
                    n => Some(n),
                };
                if evicted_cache_block.is_some() {
                    self.metrics.cache_page_evictions_counter.inc();
                }
                self.metrics.block_entry_evictions_counter.inc();
                EvictResult::Evicted(evicted_cache_block)
            } else {
                EvictResult::Pinned
            }
        } else {
            EvictResult::Vacant
        }
    }

    /// Resize the local file cache.
    pub fn resize_file_cache(&self, num_blocks: u32) {
        let old_num_blocks = self.block_map.get_num_buckets() as u32;

        if old_num_blocks < num_blocks {
            if let Err(err) = self.block_map.grow(num_blocks) {
                tracing::warn!(
                    "could not grow file cache to {} blocks (old size {}): {}",
                    num_blocks,
                    old_num_blocks,
                    err
                );
            }
        } else {
            // TODO: Shrinking not implemented yet
        }
    }

    pub fn dump_map(&self, _dst: &mut dyn std::io::Write) {
        //FIXME self.cache_map.start_read().dump(dst);
    }
}

impl<T: metric::group::Encoding> MetricGroup<T> for IntegratedCacheWriteAccess<'_>
where
    CounterState: MetricEncoding<T>,
    GaugeState: MetricEncoding<T>,
{
    fn collect_group_into(&self, enc: &mut T) -> Result<(), <T as metric::group::Encoding>::Err> {
        // Update gauges
        self.metrics
            .block_map_num_buckets
            .set(self.block_map.get_num_buckets() as i64);
        self.metrics
            .block_map_num_buckets_in_use
            .set(self.block_map.get_num_buckets_in_use() as i64);
        self.metrics
            .relsize_cache_num_buckets
            .set(self.relsize_cache.get_num_buckets() as i64);
        self.metrics
            .relsize_cache_num_buckets_in_use
            .set(self.relsize_cache.get_num_buckets_in_use() as i64);

        if let Some(file_cache) = &self.file_cache {
            file_cache.collect_group_into(enc)?;
        }

        self.metrics.collect_group_into(enc)
    }
}

/// Read relation size from the cache.
///
/// This is in a separate function so that it can be shared by
/// IntegratedCacheReadAccess::get_rel_size() and IntegratedCacheWriteAccess::get_rel_size()
fn get_rel_size(
    r: &neon_shmem::hash::HashMapAccess<RelKey, RelEntry>,
    rel: &RelTag,
) -> Option<u32> {
    if let Some(rel_entry) = r.get(&RelKey::from(rel)) {
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

pub enum GetBucketResult {
    Occupied(RelTag, u32),
    Vacant,
    OutOfBounds,
}

/// Accessor for other backends
///
/// This allows backends to read pages from the cache directly, on their own, without making a
/// request to the communicator process.
impl<'t> IntegratedCacheReadAccess<'t> {
    pub fn get_rel_size(&'t self, rel: &RelTag) -> Option<u32> {
        get_rel_size(&self.relsize_cache, rel)
    }

    pub fn start_read_op(&'t self) -> BackendCacheReadOp<'t> {
        BackendCacheReadOp {
            read_guards: Vec::new(),
            map_access: self,
        }
    }

    /// Check if LFC contains the given buffer, and update its last-written LSN if not.
    ///
    /// Returns:
    ///   true if the block is in the LFC
    ///   false if it's not.
    ///
    /// If the block was not in the LFC (i.e. when this returns false), the last-written LSN
    /// value on the block is updated to the given 'lsn', so that the next read of the block
    /// will read the new version. Otherwise the caller is assumed to modify the page and
    /// to update the last-written LSN later by writing the new page.
    pub fn update_lw_lsn_for_block_if_not_cached(
        &'t self,
        rel: &RelTag,
        block_number: u32,
        lsn: Lsn,
    ) -> bool {
        let key = BlockKey::from((rel, block_number));
        let entry = self.block_map.entry(key);
        match entry {
            Entry::Occupied(e) => {
                let block_entry = e.get();
                if block_entry.cache_block.load(Ordering::Relaxed) != INVALID_CACHE_BLOCK {
                    block_entry.referenced.store(true, Ordering::Relaxed);
                    true
                } else {
                    let old_lwlsn = block_entry.lw_lsn.fetch_max(lsn);
                    if old_lwlsn >= lsn {
                        // shouldn't happen
                        tracing::warn!(
                            "attempted to move last-written LSN backwards from {old_lwlsn} to {lsn} for rel {rel} blk {block_number}"
                        );
                    }
                    false
                }
            }
            Entry::Vacant(e) => {
                if e.insert(BlockEntry {
                    lw_lsn: AtomicLsn::new(lsn.0),
                    cache_block: AtomicU64::new(INVALID_CACHE_BLOCK),
                    pinned: AtomicU64::new(0),
                    referenced: AtomicBool::new(true),
                })
                .is_ok()
                {
                    false
                } else {
                    // The hash table is full.
                    //
                    // TODO: Evict something. But for now, just set the global lw LSN instead.
                    // That's correct, but not very efficient for future reads
                    let _ = self
                        .shared
                        .global_lw_lsn
                        .fetch_max(lsn.0, Ordering::Relaxed);
                    false
                }
            }
        }
    }

    pub fn get_bucket(&self, bucket_no: usize) -> GetBucketResult {
        match self.block_map.get_at_bucket(bucket_no).as_deref() {
            None => {
                // free bucket, or out of bounds
                if bucket_no >= self.block_map.get_num_buckets() {
                    GetBucketResult::OutOfBounds
                } else {
                    GetBucketResult::Vacant
                }
            }
            Some((key, _)) => GetBucketResult::Occupied(key.rel, key.block_number),
        }
    }

    pub fn get_num_buckets_in_use(&self) -> usize {
        self.block_map.get_num_buckets_in_use()
    }
}

pub struct BackendCacheReadOp<'t> {
    read_guards: Vec<DeferredUnpin>,
    map_access: &'t IntegratedCacheReadAccess<'t>,
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
    pub fn get_page(&mut self, rel: &RelTag, block_number: u32) -> Option<u64> {
        if let Some(block_entry) = self
            .map_access
            .block_map
            .get(&BlockKey::from((rel, block_number)))
        {
            block_entry.referenced.store(true, Ordering::Relaxed);

            let cache_block = block_entry.cache_block.load(Ordering::Relaxed);
            if cache_block != INVALID_CACHE_BLOCK {
                block_entry.pinned.fetch_add(1, Ordering::Relaxed);
                self.read_guards
                    .push(DeferredUnpin(block_entry.pinned.as_ptr()));
                Some(cache_block)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn finish(self) -> bool {
        // TODO: currently, we hold a pin on the in-memory map, so concurrent invalidations are not
        // possible. But if we switch to optimistic locking, this would return 'false' if the
        // optimistic locking failed and you need to retry.
        true
    }
}

/// A hack to decrement an AtomicU64 on drop. This is used to decrement the pin count
/// of a BlockEntry. The safety depends on the fact that the BlockEntry is not evicted
/// or moved while it's pinned.
struct DeferredUnpin(*mut u64);

unsafe impl Sync for DeferredUnpin {}
unsafe impl Send for DeferredUnpin {}

impl Drop for DeferredUnpin {
    fn drop(&mut self) {
        // unpin it
        unsafe {
            let pin_ref = AtomicU64::from_ptr(self.0);
            pin_ref.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
