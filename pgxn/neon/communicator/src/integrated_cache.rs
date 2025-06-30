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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use utils::lsn::{AtomicLsn, Lsn};

use crate::file_cache::INVALID_CACHE_BLOCK;
use crate::file_cache::{CacheBlock, FileCache};
use pageserver_page_api::RelTag;

use metrics::{IntCounter, IntGauge};

use neon_shmem::hash::HashMapInit;
use neon_shmem::hash::UpdateAction;
use neon_shmem::shmem::ShmemHandle;

// in # of entries
const RELSIZE_CACHE_SIZE: u32 = 64 * 1024;

/// This struct is initialized at postmaster startup, and passed to all the processes via fork().
pub struct IntegratedCacheInitStruct<'t> {
    relsize_cache_handle: HashMapInit<'t, RelKey, RelEntry>,
    block_map_handle: HashMapInit<'t, BlockKey, BlockEntry>,
}

/// Represents write-access to the integrated cache. This is used by the communicator process.
pub struct IntegratedCacheWriteAccess<'t> {
    relsize_cache: neon_shmem::hash::HashMapAccess<'t, RelKey, RelEntry>,
    block_map: neon_shmem::hash::HashMapAccess<'t, BlockKey, BlockEntry>,

    global_lw_lsn: AtomicU64,

    pub(crate) file_cache: Option<FileCache>,

    // Fields for eviction
    clock_hand: std::sync::Mutex<usize>,

    // Metrics
    page_evictions_counter: IntCounter,
    clock_iterations_counter: IntCounter,

    // metrics from the hash map
    block_map_num_buckets: IntGauge,
    block_map_num_buckets_in_use: IntGauge,

    relsize_cache_num_buckets: IntGauge,
    relsize_cache_num_buckets_in_use: IntGauge,
}

/// Represents read-only access to the integrated cache. Backend processes have this.
pub struct IntegratedCacheReadAccess<'t> {
    relsize_cache: neon_shmem::hash::HashMapAccess<'t, RelKey, RelEntry>,
    block_map: neon_shmem::hash::HashMapAccess<'t, BlockKey, BlockEntry>,
}

impl<'t> IntegratedCacheInitStruct<'t> {
    /// Return the desired size in bytes of the fixed-size shared memory area to reserve for the
    /// integrated cache.
    pub fn shmem_size(_max_procs: u32) -> usize {
        // The relsize cache is fixed-size. The block map is allocated in a separate resizable
        // area.
        HashMapInit::<RelKey, RelEntry>::estimate_size(RELSIZE_CACHE_SIZE)
    }

    /// Initialize the shared memory segment. This runs once in postmaster. Returns a struct which
    /// will be inherited by all processes through fork.
    pub fn shmem_init(
        _max_procs: u32,
        shmem_area: &'t mut [MaybeUninit<u8>],
        initial_file_cache_size: u64,
        max_file_cache_size: u64,
    ) -> IntegratedCacheInitStruct<'t> {
        // Initialize the relsize cache in the fixed-size area
        let relsize_cache_handle =
            neon_shmem::hash::HashMapInit::init_in_fixed_area(RELSIZE_CACHE_SIZE, shmem_area);

        let max_bytes =
            HashMapInit::<BlockKey, BlockEntry>::estimate_size(max_file_cache_size as u32);

        // Initialize the block map in a separate resizable shared memory area
        let shmem_handle = ShmemHandle::new("block mapping", 0, max_bytes).unwrap();

        let block_map_handle = neon_shmem::hash::HashMapInit::init_in_shmem(
            initial_file_cache_size as u32,
            shmem_handle,
        );
        IntegratedCacheInitStruct {
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
            relsize_cache_handle,
            block_map_handle,
        } = self;
        IntegratedCacheWriteAccess {
            relsize_cache: relsize_cache_handle.attach_writer(),
            block_map: block_map_handle.attach_writer(),
            global_lw_lsn: AtomicU64::new(lsn.0),
            file_cache,
            clock_hand: std::sync::Mutex::new(0),

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

            block_map_num_buckets: metrics::IntGauge::new(
                "block_map_num_buckets",
                "Allocated size of the block cache hash map",
            )
            .unwrap(),
            block_map_num_buckets_in_use: metrics::IntGauge::new(
                "block_map_num_buckets_in_use",
                "Number of buckets in use in the block cache hash map",
            )
            .unwrap(),

            relsize_cache_num_buckets: metrics::IntGauge::new(
                "relsize_cache_num_buckets",
                "Allocated size of the relsize cache hash map",
            )
            .unwrap(),
            relsize_cache_num_buckets_in_use: metrics::IntGauge::new(
                "relsize_cache_num_buckets_in_use",
                "Number of buckets in use in the relsize cache hash map",
            )
            .unwrap(),
        }
    }

    /// Initialize access to the integrated cache for a backend process
    pub fn backend_init(self) -> IntegratedCacheReadAccess<'t> {
        let IntegratedCacheInitStruct {
            relsize_cache_handle,
            block_map_handle,
        } = self;

        IntegratedCacheReadAccess {
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

impl<'t> IntegratedCacheWriteAccess<'t> {
    pub fn get_rel_size(&'t self, rel: &RelTag) -> CacheResult<u32> {
        if let Some(nblocks) = get_rel_size(&self.relsize_cache, rel) {
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
            let lsn = Lsn(self.global_lw_lsn.load(Ordering::Relaxed));
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
            let lsn = Lsn(self.global_lw_lsn.load(Ordering::Relaxed));
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
        let result =
            self.relsize_cache
                .update_with_fn(&RelKey::from(rel), |existing| match existing {
                    None => {
                        tracing::info!("inserting rel entry for {rel:?}, {nblocks} blocks");
                        UpdateAction::Insert(RelEntry {
                            nblocks: AtomicU32::new(nblocks),
                        })
                    }
                    Some(e) => {
                        tracing::info!("updating rel entry for {rel:?}, {nblocks} blocks");
                        e.nblocks.store(nblocks, Ordering::Relaxed);
                        UpdateAction::Nothing
                    }
                });

        // FIXME: what to do if we run out of memory? Evict other relation entries?
        result.expect("out of memory");
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

            let res = self.block_map.update_with_fn(&key, |existing| {
                if let Some(block_entry) = existing {
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
                // if there was no existing entry, we will insert one, but not yet
                UpdateAction::Nothing
            });

            // FIXME: what to do if we run out of memory? Evict other relation entries? Remove
            // block entries first?
            res.expect("out of memory");

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
            let res = self.block_map.update_with_fn(&key, |existing| {
                assert_eq!(found_existing, existing.is_some());
                if let Some(block_entry) = existing {
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
                    UpdateAction::Nothing
                } else {
                    UpdateAction::Insert(BlockEntry {
                        lw_lsn: AtomicLsn::new(lw_lsn.0),
                        cache_block: AtomicU64::new(cache_block),
                        pinned: AtomicU64::new(0),
                        referenced: AtomicBool::new(true),
                    })
                }
            });

            // FIXME: what to do if we run out of memory? Evict other relation entries? Remove
            // block entries first?
            res.expect("out of memory");
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

            let res = self.block_map.update_with_fn(&key, |existing| {
                if let Some(block_entry) = existing {
                    // FIXME: could there be concurrent readers?
                    assert!(block_entry.pinned.load(Ordering::Relaxed) == 0);

                    let old_cache_block = block_entry.cache_block.swap(cache_block, Ordering::Relaxed);
                    if old_cache_block != INVALID_CACHE_BLOCK {
                        panic!("remember_page called in !is_write mode, but page is already cached at blk {old_cache_block}");
                    }
                    UpdateAction::Nothing
                } else {
                    UpdateAction::Insert(BlockEntry {
                        lw_lsn: AtomicLsn::new(lw_lsn.0),
                        cache_block: AtomicU64::new(cache_block),
                        pinned: AtomicU64::new(0),
                        referenced: AtomicBool::new(true),
                    })
                }
            });

            // FIXME: what to do if we run out of memory? Evict other relation entries? Remove
            // block entries first?
            res.expect("out of memory");
        }
    }

    /// Forget information about given relation in the cache. (For DROP TABLE and such)
    pub fn forget_rel(&'t self, rel: &RelTag) {
        tracing::info!("forgetting rel entry for {rel:?}");
        self.relsize_cache.remove(&RelKey::from(rel));

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

    /// Evict one block from the file cache. This is used when the file cache fills up
    /// Returns the evicted block. It's not put to the free list, so it's available for the
    /// caller to use immediately.
    pub fn try_evict_one_cache_block(&self) -> Option<CacheBlock> {
        let mut clock_hand = self.clock_hand.lock().unwrap();
        for _ in 0..100 {
            self.clock_iterations_counter.inc();

            (*clock_hand) += 1;

            let mut evict_this = false;
            let num_buckets = self.block_map.get_num_buckets();
            match self
                .block_map
                .get_bucket((*clock_hand) % num_buckets)
                .as_deref()
            {
                None => {
                    // This bucket was unused
                }
                Some(blk_entry) => {
                    if !blk_entry.referenced.swap(false, Ordering::Relaxed) {
                        // Evict this. Maybe.
                        evict_this = true;
                    }
                }
            };

            if evict_this {
                // grab the write lock
                let mut evicted_cache_block = None;
                let res =
                    self.block_map
                        .update_with_fn_at_bucket(*clock_hand % num_buckets, |old| {
                            match old {
                                None => UpdateAction::Nothing,
                                Some(old) => {
                                    // note: all the accesses to 'pinned' currently happen
                                    // within update_with_fn(), or while holding ValueReadGuard, which protects from concurrent
                                    // updates. Otherwise, another thread could set the 'pinned'
                                    // flag just after we have checked it here.
                                    if old.pinned.load(Ordering::Relaxed) != 0 {
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
                                    UpdateAction::Remove
                                }
                            }
                        });

                // Out of memory should not happen here, as we're only updating existing values,
                // not inserting new entries to the map.
                res.expect("out of memory");

                if evicted_cache_block.is_some() {
                    self.page_evictions_counter.inc();
                    return evicted_cache_block;
                }
            }
        }
        // Give up if we didn't find anything
        None
    }

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
        }
    }

    pub fn dump_map(&self, _dst: &mut dyn std::io::Write) {
        //FIXME self.cache_map.start_read().dump(dst);
    }
}

impl metrics::core::Collector for IntegratedCacheWriteAccess<'_> {
    fn desc(&self) -> Vec<&metrics::core::Desc> {
        let mut descs = Vec::new();
        descs.append(&mut self.page_evictions_counter.desc());
        descs.append(&mut self.clock_iterations_counter.desc());

        descs.append(&mut self.block_map_num_buckets.desc());
        descs.append(&mut self.block_map_num_buckets_in_use.desc());

        descs.append(&mut self.relsize_cache_num_buckets.desc());
        descs.append(&mut self.relsize_cache_num_buckets_in_use.desc());

        descs
    }
    fn collect(&self) -> Vec<metrics::proto::MetricFamily> {
        // Update gauges
        self.block_map_num_buckets
            .set(self.block_map.get_num_buckets() as i64);
        self.block_map_num_buckets_in_use
            .set(self.block_map.get_num_buckets_in_use() as i64);
        self.relsize_cache_num_buckets
            .set(self.relsize_cache.get_num_buckets() as i64);
        self.relsize_cache_num_buckets_in_use
            .set(self.relsize_cache.get_num_buckets_in_use() as i64);

        let mut values = Vec::new();
        values.append(&mut self.page_evictions_counter.collect());
        values.append(&mut self.clock_iterations_counter.collect());

        values.append(&mut self.block_map_num_buckets.collect());
        values.append(&mut self.block_map_num_buckets_in_use.collect());

        values.append(&mut self.relsize_cache_num_buckets.collect());
        values.append(&mut self.relsize_cache_num_buckets_in_use.collect());

        values
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
