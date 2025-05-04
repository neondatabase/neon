//! Integrated communicator cache
//!
//! Tracks:
//! - Relation sizes and existence
//! - Last-written LSN
//! - TODO: Block cache (also known as LFC)
//!
//! TODO: limit the size
//! TODO: concurrency
//!
//! Note: This deals with "relations", which is really just one "relation fork" in Postgres
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

use utils::lsn::Lsn;
use zerocopy::FromBytes;

use crate::file_cache::{CacheBlock, FileCache};
use pageserver_page_api::model::RelTag;

use neonart;
use neonart::TreeInitStruct;
use neonart::TreeIterator;

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

    global_lw_lsn: Lsn,

    file_cache: Option<FileCache>,
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
    pub fn shmem_init(_max_procs: u32, shmem_area: &'t mut [MaybeUninit<u8>]) -> IntegratedCacheInitStruct<'t> {
        let allocator = neonart::ArtMultiSlabAllocator::new(shmem_area);

        let handle = IntegratedCacheTreeInitStruct::new(allocator);

        // Initialize the shared memory area
        IntegratedCacheInitStruct {
            allocator,
            handle,
        }
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

        IntegratedCacheWriteAccess {
            cache_tree: tree_writer,
            global_lw_lsn: lsn,
            file_cache,
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

#[derive(Clone)]
enum TreeEntry {
    Rel(RelEntry),
    Block(BlockEntry),
}

#[derive(Clone)]
struct BlockEntry {
    lw_lsn: Lsn,
    cache_block: Option<CacheBlock>,
}

#[derive(Clone, Default)]
struct RelEntry {
    /// cached size of the relation
    nblocks: Option<u32>,
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
struct TreeKey {
    spc_oid: u32,
    db_oid: u32,
    rel_number: u32,
    fork_number: u8,
    block_number: u32,
}

impl<'a> From<&'a [u8]> for TreeKey {
    fn from(bytes: &'a [u8]) -> Self {
        Self::read_from_bytes(bytes).expect("invalid key length")
    }
}

fn key_range_for_rel_blocks(rel: &RelTag) -> Range<TreeKey> {
    Range {
        start: TreeKey {
            spc_oid: rel.spc_oid,
            db_oid: rel.db_oid,
            rel_number: rel.rel_number,
            fork_number: rel.fork_number,
            block_number: 0,
        },
        end:  TreeKey {
            spc_oid: rel.spc_oid,
            db_oid: rel.db_oid,
            rel_number: rel.rel_number,
            fork_number: rel.fork_number,
            block_number: u32::MAX,
        },
    }
}

impl From<&RelTag> for TreeKey {
    fn from(val: &RelTag) -> TreeKey {
        TreeKey {
            spc_oid: val.spc_oid,
            db_oid: val.db_oid,
            rel_number: val.rel_number,
            fork_number: val.fork_number,
            block_number: u32::MAX,
        }
    }
}

impl From<(&RelTag, u32)> for TreeKey {
    fn from(val: (&RelTag, u32)) -> TreeKey {
        TreeKey {
            spc_oid: val.0.spc_oid,
            db_oid: val.0.db_oid,
            rel_number: val.0.rel_number,
            fork_number: val.0.fork_number,
            block_number: val.1,
        }
    }
}

impl neonart::Key for TreeKey {
    const KEY_LEN: usize = 4 + 4 + 4 + 1 + 32;

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
            CacheResult::NotFound(self.global_lw_lsn)
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

            if let Some(cache_block) = block_entry.cache_block {
                self.file_cache
                    .as_ref()
                    .unwrap()
                    .read_block(cache_block, dst)
                    .await?;
                Ok(CacheResult::Found(()))
            } else {
                Ok(CacheResult::NotFound(block_entry.lw_lsn))
            }
        } else {
            Ok(CacheResult::NotFound(self.global_lw_lsn))
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

            if let Some(_cache_block) = block_entry.cache_block {
                Ok(CacheResult::Found(()))
            } else {
                Ok(CacheResult::NotFound(block_entry.lw_lsn))
            }
        } else {
            Ok(CacheResult::NotFound(self.global_lw_lsn))
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
            CacheResult::NotFound(self.global_lw_lsn)
        }
    }

    pub fn get_db_size(&'t self, _db_oid: u32) -> CacheResult<u64> {
        // fixme: is this right lsn?
        CacheResult::NotFound(self.global_lw_lsn)
    }

    pub fn remember_rel_size(&'t self, rel: &RelTag, nblocks: u32) {
        let w = self.cache_tree.start_write();
        w.insert(
            &TreeKey::from(rel),
            TreeEntry::Rel(RelEntry {
                nblocks: Some(nblocks),
            }),
        );
    }

    /// Remember the given page contents in the cache.
    pub async fn remember_page(
        &'t self,
        rel: &RelTag,
        block_number: u32,
        src: impl uring_common::buf::IoBuf + Send + Sync,
        lw_lsn: Lsn,
    ) {
        if let Some(file_cache) = self.file_cache.as_ref() {
            let w = self.cache_tree.start_write();

            let key = TreeKey::from((rel, block_number));

            let mut cache_block = None;

            w.update_with_fn(&key, |existing| {
                if let Some(existing) = existing {
                    let mut block_entry = if let TreeEntry::Block(e) = existing.clone() {
                        e
                    } else {
                        panic!("unexpected tree entry type for block key");
                    };
                    block_entry.lw_lsn = lw_lsn;
                    if block_entry.cache_block.is_none() {
                        block_entry.cache_block = Some(file_cache.alloc_block());
                    }
                    cache_block = block_entry.cache_block;
                    Some(TreeEntry::Block(block_entry))
                } else {
                    cache_block = Some(file_cache.alloc_block());
                    Some(TreeEntry::Block(BlockEntry {
                        lw_lsn: lw_lsn,
                        cache_block: cache_block,
                    }))
                }
            });
            let cache_block = cache_block.unwrap();
            file_cache
                .write_block(cache_block, src)
                .await
                .expect("error writing to cache");
        }
    }

    /// Forget information about given relation in the cache. (For DROP TABLE and such)
    pub fn forget_rel(&'t self, rel: &RelTag) {
        let w = self.cache_tree.start_write();
        w.remove(&TreeKey::from(rel));

        // also forget all cached blocks for the relation
        let mut iter = TreeIterator::new(&key_range_for_rel_blocks(rel));
        while let Some((k, _v)) = iter.next(self.cache_tree.start_read()) {
            let w = self.cache_tree.start_write();
            w.remove(&k);
        }
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

        if let Some(nblocks) = rel_entry.nblocks {
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

            block_entry.cache_block
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
