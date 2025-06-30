//! Adaptive Radix Tree (ART) implementation, with Optimistic Lock Coupling.
//!
//! The data structure is described in these two papers:
//!
//! [1] Leis, V. & Kemper, Alfons & Neumann, Thomas. (2013).
//!     The adaptive radix tree: ARTful indexing for main-memory databases.
//!     Proceedings - International Conference on Data Engineering. 38-49. 10.1109/ICDE.2013.6544812.
//!     https://db.in.tum.de/~leis/papers/ART.pdf
//!
//! [2] Leis, Viktor & Scheibner, Florian & Kemper, Alfons & Neumann, Thomas. (2016).
//!     The ART of practical synchronization.
//!     1-8. 10.1145/2933349.2933352.
//!     https://db.in.tum.de/~leis/papers/artsync.pdf
//!
//! [1] describes the base data structure, and [2] describes the Optimistic Lock Coupling that we
//! use.
//!
//! The papers mention a few different variants. We have made the following choices in this
//! implementation:
//!
//! - All keys have the same length
//!
//! - Single-value leaves.
//!
//! - For collapsing inner nodes, we use the Pessimistic approach, where each inner node stores a
//!   variable length "prefix", which stores the keys of all the one-way nodes which have been
//!   removed. However, similar to the "hybrid" approach described in the paper, each node only has
//!   space for a constant-size prefix of 8 bytes. If a node would have a longer prefix, then we
//!   create create one-way nodes to store them. (There was no particular reason for this choice,
//!   the "hybrid" approach described in the paper might be better.)
//!
//! - For concurrency, we use Optimistic Lock Coupling. The paper [2] also describes another method,
//!   ROWEX, which generally performs better when there is contention, but that is not important
//!   for use and Optimisic Lock Coupling is simpler to implement.
//!
//! ## Requirements
//!
//! This data structure is currently used for the integrated LFC, relsize and last-written LSN cache
//! in the compute communicator, part of the 'neon' Postgres extension. We have some unique
//! requirements, which is why we had to write our own. Namely:
//!
//! - The data structure has to live in fixed-sized shared memory segment. That rules out any
//!   built-in Rust collections and most crates. (Except possibly with the 'allocator_api' rust
//!   feature, which still nightly-only experimental as of this writing).
//!
//! - The data structure is accessed from multiple processes. Only one process updates the data
//!   structure, but other processes perform reads. That rules out using built-in Rust locking
//!   primitives like Mutex and RwLock, and most crates too.
//!
//! - Within the one process with write-access, multiple threads can perform updates concurrently.
//!   That rules out using PostgreSQL LWLocks for the locking.
//!
//! The implementation is generic, and doesn't depend on any PostgreSQL specifics, but it has been
//! written with that usage and the above constraints in mind. Some noteworthy assumptions:
//!
//! - Contention is assumed to be rare. In the integrated cache in PostgreSQL, there's higher level
//!   locking in the PostgreSQL buffer manager, which ensures that two backends should not try to
//!   read / write the same page at the same time. (Prefetching can conflict with actual reads,
//!   however.)
//!
//!  - The keys in the integrated cache are 17 bytes long.
//!
//! ## Usage
//!
//! Because this is designed to be used as a Postgres shared memory data structure, initialization
//! happens in three stages:
//!
//! 0. A fixed area of shared memory is allocated at postmaster startup.
//!
//! 1. TreeInitStruct::new() is called to initialize it, still in Postmaster process, before any
//!    other process or thread is running. It returns a TreeInitStruct, which is inherited by all
//!    the processes through fork().
//!
//! 2. One process may have write-access to the struct, by calling
//!    [TreeInitStruct::attach_writer]. (That process is the communicator process.)
//!
//! 3. Other processes get read-access to the struct, by calling [TreeInitStruct::attach_reader]
//!
//! "Write access" means that you can insert / update / delete values in the tree.
//!
//! NOTE: The Values stored in the tree are sometimes moved, when a leaf node fills up and a new
//! larger node needs to be allocated. The versioning and epoch-based allocator ensure that the data
//! structure stays consistent, but if the Value has interior mutability, like atomic fields,
//! updates to such fields might be lost if the leaf node is concurrently moved! If that becomes a
//! problem, the version check could be passed up to the caller, so that the caller could detect the
//! lost updates and retry the operation.
//!
//! ## Implementation
//!
//! node_ptr: Provides low-level implementations of the four different node types (eight actually,
//! since there is an Internal and Leaf variant of each)
//!
//! lock_and_version.rs: Provides an abstraction for the combined lock and version counter on each
//! node.
//!
//! node_ref.rs: The code in node_ptr.rs deals with raw pointers. node_ref.rs provides more type-safe
//!   abstractions on top.
//!
//! algorithm.rs: Contains the functions to implement lookups and updates in the tree
//!
//! allocator.rs: Provides a facility to allocate memory for the tree nodes. (We must provide our
//!   own abstraction for that because we need the data structure to live in a pre-allocated shared
//!   memory segment).
//!
//! epoch.rs: The data structure requires that when a node is removed from the tree, it is not
//!   immediately deallocated, but stays around for as long as concurrent readers might still have
//!   pointers to them. This is enforced by an epoch system. This is similar to
//!   e.g. crossbeam_epoch, but we couldn't use that either because it has to work across processes
//!   communicating over the shared memory segment.
//!
//! ## See also
//!
//! There are some existing Rust ART implementations out there, but none of them filled all
//! the requirements:
//!
//! - https://github.com/XiangpengHao/congee
//! - https://github.com/declanvk/blart
//!
//! ## TODO
//!
//! - Removing values has not been implemented

mod algorithm;
pub mod allocator;
mod epoch;

use algorithm::RootPtr;
use algorithm::node_ptr::NodePtr;

use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::epoch::EpochPin;

#[cfg(test)]
mod tests;

use allocator::ArtAllocator;
pub use allocator::ArtMultiSlabAllocator;
pub use allocator::OutOfMemoryError;

/// Fixed-length key type.
///
pub trait Key: Debug {
    const KEY_LEN: usize;

    fn as_bytes(&self) -> &[u8];
}

/// Values stored in the tree
///
/// Values need to be Cloneable, because when a node "grows", the value is copied to a new node and
/// the old sticks around until all readers that might see the old value are gone.
// fixme obsolete, no longer needs Clone
pub trait Value {}

const MAX_GARBAGE: usize = 1024;

/// The root of the tree, plus other tree-wide data. This is stored in the shared memory.
pub struct Tree<V: Value> {
    /// For simplicity, so that we never need to grow or shrink the root, the root node is always an
    /// Internal256 node. Also, it never has a prefix (that's actually a bit wasteful, incurring one
    /// indirection to every lookup)
    root: RootPtr<V>,

    writer_attached: AtomicBool,

    epoch: epoch::EpochShared,
}

unsafe impl<V: Value + Sync> Sync for Tree<V> {}
unsafe impl<V: Value + Send> Send for Tree<V> {}

struct GarbageQueue<V>(VecDeque<(NodePtr<V>, u64)>);

unsafe impl<V: Value + Sync> Sync for GarbageQueue<V> {}
unsafe impl<V: Value + Send> Send for GarbageQueue<V> {}

impl<V> GarbageQueue<V> {
    fn new() -> GarbageQueue<V> {
        GarbageQueue(VecDeque::with_capacity(MAX_GARBAGE))
    }

    fn remember_obsolete_node(&mut self, ptr: NodePtr<V>, epoch: u64) {
        self.0.push_front((ptr, epoch));
    }

    fn next_obsolete(&mut self, cutoff_epoch: u64) -> Option<NodePtr<V>> {
        if let Some(back) = self.0.back() {
            if back.1 < cutoff_epoch {
                return Some(self.0.pop_back().unwrap().0);
            }
        }
        None
    }
}

/// Struct created at postmaster startup
pub struct TreeInitStruct<'t, K: Key, V: Value, A: ArtAllocator<V>> {
    tree: &'t Tree<V>,

    allocator: &'t A,

    phantom_key: PhantomData<K>,
}

/// The worker process has a reference to this. The write operations are only safe
/// from the worker process
pub struct TreeWriteAccess<'t, K: Key, V: Value, A: ArtAllocator<V>>
where
    K: Key,
    V: Value,
{
    tree: &'t Tree<V>,

    pub allocator: &'t A,

    epoch_handle: epoch::LocalHandle<'t>,

    phantom_key: PhantomData<K>,

    /// Obsolete nodes that cannot be recycled until their epoch expires.
    garbage: spin::Mutex<GarbageQueue<V>>,
}

/// The backends have a reference to this. It cannot be used to modify the tree
pub struct TreeReadAccess<'t, K: Key, V: Value>
where
    K: Key,
    V: Value,
{
    tree: &'t Tree<V>,

    epoch_handle: epoch::LocalHandle<'t>,

    phantom_key: PhantomData<K>,
}

impl<'t, K: Key, V: Value, A: ArtAllocator<V>> TreeInitStruct<'t, K, V, A> {
    pub fn new(allocator: &'t A) -> TreeInitStruct<'t, K, V, A> {
        let tree_ptr = allocator.alloc_tree();
        let tree_ptr = NonNull::new(tree_ptr).expect("out of memory");
        let init = Tree {
            root: algorithm::new_root(allocator).expect("out of memory"),
            writer_attached: AtomicBool::new(false),
            epoch: epoch::EpochShared::new(),
        };
        unsafe { tree_ptr.write(init) };

        TreeInitStruct {
            tree: unsafe { tree_ptr.as_ref() },
            allocator,
            phantom_key: PhantomData,
        }
    }

    pub fn attach_writer(self) -> TreeWriteAccess<'t, K, V, A> {
        let previously_attached = self.tree.writer_attached.swap(true, Ordering::Relaxed);
        if previously_attached {
            panic!("writer already attached");
        }
        TreeWriteAccess {
            tree: self.tree,
            allocator: self.allocator,
            phantom_key: PhantomData,
            epoch_handle: self.tree.epoch.register(),
            garbage: spin::Mutex::new(GarbageQueue::new()),
        }
    }

    pub fn attach_reader(self) -> TreeReadAccess<'t, K, V> {
        TreeReadAccess {
            tree: self.tree,
            phantom_key: PhantomData,
            epoch_handle: self.tree.epoch.register(),
        }
    }
}

impl<'t, K: Key, V: Value, A: ArtAllocator<V>> TreeWriteAccess<'t, K, V, A> {
    pub fn start_write<'g>(&'t self) -> TreeWriteGuard<'g, K, V, A>
    where
        't: 'g,
    {
        TreeWriteGuard {
            tree_writer: self,
            epoch_pin: self.epoch_handle.pin(),
            phantom_key: PhantomData,
            created_garbage: false,
        }
    }

    pub fn start_read(&'t self) -> TreeReadGuard<'t, K, V> {
        TreeReadGuard {
            tree: self.tree,
            epoch_pin: self.epoch_handle.pin(),
            phantom_key: PhantomData,
        }
    }
}

impl<'t, K: Key, V: Value> TreeReadAccess<'t, K, V> {
    pub fn start_read(&'t self) -> TreeReadGuard<'t, K, V> {
        TreeReadGuard {
            tree: self.tree,
            epoch_pin: self.epoch_handle.pin(),
            phantom_key: PhantomData,
        }
    }
}

pub struct TreeReadGuard<'e, K, V>
where
    K: Key,
    V: Value,
{
    tree: &'e Tree<V>,

    epoch_pin: EpochPin<'e>,
    phantom_key: PhantomData<K>,
}

impl<'e, K: Key, V: Value> TreeReadGuard<'e, K, V> {
    pub fn get(&'e self, key: &K) -> Option<&'e V> {
        algorithm::search(key, self.tree.root, &self.epoch_pin)
    }
}

pub struct TreeWriteGuard<'e, K, V, A>
where
    K: Key,
    V: Value,
    A: ArtAllocator<V>,
{
    tree_writer: &'e TreeWriteAccess<'e, K, V, A>,

    epoch_pin: EpochPin<'e>,
    phantom_key: PhantomData<K>,

    created_garbage: bool,
}

pub enum UpdateAction<V> {
    Nothing,
    Insert(V),
    Remove,
}

impl<'e, K: Key, V: Value, A: ArtAllocator<V>> TreeWriteGuard<'e, K, V, A> {
    /// Get a value
    pub fn get(&'e mut self, key: &K) -> Option<&'e V> {
        algorithm::search(key, self.tree_writer.tree.root, &self.epoch_pin)
    }

    /// Insert a value
    pub fn insert(self, key: &K, value: V) -> Result<bool, OutOfMemoryError> {
        let mut success = None;

        self.update_with_fn(key, |existing| {
            if existing.is_some() {
                success = Some(false);
                UpdateAction::Nothing
            } else {
                success = Some(true);
                UpdateAction::Insert(value)
            }
        })?;
        Ok(success.expect("value_fn not called"))
    }

    /// Remove value. Returns true if it existed
    pub fn remove(self, key: &K) -> bool {
        let mut result = false;
        // FIXME: It's not clear if OOM is expected while removing. It seems
        // not nice, but shrinking a node can OOM. Then again, we could opt
        // to not shrink a node if we cannot allocate, to live a little longer.
        self.update_with_fn(key, |existing| match existing {
            Some(_) => {
                result = true;
                UpdateAction::Remove
            }
            None => UpdateAction::Nothing,
        })
        .expect("out of memory while removing");
        result
    }

    /// Try to remove value and return the old value.
    pub fn remove_and_return(self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let mut old = None;
        self.update_with_fn(key, |existing| {
            old = existing.cloned();
            UpdateAction::Remove
        })
        .expect("out of memory while removing");
        old
    }

    /// Update key using the given function. All the other modifying operations are based on this.
    ///
    /// The function is passed a reference to the existing value, if any. If the function
    /// returns None, the value is removed from the tree (or if there was no existing value,
    /// does nothing). If the function returns Some, the existing value is replaced, of if there
    /// was no existing value, it is inserted. FIXME: update comment
    pub fn update_with_fn<F>(mut self, key: &K, value_fn: F) -> Result<(), OutOfMemoryError>
    where
        F: FnOnce(Option<&V>) -> UpdateAction<V>,
    {
        algorithm::update_fn(key, value_fn, self.tree_writer.tree.root, &mut self)?;

        if self.created_garbage {
            let _ = self.collect_garbage();
        }
        Ok(())
    }

    fn remember_obsolete_node(&mut self, ptr: NodePtr<V>) {
        self.tree_writer
            .garbage
            .lock()
            .remember_obsolete_node(ptr, self.epoch_pin.epoch);
        self.created_garbage = true;
    }

    // returns number of nodes recycled
    fn collect_garbage(&self) -> usize {
        self.tree_writer.tree.epoch.advance();
        self.tree_writer.tree.epoch.broadcast();

        let cutoff_epoch = self.tree_writer.tree.epoch.get_oldest();

        let mut result = 0;
        let mut garbage_queue = self.tree_writer.garbage.lock();
        while let Some(ptr) = garbage_queue.next_obsolete(cutoff_epoch) {
            ptr.deallocate(self.tree_writer.allocator);
            result += 1;
        }
        result
    }
}

pub struct TreeIterator<K>
where
    K: Key + for<'a> From<&'a [u8]>,
{
    done: bool,
    pub next_key: Vec<u8>,
    max_key: Option<Vec<u8>>,

    phantom_key: PhantomData<K>,
}

impl<K> TreeIterator<K>
where
    K: Key + for<'a> From<&'a [u8]>,
{
    pub fn new_wrapping() -> TreeIterator<K> {
        TreeIterator {
            done: false,
            next_key: vec![0; K::KEY_LEN],
            max_key: None,
            phantom_key: PhantomData,
        }
    }

    pub fn new(range: &std::ops::Range<K>) -> TreeIterator<K> {
        let result = TreeIterator {
            done: false,
            next_key: Vec::from(range.start.as_bytes()),
            max_key: Some(Vec::from(range.end.as_bytes())),
            phantom_key: PhantomData,
        };
        assert_eq!(result.next_key.len(), K::KEY_LEN);
        assert_eq!(result.max_key.as_ref().unwrap().len(), K::KEY_LEN);

        result
    }

    pub fn next<'g, V>(&mut self, read_guard: &'g TreeReadGuard<'g, K, V>) -> Option<(K, &'g V)>
    where
        V: Value,
    {
        if self.done {
            return None;
        }

        let mut wrapped_around = false;
        loop {
            assert_eq!(self.next_key.len(), K::KEY_LEN);
            if let Some((k, v)) =
                algorithm::iter_next(&self.next_key, read_guard.tree.root, &read_guard.epoch_pin)
            {
                assert_eq!(k.len(), K::KEY_LEN);
                assert_eq!(self.next_key.len(), K::KEY_LEN);

                // Check if we reached the end of the range
                if let Some(max_key) = &self.max_key {
                    if k.as_slice() >= max_key.as_slice() {
                        self.done = true;
                        break None;
                    }
                }

                // increment the key
                self.next_key = k.clone();
                increment_key(self.next_key.as_mut_slice());
                let k = k.as_slice().into();

                break Some((k, v));
            } else {
                if self.max_key.is_some() {
                    self.done = true;
                } else {
                    // Start from beginning
                    if !wrapped_around {
                        for i in 0..K::KEY_LEN {
                            self.next_key[i] = 0;
                        }
                        wrapped_around = true;
                        continue;
                    } else {
                        // The tree is completely empty
                        // FIXME: perhaps we should remember the starting point instead.
                        // Currently this will scan some ranges twice.
                        break None;
                    }
                }
                break None;
            }
        }
    }
}

fn increment_key(key: &mut [u8]) -> bool {
    for i in (0..key.len()).rev() {
        let (byte, overflow) = key[i].overflowing_add(1);
        key[i] = byte;
        if !overflow {
            return false;
        }
    }
    true
}

// Debugging functions
impl<'e, K: Key, V: Value + Debug, A: ArtAllocator<V>> TreeWriteGuard<'e, K, V, A> {
    pub fn dump(&mut self, dst: &mut dyn std::io::Write) {
        algorithm::dump_tree(self.tree_writer.tree.root, &self.epoch_pin, dst)
    }
}
impl<'e, K: Key, V: Value + Debug> TreeReadGuard<'e, K, V> {
    pub fn dump(&mut self, dst: &mut dyn std::io::Write) {
        algorithm::dump_tree(self.tree.root, &self.epoch_pin, dst)
    }
}
impl<'e, K: Key, V: Value> TreeWriteAccess<'e, K, V, ArtMultiSlabAllocator<'e, V>> {
    pub fn get_statistics(&self) -> ArtTreeStatistics {
        self.allocator.get_statistics();
        ArtTreeStatistics {
            blocks: self.allocator.inner.block_allocator.get_statistics(),
            slabs: self.allocator.get_statistics(),
            epoch: self.tree.epoch.get_current(),
            oldest_epoch: self.tree.epoch.get_oldest(),
            num_garbage: self.garbage.lock().0.len() as u64,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ArtTreeStatistics {
    pub blocks: allocator::block::BlockAllocatorStats,
    pub slabs: allocator::ArtMultiSlabStats,

    pub epoch: u64,
    pub oldest_epoch: u64,
    pub num_garbage: u64,
}
