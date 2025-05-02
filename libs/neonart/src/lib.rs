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
//! - Multi-value leaves. The values are stored directly in one of the four different leaf node
//!   types.
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

use std::fmt::Debug;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::epoch::EpochPin;

#[cfg(test)]
mod tests;

use allocator::ArtAllocator;
pub use allocator::ArtMultiSlabAllocator;

/// Fixed-length key type.
///
pub trait Key: Clone + Debug {
    const KEY_LEN: usize;

    fn as_bytes(&self) -> &[u8];
}

/// Values stored in the tree
///
/// Values need to be Cloneable, because when a node "grows", the value is copied to a new node and
/// the old sticks around until all readers that might see the old value are gone.
pub trait Value: Clone {}

const MAX_GARBAGE: usize = 1024;

pub struct Tree<V: Value> {
    root: RootPtr<V>,

    writer_attached: AtomicBool,

    epoch: epoch::EpochShared,

    garbage: spin::Mutex<GarbageQueue<V>>,
}

unsafe impl<V: Value + Sync> Sync for Tree<V> {}
unsafe impl<V: Value + Send> Send for Tree<V> {}

struct GarbageQueueFullError();

struct GarbageQueue<V> {
    slots: [(NodePtr<V>, u64); MAX_GARBAGE],
    front: usize,
    back: usize,
}
impl<V> GarbageQueue<V> {
    fn new() -> GarbageQueue<V> {
        GarbageQueue {
            slots: [const { (NodePtr::null(), 0) }; MAX_GARBAGE],
            front: 0,
            back: 0,
        }
    }

    fn remember_obsolete_node(
        &mut self,
        ptr: NodePtr<V>,
        epoch: u64,
    ) -> Result<(), GarbageQueueFullError> {
        if self.front == self.back.wrapping_add(MAX_GARBAGE) {
            return Err(GarbageQueueFullError());
        }

        self.slots[self.front % MAX_GARBAGE] = (ptr, epoch);
        self.front = self.front.wrapping_add(1);
        Ok(())
    }

    fn next_obsolete(&mut self, cutoff_epoch: u64) -> Option<NodePtr<V>> {
        if self.front == self.back {
            return None;
        }
        let slot = &self.slots[self.back % MAX_GARBAGE];
        // FIXME: performing wrapping comparison
        if slot.1 < cutoff_epoch {
            self.back += 1;
            return Some(slot.0);
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

    allocator: &'t A,

    epoch_handle: epoch::LocalHandle<'t>,

    phantom_key: PhantomData<K>,
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

impl<'a, 't: 'a, K: Key, V: Value, A: ArtAllocator<V>> TreeInitStruct<'t, K, V, A> {
    pub fn new(allocator: &'t A) -> TreeInitStruct<'t, K, V, A> {
        let tree_ptr = allocator.alloc_tree();
        let tree_ptr = NonNull::new(tree_ptr).expect("out of memory");
        let init = Tree {
            root: algorithm::new_root(allocator),
            writer_attached: AtomicBool::new(false),
            epoch: epoch::EpochShared::new(),
            garbage: spin::Mutex::new(GarbageQueue::new()),
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

impl<'t, K: Key + Clone, V: Value, A: ArtAllocator<V>> TreeWriteAccess<'t, K, V, A> {
    pub fn start_write(&'t self) -> TreeWriteGuard<'t, K, V, A> {
        // TODO: grab epoch guard
        TreeWriteGuard {
            allocator: self.allocator,
            tree: &self.tree,
            epoch_pin: self.epoch_handle.pin(),
            phantom_key: PhantomData,
        }
    }

    pub fn start_read(&'t self) -> TreeReadGuard<'t, K, V> {
        TreeReadGuard {
            tree: &self.tree,
            epoch_pin: self.epoch_handle.pin(),
            phantom_key: PhantomData,
        }
    }

    pub fn collect_garbage(&'t self) {
        self.tree.epoch.advance();
        self.tree.epoch.broadcast();

        let cutoff_epoch = self.tree.epoch.get_oldest();

        let mut garbage_queue = self.tree.garbage.lock();
        while let Some(ptr) = garbage_queue.next_obsolete(cutoff_epoch) {
            ptr.deallocate(self.allocator);
        }
    }
}

impl<'t, K: Key + Clone, V: Value> TreeReadAccess<'t, K, V> {
    pub fn start_read(&'t self) -> TreeReadGuard<'t, K, V> {
        TreeReadGuard {
            tree: &self.tree,
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
    pub fn get(&self, key: &K) -> Option<V> {
        algorithm::search(key, self.tree.root, &self.epoch_pin)
    }
}

pub struct TreeWriteGuard<'e, K, V, A>
where
    K: Key,
    V: Value,
{
    tree: &'e Tree<V>,
    allocator: &'e A,

    epoch_pin: EpochPin<'e>,
    phantom_key: PhantomData<K>,
}

impl<'t, K: Key, V: Value, A: ArtAllocator<V>> TreeWriteGuard<'t, K, V, A> {
    pub fn insert(&mut self, key: &K, value: V) {
        self.update_with_fn(key, |_| Some(value))
    }

    pub fn update_with_fn<F>(&mut self, key: &K, value_fn: F)
    where
        F: FnOnce(Option<&V>) -> Option<V>,
    {
        algorithm::update_fn(key, value_fn, self.tree.root, self)
    }

    pub fn get(&mut self, key: &K) -> Option<V> {
        algorithm::search(key, self.tree.root, &self.epoch_pin)
    }

    fn remember_obsolete_node(&'t self, ptr: NodePtr<V>) -> Result<(), GarbageQueueFullError> {
        self.tree
            .garbage
            .lock()
            .remember_obsolete_node(ptr, self.epoch_pin.epoch)
    }
}

impl<'t, K: Key, V: Value + Debug> TreeReadGuard<'t, K, V> {
    pub fn dump(&mut self) {
        algorithm::dump_tree(self.tree.root, &self.epoch_pin)
    }
}
