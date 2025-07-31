//! Resizable hash table implementation on top of byte-level storage (either a [`ShmemHandle`] or a fixed byte array).
//!
//! This hash table has two major components: the bucket array and the dictionary. Each bucket within the
//! bucket array contains a `Option<(K, V)>` and an index of another bucket. In this way there is both an
//! implicit freelist within the bucket array (`None` buckets point to other `None` entries) and various hash
//! chains within the bucket array (a Some bucket will point to other Some buckets that had the same hash).
//!
//! Buckets are never moved unless they are within a region that is being shrunk, and so the actual hash-
//! dependent component is done with the dictionary. When a new key is inserted into the map, a position
//! within the dictionary is decided based on its hash, the data is inserted into an empty bucket based
//! off of the freelist, and then the index of said bucket is placed in the dictionary.
//!
//! This map is resizable (if initialized on top of a [`ShmemHandle`]). Both growing and shrinking happen
//! in-place and are at a high level achieved by expanding/reducing the bucket array and rebuilding the
//! dictionary by rehashing all keys.
//!
//! Concurrency is managed very simply: the entire map is guarded by one shared-memory RwLock.

use std::hash::{BuildHasher, Hash};
use std::mem::MaybeUninit;

use crate::shmem::ShmemHandle;
use crate::{shmem, sync::*};

mod core;
pub mod entry;

#[cfg(test)]
mod tests;

use core::{Bucket, CoreHashMap, INVALID_POS};
use entry::{Entry, OccupiedEntry, PrevPos, VacantEntry};

use thiserror::Error;

/// Error type for a hashmap shrink operation.
#[derive(Error, Debug)]
pub enum HashMapShrinkError {
    /// There was an error encountered while resizing the memory area.
    #[error("shmem resize failed: {0}")]
    ResizeError(shmem::Error),
    /// Occupied entries in to-be-shrunk space were encountered beginning at the given index.
    #[error("occupied entry in deallocated space found at {0}")]
    RemainingEntries(usize),
}

/// This represents a hash table that (possibly) lives in shared memory.
/// If a new process is launched with fork(), the child process inherits
/// this struct.
#[must_use]
pub struct HashMapInit<'a, K, V, S = rustc_hash::FxBuildHasher> {
    shmem_handle: Option<ShmemHandle>,
    shared_ptr: *mut HashMapShared<'a, K, V>,
    shared_size: usize,
    hasher: S,
    num_buckets: u32,
}

/// This is a per-process handle to a hash table that (possibly) lives in shared memory.
/// If a child process is launched with fork(), the child process should
/// get its own HashMapAccess by calling HashMapInit::attach_writer/reader().
///
/// XXX: We're not making use of it at the moment, but this struct could
/// hold process-local information in the future.
pub struct HashMapAccess<'a, K, V, S = rustc_hash::FxBuildHasher> {
    shmem_handle: Option<ShmemHandle>,
    shared_ptr: *mut HashMapShared<'a, K, V>,
    hasher: S,
}

unsafe impl<K: Sync, V: Sync, S> Sync for HashMapAccess<'_, K, V, S> {}
unsafe impl<K: Send, V: Send, S> Send for HashMapAccess<'_, K, V, S> {}

impl<'a, K: Clone + Hash + Eq, V, S> HashMapInit<'a, K, V, S> {
    /// Change the 'hasher' used by the hash table.
    ///
    /// NOTE: This must be called right after creating the hash table,
    /// before inserting any entries and before calling attach_writer/reader.
    /// Otherwise different accessors could be using different hash function,
    /// with confusing results.
    pub fn with_hasher<T: BuildHasher>(self, hasher: T) -> HashMapInit<'a, K, V, T> {
        HashMapInit {
            hasher,
            shmem_handle: self.shmem_handle,
            shared_ptr: self.shared_ptr,
            shared_size: self.shared_size,
            num_buckets: self.num_buckets,
        }
    }

    /// Loosely (over)estimate the size needed to store a hash table with `num_buckets` buckets.
    pub fn estimate_size(num_buckets: u32) -> usize {
        // add some margin to cover alignment etc.
        CoreHashMap::<K, V>::estimate_size(num_buckets) + size_of::<HashMapShared<K, V>>() + 1000
    }

    fn new(
        num_buckets: u32,
        shmem_handle: Option<ShmemHandle>,
        area_ptr: *mut u8,
        area_size: usize,
        hasher: S,
    ) -> Self {
        let mut ptr: *mut u8 = area_ptr;
        let end_ptr: *mut u8 = unsafe { ptr.add(area_size) };

        // carve out area for the One Big Lock (TM) and the HashMapShared.
        ptr = unsafe { ptr.add(ptr.align_offset(align_of::<libc::pthread_rwlock_t>())) };
        let raw_lock_ptr = ptr;
        ptr = unsafe { ptr.add(size_of::<libc::pthread_rwlock_t>()) };
        ptr = unsafe { ptr.add(ptr.align_offset(align_of::<HashMapShared<K, V>>())) };
        let shared_ptr: *mut HashMapShared<K, V> = ptr.cast();
        ptr = unsafe { ptr.add(size_of::<HashMapShared<K, V>>()) };

        // carve out the buckets
        ptr = unsafe { ptr.byte_add(ptr.align_offset(align_of::<core::Bucket<K, V>>())) };
        let buckets_ptr = ptr;
        ptr = unsafe { ptr.add(size_of::<core::Bucket<K, V>>() * num_buckets as usize) };

        // use remaining space for the dictionary
        ptr = unsafe { ptr.byte_add(ptr.align_offset(align_of::<u32>())) };
        assert!(ptr.addr() < end_ptr.addr());
        let dictionary_ptr = ptr;
        let dictionary_size = unsafe { end_ptr.byte_offset_from(ptr) / size_of::<u32>() as isize };
        assert!(dictionary_size > 0);

        let buckets =
            unsafe { std::slice::from_raw_parts_mut(buckets_ptr.cast(), num_buckets as usize) };
        let dictionary = unsafe {
            std::slice::from_raw_parts_mut(dictionary_ptr.cast(), dictionary_size as usize)
        };

        let hashmap = CoreHashMap::new(buckets, dictionary);
        unsafe {
            let lock = RwLock::from_raw(PthreadRwLock::new(raw_lock_ptr.cast()), hashmap);
            std::ptr::write(shared_ptr, lock);
        }

        Self {
            num_buckets,
            shmem_handle,
            shared_ptr,
            shared_size: area_size,
            hasher,
        }
    }

    /// Attach to a hash table for writing.
    pub fn attach_writer(self) -> HashMapAccess<'a, K, V, S> {
        HashMapAccess {
            shmem_handle: self.shmem_handle,
            shared_ptr: self.shared_ptr,
            hasher: self.hasher,
        }
    }

    /// Initialize a table for reading. Currently identical to [`HashMapInit::attach_writer`].
    ///
    /// This is a holdover from a previous implementation and is being kept around for
    /// backwards compatibility reasons.
    pub fn attach_reader(self) -> HashMapAccess<'a, K, V, S> {
        self.attach_writer()
    }
}

/// Hash table data that is actually stored in the shared memory area.
///
/// NOTE: We carve out the parts from a contiguous chunk. Growing and shrinking the hash table
/// relies on the memory layout! The data structures are laid out in the contiguous shared memory
/// area as follows:
///
/// [`libc::pthread_rwlock_t`]
/// [`HashMapShared`]
/// buckets
/// dictionary
///
/// In between the above parts, there can be padding bytes to align the parts correctly.
type HashMapShared<'a, K, V> = RwLock<CoreHashMap<'a, K, V>>;

impl<'a, K, V> HashMapInit<'a, K, V, rustc_hash::FxBuildHasher>
where
    K: Clone + Hash + Eq,
{
    /// Place the hash table within a user-supplied fixed memory area.
    pub fn with_fixed(num_buckets: u32, area: &'a mut [MaybeUninit<u8>]) -> Self {
        Self::new(
            num_buckets,
            None,
            area.as_mut_ptr().cast(),
            area.len(),
            rustc_hash::FxBuildHasher,
        )
    }

    /// Place a new hash map in the given shared memory area
    ///
    /// # Panics
    /// Will panic on failure to resize area to expected map size.
    pub fn with_shmem(num_buckets: u32, shmem: ShmemHandle) -> Self {
        let size = Self::estimate_size(num_buckets);
        shmem
            .set_size(size)
            .expect("could not resize shared memory area");
        let ptr = shmem.data_ptr.as_ptr().cast();
        Self::new(
            num_buckets,
            Some(shmem),
            ptr,
            size,
            rustc_hash::FxBuildHasher,
        )
    }

    /// Make a resizable hash map within a new shared memory area with the given name.
    pub fn new_resizeable_named(num_buckets: u32, max_buckets: u32, name: &str) -> Self {
        let size = Self::estimate_size(num_buckets);
        let max_size = Self::estimate_size(max_buckets);
        let shmem =
            ShmemHandle::new(name, size, max_size).expect("failed to make shared memory area");
        let ptr = shmem.data_ptr.as_ptr().cast();

        Self::new(
            num_buckets,
            Some(shmem),
            ptr,
            size,
            rustc_hash::FxBuildHasher,
        )
    }

    /// Make a resizable hash map within a new anonymous shared memory area.
    pub fn new_resizeable(num_buckets: u32, max_buckets: u32) -> Self {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let val = COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!("neon_shmem_hmap{val}");
        Self::new_resizeable_named(num_buckets, max_buckets, &name)
    }
}

impl<'a, K, V, S: BuildHasher> HashMapAccess<'a, K, V, S>
where
    K: Clone + Hash + Eq,
{
    /// Hash a key using the map's hasher.
    #[inline]
    fn get_hash_value(&self, key: &K) -> u64 {
        self.hasher.hash_one(key)
    }

    fn entry_with_hash(&self, key: K, hash: u64) -> Entry<'a, '_, K, V> {
        let mut map = unsafe { self.shared_ptr.as_ref() }.unwrap().write();
        let dict_pos = hash as usize % map.dictionary.len();
        let first = map.dictionary[dict_pos];
        if first == INVALID_POS {
            // no existing entry
            return Entry::Vacant(VacantEntry {
                map,
                key,
                dict_pos: dict_pos as u32,
            });
        }

        let mut prev_pos = PrevPos::First(dict_pos as u32);
        let mut next = first;
        loop {
            let bucket = &mut map.buckets[next as usize];
            let (bucket_key, _bucket_value) = bucket.inner.as_mut().expect("entry is in use");
            if *bucket_key == key {
                // found existing entry
                return Entry::Occupied(OccupiedEntry {
                    map,
                    _key: key,
                    prev_pos,
                    bucket_pos: next,
                });
            }

            if bucket.next == INVALID_POS {
                // No existing entry
                return Entry::Vacant(VacantEntry {
                    map,
                    key,
                    dict_pos: dict_pos as u32,
                });
            }
            prev_pos = PrevPos::Chained(next);
            next = bucket.next;
        }
    }

    /// Get a reference to the corresponding value for a key.
    pub fn get<'e>(&'e self, key: &K) -> Option<ValueReadGuard<'e, V>> {
        let hash = self.get_hash_value(key);
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap().read();
        RwLockReadGuard::try_map(map, |m| m.get_with_hash(key, hash)).ok()
    }

    /// Get a reference to the entry containing a key.
    ///
    /// NB: THis takes a write lock as there's no way to distinguish whether the intention
    /// is to use the entry for reading or for writing in advance.
    pub fn entry(&self, key: K) -> Entry<'a, '_, K, V> {
        let hash = self.get_hash_value(&key);
        self.entry_with_hash(key, hash)
    }

    /// Remove a key given its hash. Returns the associated value if it existed.
    pub fn remove(&self, key: &K) -> Option<V> {
        let hash = self.get_hash_value(key);
        match self.entry_with_hash(key.clone(), hash) {
            Entry::Occupied(e) => Some(e.remove()),
            Entry::Vacant(_) => None,
        }
    }

    /// Insert/update a key. Returns the previous associated value if it existed.
    ///
    /// # Errors
    /// Will return [`core::FullError`] if there is no more space left in the map.
    pub fn insert(&self, key: K, value: V) -> Result<Option<V>, core::FullError> {
        let hash = self.get_hash_value(&key);
        match self.entry_with_hash(key.clone(), hash) {
            Entry::Occupied(mut e) => Ok(Some(e.insert(value))),
            Entry::Vacant(e) => {
                _ = e.insert(value)?;
                Ok(None)
            }
        }
    }

    /// Optionally return the entry for a bucket at a given index if it exists.
    ///
    /// Has more overhead than one would intuitively expect: performs both a clone of the key
    /// due to the [`OccupiedEntry`] type owning the key and also a hash of the key in order
    /// to enable repairing the hash chain if the entry is removed.
    pub fn entry_at_bucket(&self, pos: usize) -> Option<OccupiedEntry<'a, '_, K, V>> {
        let map = unsafe { self.shared_ptr.as_mut() }.unwrap().write();
        if pos >= map.buckets.len() {
            return None;
        }

        let entry = map.buckets[pos].inner.as_ref();
        match entry {
            Some((key, _)) => Some(OccupiedEntry {
                _key: key.clone(),
                bucket_pos: pos as u32,
                prev_pos: entry::PrevPos::Unknown(self.get_hash_value(key)),
                map,
            }),
            _ => None,
        }
    }

    /// Returns the number of buckets in the table.
    pub fn get_num_buckets(&self) -> usize {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap().read();
        map.get_num_buckets()
    }

    /// Return the key and value stored in bucket with given index. This can be used to
    /// iterate through the hash map.
    // TODO: An Iterator might be nicer. The communicator's clock algorithm needs to
    // _slowly_ iterate through all buckets with its clock hand,  without holding a lock.
    // If we switch to an Iterator, it must not hold the lock.
    pub fn get_at_bucket(&self, pos: usize) -> Option<ValueReadGuard<'_, (K, V)>> {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap().read();
        if pos >= map.buckets.len() {
            return None;
        }
        RwLockReadGuard::try_map(map, |m| m.buckets[pos].inner.as_ref()).ok()
    }

    /// Returns the index of the bucket a given value corresponds to.
    pub fn get_bucket_for_value(&self, val_ptr: *const V) -> usize {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap().read();

        let origin = map.buckets.as_ptr();
        let idx = (val_ptr as usize - origin as usize) / size_of::<Bucket<K, V>>();
        assert!(idx < map.buckets.len());

        idx
    }

    /// Returns the number of occupied buckets in the table.
    pub fn get_num_buckets_in_use(&self) -> usize {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap().read();
        map.buckets_in_use as usize
    }

    /// Clears all entries in a table. Does not reset any shrinking operations.
    pub fn clear(&self) {
        let mut map = unsafe { self.shared_ptr.as_mut() }.unwrap().write();
        map.clear();
    }

    /// Perform an in-place rehash of some region (0..`rehash_buckets`) of the table and reset
    /// the `buckets` and `dictionary` slices to be as long as `num_buckets`. Resets the freelist
    /// in the process.
    fn rehash_dict(
        &self,
        inner: &mut CoreHashMap<'a, K, V>,
        buckets_ptr: *mut core::Bucket<K, V>,
        end_ptr: *mut u8,
        num_buckets: u32,
        rehash_buckets: u32,
    ) {
        inner.free_head = INVALID_POS;

        let buckets;
        let dictionary;
        unsafe {
            let buckets_end_ptr = buckets_ptr.add(num_buckets as usize);
            let dictionary_ptr: *mut u32 = buckets_end_ptr
                .byte_add(buckets_end_ptr.align_offset(align_of::<u32>()))
                .cast();
            let dictionary_size: usize =
                end_ptr.byte_offset_from(buckets_end_ptr) as usize / size_of::<u32>();

            buckets = std::slice::from_raw_parts_mut(buckets_ptr, num_buckets as usize);
            dictionary = std::slice::from_raw_parts_mut(dictionary_ptr, dictionary_size);
        }
        for e in dictionary.iter_mut() {
            *e = INVALID_POS;
        }

        for (i, bucket) in buckets.iter_mut().enumerate().take(rehash_buckets as usize) {
            if bucket.inner.is_none() {
                bucket.next = inner.free_head;
                inner.free_head = i as u32;
                continue;
            }

            let hash = self.hasher.hash_one(&bucket.inner.as_ref().unwrap().0);
            let pos: usize = (hash % dictionary.len() as u64) as usize;
            bucket.next = dictionary[pos];
            dictionary[pos] = i as u32;
        }

        inner.dictionary = dictionary;
        inner.buckets = buckets;
    }

    /// Rehash the map without growing or shrinking.
    pub fn shuffle(&self) {
        let mut map = unsafe { self.shared_ptr.as_mut() }.unwrap().write();
        let num_buckets = map.get_num_buckets() as u32;
        let size_bytes = HashMapInit::<K, V, S>::estimate_size(num_buckets);
        let end_ptr: *mut u8 = unsafe { self.shared_ptr.byte_add(size_bytes).cast() };
        let buckets_ptr = map.buckets.as_mut_ptr();
        self.rehash_dict(&mut map, buckets_ptr, end_ptr, num_buckets, num_buckets);
    }

    /// Grow the number of buckets within the table.
    ///
    /// 1. Grows the underlying shared memory area
    /// 2. Initializes new buckets and overwrites the current dictionary
    /// 3. Rehashes the dictionary
    ///
    /// # Panics
    /// Panics if called on a map initialized with [`HashMapInit::with_fixed`].
    ///
    /// # Errors
    /// Returns an [`shmem::Error`] if any errors occur resizing the memory region.
    pub fn grow(&self, num_buckets: u32) -> Result<(), shmem::Error> {
        let mut map = unsafe { self.shared_ptr.as_mut() }.unwrap().write();
        let old_num_buckets = map.buckets.len() as u32;

        assert!(
            num_buckets >= old_num_buckets,
            "grow called with a smaller number of buckets"
        );
        if num_buckets == old_num_buckets {
            return Ok(());
        }
        let shmem_handle = self
            .shmem_handle
            .as_ref()
            .expect("grow called on a fixed-size hash table");

        let size_bytes = HashMapInit::<K, V, S>::estimate_size(num_buckets);
        shmem_handle.set_size(size_bytes)?;
        let end_ptr: *mut u8 = unsafe { shmem_handle.data_ptr.as_ptr().add(size_bytes) };

        // Initialize new buckets. The new buckets are linked to the free list.
        // NB: This overwrites the dictionary!
        let buckets_ptr = map.buckets.as_mut_ptr();
        unsafe {
            for i in old_num_buckets..num_buckets {
                let bucket = buckets_ptr.add(i as usize);
                bucket.write(core::Bucket {
                    next: if i < num_buckets - 1 {
                        i + 1
                    } else {
                        map.free_head
                    },
                    inner: None,
                });
            }
        }

        self.rehash_dict(&mut map, buckets_ptr, end_ptr, num_buckets, old_num_buckets);
        map.free_head = old_num_buckets;

        Ok(())
    }

    /// Begin a shrink, limiting all new allocations to be in buckets with index below `num_buckets`.
    ///
    /// # Panics
    /// Panics if called on a map initialized with [`HashMapInit::with_fixed`] or if `num_buckets` is
    /// greater than the number of buckets in the map.
    pub fn begin_shrink(&mut self, num_buckets: u32) {
        let mut map = unsafe { self.shared_ptr.as_mut() }.unwrap().write();
        assert!(
            num_buckets <= map.get_num_buckets() as u32,
            "shrink called with a larger number of buckets"
        );
        _ = self
            .shmem_handle
            .as_ref()
            .expect("shrink called on a fixed-size hash table");
        map.alloc_limit = num_buckets;
    }

    /// If a shrink operation is underway, returns the target size of the map. Otherwise, returns None.
    pub fn shrink_goal(&self) -> Option<usize> {
        let map = unsafe { self.shared_ptr.as_mut() }.unwrap().read();
        let goal = map.alloc_limit;
        if goal == INVALID_POS {
            None
        } else {
            Some(goal as usize)
        }
    }

    /// Complete a shrink after caller has evicted entries, removing the unused buckets and rehashing.
    ///
    /// # Panics
    /// The following cases result in a panic:
    /// - Calling this function on a map initialized with [`HashMapInit::with_fixed`].
    /// - Calling this function on a map when no shrink operation is in progress.
    pub fn finish_shrink(&self) -> Result<(), HashMapShrinkError> {
        let mut map = unsafe { self.shared_ptr.as_mut() }.unwrap().write();
        assert!(
            map.alloc_limit != INVALID_POS,
            "called finish_shrink when no shrink is in progress"
        );

        let num_buckets = map.alloc_limit;

        if map.get_num_buckets() == num_buckets as usize {
            return Ok(());
        }

        assert!(
            map.buckets_in_use <= num_buckets,
            "called finish_shrink before enough entries were removed"
        );

        for i in (num_buckets as usize)..map.buckets.len() {
            if map.buckets[i].inner.is_some() {
                return Err(HashMapShrinkError::RemainingEntries(i));
            }
        }

        let shmem_handle = self
            .shmem_handle
            .as_ref()
            .expect("shrink called on a fixed-size hash table");

        let size_bytes = HashMapInit::<K, V, S>::estimate_size(num_buckets);
        if let Err(e) = shmem_handle.set_size(size_bytes) {
            return Err(HashMapShrinkError::ResizeError(e));
        }
        let end_ptr: *mut u8 = unsafe { shmem_handle.data_ptr.as_ptr().add(size_bytes) };
        let buckets_ptr = map.buckets.as_mut_ptr();
        self.rehash_dict(&mut map, buckets_ptr, end_ptr, num_buckets, num_buckets);
        map.alloc_limit = INVALID_POS;

        Ok(())
    }
}
