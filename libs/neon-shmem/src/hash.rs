//! Hash table implementation on top of 'shmem'
//!
//! Features required in the long run by the communicator project:
//!
//! [X] Accessible from both Postgres processes and rust threads in the communicator process
//! [X] Low latency
//! [ ] Scalable to lots of concurrent accesses (currently relies on caller for locking)
//! [ ] Resizable

use std::fmt::Debug;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::mem::MaybeUninit;

use crate::shmem::ShmemHandle;

mod core;
pub mod entry;

#[cfg(test)]
mod tests;

use core::CoreHashMap;
use entry::{Entry, OccupiedEntry};

#[derive(Debug)]
pub struct OutOfMemoryError();

pub struct HashMapInit<'a, K, V> {
    // Hash table can be allocated in a fixed memory area, or in a resizeable ShmemHandle.
    shmem_handle: Option<ShmemHandle>,
    shared_ptr: *mut HashMapShared<'a, K, V>,
}

pub struct HashMapAccess<'a, K, V> {
    shmem_handle: Option<ShmemHandle>,
    shared_ptr: *mut HashMapShared<'a, K, V>,
}

unsafe impl<'a, K: Sync, V: Sync> Sync for HashMapAccess<'a, K, V> {}
unsafe impl<'a, K: Send, V: Send> Send for HashMapAccess<'a, K, V> {}

impl<'a, K, V> HashMapInit<'a, K, V> {
    pub fn attach_writer(self) -> HashMapAccess<'a, K, V> {
        HashMapAccess {
            shmem_handle: self.shmem_handle,
            shared_ptr: self.shared_ptr,
        }
    }

    pub fn attach_reader(self) -> HashMapAccess<'a, K, V> {
        // no difference to attach_writer currently
        self.attach_writer()
    }
}

/// This is stored in the shared memory area
///
/// NOTE: We carve out the parts from a contiguous chunk. Growing and shrinking the hash table
/// relies on the memory layout! The data structures are laid out in the contiguous shared memory
/// area as follows:
///
/// HashMapShared
/// [buckets]
/// [dictionary]
///
/// In between the above parts, there can be padding bytes to align the parts correctly.
struct HashMapShared<'a, K, V> {
    inner: CoreHashMap<'a, K, V>,
}

impl<'a, K, V> HashMapInit<'a, K, V>
where
    K: Clone + Hash + Eq,
{
    pub fn estimate_size(num_buckets: u32) -> usize {
        // add some margin to cover alignment etc.
        CoreHashMap::<K, V>::estimate_size(num_buckets) + size_of::<HashMapShared<K, V>>() + 1000
    }

    pub fn init_in_fixed_area(
        num_buckets: u32,
        area: &'a mut [MaybeUninit<u8>],
    ) -> HashMapInit<'a, K, V> {
        Self::init_common(num_buckets, None, area.as_mut_ptr().cast(), area.len())
    }

    /// Initialize a new hash map in the given shared memory area
    pub fn init_in_shmem(num_buckets: u32, mut shmem: ShmemHandle) -> HashMapInit<'a, K, V> {
        let size = Self::estimate_size(num_buckets);
        shmem
            .set_size(size)
            .expect("could not resize shared memory area");

        let ptr = unsafe { shmem.data_ptr.as_mut() };
        Self::init_common(num_buckets, Some(shmem), ptr, size)
    }

    fn init_common(
        num_buckets: u32,
        shmem_handle: Option<ShmemHandle>,
        area_ptr: *mut u8,
        area_len: usize,
    ) -> HashMapInit<'a, K, V> {
        // carve out the HashMapShared struct from the area.
        let mut ptr: *mut u8 = area_ptr;
        let end_ptr: *mut u8 = unsafe { area_ptr.add(area_len) };
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
            std::ptr::write(shared_ptr, HashMapShared { inner: hashmap });
        }

        HashMapInit {
            shmem_handle: shmem_handle,
            shared_ptr,
        }
    }
}

impl<'a, K, V> HashMapAccess<'a, K, V>
where
    K: Clone + Hash + Eq,
{
    pub fn get_hash_value(&self, key: &K) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_with_hash<'e>(&'e self, key: &K, hash: u64) -> Option<&'e V> {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();

        map.inner.get_with_hash(key, hash)
    }

    pub fn entry_with_hash(&mut self, key: K, hash: u64) -> Entry<'a, '_, K, V> {
        let map = unsafe { self.shared_ptr.as_mut() }.unwrap();

        map.inner.entry_with_hash(key, hash)
    }

    pub fn remove_with_hash(&mut self, key: &K, hash: u64) {
        let map = unsafe { self.shared_ptr.as_mut() }.unwrap();

        match map.inner.entry_with_hash(key.clone(), hash) {
            Entry::Occupied(e) => {
                e.remove();
            }
            Entry::Vacant(_) => {}
        };
    }

    pub fn entry_at_bucket(&mut self, pos: usize) -> Option<OccupiedEntry<'a, '_, K, V>> {
        let map = unsafe { self.shared_ptr.as_mut() }.unwrap();
        map.inner.entry_at_bucket(pos)
    }

    pub fn get_num_buckets(&self) -> usize {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();
        map.inner.get_num_buckets()
    }

    /// Return the key and value stored in bucket with given index. This can be used to
    /// iterate through the hash map. (An Iterator might be nicer. The communicator's
    /// clock algorithm needs to _slowly_ iterate through all buckets with its clock hand,
    /// without holding a lock. If we switch to an Iterator, it must not hold the lock.)
    pub fn get_at_bucket(&self, pos: usize) -> Option<&(K, V)> {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();

        if pos >= map.inner.buckets.len() {
            return None;
        }
        let bucket = &map.inner.buckets[pos];
        bucket.inner.as_ref()
    }

    pub fn get_bucket_for_value(&self, val_ptr: *const V) -> usize {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();

        let origin = map.inner.buckets.as_ptr();
        let idx = (val_ptr as usize - origin as usize) / (size_of::<V>() as usize);
        assert!(idx < map.inner.buckets.len());

        idx
    }

    // for metrics
    pub fn get_num_buckets_in_use(&self) -> usize {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();
        map.inner.buckets_in_use as usize
    }

    /// Grow
    ///
    /// 1. grow the underlying shared memory area
    /// 2. Initialize new buckets. This overwrites the current dictionary
    /// 3. Recalculate the dictionary
    pub fn grow(&mut self, num_buckets: u32) -> Result<(), crate::shmem::Error> {
        let map = unsafe { self.shared_ptr.as_mut() }.unwrap();
        let inner = &mut map.inner;
        let old_num_buckets = inner.buckets.len() as u32;

        if num_buckets < old_num_buckets {
            panic!("grow called with a smaller number of buckets");
        }
        if num_buckets == old_num_buckets {
            return Ok(());
        }
        let shmem_handle = self
            .shmem_handle
            .as_ref()
            .expect("grow called on a fixed-size hash table");

        let size_bytes = HashMapInit::<K, V>::estimate_size(num_buckets);
        shmem_handle.set_size(size_bytes)?;
        let end_ptr: *mut u8 = unsafe { shmem_handle.data_ptr.as_ptr().add(size_bytes) };

        // Initialize new buckets. The new buckets are linked to the free list. NB: This overwrites
        // the dictionary!
        let buckets_ptr = inner.buckets.as_mut_ptr();
        unsafe {
            for i in old_num_buckets..num_buckets {
                let bucket_ptr = buckets_ptr.add(i as usize);
                bucket_ptr.write(core::Bucket {
                    next: if i < num_buckets {
                        i as u32 + 1
                    } else {
                        inner.free_head
                    },
                    inner: None,
                });
            }
        }

        // Recalculate the dictionary
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
        for i in 0..dictionary.len() {
            dictionary[i] = core::INVALID_POS;
        }

        for i in 0..old_num_buckets as usize {
            if buckets[i].inner.is_none() {
                continue;
            }

            let mut hasher = DefaultHasher::new();
            buckets[i].inner.as_ref().unwrap().0.hash(&mut hasher);
            let hash = hasher.finish();

            let pos: usize = (hash % dictionary.len() as u64) as usize;
            buckets[i].next = dictionary[pos];
            dictionary[pos] = i as u32;
        }

        // Finally, update the CoreHashMap struct
        inner.dictionary = dictionary;
        inner.buckets = buckets;
        inner.free_head = old_num_buckets;

        Ok(())
    }

    // TODO: Shrinking is a multi-step process that requires co-operation from the caller
    //
    // 1. The caller must first call begin_shrink(). That forbids allocation of higher-numbered
    // buckets.
    //
    // 2. Next, the caller must evict all entries in higher-numbered buckets.
    //
    // 3. Finally, call finish_shrink(). This recomputes the dictionary and shrinks the underlying
    //    shmem area
}
