//! Hash table implementation on top of 'shmem'
//!
//! Features required in the long run by the communicator project:
//!
//! [X] Accessible from both Postgres processes and rust threads in the communicator process
//! [X] Low latency
//! [ ] Scalable to lots of concurrent accesses (currently uses a single spinlock)
//! [ ] Resizable

use std::fmt::Debug;
use std::hash::Hash;
use std::mem::MaybeUninit;
use std::ops::Deref;

use crate::shmem::ShmemHandle;

use spin;

mod core;

#[cfg(test)]
mod tests;

use core::CoreHashMap;

pub enum UpdateAction<V> {
    Nothing,
    Insert(V),
    Remove,
}

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

// This is stored in the shared memory area
struct HashMapShared<'a, K, V> {
    inner: spin::RwLock<CoreHashMap<'a, K, V>>,
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
        // carve out HashMapShared from the area. This does not include the hashmap's dictionary
        // and buckets.
        let mut ptr: *mut u8 = area_ptr;
        ptr = unsafe { ptr.add(ptr.align_offset(align_of::<HashMapShared<K, V>>())) };
        let shared_ptr: *mut HashMapShared<K, V> = ptr.cast();
        ptr = unsafe { ptr.add(size_of::<HashMapShared<K, V>>()) };

        // the rest of the space is given to the hash map's dictionary and buckets
        let remaining_area = unsafe {
            std::slice::from_raw_parts_mut(ptr, area_len - ptr.offset_from(area_ptr) as usize)
        };

        let hashmap = CoreHashMap::new(num_buckets, remaining_area);
        unsafe {
            std::ptr::write(
                shared_ptr,
                HashMapShared {
                    inner: spin::RwLock::new(hashmap),
                },
            );
        }

        HashMapInit {
            shmem_handle,
            shared_ptr,
        }
    }
}

impl<'a, K, V> HashMapAccess<'a, K, V>
where
    K: Clone + Hash + Eq,
{
    pub fn get<'e>(&'e self, key: &K) -> Option<ValueReadGuard<'e, K, V>> {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();
        let lock_guard = map.inner.read();

        match lock_guard.get(key) {
            None => None,
            Some(val_ref) => {
                let val_ptr = std::ptr::from_ref(val_ref);
                Some(ValueReadGuard {
                    _lock_guard: lock_guard,
                    value: val_ptr,
                })
            }
        }
    }

    /// Insert a value
    pub fn insert(&self, key: &K, value: V) -> Result<bool, OutOfMemoryError> {
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
    pub fn remove(&self, key: &K) -> bool {
        let mut result = false;
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

    /// Update key using the given function. All the other modifying operations are based on this.
    pub fn update_with_fn<F>(&self, key: &K, value_fn: F) -> Result<(), OutOfMemoryError>
    where
        F: FnOnce(Option<&V>) -> UpdateAction<V>,
    {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();
        let mut lock_guard = map.inner.write();

        let old_val = lock_guard.get(key);
        let action = value_fn(old_val);
        match (old_val, action) {
            (_, UpdateAction::Nothing) => {}
            (_, UpdateAction::Insert(new_val)) => {
                let _ = lock_guard.insert(key, new_val);
            }
            (None, UpdateAction::Remove) => panic!("Remove action with no old value"),
            (Some(_), UpdateAction::Remove) => {
                let _ = lock_guard.remove(key);
            }
        }

        Ok(())
    }

    /// Update key using the given function. All the other modifying operations are based on this.
    pub fn update_with_fn_at_bucket<F>(
        &self,
        pos: usize,
        value_fn: F,
    ) -> Result<(), OutOfMemoryError>
    where
        F: FnOnce(Option<&V>) -> UpdateAction<V>,
    {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();
        let mut lock_guard = map.inner.write();

        let old_val = lock_guard.get_bucket(pos);
        let action = value_fn(old_val.map(|(_k, v)| v));
        match (old_val, action) {
            (_, UpdateAction::Nothing) => {}
            (_, UpdateAction::Insert(_new_val)) => panic!("cannot insert without key"),
            (None, UpdateAction::Remove) => panic!("Remove action with no old value"),
            (Some((key, _value)), UpdateAction::Remove) => {
                let key = key.clone();
                let _ = lock_guard.remove(&key);
            }
        }

        Ok(())
    }

    pub fn get_num_buckets(&self) -> usize {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();
        map.inner.read().get_num_buckets()
    }

    /// Return the key and value stored in bucket with given index. This can be used to
    /// iterate through the hash map. (An Iterator might be nicer. The communicator's
    /// clock algorithm needs to _slowly_ iterate through all buckets with its clock hand,
    /// without holding a lock. If we switch to an Iterator, it must not hold the lock.)
    pub fn get_bucket<'e>(&'e self, pos: usize) -> Option<ValueReadGuard<'e, K, V>> {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();
        let lock_guard = map.inner.read();

        match lock_guard.get_bucket(pos) {
            None => None,
            Some((_key, val_ref)) => {
                let val_ptr = std::ptr::from_ref(val_ref);
                Some(ValueReadGuard {
                    _lock_guard: lock_guard,
                    value: val_ptr,
                })
            }
        }
    }

    // for metrics
    pub fn get_num_buckets_in_use(&self) -> usize {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();
        map.inner.read().buckets_in_use as usize
    }

    /// Grow
    ///
    /// 1. grow the underlying shared memory area
    /// 2. Initialize new buckets. This overwrites the current dictionary
    /// 3. Recalculate the dictionary
    pub fn grow(&self, num_buckets: u32) -> Result<(), crate::shmem::Error> {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();
        let mut lock_guard = map.inner.write();
        let inner = &mut *lock_guard;
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
                    hash: 0,
                    next: if i < num_buckets {
                        i + 1
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
        for item in dictionary.iter_mut() {
            *item = core::INVALID_POS;
        }

        #[allow(clippy::needless_range_loop)]
        for i in 0..old_num_buckets as usize {
            if buckets[i].inner.is_none() {
                continue;
            }
            let pos: usize = (buckets[i].hash % dictionary.len() as u64) as usize;
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

pub struct ValueReadGuard<'a, K, V> {
    _lock_guard: spin::RwLockReadGuard<'a, CoreHashMap<'a, K, V>>,
    value: *const V,
}

impl<'a, K, V> Deref for ValueReadGuard<'a, K, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        // SAFETY: The `lock_guard` ensures that the underlying map (and thus the value pointed to
        // by `value`) remains valid for the lifetime `'a`. The `value` has been obtained from a
        // valid reference within the map.
        unsafe { &*self.value }
    }
}
