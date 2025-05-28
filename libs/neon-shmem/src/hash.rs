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

pub struct HashMapInit<'a, K, V>
{
    // Hash table can be allocated in a fixed memory area, or in a resizeable ShmemHandle.
    shmem: Option<ShmemHandle>,
    shared_ptr: *mut HashMapShared<'a, K, V>,
}

pub struct HashMapAccess<'a, K, V> {
    _shmem: Option<ShmemHandle>,
    shared_ptr: *mut HashMapShared<'a, K, V>,
}

unsafe impl<'a, K: Sync, V: Sync> Sync for HashMapAccess<'a, K, V> {}
unsafe impl<'a, K: Send, V: Send> Send for HashMapAccess<'a, K, V> {}

impl<'a, K, V> HashMapInit<'a, K, V> {
    pub fn attach_writer(self) -> HashMapAccess<'a, K, V> {
        HashMapAccess {
            _shmem: self.shmem,
            shared_ptr: self.shared_ptr,
        }
    }

    pub fn attach_reader(self) -> HashMapAccess<'a, K, V> {
        // no difference to attach_writer currently
        self.attach_writer()
    }
}

// This is stored in the shared memory area
struct HashMapShared<'a, K, V>
{
    inner: spin::RwLock<CoreHashMap<'a, K, V>>,
}

impl<'a, K, V> HashMapInit<'a, K, V>
where K: Clone + Hash + Eq,
{
    pub fn estimate_size(num_buckets: u32) -> usize {
        // add some margin to cover alignment etc.
        CoreHashMap::<K, V>::estimate_size(num_buckets) + size_of::<HashMapShared<K, V>>() + 1000
    }
    
    pub fn init_in_fixed_area(num_buckets: u32, area: &'a mut [MaybeUninit<u8>]) -> HashMapInit<'a, K, V> {
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

    fn init_common(num_buckets: u32, shmem_handle: Option<ShmemHandle>, area_ptr: *mut u8, area_len: usize) -> HashMapInit<'a, K, V> {
        // carve out HashMapShared from the area. This does not include the hashmap's dictionary
        // and buckets.
        let mut ptr: *mut u8 = area_ptr;
        ptr = unsafe { ptr.add(ptr.align_offset(align_of::<HashMapShared<K, V>>())) };
        let shared_ptr: *mut HashMapShared<K, V> = ptr.cast();
        ptr = unsafe { ptr.add(size_of::<HashMapShared<K, V>>()) };

        // the rest of the space is given to the hash map's dictionary and buckets
        let remaining_area = unsafe {
            std::slice::from_raw_parts_mut(
                ptr,
                area_len - ptr.offset_from(area_ptr) as usize,
            )
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
            shmem: shmem_handle,
            shared_ptr,
        }
    }
    
}

impl<'a, K, V> HashMapAccess<'a, K, V>
    where K: Clone + Hash + Eq,
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
            if let Some(_) = existing {
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
