use std::hash::{BuildHasher, Hash};
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;

use crate::shmem::ShmemHandle;
use crate::{shmem, sync::*};

mod core;
mod bucket;
pub mod entry;

#[cfg(test)]
mod tests;

use core::{
	CoreHashMap, DictShard, EntryKey, EntryType,
	FullError, MaybeUninitDictShard
};
use bucket::{Bucket, BucketIdx};
use entry::Entry;

/// Wrapper struct around multiple [`ShmemHandle`]s.
struct HashMapHandles {
	keys_shmem: ShmemHandle,
	idxs_shmem: ShmemHandle,
	vals_shmem: ShmemHandle,
}

/// This represents a hash table that (possibly) lives in shared memory.
/// If a new process is launched with fork(), the child process inherits
/// this struct.
#[must_use]
pub struct HashMapInit<'a, K, V, S = rustc_hash::FxBuildHasher> {
    shmem_handles: Option<HashMapHandles>,
    shared_ptr: *mut HashMapShared<'a, K, V>,
    hasher: S,
    num_buckets: usize,
	num_shards: usize,
	resize_lock: Mutex<()>,
}

/// This is a per-process handle to a hash table that (possibly) lives in shared memory.
/// If a child process is launched with fork(), the child process should
/// get its own HashMapAccess by calling HashMapInit::attach_writer/reader().
///
/// XXX: We're not making use of it at the moment, but this struct could
/// hold process-local information in the future.
pub struct HashMapAccess<'a, K, V, S = rustc_hash::FxBuildHasher> {
    shmem_handles: Option<HashMapHandles>,
    shared_ptr: *mut HashMapShared<'a, K, V>,
    hasher: S,
	resize_lock: Mutex<()>,
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
	///
	/// TODO(quantumish): consider splitting out into a separate builder type?
    pub fn with_hasher<T: BuildHasher>(self, hasher: T) -> HashMapInit<'a, K, V, T> {
        HashMapInit {
            hasher,
            shmem_handles: self.shmem_handles,
            shared_ptr: self.shared_ptr,
            num_buckets: self.num_buckets,
			num_shards: self.num_shards,
			resize_lock: self.resize_lock,
        }
    }

    /// Loosely (over)estimate the size needed to store a hash table with `num_buckets` buckets.
    pub fn estimate_sizes(num_buckets: usize, num_shards: usize) -> (usize, usize, usize) {
		(
			(size_of::<EntryKey<K>>() * num_buckets)
				+ (size_of::<libc::pthread_rwlock_t>() * num_shards)
				+ (size_of::<RwLock<DictShard<'_, K>>>() * num_shards)
				+ size_of::<HashMapShared<K, V>>()
				+ 1000,
			(size_of::<BucketIdx>() * num_buckets)+ 1000,
			(size_of::<Bucket<V>>() * num_buckets) + 1000
		)
	}

	fn carve_space<T>(ptr: &mut *mut u8, amount: usize) -> *mut T {
		*ptr = unsafe { ptr.byte_add(ptr.align_offset(align_of::<T>())) };
        let out = ptr.cast();
        *ptr = unsafe { ptr.add(size_of::<T>() * amount) };
		out
	}
	
    fn new(
        num_buckets: usize,
		num_shards: usize,
        mut keys_ptr: *mut u8,
		mut idxs_ptr: *mut u8,
		mut vals_ptr: *mut u8,
        shmem_handles: Option<HashMapHandles>,
        hasher: S,
    ) -> Self {
		// Set up the main area: hashmap info at front, keys at back
		let mutex_ptr = Self::carve_space::<libc::pthread_mutex_t>(&mut keys_ptr, 1);
		let shared_ptr = Self::carve_space::<HashMapShared<K, V>>(&mut keys_ptr, 1);
		let shards_ptr = Self::carve_space::<RwLock<DictShard<'_, K>>>(&mut keys_ptr, num_shards);
		let locks_ptr = Self::carve_space::<libc::pthread_rwlock_t>(&mut keys_ptr, num_shards);
		let keys_ptr = Self::carve_space::<EntryKey<K>>(&mut keys_ptr, num_buckets);
		
		// Set up the area of bucket idxs and the area of buckets. Not much to do!
		let idxs_ptr = Self::carve_space::<BucketIdx>(&mut idxs_ptr, num_buckets);
		let vals_ptr = Self::carve_space::<Bucket<V>>(&mut vals_ptr, num_buckets);

		// Initialize the shards.
		let shards_uninit: &mut [MaybeUninit<RwLock<MaybeUninitDictShard<'_, K>>>] =
            unsafe { std::slice::from_raw_parts_mut(shards_ptr.cast(), num_shards) };
		let shard_size = num_buckets / num_shards;
		for i in 0..num_shards {
			let size = ((i + 1) * shard_size).min(num_buckets) - (i * shard_size);
			unsafe {
				shards_uninit[i].write(RwLock::from_raw(
					PthreadRwLock::new(NonNull::new_unchecked(locks_ptr.add(i))),
					MaybeUninitDictShard {
						keys: std::slice::from_raw_parts_mut(keys_ptr.add(i * shard_size).cast(), size),
						idxs: std::slice::from_raw_parts_mut(idxs_ptr.add(i * shard_size).cast(), size)
					}
				));
			};
		}
		let shards: &mut [RwLock<MaybeUninitDictShard<'_, K>>] =
            unsafe { std::slice::from_raw_parts_mut(shards_ptr.cast(), num_shards) };
        let buckets =
            unsafe { std::slice::from_raw_parts_mut(vals_ptr.cast(), num_buckets) };

        let hashmap = CoreHashMap::new(buckets, shards);
        unsafe { std::ptr::write(shared_ptr, hashmap); }

		let resize_lock = Mutex::from_raw(
			unsafe { PthreadMutex::new(NonNull::new_unchecked(mutex_ptr)) }, ()
		);
		
        Self {
			num_shards,
            num_buckets,
            shmem_handles,
            shared_ptr,
            hasher,
			resize_lock, 
        }
    }

    /// Attach to a hash table for writing.
    pub fn attach_writer(self) -> HashMapAccess<'a, K, V, S> {
        HashMapAccess {
            shmem_handles: self.shmem_handles,
            shared_ptr: self.shared_ptr,
            hasher: self.hasher,
			resize_lock: self.resize_lock,
        }
    }

    /// Initialize a table for reading. Currently identical to [`HashMapInit::attach_writer`].
    pub fn attach_reader(self) -> HashMapAccess<'a, K, V, S> {
        self.attach_writer()
    }
}

type HashMapShared<'a, K, V> = CoreHashMap<'a, K, V>;

impl<'a, K, V> HashMapInit<'a, K, V, rustc_hash::FxBuildHasher>
where
    K: Clone + Hash + Eq,
{
    /// Place the hash table within a user-supplied fixed memory area.
    pub fn with_fixed(
		num_buckets: usize,
		num_shards: usize,
		area: &'a mut [MaybeUninit<u8>]
	) -> Self {
		let (keys_size, idxs_size, _) = Self::estimate_sizes(num_buckets, num_shards);
		let ptr = area.as_mut_ptr().cast();
        Self::new(
            num_buckets,
			num_shards,
            ptr,
			unsafe { ptr.add(keys_size) },
			unsafe { ptr.add(keys_size).add(idxs_size) },
            None,
            rustc_hash::FxBuildHasher,
        )
    }

    /// Place a new hash map in the given shared memory area
    ///
    /// # Panics
    /// Will panic on failure to resize area to expected map size.
    pub fn with_shmems(
		num_buckets: usize,
		num_shards: usize,
		keys_shmem: ShmemHandle,
		idxs_shmem: ShmemHandle,
		vals_shmem: ShmemHandle,
	) -> Self {
		let (keys_size, idxs_size, vals_size) = Self::estimate_sizes(num_buckets, num_shards);
        keys_shmem.set_size(keys_size).expect("could not resize shared memory area");
        idxs_shmem.set_size(idxs_size).expect("could not resize shared memory area");
        vals_shmem.set_size(vals_size).expect("could not resize shared memory area");
        Self::new(
            num_buckets,
			num_shards,
            keys_shmem.data_ptr.as_ptr().cast(),
			idxs_shmem.data_ptr.as_ptr().cast(),
			vals_shmem.data_ptr.as_ptr().cast(),
            Some(HashMapHandles { keys_shmem, idxs_shmem, vals_shmem }),
            rustc_hash::FxBuildHasher,
        )
    }

    /// Make a resizable hash map within a new shared memory area with the given name.
    pub fn new_resizeable_named(
		num_buckets: usize,
		max_buckets: usize,
		num_shards: usize,
		name: &str
	) -> Self {
		let (keys_size, idxs_size, vals_size) = Self::estimate_sizes(num_buckets, num_shards);
		let (keys_max, idxs_max, vals_max) = Self::estimate_sizes(max_buckets, num_shards);
        let keys_shmem = ShmemHandle::new(&format!("{name}_keys"), keys_size, keys_max)
			.expect("failed to make shared memory area");
		let idxs_shmem = ShmemHandle::new(&format!("{name}_idxs"), idxs_size, idxs_max)
			.expect("failed to make shared memory area");
		let vals_shmem = ShmemHandle::new(&format!("{name}_vals"), vals_size, vals_max)
			.expect("failed to make shared memory area");
        Self::new(
            num_buckets,
			num_shards,
            keys_shmem.data_ptr.as_ptr().cast(),
			idxs_shmem.data_ptr.as_ptr().cast(),
			vals_shmem.data_ptr.as_ptr().cast(),
            Some(HashMapHandles { keys_shmem, idxs_shmem, vals_shmem }),
            rustc_hash::FxBuildHasher,
        )
    }

    /// Make a resizable hash map within a new anonymous shared memory area.
    pub fn new_resizeable(
		num_buckets: usize,
		max_buckets: usize,
		num_shards: usize,
	) -> Self {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let val = COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!("neon_shmem_hmap{val}");
        Self::new_resizeable_named(num_buckets, max_buckets, num_shards, &name)
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

    /// Get a reference to the corresponding value for a key.
    pub fn get<'e>(&'e self, key: &K) -> Option<ValueReadGuard<'e, V>> {
        let hash = self.get_hash_value(key);
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();
		map.get_with_hash(key, hash)
    }

    /// Get a reference to the entry containing a key.
    pub fn entry(&self, key: K) -> Result<Entry<'a, K, V>, FullError> {
        let hash = self.get_hash_value(&key);
		let map = unsafe { self.shared_ptr.as_mut() }.unwrap();
        map.entry_with_hash(key, hash)
    }

    /// Remove a key given its hash. Returns the associated value if it existed.
    pub fn remove(&self, key: &K) -> Option<V> {
		let hash = self.get_hash_value(key);
		let map = unsafe { self.shared_ptr.as_mut() }.unwrap();
        match map.entry_with_hash(key.clone(), hash) {
            Ok(Entry::Occupied(mut e)) => Some(e.remove()),
            _ => None,
        }
    }

    /// Insert/update a key. Returns the previous associated value if it existed.
    ///
    /// # Errors
    /// Will return [`core::FullError`] if there is no more space left in the map.
    pub fn insert(&self, key: K, value: V) -> Result<Option<V>, core::FullError> {
        let hash = self.get_hash_value(&key);
		let map = unsafe { self.shared_ptr.as_mut() }.unwrap();
        match map.entry_with_hash(key.clone(), hash)? {
            Entry::Occupied(mut e) => Ok(Some(e.insert(value))),
            Entry::Vacant(e) => {
                _ = e.insert(value);
                Ok(None)
            }
        }
    }

    pub unsafe fn get_at_bucket(&self, pos: usize) -> Option<&V> {
        let map = unsafe { self.shared_ptr.as_mut() }.unwrap();
        if pos >= map.bucket_arr.buckets.len() {
            return None;
        }

		let bucket = &map.bucket_arr.buckets[pos];
		if bucket.next.load(Ordering::Relaxed) == BucketIdx::RESERVED {
			Some(unsafe { bucket.val.assume_init_ref() })
		} else {
			None
		}
    }

    /// bucket the number of buckets in the table.
    pub fn get_num_buckets(&self) -> usize {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();
        map.get_num_buckets()
    }

    /// Returns the index of the bucket a given value corresponds to.
    pub fn get_bucket_for_value(&self, val_ptr: *const V) -> usize {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();

        let origin = map.bucket_arr.buckets.as_ptr();
        let idx = (val_ptr as usize - origin as usize) / size_of::<Bucket<V>>();
        assert!(idx < map.bucket_arr.buckets.len());

        idx
    }

    /// Returns the number of occupied buckets in the table.
    pub fn get_num_buckets_in_use(&self) -> usize {
        let map = unsafe { self.shared_ptr.as_ref() }.unwrap();
        map.bucket_arr.buckets_in_use.load(Ordering::Relaxed)
    }

    /// Clears all entries in a table. Does not reset any shrinking operations.
    pub fn clear(&self) {
        let map = unsafe { self.shared_ptr.as_mut() }.unwrap();
        map.clear();
	}
	
    pub fn rehash(
		&self,
		shards: &mut Vec<RwLockWriteGuard<'_, DictShard<'_, K>>>,
		rehash_buckets: usize
	) {
        let map = unsafe { self.shared_ptr.as_mut() }.unwrap();
		assert!(rehash_buckets <= map.get_num_buckets(), "rehashing subset of buckets");
		shards.iter_mut().for_each(|x| x.keys.iter_mut().for_each(|key| {
			if let EntryType::Occupied = key.tag {
				key.tag = EntryType::Rehash;
			}
		}));
		
		todo!("solution with no memory allocation: split out metadata?")
    }

	pub fn shuffle(&self) {
        let map = unsafe { self.shared_ptr.as_mut() }.unwrap();
		let mut shards: Vec<_> = map.dict_shards.iter().map(|x| x.write()).collect();
		self.rehash(&mut shards, map.get_num_buckets());
    }
	
	fn reshard(&self, shards: &mut Vec<RwLockWriteGuard<'_, DictShard<'_, K>>>, num_buckets: usize) {
		let map = unsafe { self.shared_ptr.as_mut() }.unwrap();
		let shard_size = num_buckets / map.dict_shards.len();
		for i in 0..map.dict_shards.len() {
			let size = ((i + 1) * shard_size).min(num_buckets) - (i * shard_size);
			unsafe {
				shards[i].keys = std::slice::from_raw_parts_mut(shards[i].keys.as_mut_ptr(), size);
				shards[i].idxs = std::slice::from_raw_parts_mut(shards[i].idxs.as_mut_ptr(), size);
			}
		}
	}

	fn resize_shmem(&self, num_buckets: usize) -> Result<(), shmem::Error> {
		let map = unsafe { self.shared_ptr.as_mut() }.unwrap();
		let shmem_handles = self
            .shmem_handles
            .as_ref()
            .expect("grow called on a fixed-size hash table");

		let (keys_size, idxs_size, vals_size) =
			HashMapInit::<K, V, S>::estimate_sizes(num_buckets, map.dict_shards.len());
        shmem_handles.keys_shmem.set_size(keys_size)?;
		shmem_handles.idxs_shmem.set_size(idxs_size)?;
		shmem_handles.vals_shmem.set_size(vals_size)?;
		Ok(())
	}

    pub fn grow(&self, num_buckets: usize) -> Result<(), shmem::Error> {
        let map = unsafe { self.shared_ptr.as_mut() }.unwrap();
		let _resize_guard = self.resize_lock.lock();
		let mut shards: Vec<_> = map.dict_shards.iter().map(|x| x.write()).collect();

		let old_num_buckets = map.bucket_arr.buckets.len();
        assert!(
            num_buckets >= old_num_buckets,
            "grow called with a smaller number of buckets"
        );
        if num_buckets == old_num_buckets {
            return Ok(());
        }

		// Grow memory areas and initialize each of them.
		self.resize_shmem(num_buckets)?;                
        unsafe {
			let buckets_ptr = map.bucket_arr.buckets.as_mut_ptr();
            for i in old_num_buckets..num_buckets {
                let bucket = buckets_ptr.add(i);
                bucket.write(Bucket::empty(
                    if i < num_buckets - 1 {
                        BucketIdx::new(i + 1)
                    } else {
                        map.bucket_arr.free_head.load(Ordering::Relaxed)
                    }
                ));
            }

			// TODO(quantumish) a bit questionable to use pointers here
			let first_shard = &mut shards[0];
			let keys_ptr = first_shard.keys.as_mut_ptr();			
			for i in old_num_buckets..num_buckets {
                let key = keys_ptr.add(i);
                key.write(EntryKey {
					tag: EntryType::Empty,
					val: MaybeUninit::uninit(),
				});
            }
			
			let idxs_ptr = first_shard.idxs.as_mut_ptr();
			for i in old_num_buckets..num_buckets {
                let idx = idxs_ptr.add(i);
                idx.write(BucketIdx::INVALID);
            }
        }

		self.reshard(&mut shards, num_buckets);
        self.rehash(&mut shards, old_num_buckets);
        map.bucket_arr.free_head.store(
			BucketIdx::new(old_num_buckets), Ordering::Relaxed
		);
        Ok(())
    }

    pub fn begin_shrink(&mut self, num_buckets: usize) {
        let map = unsafe { self.shared_ptr.as_mut() }.unwrap();
		let _resize_guard = self.resize_lock.lock();
        assert!(
            num_buckets <= map.get_num_buckets(),
            "shrink called with a larger number of buckets"
        );
        _ = self
            .shmem_handles
            .as_ref()
            .expect("shrink called on a fixed-size hash table");
        map.bucket_arr.alloc_limit.store(
			BucketIdx::new(num_buckets), Ordering::SeqCst
		);
    }

	// TODO(quantumish): Safety? Maybe replace this with expanded version of finish_shrink?
    pub fn shrink_goal(&self) -> Option<usize> {
        let map = unsafe { self.shared_ptr.as_mut() }.unwrap();
        let goal = map.bucket_arr.alloc_limit.load(Ordering::Relaxed);
		goal.pos_checked()
	}

    pub fn finish_shrink(&self) -> Result<(), shmem::Error> {
        let map = unsafe { self.shared_ptr.as_mut() }.unwrap();
		let _resize_guard = self.resize_lock.lock();
		let mut shards: Vec<_> = map.dict_shards.iter().map(|x| x.write()).collect();
		
        let num_buckets = map.bucket_arr.alloc_limit
			.load(Ordering::Relaxed)
			.pos_checked()
			.expect("called finish_shrink when no shrink is in progress");
        
        if map.get_num_buckets() == num_buckets {
            return Ok(());
        }

        assert!(
            map.bucket_arr.buckets_in_use.load(Ordering::Relaxed) <= num_buckets,
            "called finish_shrink before enough entries were removed"
        );

		// let shard_size = shards[0].len();
        // for i in (num_buckets as usize)..map.buckets.len() {
		// 	let shard_start = num_buckets / shard_size;
		// 	let shard = shards[shard_start];
		// 	let entry_start = num_buckets % shard_size;
		// 	for entry_idx in entry_start..shard.len() {
				
		// 	}
			
		// 	if let EntryKey::Occupied(v) = map.[i].inner.take() {
        //         // alloc_bucket increases count, so need to decrease since we're just moving
        //         map.buckets_in_use.fetch_sub(1, Ordering::Relaxed);
        //         map.alloc_bucket(k, v).unwrap();
        //     }
        // }

		todo!("dry way to handle reinsertion");

		self.resize_shmem(num_buckets)?;

		self.reshard(&mut shards, num_buckets);
		
        self.rehash(&mut shards, num_buckets);
        map.bucket_arr.alloc_limit.store(BucketIdx::INVALID, Ordering::Relaxed);

        Ok(())
    }
}
