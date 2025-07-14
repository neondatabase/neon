//! Simple hash table with chaining.

use std::hash::Hash;
use std::mem::MaybeUninit;
use std::sync::atomic::{Ordering, AtomicUsize};

use crate::sync::*;
use crate::hash::{
	entry::*,
	bucket::{BucketArray, Bucket, BucketIdx}
};

#[derive(PartialEq, Eq, Clone, Copy)]
pub(crate) enum EntryType {
	Occupied,
	Rehash,
	Tombstone,
	RehashTombstone,
	Empty,
}

pub(crate) struct EntryKey<K> {
	pub(crate) tag: EntryType,
	pub(crate) val: MaybeUninit<K>,
}

pub(crate) struct DictShard<'a, K> {
	pub(crate) keys: &'a mut [EntryKey<K>],
	pub(crate) idxs: &'a mut [BucketIdx],
}

impl<'a, K> DictShard<'a, K> {
	fn len(&self) -> usize {
		self.keys.len()
	}
}

pub(crate) struct MaybeUninitDictShard<'a, K> {
	pub(crate) keys: &'a mut [MaybeUninit<EntryKey<K>>],
	pub(crate) idxs: &'a mut [MaybeUninit<BucketIdx>],
}

/// Core hash table implementation.
pub(crate) struct CoreHashMap<'a, K, V> {
	/// Dictionary used to map hashes to bucket indices.
    pub(crate) dict_shards: &'a mut [RwLock<DictShard<'a, K>>],
	pub(crate) bucket_arr: BucketArray<'a, V>,
	pub(crate) rehash_index: AtomicUsize,	
	pub(crate) rehash_end: AtomicUsize,
}

/// Error for when there are no empty buckets left but one is needed.
#[derive(Debug, PartialEq)]
pub struct FullError();

impl<'a, K: Clone + Hash + Eq, V> CoreHashMap<'a, K, V> {
    pub fn new(
        buckets: &'a mut [MaybeUninit<Bucket<V>>],
        dict_shards: &'a mut [RwLock<MaybeUninitDictShard<'a, K>>],
    ) -> Self {
        // Initialize the buckets
		for i in 0..buckets.len() {
			buckets[i].write(Bucket::empty(
				if i < buckets.len() - 1 {
					BucketIdx::new(i + 1)
				} else {
					BucketIdx::INVALID
				})
			);
        }

        // Initialize the dictionary
		for shard in dict_shards.iter_mut() {
			let mut dicts = shard.write();
			for e in dicts.keys.iter_mut() {
				e.write(EntryKey {
					tag: EntryType::Empty,
					val: MaybeUninit::uninit(),
				});
			}
			for e in dicts.idxs.iter_mut() {
				e.write(BucketIdx::INVALID);
			}
		}

        // TODO: use std::slice::assume_init_mut() once it stabilizes
        let buckets =
            unsafe { std::slice::from_raw_parts_mut(buckets.as_mut_ptr().cast(),
													buckets.len()) };
        let dict_shards = unsafe {
            std::slice::from_raw_parts_mut(dict_shards.as_mut_ptr().cast(),
										   dict_shards.len())
        };

        Self {
            dict_shards,
			rehash_index: buckets.len().into(),
			rehash_end: buckets.len().into(),
			bucket_arr: BucketArray::new(buckets),
        }
    }

	// TODO(quantumish): off by one for return value logic?
	pub	fn do_rehash(&mut self) -> bool {
		// TODO(quantumish): refactor these out into settable quantities
		const REHASH_CHUNK_SIZE: usize = 10;
		const REHASH_ATTEMPTS: usize = 5;

		let end = self.rehash_end.load(Ordering::Relaxed);
		let mut ind = self.rehash_index.load(Ordering::Relaxed);
		let mut i = 0;
		loop {
			if ind >= end {
				// TODO(quantumish) questionable?
				self.rehash_index.store(end, Ordering::Relaxed);
				return true;
			}
			if i > REHASH_ATTEMPTS {
				break;
			}
			match self.rehash_index.compare_exchange_weak(
				ind, ind + REHASH_CHUNK_SIZE,
				Ordering::Relaxed, Ordering::Relaxed
			) {
				Err(new_ind) => ind = new_ind,
				Ok(_) => break,
			}
			i += 1;
		}

		todo!("actual rehashing");		
		false
	}

	pub fn get_with_hash(&'a self, key: &K, hash: u64) -> Option<ValueReadGuard<'a, V>> {
		let ind = self.rehash_index.load(Ordering::Relaxed);
		let end = self.rehash_end.load(Ordering::Relaxed);

		let first = ind >= end || ind < end/2;
		if let Some(res) = self.get(key, hash, first) { 
			return Some(res);
		}
		if ind < end && let Some(res) = self.get(key, hash, !first) {
			return Some(res);
		}
		None
	}

	pub fn entry_with_hash(&'a mut self, key: K, hash: u64) -> Result<Entry<'a, K, V>, FullError> {
		let ind = self.rehash_index.load(Ordering::Relaxed);
		let end = self.rehash_end.load(Ordering::Relaxed);

		if ind < end {
			if let Ok(Entry::Occupied(res)) = self.entry(key.clone(), hash, true) {
				return Ok(Entry::Occupied(res));
			} else {
				return self.entry(key, hash, false);
			}
		} else {
			return self.entry(key.clone(), hash, true);
		}
	}
	
    /// Get the value associated with a key (if it exists) given its hash.
    fn get(&'a self, key: &K, hash: u64, ignore_remap: bool) -> Option<ValueReadGuard<'a, V>> {	
		let num_buckets = self.get_num_buckets();
		let shard_size = num_buckets / self.dict_shards.len();
		let bucket_pos = hash as usize % num_buckets;
		let shard_start = bucket_pos / shard_size;
		for off in 0..self.dict_shards.len() {
			let shard_idx = (shard_start + off) % self.dict_shards.len();
			let shard = self.dict_shards[shard_idx].read();
			let entry_start = if off == 0 { bucket_pos % shard_size } else { 0 };
			for entry_idx in entry_start..shard.len() {
				match shard.keys[entry_idx].tag {
					EntryType::Empty => return None,
					EntryType::Tombstone | EntryType::RehashTombstone => continue, 
					t @ (EntryType::Occupied | EntryType::Rehash) => {
						if (t == EntryType::Occupied && ignore_remap) || (t == EntryType::Rehash && !ignore_remap) {
							let cand_key = unsafe { shard.keys[entry_idx].val.assume_init_ref() };
							if cand_key == key {
								let bucket_idx = shard.idxs[entry_idx].pos_checked()
									.expect("position is valid");
								return Some(RwLockReadGuard::map(
									shard, |_| self.bucket_arr.buckets[bucket_idx].as_ref()
								));
							}
						} 
					},
				}
			}
		}
		None
	}

    fn entry(&'a mut self, key: K, hash: u64, ignore_remap: bool) -> Result<Entry<'a, K, V>, FullError> {
		// We need to keep holding on the locks for each shard we process since if we don't find the
		// key anywhere, we want to insert it at the earliest possible position (which may be several
		// shards away). Ideally cross-shard chains are quite rare, so this shouldn't be a big deal.
		let mut shards = Vec::new();
		let mut insert_pos = None;
		let mut insert_shard = None;

		let num_buckets = self.get_num_buckets();
		let shard_size = num_buckets / self.dict_shards.len();
		let bucket_pos = hash as usize % num_buckets;
		let shard_start = bucket_pos / shard_size;
		for off in 0..self.dict_shards.len() {
			let shard_idx = (shard_start + off) % self.dict_shards.len();			
			let shard = self.dict_shards[shard_idx].write();
			let mut inserted = false;
			let entry_start = if off == 0 { bucket_pos % shard_size } else { 0 };
			for entry_idx in entry_start..shard.len() {
				match shard.keys[entry_idx].tag {
					EntryType::Empty => {
						let (shard, shard_pos) = match (insert_shard, insert_pos) {
							(Some(s), Some(p)) => (s, p),
							(None, Some(p)) => (shard, p),
							(None, None) => (shard, entry_idx),
							_ => unreachable!()
						};
						return Ok(Entry::Vacant(VacantEntry {
							_key: key,
							shard,
							shard_pos,
							bucket_arr: &mut self.bucket_arr,
						}))
					},
					EntryType::Tombstone | EntryType::RehashTombstone => {
						if insert_pos.is_none() {
							insert_pos = Some(entry_idx);
							inserted = true;
						}
					},
					t @ (EntryType::Occupied | EntryType::Rehash) => {
						if (t == EntryType::Occupied && ignore_remap) || (t == EntryType::Rehash && !ignore_remap) {
							let cand_key = unsafe { shard.keys[entry_idx].val.assume_init_ref() };
							if *cand_key == key {
								let bucket_pos = shard.idxs[entry_idx].pos_checked().unwrap();
								return Ok(Entry::Occupied(OccupiedEntry {
									_key: key,
									shard,
									shard_pos: entry_idx,
									bucket_pos,
									bucket_arr: &mut self.bucket_arr,
								}));
							}
						}	
					}
				} 
			}
			if inserted {
				insert_shard = Some(shard)
			} else {
				shards.push(shard);
			}
		}
		
		if let (Some(shard), Some(shard_pos)) = (insert_shard, insert_pos) {
			Ok(Entry::Vacant(VacantEntry {
				_key: key,
				shard,
				shard_pos,
				bucket_arr: &mut self.bucket_arr,
			}))
		} else {
			Err(FullError{})
		}
	}
	
    /// Get number of buckets in map.
    pub fn get_num_buckets(&self) -> usize {
        self.bucket_arr.buckets.len()
    }

    pub fn clear(&mut self) {
		let mut shards: Vec<_> = self.dict_shards.iter().map(|x| x.write()).collect();
        for shard in shards.iter_mut() {
			for e in shard.keys.iter_mut() {
				e.tag = EntryType::Empty;
			}
			for e in shard.idxs.iter_mut() {
				*e = BucketIdx::INVALID;
			}
		}

        self.bucket_arr.clear();
    }
}
 
