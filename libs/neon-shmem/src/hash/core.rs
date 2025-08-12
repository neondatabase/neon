//! Simple hash table with chaining.

use std::cell::UnsafeCell;
use std::hash::Hash;
use std::mem::MaybeUninit;
use std::sync::atomic::{Ordering, AtomicUsize};

use crate::sync::*;
use crate::hash::{
	entry::*,
	bucket::{BucketArray, Bucket, BucketIdx}
};

#[derive(PartialEq, Eq, Clone, Copy)]
pub(crate) enum EntryTag {
	Occupied,
	Rehash,
	Tombstone,
	RehashTombstone,
	Empty,
}

pub(crate) enum MapEntryType {
	Occupied,
	Tombstone,
	Empty,
	Skip
}

pub(crate) struct EntryKey<K> {
	pub(crate) tag: EntryTag,
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
        buckets_cell: &'a UnsafeCell<[MaybeUninit<Bucket<V>>]>,
        dict_shards: &'a mut [RwLock<MaybeUninitDictShard<'a, K>>],
    ) -> Self {
		let buckets = unsafe { &mut *buckets_cell.get() };
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
					tag: EntryTag::Empty,
					val: MaybeUninit::uninit(),
				});
			}
			for e in dicts.idxs.iter_mut() {
				e.write(BucketIdx::INVALID);
			}
		}

        let buckets_cell = unsafe {
			&*(buckets_cell as *const _ as *const UnsafeCell<_>)
		};
        // TODO: use std::slice::assume_init_mut() once it stabilizes
        let dict_shards = unsafe {
            std::slice::from_raw_parts_mut(dict_shards.as_mut_ptr().cast(),
										   dict_shards.len())
        };

        Self {
            dict_shards,
			rehash_index: buckets.len().into(),
			rehash_end: buckets.len().into(),
			bucket_arr: BucketArray::new(buckets_cell),
        }
    }
	
	pub fn get_with_hash(&'a self, key: &K, hash: u64) -> Option<ValueReadGuard<'a, V>> {
		let ind = self.rehash_index.load(Ordering::Relaxed);
		let end = self.rehash_end.load(Ordering::Relaxed);

		let res = self.get(key, hash, |tag| match tag {
			EntryTag::Empty => MapEntryType::Empty,
			EntryTag::Occupied => MapEntryType::Occupied,
			_ => MapEntryType::Tombstone,
		});
		if res.is_some() {
			return res;
		}
		
		if ind < end {
			self.get(key, hash, |tag| match tag {
				EntryTag::Empty => MapEntryType::Empty,
				EntryTag::Rehash => MapEntryType::Occupied,
				_ => MapEntryType::Tombstone,
			})
		} else { 
			None
		}
	}
	
	pub fn entry_with_hash(&'a mut self, key: K, hash: u64) -> Result<Entry<'a, K, V>, FullError> {
		let ind = self.rehash_index.load(Ordering::Relaxed);
		let end = self.rehash_end.load(Ordering::Relaxed);

		let res = self.entry(key.clone(), hash, |tag| match tag {
			EntryTag::Empty => MapEntryType::Empty,
			EntryTag::Occupied => MapEntryType::Occupied,
			EntryTag::Rehash => MapEntryType::Skip,
			_ => MapEntryType::Tombstone,
		});
		if ind < end {
			if let Ok(Entry::Occupied(_)) = res {
				res
			} else {
				self.entry(key, hash, |tag| match tag {
					EntryTag::Empty => MapEntryType::Empty,
					EntryTag::Occupied => MapEntryType::Skip,
					EntryTag::Rehash => MapEntryType::Occupied,
					_ => MapEntryType::Tombstone
				})
			}
		} else {
			res
		}
	}
	
    /// Get the value associated with a key (if it exists) given its hash.
    fn get<F>(&'a self, key: &K, hash: u64, f: F) -> Option<ValueReadGuard<'a, V>>
	    where F: Fn(EntryTag) -> MapEntryType
	{	
		let num_buckets = self.get_num_buckets();
		let shard_size = num_buckets / self.dict_shards.len();
		let bucket_pos = hash as usize % num_buckets;
		let shard_start = bucket_pos / shard_size;
		for off in 0..self.dict_shards.len() {
			let shard_idx = (shard_start + off) % self.dict_shards.len();
			let shard = self.dict_shards[shard_idx].read();
			let entry_start = if off == 0 { bucket_pos % shard_size } else { 0 };
			for entry_idx in entry_start..shard.len() {
				match f(shard.keys[entry_idx].tag) {
					MapEntryType::Empty => return None,
					MapEntryType::Tombstone | MapEntryType::Skip => continue, 
					MapEntryType::Occupied => {
						let cand_key = unsafe { shard.keys[entry_idx].val.assume_init_ref() };
						if cand_key == key {
							let bucket_idx = shard.idxs[entry_idx].next_checked()
								.expect("position is valid");
							return Some(RwLockReadGuard::map(
								shard, |_| self.bucket_arr[bucket_idx].as_ref()
							));
						} 
					},
				}
			}
		}
		None
	}

    pub fn entry<F>(&'a self, key: K, hash: u64, f: F) -> Result<Entry<'a, K, V>, FullError>
	    where F: Fn(EntryTag) -> MapEntryType
	{
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
				match f(shard.keys[entry_idx].tag) {
					MapEntryType::Skip => continue,
					MapEntryType::Empty => {
						let ((shard, idx), shard_pos) = match (insert_shard, insert_pos) {
							(Some((s, i)), Some(p)) => ((s, i), p),
							(None, Some(p)) => ((shard, shard_idx), p),
							(None, None) => ((shard, shard_idx), entry_idx),
							_ => unreachable!()
						};
						return Ok(Entry::Vacant(VacantEntry {
							_key: key,
							shard,
							shard_pos,
							key_pos: (shard_size * idx) + shard_pos,
							bucket_arr: &self.bucket_arr,
						}))
					},
					MapEntryType::Tombstone => {
						if insert_pos.is_none() {
							insert_pos = Some(entry_idx);
							inserted = true;
						}
					},
					MapEntryType::Occupied => {
						let cand_key = unsafe { shard.keys[entry_idx].val.assume_init_ref() };
						if *cand_key == key {
							let bucket_pos = shard.idxs[entry_idx].next_checked().unwrap();
							return Ok(Entry::Occupied(OccupiedEntry {
								shard,
								shard_pos: entry_idx,
								bucket_pos,
								bucket_arr: &self.bucket_arr,
							}));
						}	
					}
				} 
			}
			if inserted {
				insert_shard = Some((shard, shard_idx));
			} else {
				shards.push(shard);
			}
		}
		
		if let (Some((shard, idx)), Some(shard_pos)) = (insert_shard, insert_pos) {
			Ok(Entry::Vacant(VacantEntry {
				_key: key,
				shard,
				shard_pos,
				key_pos: (shard_size * idx) + shard_pos,
				bucket_arr: &self.bucket_arr,
			}))
		} else {
			Err(FullError{})
		}
	}
	
    /// Get number of buckets in map.
    pub fn get_num_buckets(&self) -> usize {
        self.bucket_arr.len()
    }

    pub fn clear(&mut self) {
		let mut shards: Vec<_> = self.dict_shards.iter().map(|x| x.write()).collect();
        for shard in shards.iter_mut() {
			for e in shard.keys.iter_mut() {
				e.tag = EntryTag::Empty;
			}
			for e in shard.idxs.iter_mut() {
				*e = BucketIdx::INVALID;
			}
		}

        self.bucket_arr.clear();
    }
}
 
