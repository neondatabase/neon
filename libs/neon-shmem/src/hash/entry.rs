//! Equivalent of [`std::collections::hash_map::Entry`] for this hashmap.

use crate::hash::{
	core::{DictShard, EntryTag},
	bucket::{BucketArray, BucketIdx}
};
use crate::sync::{RwLockWriteGuard, ValueWriteGuard};

use std::hash::Hash;

pub enum Entry<'a, K, V> {
    Occupied(OccupiedEntry<'a, K, V>),
    Vacant(VacantEntry<'a, K, V>),
}

pub struct OccupiedEntry<'a, K, V> {
    /// Mutable reference to the shard of the map the entry is in.
    pub(crate) shard: RwLockWriteGuard<'a, DictShard<'a, K>>,
	/// The position of the entry in the shard.
    pub(crate) shard_pos: usize,
	/// True logical position of the entry in the map.
	pub(crate) key_pos: usize,
	/// Mutable reference to the bucket array containing entry.
	pub(crate) bucket_arr: &'a BucketArray<'a, V>,
    /// The position of the bucket in the [`CoreHashMap`] bucket array.
    pub(crate) bucket_pos: usize,
}

impl<K, V> OccupiedEntry<'_, K, V> {
    pub fn get(&self) -> &V {
		self.bucket_arr[self.bucket_pos].as_ref()
    }

    pub fn get_mut(&mut self) -> &mut V {
		self.bucket_arr.get_mut(self.bucket_pos).as_mut()
    }

    /// Inserts a value into the entry, replacing (and returning) the existing value.
    pub fn insert(&mut self, value: V) -> V {
        self.bucket_arr.get_mut(self.bucket_pos).replace(value)
    }

    /// Removes the entry from the hash map, returning the value originally stored within it.
    pub fn remove(&mut self) -> V {
		self.shard.idxs[self.shard_pos] = BucketIdx::INVALID;
		self.shard.keys[self.shard_pos].tag = EntryTag::Tombstone;
        self.bucket_arr.dealloc_bucket(self.bucket_pos)
    }
}

/// An abstract view into a vacant entry within the map.
pub struct VacantEntry<'a, K, V> {
    /// The key of the occupied entry
    pub(crate) _key: K,
    /// Mutable reference to the shard of the map the entry is in.
    pub(crate) shard: RwLockWriteGuard<'a, DictShard<'a, K>>,
	/// The position of the entry in the shard.
    pub(crate) shard_pos: usize,
	/// True logical position of the entry in the map.
	pub(crate) key_pos: usize,
	/// Mutable reference to the bucket array containing entry.
	pub(crate) bucket_arr: &'a BucketArray<'a, V>,
}

impl<'a, K: Clone + Hash + Eq, V> VacantEntry<'a, K, V> {
    /// Insert a value into the vacant entry, finding and populating an empty bucket in the process.
    pub fn insert(mut self, value: V) -> ValueWriteGuard<'a, V> {
		let pos = self.bucket_arr.alloc_bucket(value, self.key_pos)
			.expect("bucket is available if entry is");
		self.shard.keys[self.shard_pos].tag = EntryTag::Occupied;
		self.shard.keys[self.shard_pos].val.write(self._key);
		let idx = pos.next_checked().expect("position is valid");
		self.shard.idxs[self.shard_pos] = pos;

        RwLockWriteGuard::map(self.shard, |_| {
            self.bucket_arr.get_mut(idx).as_mut()
        })
    }
}
	

