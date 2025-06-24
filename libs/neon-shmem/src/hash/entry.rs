//! Like std::collections::hash_map::Entry;

use crate::hash::core::{CoreHashMap, FullError, INVALID_POS};

use std::hash::Hash;
use std::mem;

/// View into an entry in the map (either vacant or occupied).
pub enum Entry<'a, 'b, K, V> {
	Occupied(OccupiedEntry<'a, 'b, K, V>),
    Vacant(VacantEntry<'a, 'b, K, V>),
}

/// Enum representing the previous position within a chain.
#[derive(Clone, Copy)]
pub(crate) enum PrevPos {
	/// Starting index within the dictionary.  
    First(u32),
	/// Regular index within the buckets.
    Chained(u32),
	/// Unknown - e.g. the associated entry was retrieved by index instead of chain.
	Unknown,
}

/// View into an occupied entry within the map.
pub struct OccupiedEntry<'a, 'b, K, V> {
	/// Mutable reference to the map containing this entry.
	pub(crate) map: &'b mut CoreHashMap<'a, K, V>,
	/// The key of the occupied entry
    pub(crate) _key: K,
	/// The index of the previous entry in the chain.
    pub(crate) prev_pos: PrevPos,
	/// The position of the bucket in the CoreHashMap's buckets array.
    pub(crate) bucket_pos: u32, 
}

impl<'a, 'b, K, V> OccupiedEntry<'a, 'b, K, V> {
    pub fn get(&self) -> &V {
        &self.map.buckets[self.bucket_pos as usize]
            .inner
            .as_ref()
            .unwrap()
            .1
    }

    pub fn get_mut(&mut self) -> &mut V {
        &mut self.map.buckets[self.bucket_pos as usize]
            .inner
            .as_mut()
            .unwrap()
            .1
    }

	/// Inserts a value into the entry, replacing (and returning) the existing value.
    pub fn insert(&mut self, value: V) -> V {
        let bucket = &mut self.map.buckets[self.bucket_pos as usize];
        // This assumes inner is Some, which it must be for an OccupiedEntry
        let old_value = mem::replace(&mut bucket.inner.as_mut().unwrap().1, value);
        old_value
    }

	/// Removes the entry from the hash map, returning the value originally stored within it.
    pub fn remove(self) -> V {
        // CoreHashMap::remove returns Option<(K, V)>. We know it's Some for an OccupiedEntry.
        let bucket = &mut self.map.buckets[self.bucket_pos as usize];

        // unlink it from the chain
        match self.prev_pos {
            PrevPos::First(dict_pos) => self.map.dictionary[dict_pos as usize] = bucket.next,
            PrevPos::Chained(bucket_pos) => {
                self.map.buckets[bucket_pos as usize].next = bucket.next
            },
			PrevPos::Unknown => panic!("can't safely remove entry with unknown previous entry"),
        }

        // and add it to the freelist        
        let bucket = &mut self.map.buckets[self.bucket_pos as usize];
        let old_value = bucket.inner.take();
		bucket.next = self.map.free_head;
        self.map.free_head = self.bucket_pos;
        self.map.buckets_in_use -= 1;

        return old_value.unwrap().1;
    }
}

/// An abstract view into a vacant entry within the map.
pub struct VacantEntry<'a, 'b, K, V> {
	/// Mutable reference to the map containing this entry.
    pub(crate) map: &'b mut CoreHashMap<'a, K, V>,
	/// The key to be inserted into this entry.
    pub(crate) key: K,
	/// The position within the dictionary corresponding to the key's hash.
    pub(crate) dict_pos: u32,
}

impl<'a, 'b, K: Clone + Hash + Eq, V> VacantEntry<'a, 'b, K, V> {
	/// Insert a value into the vacant entry, finding and populating an empty bucket in the process.
    pub fn insert(self, value: V) -> Result<&'b mut V, FullError> {
        let pos = self.map.alloc_bucket(self.key, value)?;
        if pos == INVALID_POS {
            return Err(FullError());
        }
        let bucket = &mut self.map.buckets[pos as usize];
        bucket.next = self.map.dictionary[self.dict_pos as usize];
        self.map.dictionary[self.dict_pos as usize] = pos;

        let result = &mut self.map.buckets[pos as usize].inner.as_mut().unwrap().1;
        return Ok(result);
    }
}
