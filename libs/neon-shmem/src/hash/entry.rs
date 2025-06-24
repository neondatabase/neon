//! Like std::collections::hash_map::Entry;

use crate::hash::core::{CoreHashMap, FullError, INVALID_POS};

use std::hash::Hash;
use std::mem;

pub enum Entry<'a, 'b, K, V> {
    Occupied(OccupiedEntry<'a, 'b, K, V>),
    Vacant(VacantEntry<'a, 'b, K, V>),
}

/// Helper enum representing the previous position within a hashmap chain.
#[derive(Clone, Copy)]
pub(crate) enum PrevPos {
	/// Starting index within the dictionary.  
    First(u32),
	/// Regular index within the buckets.
    Chained(u32),
	/// Unknown - e.g. the associated entry was retrieved by index instead of chain.
	Unknown,
}

impl PrevPos {
	/// Unwrap an index from a `PrevPos::First`, panicking otherwise.
	pub fn unwrap_first(&self) -> u32 {
		match self {
			Self::First(i) => *i,
			_ => panic!("not first entry in chain")
		}
	}
}

pub struct OccupiedEntry<'a, 'b, K, V> {
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
        self.map.vals[self.bucket_pos as usize]
            .as_ref()
            .unwrap()
    }

    pub fn get_mut(&mut self) -> &mut V {
        self.map.vals[self.bucket_pos as usize]
            .as_mut()
            .unwrap()
    }

    pub fn insert(&mut self, value: V) -> V {
        let bucket = &mut self.map.vals[self.bucket_pos as usize];
        // This assumes inner is Some, which it must be for an OccupiedEntry
        let old_value = mem::replace(bucket.as_mut().unwrap(), value);
        old_value
    }

    pub fn remove(self) -> V {
        // CoreHashMap::remove returns Option<(K, V)>. We know it's Some for an OccupiedEntry.
        let keylink = &mut self.map.keys[self.bucket_pos as usize];

        // unlink it from the chain
        match self.prev_pos {
            PrevPos::First(dict_pos) => self.map.dictionary[dict_pos as usize] = keylink.next,
            PrevPos::Chained(bucket_pos) => {
                self.map.keys[bucket_pos as usize].next = keylink.next
            },
			PrevPos::Unknown => panic!("can't safely remove entry with unknown previous entry"),
        }

        // and add it to the freelist        
        let keylink = &mut self.map.keys[self.bucket_pos as usize];
        keylink.inner = None;
		keylink.next = self.map.free_head;
		let old_value = self.map.vals[self.bucket_pos as usize].take();
        self.map.free_head = self.bucket_pos;
        self.map.buckets_in_use -= 1;

        return old_value.unwrap();
    }
}

pub struct VacantEntry<'a, 'b, K, V> {
    pub(crate) map: &'b mut CoreHashMap<'a, K, V>,
    pub(crate) key: K, // The key to insert
    pub(crate) dict_pos: u32,
}

impl<'a, 'b, K: Clone + Hash + Eq, V> VacantEntry<'a, 'b, K, V> {
    pub fn insert(self, value: V) -> Result<&'b mut V, FullError> {
        let pos = self.map.alloc_bucket(self.key, value)?;
        if pos == INVALID_POS {
            return Err(FullError());
        }
        self.map.keys[pos as usize].next = self.map.dictionary[self.dict_pos as usize];
        self.map.dictionary[self.dict_pos as usize] = pos;

        let result = self.map.vals[pos as usize].as_mut().unwrap();
        return Ok(result);
    }
}
