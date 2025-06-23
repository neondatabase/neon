//! Like std::collections::hash_map::Entry;

use crate::hash::core::{CoreHashMap, FullError, INVALID_POS};

use std::hash::Hash;
use std::mem;

pub enum Entry<'a, 'b, K, V> {
    Occupied(OccupiedEntry<'a, 'b, K, V>),
    Vacant(VacantEntry<'a, 'b, K, V>),
}

#[derive(Clone, Copy)]
pub(crate) enum PrevPos {
    First(u32),
    Chained(u32),
}

pub struct OccupiedEntry<'a, 'b, K, V> {
    pub(crate) map: &'b mut CoreHashMap<'a, K, V>,
    pub(crate) _key: K, // The key of the occupied entry
    pub(crate) prev_pos: PrevPos,
    pub(crate) bucket_pos: u32, // The position of the bucket in the CoreHashMap's buckets array
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
            }
        }

        // and add it to the freelist        
		if self.map.free_head != INVALID_POS {
			self.map.prevs[self.map.free_head as usize] = PrevPos::Chained(self.bucket_pos);
		}
        let keylink = &mut self.map.keys[self.bucket_pos as usize];
        keylink.inner = None;
		keylink.next = self.map.free_head;
		self.map.prevs[self.bucket_pos as usize] = PrevPos::First(INVALID_POS);
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
        let pos = self.map.alloc_bucket(self.key, value, self.dict_pos)?;
        if pos == INVALID_POS {
            return Err(FullError());
        }
        let prev = &mut self.map.prevs[pos as usize];
		if let PrevPos::First(INVALID_POS) = prev {
			*prev = PrevPos::First(self.dict_pos);
		}
        self.map.keys[pos as usize].next = self.map.dictionary[self.dict_pos as usize];
        self.map.dictionary[self.dict_pos as usize] = pos;

        let result = self.map.vals[pos as usize].as_mut().unwrap();
        return Ok(result);
    }
}
