//! Equivalent of [`std::collections::hash_map::Entry`] for this hashmap.

use crate::hash::core::{CoreHashMap, FullError, INVALID_POS};
use crate::sync::{RwLockWriteGuard, ValueWriteGuard};

use std::hash::Hash;
use std::mem;

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
    Unknown(u64),
}

pub struct OccupiedEntry<'a, 'b, K, V> {
    /// Mutable reference to the map containing this entry.
    pub(crate) map: RwLockWriteGuard<'b, CoreHashMap<'a, K, V>>,
    /// The key of the occupied entry
    pub(crate) _key: K,
    /// The index of the previous entry in the chain.
    pub(crate) prev_pos: PrevPos,
    /// The position of the bucket in the [`CoreHashMap`] bucket array.
    pub(crate) bucket_pos: u32,
}

impl<K, V> OccupiedEntry<'_, '_, K, V> {
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
        mem::replace(&mut bucket.inner.as_mut().unwrap().1, value)
    }

    /// Removes the entry from the hash map, returning the value originally stored within it.
    ///
    /// This may result in multiple bucket accesses if the entry was obtained by index as the
    /// previous chain entry needs to be discovered in this case.
    pub fn remove(mut self) -> V {
        // If this bucket was queried by index, go ahead and follow its chain from the start.
        let prev = if let PrevPos::Unknown(hash) = self.prev_pos {
            let dict_idx = hash as usize % self.map.dictionary.len();
            let mut prev = PrevPos::First(dict_idx as u32);
            let mut curr = self.map.dictionary[dict_idx];
            while curr != self.bucket_pos {
                assert!(curr != INVALID_POS);
                prev = PrevPos::Chained(curr);
                curr = self.map.buckets[curr as usize].next;
            }
            prev
        } else {
            self.prev_pos
        };

        // CoreHashMap::remove returns Option<(K, V)>. We know it's Some for an OccupiedEntry.
        let bucket = &mut self.map.buckets[self.bucket_pos as usize];

        // unlink it from the chain
        match prev {
            PrevPos::First(dict_pos) => {
                self.map.dictionary[dict_pos as usize] = bucket.next;
            }
            PrevPos::Chained(bucket_pos) => {
                self.map.buckets[bucket_pos as usize].next = bucket.next;
            }
            _ => unreachable!(),
        }

        // and add it to the freelist
        let free = self.map.free_head;
        let bucket = &mut self.map.buckets[self.bucket_pos as usize];
        let old_value = bucket.inner.take();
        bucket.next = free;
        self.map.free_head = self.bucket_pos;
        self.map.buckets_in_use -= 1;

        old_value.unwrap().1
    }
}

/// An abstract view into a vacant entry within the map.
pub struct VacantEntry<'a, 'b, K, V> {
    /// Mutable reference to the map containing this entry.
    pub(crate) map: RwLockWriteGuard<'b, CoreHashMap<'a, K, V>>,
    /// The key to be inserted into this entry.
    pub(crate) key: K,
    /// The position within the dictionary corresponding to the key's hash.
    pub(crate) dict_pos: u32,
}

impl<'b, K: Clone + Hash + Eq, V> VacantEntry<'_, 'b, K, V> {
    /// Insert a value into the vacant entry, finding and populating an empty bucket in the process.
    ///
    /// # Errors
    /// Will return [`FullError`] if there are no unoccupied buckets in the map.
    pub fn insert(mut self, value: V) -> Result<ValueWriteGuard<'b, V>, FullError> {
        let pos = self.map.alloc_bucket(self.key, value)?;
        self.map.buckets[pos as usize].next = self.map.dictionary[self.dict_pos as usize];
        self.map.dictionary[self.dict_pos as usize] = pos;

        Ok(RwLockWriteGuard::map(self.map, |m| {
            &mut m.buckets[pos as usize].inner.as_mut().unwrap().1
        }))
    }
}
