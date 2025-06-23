//! Simple hash table with chaining
//!
//! # Resizing
//!

use std::hash::Hash;
use std::mem::MaybeUninit;

use crate::hash::entry::{Entry, OccupiedEntry, PrevPos, VacantEntry};

pub(crate) const INVALID_POS: u32 = u32::MAX;

// Bucket
// pub(crate) struct Bucket<K, V> {
//     pub(crate) next: u32,
// 	pub(crate) prev: PrevPos,
//     pub(crate) inner: Option<(K, V)>,
// }

pub(crate) struct LinkedKey<K> {
	pub(crate) inner: Option<K>,
	pub(crate) next: u32,	
}

pub(crate) struct CoreHashMap<'a, K, V> {
    pub(crate) dictionary: &'a mut [u32],
    pub(crate) keys: &'a mut [LinkedKey<K>],
	pub(crate) vals: &'a mut [Option<V>],
	pub(crate) prevs: &'a mut [PrevPos],
    pub(crate) free_head: u32,

    pub(crate) _user_list_head: u32,
	/// Maximum index of a bucket allowed to be allocated. INVALID_POS if no limit.
	pub(crate) alloc_limit: u32,

    // metrics
    pub(crate) buckets_in_use: u32,
}

#[derive(Debug)]
pub struct FullError();

impl<'a, K: Hash + Eq, V> CoreHashMap<'a, K, V>
where
    K: Clone + Hash + Eq,
{
    const FILL_FACTOR: f32 = 0.60;

    pub fn estimate_size(num_buckets: u32) -> usize {
        let mut size = 0;

        // buckets
        size += (size_of::<LinkedKey<K>>() + size_of::<Option<V>>() + size_of::<PrevPos>())
			* num_buckets as usize;

        // dictionary
        size += (f32::ceil((size_of::<u32>() * num_buckets as usize) as f32 / Self::FILL_FACTOR))
            as usize;

        size
    }	

    pub fn new(
        keys: &'a mut [MaybeUninit<LinkedKey<K>>],
		vals: &'a mut [MaybeUninit<Option<V>>],
		prevs: &'a mut [MaybeUninit<PrevPos>],
        dictionary: &'a mut [MaybeUninit<u32>],
    ) -> CoreHashMap<'a, K, V> {
        // Initialize the buckets
        for i in 0..keys.len() {
            keys[i].write(LinkedKey {
				next: if i < keys.len() - 1 {
                    i as u32 + 1
                } else {
                    INVALID_POS
                },
				inner: None,
			});
		}
		for i in 0..vals.len() {
            vals[i].write(None);
		}
		for i in 0..prevs.len() {
			prevs[i].write(if i > 0 {
				PrevPos::Chained(i as u32 - 1)
			} else {
				PrevPos::First(INVALID_POS)
			});
		}
			
        // Initialize the dictionary
        for i in 0..dictionary.len() {
            dictionary[i].write(INVALID_POS);
        }

        // TODO: use std::slice::assume_init_mut() once it stabilizes
        let keys =
            unsafe { std::slice::from_raw_parts_mut(keys.as_mut_ptr().cast(), keys.len()) };
		let vals =
            unsafe { std::slice::from_raw_parts_mut(vals.as_mut_ptr().cast(), vals.len()) };
		let prevs =
            unsafe { std::slice::from_raw_parts_mut(prevs.as_mut_ptr().cast(), prevs.len()) };
        let dictionary = unsafe {
            std::slice::from_raw_parts_mut(dictionary.as_mut_ptr().cast(), dictionary.len())
        };

        CoreHashMap {
            dictionary,
            keys,
			vals,
			prevs,
            free_head: 0,
            buckets_in_use: 0,
            _user_list_head: INVALID_POS,
			alloc_limit: INVALID_POS,
        }
    }

    pub fn get_with_hash(&self, key: &K, hash: u64) -> Option<&V> {
        let mut next = self.dictionary[hash as usize % self.dictionary.len()];
        loop {
            if next == INVALID_POS {
                return None;
            }

            let keylink = &self.keys[next as usize];
            let bucket_key = keylink.inner.as_ref().expect("entry is in use");
            if bucket_key == key {
                return Some(self.vals[next as usize].as_ref().unwrap());
            }
            next = keylink.next;
        }
    }

    // all updates are done through Entry
    pub fn entry_with_hash(&mut self, key: K, hash: u64) -> Entry<'a, '_, K, V> {
        let dict_pos = hash as usize % self.dictionary.len();
        let first = self.dictionary[dict_pos];
        if first == INVALID_POS {
            // no existing entry
            return Entry::Vacant(VacantEntry {
                map: self,
                key,
                dict_pos: dict_pos as u32,
            });
        }

        let mut prev_pos = PrevPos::First(dict_pos as u32);
        let mut next = first;
        loop {
            let keylink = &mut self.keys[next as usize];
            let bucket_key = keylink.inner.as_mut().expect("entry is in use");
            if *bucket_key == key {
                // found existing entry
                return Entry::Occupied(OccupiedEntry {
                    map: self,
                    _key: key,
                    prev_pos,
                    bucket_pos: next,
                });
            }

            if keylink.next == INVALID_POS {
                // No existing entry
                return Entry::Vacant(VacantEntry {
                    map: self,
                    key,
                    dict_pos: dict_pos as u32,
                });
            }
            prev_pos = PrevPos::Chained(next);
            next = keylink.next;
        }
    }

    pub fn get_num_buckets(&self) -> usize {
        self.keys.len()
    }

	pub fn is_shrinking(&self) -> bool {
		self.alloc_limit != INVALID_POS
	}


	// TODO(quantumish): How does this interact with an ongoing shrink?
	pub fn clear(&mut self) {
		for i in 0..self.keys.len() {
            self.keys[i] = LinkedKey {
                next: if i < self.keys.len() - 1 {
                    i as u32 + 1
                } else {
                    INVALID_POS
                },				
                inner: None,
            }
        }
		for i in 0..self.prevs.len() {
			self.prevs[i] = if i > 0 {
				PrevPos::Chained(i as u32 - 1)
			} else {
				PrevPos::First(INVALID_POS)
			}
		}
		for i in 0..self.vals.len() {
			self.vals[i] = None;
		}

        for i in 0..self.dictionary.len() {
            self.dictionary[i] = INVALID_POS;
        }

		self.buckets_in_use = 0;
		self.alloc_limit = INVALID_POS;
	}
	
    pub fn entry_at_bucket(&mut self, pos: usize) -> Option<OccupiedEntry<'a, '_, K, V>> {
		if pos >= self.keys.len() {
			return None;
		}

		let prev = self.prevs[pos];
		let entry = self.keys[pos].inner.as_ref();
		match entry {
			Some(key) => Some(OccupiedEntry {
				_key: key.clone(),
				bucket_pos: pos as u32,
				prev_pos: prev,
				map: self,
			}),
			_ => None,
		}		
    }

	/// Find the position of an unused bucket via the freelist and initialize it. 
    pub(crate) fn alloc_bucket(&mut self, key: K, value: V, dict_pos: u32) -> Result<u32, FullError> {
        let mut pos = self.free_head;

		// Find the first bucket we're *allowed* to use.
		let mut prev = PrevPos::First(self.free_head);
		while pos != INVALID_POS && pos >= self.alloc_limit {
			let keylink = &mut self.keys[pos as usize];
			prev = PrevPos::Chained(pos);
			pos = keylink.next;
		}
		if pos == INVALID_POS {
			return Err(FullError());
		}

		// Repair the freelist.
		match prev {
			PrevPos::First(_) => {
				let next_pos = self.keys[pos as usize].next;
				self.free_head = next_pos;		
				if next_pos != INVALID_POS {
					self.prevs[next_pos as usize] = PrevPos::First(dict_pos);
				}
			}
			PrevPos::Chained(p) => if p != INVALID_POS {
				let next_pos = self.keys[pos as usize].next;
				self.keys[p as usize].next = next_pos;
				if next_pos != INVALID_POS {
					self.prevs[next_pos as usize] = PrevPos::Chained(p);
				}
			},
		}

		// Initialize the bucket.
		let keylink = &mut self.keys[pos as usize];
		self.buckets_in_use += 1;
        keylink.next = INVALID_POS;		
        keylink.inner = Some(key);
		self.vals[pos as usize] = Some(value);

        return Ok(pos);
    }
}

	
