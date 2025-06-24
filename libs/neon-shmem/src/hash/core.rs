//! Simple hash table with chaining
//!
//! # Resizing
//!

use std::hash::Hash;
use std::mem::MaybeUninit;

use crate::hash::entry::{Entry, OccupiedEntry, PrevPos, VacantEntry};

pub(crate) const INVALID_POS: u32 = u32::MAX;

// Bucket
pub(crate) struct Bucket<K, V> {
	pub(crate) next: u32,
    pub(crate) inner: Option<(K, V)>,
}

pub(crate) struct CoreHashMap<'a, K, V> {
	/// Dictionary used to map hashes to bucket indices.	
    pub(crate) dictionary: &'a mut [u32],
	/// Buckets containing key-value pairs.
    pub(crate) buckets: &'a mut [Bucket<K, V>],
	/// Head of the freelist.
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
        size += size_of::<Bucket<K, V>>() * num_buckets as usize;

        // dictionary
        size += (f32::ceil((size_of::<u32>() * num_buckets as usize) as f32 / Self::FILL_FACTOR))
            as usize;

        size
    }	

    pub fn new(
        buckets: &'a mut [MaybeUninit<Bucket<K, V>>],
        dictionary: &'a mut [MaybeUninit<u32>],
    ) -> CoreHashMap<'a, K, V> {
        // Initialize the buckets
        for i in 0..buckets.len() {
            buckets[i].write(Bucket {
                next: if i < buckets.len() - 1 {
                    i as u32 + 1
                } else {
                    INVALID_POS
                },				
                inner: None,
            });
        }

        // Initialize the dictionary
        for i in 0..dictionary.len() {
            dictionary[i].write(INVALID_POS);
        }

        // TODO: use std::slice::assume_init_mut() once it stabilizes
        let buckets =
            unsafe { std::slice::from_raw_parts_mut(buckets.as_mut_ptr().cast(), buckets.len()) };
        let dictionary = unsafe {
            std::slice::from_raw_parts_mut(dictionary.as_mut_ptr().cast(), dictionary.len())
        };

        CoreHashMap {
            dictionary,
            buckets,
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

            let bucket = &self.buckets[next as usize];
            let (bucket_key, bucket_value) = bucket.inner.as_ref().expect("entry is in use");
            if bucket_key == key {
                return Some(&bucket_value);
            }
            next = bucket.next;
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
            let bucket = &mut self.buckets[next as usize];
            let (bucket_key, _bucket_value) = bucket.inner.as_mut().expect("entry is in use");
            if *bucket_key == key {
                // found existing entry
                return Entry::Occupied(OccupiedEntry {
                    map: self,
                    _key: key,
                    prev_pos,
                    bucket_pos: next,
                });
            }

            if bucket.next == INVALID_POS {
                // No existing entry
                return Entry::Vacant(VacantEntry {
                    map: self,
                    key,
                    dict_pos: dict_pos as u32,
                });
            }
            prev_pos = PrevPos::Chained(next);
            next = bucket.next;
        }
    }

    pub fn get_num_buckets(&self) -> usize {
        self.buckets.len()
    }

	pub fn is_shrinking(&self) -> bool {
		self.alloc_limit != INVALID_POS
	}

	/// Clears all entries from the hashmap.
	/// Does not reset any allocation limits, but does clear any entries beyond them.
	pub fn clear(&mut self) {
		for i in 0..self.buckets.len() {
            self.buckets[i] = Bucket {
                next: if i < self.buckets.len() - 1 {
                    i as u32 + 1
                } else {
                    INVALID_POS
                },				
                inner: None,
            }
        }

        for i in 0..self.dictionary.len() {
            self.dictionary[i] = INVALID_POS;
        }

		self.buckets_in_use = 0;
	}
	
    pub fn entry_at_bucket(&mut self, pos: usize) -> Option<OccupiedEntry<'a, '_, K, V>> {
		if pos >= self.buckets.len() {
			return None;
		}

		let entry = self.buckets[pos].inner.as_ref();
		match entry {
			Some((key, _)) => Some(OccupiedEntry {
				_key: key.clone(),
				bucket_pos: pos as u32,
				prev_pos: PrevPos::Unknown,
				map: self,
			}),
			_ => None,
		}		
    }

	/// Find the position of an unused bucket via the freelist and initialize it. 
    pub(crate) fn alloc_bucket(&mut self, key: K, value: V) -> Result<u32, FullError> {
        let mut pos = self.free_head;

		// Find the first bucket we're *allowed* to use.
		let mut prev = PrevPos::First(self.free_head);
		while pos != INVALID_POS && pos >= self.alloc_limit {
			let bucket = &mut self.buckets[pos as usize];
			prev = PrevPos::Chained(pos);
			pos = bucket.next;
		}
		if pos == INVALID_POS {
			return Err(FullError());
		}

		// Repair the freelist.
		match prev {
			PrevPos::First(_) => {
				let next_pos = self.buckets[pos as usize].next;
				self.free_head = next_pos;				
			}
			PrevPos::Chained(p) => if p != INVALID_POS {
				let next_pos = self.buckets[pos as usize].next;
				self.buckets[p as usize].next = next_pos;
			},
			PrevPos::Unknown => unreachable!()
		}

		// Initialize the bucket.
		let bucket = &mut self.buckets[pos as usize];
		self.buckets_in_use += 1;
        bucket.next = INVALID_POS;
        bucket.inner = Some((key, value));

        return Ok(pos);
    }
}

	
