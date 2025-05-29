//! Simple hash table with chaining
//!
//! # Resizing
//!

use std::hash::{DefaultHasher, Hash, Hasher};
use std::mem::MaybeUninit;

pub(crate) const INVALID_POS: u32 = u32::MAX;

// Bucket
pub(crate) struct Bucket<K, V> {
    pub(crate) hash: u64,
    pub(crate) next: u32,
    pub(crate) inner: Option<(K, V)>,
}

pub(crate) struct CoreHashMap<'a, K, V> {
    pub(crate) dictionary: &'a mut [u32],
    pub(crate) buckets: &'a mut [Bucket<K, V>],
    pub(crate) free_head: u32,

    // metrics
    pub(crate) buckets_in_use: u32,
}

pub struct FullError();

impl<'a, K, V> CoreHashMap<'a, K, V>
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

    pub fn new(num_buckets: u32, area: &'a mut [u8]) -> CoreHashMap<'a, K, V> {
        let len = area.len();

        let mut ptr: *mut u8 = area.as_mut_ptr();
        let end_ptr: *mut u8 = unsafe { area.as_mut_ptr().add(len) };

        // carve out the buckets
        ptr = unsafe { ptr.byte_add(ptr.align_offset(align_of::<Bucket<K, V>>())) };
        let buckets_ptr = ptr;
        ptr = unsafe { ptr.add(size_of::<Bucket<K, V>>() * num_buckets as usize) };

        // use remaining space for the dictionary
        ptr = unsafe { ptr.byte_add(ptr.align_offset(align_of::<u32>())) };
        let dictionary_ptr = ptr;

        assert!(ptr.addr() < end_ptr.addr());
        let dictionary_size = unsafe { end_ptr.byte_offset_from(ptr) / size_of::<u32>() as isize };
        assert!(dictionary_size > 0);

        // Initialize the buckets
        let buckets = {
            let buckets_ptr: *mut MaybeUninit<Bucket<K, V>> = buckets_ptr.cast();
            let buckets =
                unsafe { std::slice::from_raw_parts_mut(buckets_ptr, num_buckets as usize) };
            for i in 0..buckets.len() {
                buckets[i].write(Bucket {
                    hash: 0,
                    next: if i < buckets.len() - 1 {
                        i as u32 + 1
                    } else {
                        INVALID_POS
                    },
                    inner: None,
                });
            }
            // TODO: use std::slice::assume_init_mut() once it stabilizes
            unsafe { std::slice::from_raw_parts_mut(buckets_ptr.cast(), num_buckets as usize) }
        };

        // Initialize the dictionary
        let dictionary = {
            let dictionary_ptr: *mut MaybeUninit<u32> = dictionary_ptr.cast();
            let dictionary =
                unsafe { std::slice::from_raw_parts_mut(dictionary_ptr, dictionary_size as usize) };

            for i in 0..dictionary.len() {
                dictionary[i].write(INVALID_POS);
            }
            // TODO: use std::slice::assume_init_mut() once it stabilizes
            unsafe {
                std::slice::from_raw_parts_mut(dictionary_ptr.cast(), dictionary_size as usize)
            }
        };

        CoreHashMap {
            dictionary,
            buckets,
            free_head: 0,
            buckets_in_use: 0,
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

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

    pub fn insert(&mut self, key: &K, value: V) -> Result<(), FullError> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        let first = self.dictionary[hash as usize % self.dictionary.len()];
        if first == INVALID_POS {
            // no existing entry
            let pos = self.alloc_bucket(key.clone(), value, hash)?;
            if pos == INVALID_POS {
                return Err(FullError());
            }
            self.dictionary[hash as usize % self.dictionary.len()] = pos;
            return Ok(());
        }

        let mut next = first;
        loop {
            let bucket = &mut self.buckets[next as usize];
            let (bucket_key, bucket_value) = bucket.inner.as_mut().expect("entry is in use");
            if bucket_key == key {
                // found existing entry, update its value
                *bucket_value = value;
                return Ok(());
            }

            if bucket.next == INVALID_POS {
                // No existing entry found. Append to the chain
                let pos = self.alloc_bucket(key.clone(), value, hash)?;
                if pos == INVALID_POS {
                    return Err(FullError());
                }
                self.buckets[next as usize].next = pos;
                return Ok(());
            }
            next = bucket.next;
        }
    }

    pub fn remove(&mut self, key: &K) -> Result<(), FullError> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        let mut next = self.dictionary[hash as usize % self.dictionary.len()];
        let mut prev_pos: u32 = INVALID_POS;
        loop {
            if next == INVALID_POS {
                // no existing entry
                return Ok(());
            }
            let bucket = &mut self.buckets[next as usize];
            let (bucket_key, _) = bucket.inner.as_mut().expect("entry is in use");
            if bucket_key == key {
                // found existing entry, unlink it from the chain
                if prev_pos == INVALID_POS {
                    self.dictionary[hash as usize % self.dictionary.len()] = bucket.next;
                } else {
                    self.buckets[prev_pos as usize].next = bucket.next;
                }

                // and add it to the freelist
                let bucket = &mut self.buckets[next as usize];
                bucket.hash = 0;
                bucket.inner = None;
                bucket.next = self.free_head;
                self.free_head = next;
                self.buckets_in_use -= 1;
                return Ok(());
            }
            prev_pos = next;
            next = bucket.next;
        }
    }

    pub fn get_num_buckets(&self) -> usize {
        self.buckets.len()
    }

    pub fn get_bucket(&self, pos: usize) -> Option<&(K, V)> {
        if pos >= self.buckets.len() {
            return None;
        }

        self.buckets[pos].inner.as_ref()
    }

    fn alloc_bucket(&mut self, key: K, value: V, hash: u64) -> Result<u32, FullError> {
        let pos = self.free_head;
        if pos == INVALID_POS {
            return Err(FullError());
        }

        let bucket = &mut self.buckets[pos as usize];
        self.free_head = bucket.next;
        self.buckets_in_use += 1;

        bucket.hash = hash;
        bucket.next = INVALID_POS;
        bucket.inner = Some((key, value));

        return Ok(pos);
    }
}
