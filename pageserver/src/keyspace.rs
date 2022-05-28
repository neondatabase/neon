use crate::repository::{key_range_size, singleton_range, Key};
use postgres_ffi::pg_constants;
use std::ops::Range;

///
/// Represents a set of Keys, in a compact form.
///
#[derive(Clone, Debug)]
pub struct KeySpace {
    /// Contiguous ranges of keys that belong to the key space. In key order,
    /// and with no overlap.
    pub ranges: Vec<Range<Key>>,
}

impl KeySpace {
    ///
    /// Partition a key space into roughly chunks of roughly 'target_size' bytes
    /// in each partition.
    ///
    pub fn partition(&self, target_size: u64) -> KeyPartitioning {
        // Assume that each value is 8k in size.
        let target_nblocks = (target_size / pg_constants::BLCKSZ as u64) as usize;

        let mut parts = Vec::new();
        let mut current_part = Vec::new();
        let mut current_part_size: usize = 0;
        for range in &self.ranges {
            // If appending the next contiguous range in the keyspace to the current
            // partition would cause it to be too large, start a new partition.
            let this_size = key_range_size(range) as usize;
            if current_part_size + this_size > target_nblocks && !current_part.is_empty() {
                parts.push(KeySpace {
                    ranges: current_part,
                });
                current_part = Vec::new();
                current_part_size = 0;
            }

            // If the next range is larger than 'target_size', split it into
            // 'target_size' chunks.
            let mut remain_size = this_size;
            let mut start = range.start;
            while remain_size > target_nblocks {
                let next = start.add(target_nblocks as u32);
                parts.push(KeySpace {
                    ranges: vec![start..next],
                });
                start = next;
                remain_size -= target_nblocks
            }
            current_part.push(start..range.end);
            current_part_size += remain_size;
        }

        // add last partition that wasn't full yet.
        if !current_part.is_empty() {
            parts.push(KeySpace {
                ranges: current_part,
            });
        }

        KeyPartitioning { parts }
    }
}

///
/// Represents a partitioning of the key space.
///
/// The only kind of partitioning we do is to partition the key space into
/// partitions that are roughly equal in physical size (see KeySpace::partition).
/// But this data structure could represent any partitioning.
///
#[derive(Clone, Debug, Default)]
pub struct KeyPartitioning {
    pub parts: Vec<KeySpace>,
}

impl KeyPartitioning {
    pub fn new() -> Self {
        KeyPartitioning { parts: Vec::new() }
    }
}

///
/// A helper object, to collect a set of keys and key ranges into a KeySpace
/// object. This takes care of merging adjacent keys and key ranges into
/// contiguous ranges.
///
#[derive(Clone, Debug, Default)]
pub struct KeySpaceAccum {
    accum: Option<Range<Key>>,

    ranges: Vec<Range<Key>>,
}

impl KeySpaceAccum {
    pub fn new() -> Self {
        Self {
            accum: None,
            ranges: Vec::new(),
        }
    }

    pub fn add_key(&mut self, key: Key) {
        self.add_range(singleton_range(key))
    }

    pub fn add_range(&mut self, range: Range<Key>) {
        match self.accum.as_mut() {
            Some(accum) => {
                if range.start == accum.end {
                    accum.end = range.end;
                } else {
                    assert!(range.start > accum.end);
                    self.ranges.push(accum.clone());
                    *accum = range;
                }
            }
            None => self.accum = Some(range),
        }
    }

    pub fn to_keyspace(mut self) -> KeySpace {
        if let Some(accum) = self.accum.take() {
            self.ranges.push(accum);
        }
        KeySpace {
            ranges: self.ranges,
        }
    }
}
