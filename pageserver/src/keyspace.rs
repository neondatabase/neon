use std::ops::Range;

use crate::repository::{key_range_size, singleton_range, Key};

use postgres_ffi::pg_constants;

// Target file size, when creating iage and delta layers
pub const TARGET_FILE_SIZE_BYTES: u64 = 128 * 1024 * 1024; // 128 MB

///
/// Represents a set of Keys, in a compact form.
///
#[derive(Debug, Clone)]
pub struct KeyPartitioning {
    accum: Option<Range<Key>>,

    ranges: Vec<Range<Key>>,

    pub partitions: Vec<Vec<Range<Key>>>,
}

impl KeyPartitioning {
    pub fn new() -> Self {
        KeyPartitioning {
            accum: None,
            ranges: Vec::new(),
            partitions: Vec::new(),
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

    pub fn repartition(&mut self, target_size: u64) {
        let target_nblocks = (target_size / pg_constants::BLCKSZ as u64) as usize;
        if let Some(accum) = self.accum.take() {
            self.ranges.push(accum);
        }

        self.partitions = Vec::new();

        let mut current_part = Vec::new();
        let mut current_part_size: usize = 0;
        for range in &self.ranges {
            let this_size = key_range_size(range) as usize;

            if current_part_size + this_size > target_nblocks && !current_part.is_empty() {
                self.partitions.push(current_part);
                current_part = Vec::new();
                current_part_size = 0;
            }

            let mut remain_size = this_size;
            let mut start = range.start;
            while remain_size > target_nblocks {
                let next = start.add(target_nblocks as u32);
                self.partitions.push(vec![start..next]);
                start = next;
                remain_size -= target_nblocks
            }
            current_part.push(start..range.end);
            current_part_size += remain_size;
        }
        if !current_part.is_empty() {
            self.partitions.push(current_part);
        }
    }
}

impl Default for KeyPartitioning {
    fn default() -> Self {
        Self::new()
    }
}
