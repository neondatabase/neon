use crate::repository::{key_range_size, singleton_range, Key};
use postgres_ffi::BLCKSZ;
use std::ops::Range;
use tracing::debug;

///
/// Represents a set of Keys, in a compact form.
///
#[derive(Clone, Debug, Default)]
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
        let target_nblocks = (target_size / BLCKSZ as u64) as usize;

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

    /// Add range to keyspace.
    ///
    /// Unlike KeySpaceAccum, it accepts key ranges in any order and overlapping ranges.
    pub fn add_range(&mut self, range: Range<Key>) {
        let start = range.start;
        let mut end = range.end;
        let mut prev_index = match self.ranges.binary_search_by_key(&end, |r| r.start) {
            Ok(index) => index,
            Err(0) => {
                self.ranges.insert(0, range);
                return;
            }
            Err(index) => index - 1,
        };
        loop {
            let mut prev = &mut self.ranges[prev_index];
            if prev.end >= start {
                // two ranges overlap
                if prev.start <= start {
                    // combine with prev range
                    if prev.end < end {
                        prev.end = end;
                        debug!("Extend wanted image {}..{}", prev.start, end);
                    }
                    return;
                } else {
                    if prev.end > end {
                        end = prev.end;
                    }
                    self.ranges.remove(prev_index);
                }
            } else {
                prev_index += 1; // insert after prev range
                break;
            }
            if prev_index == 0 {
                break;
            }
            prev_index -= 1;
        }
        debug!("Wanted image {}..{}", start, end);
        self.ranges.insert(prev_index, start..end);
    }

    ///
    /// Check if key space contains overlapping range
    ///
    pub fn overlaps(&self, range: &Range<Key>) -> bool {
        match self.ranges.binary_search_by_key(&range.end, |r| r.start) {
            Ok(_) => false,
            Err(0) => false,
            Err(index) => self.ranges[index - 1].end > range.start,
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write;

    // Helper function to create a key range.
    //
    // Make the tests below less verbose.
    fn kr(irange: Range<i128>) -> Range<Key> {
        Key::from_i128(irange.start)..Key::from_i128(irange.end)
    }

    #[allow(dead_code)]
    fn dump_keyspace(ks: &KeySpace) {
        for r in ks.ranges.iter() {
            println!("  {}..{}", r.start.to_i128(), r.end.to_i128());
        }
    }

    fn assert_ks_eq(actual: &KeySpace, expected: Vec<Range<Key>>) {
        if actual.ranges != expected {
            let mut msg = String::new();

            writeln!(msg, "expected:").unwrap();
            for r in &expected {
                writeln!(msg, "  {}..{}", r.start.to_i128(), r.end.to_i128()).unwrap();
            }
            writeln!(msg, "got:").unwrap();
            for r in &actual.ranges {
                writeln!(msg, "  {}..{}", r.start.to_i128(), r.end.to_i128()).unwrap();
            }
            panic!("{}", msg);
        }
    }

    #[test]
    fn keyspace_add_range() {
        // two separate ranges
        //
        // #####
        //         #####
        let mut ks = KeySpace::default();
        ks.add_range(kr(0..10));
        ks.add_range(kr(20..30));
        assert_ks_eq(&ks, vec![kr(0..10), kr(20..30)]);

        // two separate ranges, added in reverse order
        //
        //         #####
        // #####
        let mut ks = KeySpace::default();
        ks.add_range(kr(20..30));
        ks.add_range(kr(0..10));
        assert_ks_eq(&ks, vec![kr(0..10), kr(20..30)]);

        // add range that is adjacent to the end of an existing range
        //
        // #####
        //      #####
        ks.add_range(kr(0..10));
        ks.add_range(kr(10..30));
        assert_ks_eq(&ks, vec![kr(0..30)]);

        // add range that is adjacent to the start of an existing range
        //
        //      #####
        // #####
        let mut ks = KeySpace::default();
        ks.add_range(kr(10..30));
        ks.add_range(kr(0..10));
        assert_ks_eq(&ks, vec![kr(0..30)]);

        // add range that overlaps with the end of an existing range
        //
        // #####
        //    #####
        let mut ks = KeySpace::default();
        ks.add_range(kr(0..10));
        ks.add_range(kr(5..30));
        assert_ks_eq(&ks, vec![kr(0..30)]);

        // add range that overlaps with the start of an existing range
        //
        //    #####
        // #####
        let mut ks = KeySpace::default();
        ks.add_range(kr(5..30));
        ks.add_range(kr(0..10));
        assert_ks_eq(&ks, vec![kr(0..30)]);

        // add range that is fully covered by an existing range
        //
        // #########
        //   #####
        let mut ks = KeySpace::default();
        ks.add_range(kr(0..30));
        ks.add_range(kr(10..20));
        assert_ks_eq(&ks, vec![kr(0..30)]);

        // add range that extends an existing range from both ends
        //
        //   #####
        // #########
        let mut ks = KeySpace::default();
        ks.add_range(kr(10..20));
        ks.add_range(kr(0..30));
        assert_ks_eq(&ks, vec![kr(0..30)]);

        // add a range that overlaps with two existing ranges, joining them
        //
        // #####   #####
        //    #######
        println!("joins");
        let mut ks = KeySpace::default();
        ks.add_range(kr(0..10));
        ks.add_range(kr(20..30));
        ks.add_range(kr(5..25));
        assert_ks_eq(&ks, vec![kr(0..30)]);
    }
}
