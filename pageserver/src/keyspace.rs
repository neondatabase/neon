use crate::repository::{key_range_size, singleton_range, Key};
use postgres_ffi::BLCKSZ;
use std::ops::Range;

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

    ///
    /// Check if key space contains overlapping range
    ///
    pub fn overlaps(&self, range: &Range<Key>) -> bool {
        match self.ranges.binary_search_by_key(&range.end, |r| r.start) {
            Ok(0) => false,
            Err(0) => false,
            Ok(index) => self.ranges[index - 1].end > range.start,
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

///
/// A helper object, to collect a set of keys and key ranges into a KeySpace
/// object. Key ranges may be inserted in any order and can overlap.
///
#[derive(Clone, Debug, Default)]
pub struct KeySpaceRandomAccum {
    ranges: Vec<Range<Key>>,
}

impl KeySpaceRandomAccum {
    pub fn new() -> Self {
        Self { ranges: Vec::new() }
    }

    pub fn add_key(&mut self, key: Key) {
        self.add_range(singleton_range(key))
    }

    pub fn add_range(&mut self, range: Range<Key>) {
        self.ranges.push(range);
    }

    pub fn to_keyspace(mut self) -> KeySpace {
        let mut ranges = Vec::new();
        if !self.ranges.is_empty() {
            self.ranges.sort_by_key(|r| r.start);
            let mut start = self.ranges.first().unwrap().start;
            let mut end = self.ranges.first().unwrap().end;
            for r in self.ranges {
                assert!(r.start >= start);
                if r.start > end {
                    ranges.push(start..end);
                    start = r.start;
                    end = r.end;
                } else if r.end > end {
                    end = r.end;
                }
            }
            ranges.push(start..end);
        }
        KeySpace { ranges }
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
        let mut ks = KeySpaceRandomAccum::default();
        ks.add_range(kr(0..10));
        ks.add_range(kr(20..30));
        assert_ks_eq(&ks.to_keyspace(), vec![kr(0..10), kr(20..30)]);

        // two separate ranges, added in reverse order
        //
        //         #####
        // #####
        let mut ks = KeySpaceRandomAccum::default();
        ks.add_range(kr(20..30));
        ks.add_range(kr(0..10));

        // add range that is adjacent to the end of an existing range
        //
        // #####
        //      #####
        ks.add_range(kr(0..10));
        ks.add_range(kr(10..30));
        assert_ks_eq(&ks.to_keyspace(), vec![kr(0..30)]);

        // add range that is adjacent to the start of an existing range
        //
        //      #####
        // #####
        let mut ks = KeySpaceRandomAccum::default();
        ks.add_range(kr(10..30));
        ks.add_range(kr(0..10));
        assert_ks_eq(&ks.to_keyspace(), vec![kr(0..30)]);

        // add range that overlaps with the end of an existing range
        //
        // #####
        //    #####
        let mut ks = KeySpaceRandomAccum::default();
        ks.add_range(kr(0..10));
        ks.add_range(kr(5..30));
        assert_ks_eq(&ks.to_keyspace(), vec![kr(0..30)]);

        // add range that overlaps with the start of an existing range
        //
        //    #####
        // #####
        let mut ks = KeySpaceRandomAccum::default();
        ks.add_range(kr(5..30));
        ks.add_range(kr(0..10));
        assert_ks_eq(&ks.to_keyspace(), vec![kr(0..30)]);

        // add range that is fully covered by an existing range
        //
        // #########
        //   #####
        let mut ks = KeySpaceRandomAccum::default();
        ks.add_range(kr(0..30));
        ks.add_range(kr(10..20));
        assert_ks_eq(&ks.to_keyspace(), vec![kr(0..30)]);

        // add range that extends an existing range from both ends
        //
        //   #####
        // #########
        let mut ks = KeySpaceRandomAccum::default();
        ks.add_range(kr(10..20));
        ks.add_range(kr(0..30));
        assert_ks_eq(&ks.to_keyspace(), vec![kr(0..30)]);

        // add a range that overlaps with two existing ranges, joining them
        //
        // #####   #####
        //    #######
        let mut ks = KeySpaceRandomAccum::default();
        ks.add_range(kr(0..10));
        ks.add_range(kr(20..30));
        ks.add_range(kr(5..25));
        assert_ks_eq(&ks.to_keyspace(), vec![kr(0..30)]);
    }

    #[test]
    fn keyspace_overlaps() {
        let mut ks = KeySpaceRandomAccum::default();
        ks.add_range(kr(10..20));
        ks.add_range(kr(30..40));
        let ks = ks.to_keyspace();

        //        #####      #####
        // xxxx
        assert!(!ks.overlaps(&kr(0..5)));

        //        #####      #####
        //   xxxx
        assert!(!ks.overlaps(&kr(5..9)));

        //        #####      #####
        //    xxxx
        assert!(!ks.overlaps(&kr(5..10)));

        //        #####      #####
        //     xxxx
        assert!(ks.overlaps(&kr(5..11)));

        //        #####      #####
        //        xxxx
        assert!(ks.overlaps(&kr(10..15)));

        //        #####      #####
        //         xxxx
        assert!(ks.overlaps(&kr(15..20)));

        //        #####      #####
        //           xxxx
        assert!(ks.overlaps(&kr(15..25)));

        //        #####      #####
        //              xxxx
        assert!(!ks.overlaps(&kr(22..28)));

        //        #####      #####
        //               xxxx
        assert!(!ks.overlaps(&kr(25..30)));

        //        #####      #####
        //                      xxxx
        assert!(ks.overlaps(&kr(35..35)));

        //        #####      #####
        //                        xxxx
        assert!(!ks.overlaps(&kr(40..45)));

        //        #####      #####
        //                        xxxx
        assert!(!ks.overlaps(&kr(45..50)));

        //        #####      #####
        //        xxxxxxxxxxx
        assert!(ks.overlaps(&kr(0..30))); // XXXXX This fails currently!
    }
}
