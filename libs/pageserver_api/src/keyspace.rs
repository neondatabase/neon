use postgres_ffi::BLCKSZ;
use std::ops::Range;

use crate::key::Key;
use itertools::Itertools;

///
/// Represents a set of Keys, in a compact form.
///
#[derive(Clone, Debug, Default, PartialEq, Eq)]
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

    /// Merge another keyspace into the current one.
    /// Note: the keyspaces must not ovelap (enforced via assertions)
    pub fn merge(&mut self, other: &KeySpace) {
        let all_ranges = self
            .ranges
            .iter()
            .merge_by(other.ranges.iter(), |lhs, rhs| lhs.start < rhs.start);

        let mut accum = KeySpaceAccum::new();
        let mut prev: Option<&Range<Key>> = None;
        for range in all_ranges {
            if let Some(prev) = prev {
                let overlap =
                    std::cmp::max(range.start, prev.start) < std::cmp::min(range.end, prev.end);
                assert!(
                    !overlap,
                    "Attempt to merge ovelapping keyspaces: {:?} overlaps {:?}",
                    prev, range
                );
            }

            accum.add_range(range.clone());
            prev = Some(range);
        }

        self.ranges = accum.to_keyspace().ranges;
    }

    /// Remove all keys in `other` from `self`.
    /// This can involve splitting or removing of existing ranges.
    /// Returns the removed keyspace
    pub fn remove_overlapping_with(&mut self, other: &KeySpace) -> KeySpace {
        let (self_start, self_end) = match (self.start(), self.end()) {
            (Some(start), Some(end)) => (start, end),
            _ => {
                // self is empty
                return KeySpace::default();
            }
        };

        // Key spaces are sorted by definition, so skip ahead to the first
        // potentially intersecting range. Similarly, ignore ranges that start
        // after the current keyspace ends.
        let other_ranges = other
            .ranges
            .iter()
            .skip_while(|range| self_start >= range.end)
            .take_while(|range| self_end > range.start);

        let mut removed_accum = KeySpaceRandomAccum::new();
        for range in other_ranges {
            while let Some(overlap_at) = self.overlaps_at(range) {
                let overlapped = self.ranges[overlap_at].clone();

                if overlapped.start < range.start && overlapped.end <= range.end {
                    // Higher part of the range is completely overlapped.
                    removed_accum.add_range(range.start..self.ranges[overlap_at].end);
                    self.ranges[overlap_at].end = range.start;
                }
                if overlapped.start >= range.start && overlapped.end > range.end {
                    // Lower part of the range is completely overlapped.
                    removed_accum.add_range(self.ranges[overlap_at].start..range.end);
                    self.ranges[overlap_at].start = range.end;
                }
                if overlapped.start < range.start && overlapped.end > range.end {
                    // Middle part of the range is overlapped.
                    removed_accum.add_range(range.clone());
                    self.ranges[overlap_at].end = range.start;
                    self.ranges
                        .insert(overlap_at + 1, range.end..overlapped.end);
                }
                if overlapped.start >= range.start && overlapped.end <= range.end {
                    // Whole range is overlapped
                    removed_accum.add_range(self.ranges[overlap_at].clone());
                    self.ranges.remove(overlap_at);
                }
            }
        }

        removed_accum.to_keyspace()
    }

    pub fn start(&self) -> Option<Key> {
        self.ranges.first().map(|range| range.start)
    }

    pub fn end(&self) -> Option<Key> {
        self.ranges.last().map(|range| range.end)
    }

    #[allow(unused)]
    pub fn total_size(&self) -> usize {
        self.ranges
            .iter()
            .map(|range| key_range_size(range) as usize)
            .sum()
    }

    fn overlaps_at(&self, range: &Range<Key>) -> Option<usize> {
        match self.ranges.binary_search_by_key(&range.end, |r| r.start) {
            Ok(0) => None,
            Err(0) => None,
            Ok(index) if self.ranges[index - 1].end > range.start => Some(index - 1),
            Err(index) if self.ranges[index - 1].end > range.start => Some(index - 1),
            _ => None,
        }
    }

    ///
    /// Check if key space contains overlapping range
    ///
    pub fn overlaps(&self, range: &Range<Key>) -> bool {
        self.overlaps_at(range).is_some()
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
    size: u64,
}

impl KeySpaceAccum {
    pub fn new() -> Self {
        Self {
            accum: None,
            ranges: Vec::new(),
            size: 0,
        }
    }

    #[inline(always)]
    pub fn add_key(&mut self, key: Key) {
        self.add_range(singleton_range(key))
    }

    #[inline(always)]
    pub fn add_range(&mut self, range: Range<Key>) {
        self.size += key_range_size(&range) as u64;

        match self.accum.as_mut() {
            Some(accum) => {
                if range.start == accum.end {
                    accum.end = range.end;
                } else {
                    // TODO: to efficiently support small sharding stripe sizes, we should avoid starting
                    // a new range here if the skipped region was all keys that don't belong on this shard.
                    // (https://github.com/neondatabase/neon/issues/6247)
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

    pub fn consume_keyspace(&mut self) -> KeySpace {
        std::mem::take(self).to_keyspace()
    }

    pub fn size(&self) -> u64 {
        self.size
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

    pub fn consume_keyspace(&mut self) -> KeySpace {
        let mut prev_accum = KeySpaceRandomAccum::new();
        std::mem::swap(self, &mut prev_accum);

        prev_accum.to_keyspace()
    }
}

#[inline(always)]
pub fn key_range_size(key_range: &Range<Key>) -> u32 {
    let start = key_range.start;
    let end = key_range.end;

    if end.field1 != start.field1
        || end.field2 != start.field2
        || end.field3 != start.field3
        || end.field4 != start.field4
    {
        return u32::MAX;
    }

    let start = (start.field5 as u64) << 32 | start.field6 as u64;
    let end = (end.field5 as u64) << 32 | end.field6 as u64;

    let diff = end - start;
    if diff > u32::MAX as u64 {
        u32::MAX
    } else {
        diff as u32
    }
}

pub fn singleton_range(key: Key) -> Range<Key> {
    key..key.next()
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
    fn keyspace_consume() {
        let ranges = vec![kr(0..10), kr(20..35), kr(40..45)];

        let mut accum = KeySpaceAccum::new();
        for range in &ranges {
            accum.add_range(range.clone());
        }

        let expected_size: u64 = ranges.iter().map(|r| key_range_size(r) as u64).sum();
        assert_eq!(accum.size(), expected_size);

        assert_ks_eq(&accum.consume_keyspace(), ranges.clone());
        assert_eq!(accum.size(), 0);

        assert_ks_eq(&accum.consume_keyspace(), vec![]);
        assert_eq!(accum.size(), 0);

        for range in &ranges {
            accum.add_range(range.clone());
        }
        assert_ks_eq(&accum.to_keyspace(), ranges);
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

    #[test]
    fn test_remove_full_overlapps() {
        let mut key_space1 = KeySpace {
            ranges: vec![
                Key::from_i128(1)..Key::from_i128(4),
                Key::from_i128(5)..Key::from_i128(8),
                Key::from_i128(10)..Key::from_i128(12),
            ],
        };
        let key_space2 = KeySpace {
            ranges: vec![
                Key::from_i128(2)..Key::from_i128(3),
                Key::from_i128(6)..Key::from_i128(7),
                Key::from_i128(11)..Key::from_i128(13),
            ],
        };
        let removed = key_space1.remove_overlapping_with(&key_space2);
        let removed_expected = KeySpace {
            ranges: vec![
                Key::from_i128(2)..Key::from_i128(3),
                Key::from_i128(6)..Key::from_i128(7),
                Key::from_i128(11)..Key::from_i128(12),
            ],
        };
        assert_eq!(removed, removed_expected);

        assert_eq!(
            key_space1.ranges,
            vec![
                Key::from_i128(1)..Key::from_i128(2),
                Key::from_i128(3)..Key::from_i128(4),
                Key::from_i128(5)..Key::from_i128(6),
                Key::from_i128(7)..Key::from_i128(8),
                Key::from_i128(10)..Key::from_i128(11)
            ]
        );
    }

    #[test]
    fn test_remove_partial_overlaps() {
        // Test partial ovelaps
        let mut key_space1 = KeySpace {
            ranges: vec![
                Key::from_i128(1)..Key::from_i128(5),
                Key::from_i128(7)..Key::from_i128(10),
                Key::from_i128(12)..Key::from_i128(15),
            ],
        };
        let key_space2 = KeySpace {
            ranges: vec![
                Key::from_i128(3)..Key::from_i128(6),
                Key::from_i128(8)..Key::from_i128(11),
                Key::from_i128(14)..Key::from_i128(17),
            ],
        };

        let removed = key_space1.remove_overlapping_with(&key_space2);
        let removed_expected = KeySpace {
            ranges: vec![
                Key::from_i128(3)..Key::from_i128(5),
                Key::from_i128(8)..Key::from_i128(10),
                Key::from_i128(14)..Key::from_i128(15),
            ],
        };
        assert_eq!(removed, removed_expected);

        assert_eq!(
            key_space1.ranges,
            vec![
                Key::from_i128(1)..Key::from_i128(3),
                Key::from_i128(7)..Key::from_i128(8),
                Key::from_i128(12)..Key::from_i128(14),
            ]
        );
    }

    #[test]
    fn test_remove_no_overlaps() {
        let mut key_space1 = KeySpace {
            ranges: vec![
                Key::from_i128(1)..Key::from_i128(5),
                Key::from_i128(7)..Key::from_i128(10),
                Key::from_i128(12)..Key::from_i128(15),
            ],
        };
        let key_space2 = KeySpace {
            ranges: vec![
                Key::from_i128(6)..Key::from_i128(7),
                Key::from_i128(11)..Key::from_i128(12),
                Key::from_i128(15)..Key::from_i128(17),
            ],
        };

        let removed = key_space1.remove_overlapping_with(&key_space2);
        let removed_expected = KeySpace::default();
        assert_eq!(removed, removed_expected);

        assert_eq!(
            key_space1.ranges,
            vec![
                Key::from_i128(1)..Key::from_i128(5),
                Key::from_i128(7)..Key::from_i128(10),
                Key::from_i128(12)..Key::from_i128(15),
            ]
        );
    }

    #[test]
    fn test_remove_one_range_overlaps_multiple() {
        let mut key_space1 = KeySpace {
            ranges: vec![
                Key::from_i128(1)..Key::from_i128(3),
                Key::from_i128(3)..Key::from_i128(6),
                Key::from_i128(6)..Key::from_i128(10),
                Key::from_i128(12)..Key::from_i128(15),
                Key::from_i128(17)..Key::from_i128(20),
                Key::from_i128(20)..Key::from_i128(30),
                Key::from_i128(30)..Key::from_i128(40),
            ],
        };
        let key_space2 = KeySpace {
            ranges: vec![Key::from_i128(9)..Key::from_i128(19)],
        };

        let removed = key_space1.remove_overlapping_with(&key_space2);
        let removed_expected = KeySpace {
            ranges: vec![
                Key::from_i128(9)..Key::from_i128(10),
                Key::from_i128(12)..Key::from_i128(15),
                Key::from_i128(17)..Key::from_i128(19),
            ],
        };
        assert_eq!(removed, removed_expected);

        assert_eq!(
            key_space1.ranges,
            vec![
                Key::from_i128(1)..Key::from_i128(3),
                Key::from_i128(3)..Key::from_i128(6),
                Key::from_i128(6)..Key::from_i128(9),
                Key::from_i128(19)..Key::from_i128(20),
                Key::from_i128(20)..Key::from_i128(30),
                Key::from_i128(30)..Key::from_i128(40),
            ]
        );
    }
}
