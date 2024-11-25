use postgres_ffi::BLCKSZ;
use std::ops::Range;

use crate::{
    key::Key,
    shard::{ShardCount, ShardIdentity},
};
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

impl std::fmt::Display for KeySpace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for range in &self.ranges {
            write!(f, "{}..{},", range.start, range.end)?;
        }
        write!(f, "]")
    }
}

/// A wrapper type for sparse keyspaces.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SparseKeySpace(pub KeySpace);

/// Represents a contiguous half-open range of the keyspace, masked according to a particular
/// ShardNumber's stripes: within this range of keys, only some "belong" to the current
/// shard.
///
/// When we iterate over keys within this object, we will skip any keys that don't belong
/// to this shard.
///
/// The start + end keys may not belong to the shard: these specify where layer files should
/// start  + end, but we will never actually read/write those keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardedRange<'a> {
    pub shard_identity: &'a ShardIdentity,
    pub range: Range<Key>,
}

// Calculate the size of a range within the blocks of the same relation, or spanning only the
// top page in the previous relation's space.
pub fn contiguous_range_len(range: &Range<Key>) -> u32 {
    debug_assert!(is_contiguous_range(range));
    if range.start.field6 == 0xffffffff {
        range.end.field6 + 1
    } else {
        range.end.field6 - range.start.field6
    }
}

/// Return true if this key range includes only keys in the same relation's data blocks, or
/// just spanning one relation and the logical size (0xffffffff) block of the relation before it.
///
/// Contiguous in this context means we know the keys are in use _somewhere_, but it might not
/// be on our shard.  Later in ShardedRange we do the extra work to figure out how much
/// of a given contiguous range is present on one shard.
///
/// This matters, because:
/// - Within such ranges, keys are used contiguously.  Outside such ranges it is sparse.
/// - Within such ranges, we may calculate distances using simple subtraction of field6.
pub fn is_contiguous_range(range: &Range<Key>) -> bool {
    range.start.field1 == range.end.field1
        && range.start.field2 == range.end.field2
        && range.start.field3 == range.end.field3
        && range.start.field4 == range.end.field4
        && (range.start.field5 == range.end.field5
            || (range.start.field6 == 0xffffffff && range.start.field5 + 1 == range.end.field5))
}

impl<'a> ShardedRange<'a> {
    pub fn new(range: Range<Key>, shard_identity: &'a ShardIdentity) -> Self {
        Self {
            shard_identity,
            range,
        }
    }

    /// Break up this range into chunks, each of which has at least one local key in it if the
    /// total range has at least one local key.
    pub fn fragment(self, target_nblocks: u32) -> Vec<(u32, Range<Key>)> {
        // Optimization for single-key case (e.g. logical size keys)
        if self.range.end == self.range.start.add(1) {
            return vec![(
                if self.shard_identity.is_key_disposable(&self.range.start) {
                    0
                } else {
                    1
                },
                self.range,
            )];
        }

        if !is_contiguous_range(&self.range) {
            // Ranges that span relations are not fragmented.  We only get these ranges as a result
            // of operations that act on existing layers, so we trust that the existing range is
            // reasonably small.
            return vec![(u32::MAX, self.range)];
        }

        let mut fragments: Vec<(u32, Range<Key>)> = Vec::new();

        let mut cursor = self.range.start;
        while cursor < self.range.end {
            let advance_by = self.distance_to_next_boundary(cursor);
            let is_fragment_disposable = self.shard_identity.is_key_disposable(&cursor);

            // If the previous fragment is undersized, then we seek to consume enough
            // blocks to complete it.
            let (want_blocks, merge_last_fragment) = match fragments.last_mut() {
                Some(frag) if frag.0 < target_nblocks => (target_nblocks - frag.0, Some(frag)),
                Some(frag) => {
                    // Prev block is complete, want the full number.
                    (
                        target_nblocks,
                        if is_fragment_disposable {
                            // If this current range will be empty (not shard-local data), we will merge into previous
                            Some(frag)
                        } else {
                            None
                        },
                    )
                }
                None => {
                    // First iteration, want the full number
                    (target_nblocks, None)
                }
            };

            let advance_by = if is_fragment_disposable {
                advance_by
            } else {
                std::cmp::min(advance_by, want_blocks)
            };

            let next_cursor = cursor.add(advance_by);

            let this_frag = (
                if is_fragment_disposable {
                    0
                } else {
                    advance_by
                },
                cursor..next_cursor,
            );
            cursor = next_cursor;

            if let Some(last_fragment) = merge_last_fragment {
                // Previous fragment was short or this one is empty, merge into it
                last_fragment.0 += this_frag.0;
                last_fragment.1.end = this_frag.1.end;
            } else {
                fragments.push(this_frag);
            }
        }

        fragments
    }

    /// Estimate the physical pages that are within this range, on this shard.  This returns
    /// u32::MAX if the range spans relations: this return value should be interpreted as "large".
    pub fn page_count(&self) -> u32 {
        // Special cases for single keys like logical sizes
        if self.range.end == self.range.start.add(1) {
            return if self.shard_identity.is_key_disposable(&self.range.start) {
                0
            } else {
                1
            };
        }

        // We can only do an authentic calculation of contiguous key ranges
        if !is_contiguous_range(&self.range) {
            return u32::MAX;
        }

        // Special case for single sharded tenants: our logical and physical sizes are the same
        if self.shard_identity.count < ShardCount::new(2) {
            return contiguous_range_len(&self.range);
        }

        // Normal path: step through stripes and part-stripes in the range, evaluate whether each one belongs
        // to Self, and add the stripe's block count to our total if so.
        let mut result: u64 = 0;
        let mut cursor = self.range.start;
        while cursor < self.range.end {
            // Count up to the next stripe_size boundary or end of range
            let advance_by = self.distance_to_next_boundary(cursor);

            // If this blocks in this stripe belong to us, add them to our count
            if !self.shard_identity.is_key_disposable(&cursor) {
                result += advance_by as u64;
            }

            cursor = cursor.add(advance_by);
        }

        if result > u32::MAX as u64 {
            u32::MAX
        } else {
            result as u32
        }
    }

    /// Advance the cursor to the next potential fragment boundary: this is either
    /// a stripe boundary, or the end of the range.
    fn distance_to_next_boundary(&self, cursor: Key) -> u32 {
        let distance_to_range_end = contiguous_range_len(&(cursor..self.range.end));

        if self.shard_identity.count < ShardCount::new(2) {
            // Optimization: don't bother stepping through stripes if the tenant isn't sharded.
            return distance_to_range_end;
        }

        if cursor.field6 == 0xffffffff {
            // We are wrapping from one relation's logical size to the next relation's first data block
            return 1;
        }

        let stripe_index = cursor.field6 / self.shard_identity.stripe_size.0;
        let stripe_remainder = self.shard_identity.stripe_size.0
            - (cursor.field6 - stripe_index * self.shard_identity.stripe_size.0);

        if cfg!(debug_assertions) {
            // We should never overflow field5 and field6 -- our callers check this earlier
            // and would have returned their u32::MAX cases if the input range violated this.
            let next_cursor = cursor.add(stripe_remainder);
            debug_assert!(
                next_cursor.field1 == cursor.field1
                    && next_cursor.field2 == cursor.field2
                    && next_cursor.field3 == cursor.field3
                    && next_cursor.field4 == cursor.field4
                    && next_cursor.field5 == cursor.field5
            )
        }

        std::cmp::min(stripe_remainder, distance_to_range_end)
    }

    /// Whereas `page_count` estimates the number of pages physically in this range on this shard,
    /// this function simply calculates the number of pages in the space, without accounting for those
    /// pages that would not actually be stored on this node.
    ///
    /// Don't use this function in code that works with physical entities like layer files.
    pub fn raw_size(range: &Range<Key>) -> u32 {
        if is_contiguous_range(range) {
            contiguous_range_len(range)
        } else {
            u32::MAX
        }
    }
}

impl KeySpace {
    /// Create a key space with a single range.
    pub fn single(key_range: Range<Key>) -> Self {
        Self {
            ranges: vec![key_range],
        }
    }

    /// Partition a key space into roughly chunks of roughly 'target_size' bytes
    /// in each partition.
    ///
    pub fn partition(&self, shard_identity: &ShardIdentity, target_size: u64) -> KeyPartitioning {
        // Assume that each value is 8k in size.
        let target_nblocks = (target_size / BLCKSZ as u64) as u32;

        let mut parts = Vec::new();
        let mut current_part = Vec::new();
        let mut current_part_size: usize = 0;
        for range in &self.ranges {
            // While doing partitioning, wrap the range in ShardedRange so that our size calculations
            // will respect shard striping rather than assuming all keys within a range are present.
            let range = ShardedRange::new(range.clone(), shard_identity);

            // Chunk up the range into parts that each contain up to target_size local blocks
            for (frag_on_shard_size, frag_range) in range.fragment(target_nblocks) {
                // If appending the next contiguous range in the keyspace to the current
                // partition would cause it to be too large, and our current partition
                // covers at least one block that is physically present in this shard,
                // then start a new partition
                if current_part_size + frag_on_shard_size as usize > target_nblocks as usize
                    && current_part_size > 0
                {
                    parts.push(KeySpace {
                        ranges: current_part,
                    });
                    current_part = Vec::new();
                    current_part_size = 0;
                }
                current_part.push(frag_range.start..frag_range.end);
                current_part_size += frag_on_shard_size as usize;
            }
        }

        // add last partition that wasn't full yet.
        if !current_part.is_empty() {
            parts.push(KeySpace {
                ranges: current_part,
            });
        }

        KeyPartitioning { parts }
    }

    pub fn is_empty(&self) -> bool {
        self.total_raw_size() == 0
    }

    /// Merge another keyspace into the current one.
    /// Note: the keyspaces must not overlap (enforced via assertions). To merge overlapping key ranges, use `KeySpaceRandomAccum`.
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

    /// The size of the keyspace in pages, before accounting for sharding
    pub fn total_raw_size(&self) -> usize {
        self.ranges
            .iter()
            .map(|range| ShardedRange::raw_size(range) as usize)
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

    /// Check if the keyspace contains a key
    pub fn contains(&self, key: &Key) -> bool {
        self.overlaps(&(*key..key.next()))
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

/// Represents a partitioning of the sparse key space.
#[derive(Clone, Debug, Default)]
pub struct SparseKeyPartitioning {
    pub parts: Vec<SparseKeySpace>,
}

impl KeyPartitioning {
    pub fn new() -> Self {
        KeyPartitioning { parts: Vec::new() }
    }

    /// Convert a key partitioning to a sparse partition.
    pub fn into_sparse(self) -> SparseKeyPartitioning {
        SparseKeyPartitioning {
            parts: self.parts.into_iter().map(SparseKeySpace).collect(),
        }
    }
}

impl SparseKeyPartitioning {
    /// Note: use this function with caution. Attempt to handle a sparse keyspace in the same way as a dense keyspace will
    /// cause long/dead loops.
    pub fn into_dense(self) -> KeyPartitioning {
        KeyPartitioning {
            parts: self.parts.into_iter().map(|x| x.0).collect(),
        }
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
        self.size += ShardedRange::raw_size(&range) as u64;

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

    // The total number of keys in this object, ignoring any sharding effects that might cause some of
    // the keys to be omitted in storage on this shard.
    pub fn raw_size(&self) -> u64 {
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

    pub fn add_keyspace(&mut self, keyspace: KeySpace) {
        for range in keyspace.ranges {
            self.add_range(range);
        }
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

pub fn singleton_range(key: Key) -> Range<Key> {
    key..key.next()
}

#[cfg(test)]
mod tests {
    use rand::{RngCore, SeedableRng};

    use crate::{
        models::ShardParameters,
        shard::{ShardCount, ShardNumber},
    };

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

        let expected_size: u64 = ranges
            .iter()
            .map(|r| ShardedRange::raw_size(r) as u64)
            .sum();
        assert_eq!(accum.raw_size(), expected_size);

        assert_ks_eq(&accum.consume_keyspace(), ranges.clone());
        assert_eq!(accum.raw_size(), 0);

        assert_ks_eq(&accum.consume_keyspace(), vec![]);
        assert_eq!(accum.raw_size(), 0);

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
    #[test]
    fn sharded_range_relation_gap() {
        let shard_identity = ShardIdentity::new(
            ShardNumber(0),
            ShardCount::new(4),
            ShardParameters::DEFAULT_STRIPE_SIZE,
        )
        .unwrap();

        let range = ShardedRange::new(
            Range {
                start: Key::from_hex("000000067F00000005000040100300000000").unwrap(),
                end: Key::from_hex("000000067F00000005000040130000004000").unwrap(),
            },
            &shard_identity,
        );

        // Key range spans relations, expect MAX
        assert_eq!(range.page_count(), u32::MAX);
    }

    #[test]
    fn shard_identity_keyspaces_single_key() {
        let shard_identity = ShardIdentity::new(
            ShardNumber(1),
            ShardCount::new(4),
            ShardParameters::DEFAULT_STRIPE_SIZE,
        )
        .unwrap();

        let range = ShardedRange::new(
            Range {
                start: Key::from_hex("000000067f000000010000007000ffffffff").unwrap(),
                end: Key::from_hex("000000067f00000001000000700100000000").unwrap(),
            },
            &shard_identity,
        );
        // Single-key range on logical size key
        assert_eq!(range.page_count(), 1);
    }

    /// Test the helper that we use to identify ranges which go outside the data blocks of a single relation
    #[test]
    fn contiguous_range_check() {
        assert!(!is_contiguous_range(
            &(Key::from_hex("000000067f00000001000004df00fffffffe").unwrap()
                ..Key::from_hex("000000067f00000001000004df0100000003").unwrap())
        ),);

        // The ranges goes all the way up to the 0xffffffff, including it: this is
        // not considered a rel block range because 0xffffffff stores logical sizes,
        // not blocks.
        assert!(!is_contiguous_range(
            &(Key::from_hex("000000067f00000001000004df00fffffffe").unwrap()
                ..Key::from_hex("000000067f00000001000004df0100000000").unwrap())
        ),);

        // Keys within the normal data region of a relation
        assert!(is_contiguous_range(
            &(Key::from_hex("000000067f00000001000004df0000000000").unwrap()
                ..Key::from_hex("000000067f00000001000004df0000000080").unwrap())
        ),);

        // The logical size key of one forkno, then some blocks in the next
        assert!(is_contiguous_range(
            &(Key::from_hex("000000067f00000001000004df00ffffffff").unwrap()
                ..Key::from_hex("000000067f00000001000004df0100000080").unwrap())
        ),);
    }

    #[test]
    fn shard_identity_keyspaces_forkno_gap() {
        let shard_identity = ShardIdentity::new(
            ShardNumber(1),
            ShardCount::new(4),
            ShardParameters::DEFAULT_STRIPE_SIZE,
        )
        .unwrap();

        let range = ShardedRange::new(
            Range {
                start: Key::from_hex("000000067f00000001000004df00fffffffe").unwrap(),
                end: Key::from_hex("000000067f00000001000004df0100000003").unwrap(),
            },
            &shard_identity,
        );

        // Range spanning the end of one forkno and the start of the next: we do not attempt to
        // calculate a valid size, because we have no way to know if they keys between start
        // and end are actually in use.
        assert_eq!(range.page_count(), u32::MAX);
    }

    #[test]
    fn shard_identity_keyspaces_one_relation() {
        for shard_number in 0..4 {
            let shard_identity = ShardIdentity::new(
                ShardNumber(shard_number),
                ShardCount::new(4),
                ShardParameters::DEFAULT_STRIPE_SIZE,
            )
            .unwrap();

            let range = ShardedRange::new(
                Range {
                    start: Key::from_hex("000000067f00000001000000ae0000000000").unwrap(),
                    end: Key::from_hex("000000067f00000001000000ae0000000001").unwrap(),
                },
                &shard_identity,
            );

            // Very simple case: range covering block zero of one relation, where that block maps to shard zero
            if shard_number == 0 {
                assert_eq!(range.page_count(), 1);
            } else {
                // Other shards should perceive the range's size as zero
                assert_eq!(range.page_count(), 0);
            }
        }
    }

    /// Test helper: construct a ShardedRange and call fragment() on it, returning
    /// the total page count in the range and the fragments.
    fn do_fragment(
        range_start: Key,
        range_end: Key,
        shard_identity: &ShardIdentity,
        target_nblocks: u32,
    ) -> (u32, Vec<(u32, Range<Key>)>) {
        let range = ShardedRange::new(
            Range {
                start: range_start,
                end: range_end,
            },
            shard_identity,
        );

        let page_count = range.page_count();
        let fragments = range.fragment(target_nblocks);

        // Invariant: we always get at least one fragment
        assert!(!fragments.is_empty());

        // Invariant: the first/last fragment start/end should equal the input start/end
        assert_eq!(fragments.first().unwrap().1.start, range_start);
        assert_eq!(fragments.last().unwrap().1.end, range_end);

        if page_count > 0 {
            // Invariant: every fragment must contain at least one shard-local page, if the
            // total range contains at least one shard-local page
            let all_nonzero = fragments.iter().all(|f| f.0 > 0);
            if !all_nonzero {
                eprintln!("Found a zero-length fragment: {:?}", fragments);
            }
            assert!(all_nonzero);
        } else {
            // A range with no shard-local pages should always be returned as a single fragment
            assert_eq!(fragments, vec![(0, range_start..range_end)]);
        }

        // Invariant: fragments must be ordered and non-overlapping
        let mut last: Option<Range<Key>> = None;
        for frag in &fragments {
            if let Some(last) = last {
                assert!(frag.1.start >= last.end);
                assert!(frag.1.start > last.start);
            }
            last = Some(frag.1.clone())
        }

        // Invariant: fragments respect target_nblocks
        for frag in &fragments {
            assert!(frag.0 == u32::MAX || frag.0 <= target_nblocks);
        }

        (page_count, fragments)
    }

    /// Really simple tests for fragment(), on a range that just contains a single stripe
    /// for a single tenant.
    #[test]
    fn sharded_range_fragment_simple() {
        let shard_identity = ShardIdentity::new(
            ShardNumber(0),
            ShardCount::new(4),
            ShardParameters::DEFAULT_STRIPE_SIZE,
        )
        .unwrap();

        // A range which we happen to know covers exactly one stripe which belongs to this shard
        let input_start = Key::from_hex("000000067f00000001000000ae0000000000").unwrap();
        let input_end = Key::from_hex("000000067f00000001000000ae0000008000").unwrap();

        // Ask for stripe_size blocks, we get the whole stripe
        assert_eq!(
            do_fragment(input_start, input_end, &shard_identity, 32768),
            (32768, vec![(32768, input_start..input_end)])
        );

        // Ask for more, we still get the whole stripe
        assert_eq!(
            do_fragment(input_start, input_end, &shard_identity, 10000000),
            (32768, vec![(32768, input_start..input_end)])
        );

        // Ask for target_nblocks of half the stripe size, we get two halves
        assert_eq!(
            do_fragment(input_start, input_end, &shard_identity, 16384),
            (
                32768,
                vec![
                    (16384, input_start..input_start.add(16384)),
                    (16384, input_start.add(16384)..input_end)
                ]
            )
        );
    }

    #[test]
    fn sharded_range_fragment_multi_stripe() {
        let shard_identity = ShardIdentity::new(
            ShardNumber(0),
            ShardCount::new(4),
            ShardParameters::DEFAULT_STRIPE_SIZE,
        )
        .unwrap();

        // A range which covers multiple stripes, exactly one of which belongs to the current shard.
        let input_start = Key::from_hex("000000067f00000001000000ae0000000000").unwrap();
        let input_end = Key::from_hex("000000067f00000001000000ae0000020000").unwrap();
        // Ask for all the blocks, get a fragment that covers the whole range but reports
        // its size to be just the blocks belonging to our shard.
        assert_eq!(
            do_fragment(input_start, input_end, &shard_identity, 131072),
            (32768, vec![(32768, input_start..input_end)])
        );

        // Ask for a sub-stripe quantity
        assert_eq!(
            do_fragment(input_start, input_end, &shard_identity, 16000),
            (
                32768,
                vec![
                    (16000, input_start..input_start.add(16000)),
                    (16000, input_start.add(16000)..input_start.add(32000)),
                    (768, input_start.add(32000)..input_end),
                ]
            )
        );

        // Try on a range that starts slightly after our owned stripe
        assert_eq!(
            do_fragment(input_start.add(1), input_end, &shard_identity, 131072),
            (32767, vec![(32767, input_start.add(1)..input_end)])
        );
    }

    /// Test our calculations work correctly when we start a range from the logical size key of
    /// a previous relation.
    #[test]
    fn sharded_range_fragment_starting_from_logical_size() {
        let input_start = Key::from_hex("000000067f00000001000000ae00ffffffff").unwrap();
        let input_end = Key::from_hex("000000067f00000001000000ae0100008000").unwrap();

        // Shard 0 owns the first stripe in the relation, and the preceding logical size is shard local too
        let shard_identity = ShardIdentity::new(
            ShardNumber(0),
            ShardCount::new(4),
            ShardParameters::DEFAULT_STRIPE_SIZE,
        )
        .unwrap();
        assert_eq!(
            do_fragment(input_start, input_end, &shard_identity, 0x10000),
            (0x8001, vec![(0x8001, input_start..input_end)])
        );

        // Shard 1 does not own the first stripe in the relation, but it does own the logical size (all shards
        // store all logical sizes)
        let shard_identity = ShardIdentity::new(
            ShardNumber(1),
            ShardCount::new(4),
            ShardParameters::DEFAULT_STRIPE_SIZE,
        )
        .unwrap();
        assert_eq!(
            do_fragment(input_start, input_end, &shard_identity, 0x10000),
            (0x1, vec![(0x1, input_start..input_end)])
        );
    }

    /// Test that ShardedRange behaves properly when used on un-sharded data
    #[test]
    fn sharded_range_fragment_unsharded() {
        let shard_identity = ShardIdentity::unsharded();

        let input_start = Key::from_hex("000000067f00000001000000ae0000000000").unwrap();
        let input_end = Key::from_hex("000000067f00000001000000ae0000010000").unwrap();
        assert_eq!(
            do_fragment(input_start, input_end, &shard_identity, 0x8000),
            (
                0x10000,
                vec![
                    (0x8000, input_start..input_start.add(0x8000)),
                    (0x8000, input_start.add(0x8000)..input_start.add(0x10000))
                ]
            )
        );
    }

    #[test]
    fn sharded_range_fragment_cross_relation() {
        let shard_identity = ShardIdentity::unsharded();

        // A range that spans relations: expect fragmentation to give up and return a u32::MAX size
        let input_start = Key::from_hex("000000067f00000001000000ae0000000000").unwrap();
        let input_end = Key::from_hex("000000068f00000001000000ae0000010000").unwrap();
        assert_eq!(
            do_fragment(input_start, input_end, &shard_identity, 0x8000),
            (u32::MAX, vec![(u32::MAX, input_start..input_end),])
        );

        // Same, but using a sharded identity
        let shard_identity = ShardIdentity::new(
            ShardNumber(0),
            ShardCount::new(4),
            ShardParameters::DEFAULT_STRIPE_SIZE,
        )
        .unwrap();
        assert_eq!(
            do_fragment(input_start, input_end, &shard_identity, 0x8000),
            (u32::MAX, vec![(u32::MAX, input_start..input_end),])
        );
    }

    #[test]
    fn sharded_range_fragment_tiny_nblocks() {
        let shard_identity = ShardIdentity::unsharded();

        // A range that spans relations: expect fragmentation to give up and return a u32::MAX size
        let input_start = Key::from_hex("000000067F00000001000004E10000000000").unwrap();
        let input_end = Key::from_hex("000000067F00000001000004E10000000038").unwrap();
        assert_eq!(
            do_fragment(input_start, input_end, &shard_identity, 16),
            (
                0x38,
                vec![
                    (16, input_start..input_start.add(16)),
                    (16, input_start.add(16)..input_start.add(32)),
                    (16, input_start.add(32)..input_start.add(48)),
                    (8, input_start.add(48)..input_end),
                ]
            )
        );
    }

    #[test]
    fn sharded_range_fragment_fuzz() {
        // Use a fixed seed: we don't want to explicitly pick values, but we do want
        // the test to be reproducible.
        let mut prng = rand::rngs::StdRng::seed_from_u64(0xdeadbeef);

        for _i in 0..1000 {
            let shard_identity = if prng.next_u32() % 2 == 0 {
                ShardIdentity::unsharded()
            } else {
                let shard_count = prng.next_u32() % 127 + 1;
                ShardIdentity::new(
                    ShardNumber((prng.next_u32() % shard_count) as u8),
                    ShardCount::new(shard_count as u8),
                    ShardParameters::DEFAULT_STRIPE_SIZE,
                )
                .unwrap()
            };

            let target_nblocks = prng.next_u32() % 65536 + 1;

            let start_offset = prng.next_u32() % 16384;

            // Try ranges up to 4GiB in size, that are always at least 1
            let range_size = prng.next_u32() % 8192 + 1;

            // A range that spans relations: expect fragmentation to give up and return a u32::MAX size
            let input_start = Key::from_hex("000000067F00000001000004E10000000000")
                .unwrap()
                .add(start_offset);
            let input_end = input_start.add(range_size);

            // This test's main success conditions are the invariants baked into do_fragment
            let (_total_size, fragments) =
                do_fragment(input_start, input_end, &shard_identity, target_nblocks);

            // Pick a random key within the range and check it appears in the output
            let example_key = input_start.add(prng.next_u32() % range_size);

            // Panic on unwrap if it isn't found
            let example_key_frag = fragments
                .iter()
                .find(|f| f.1.contains(&example_key))
                .unwrap();

            // Check that the fragment containing our random key has a nonzero size if
            // that key is shard-local
            let example_key_local = !shard_identity.is_key_disposable(&example_key);
            if example_key_local {
                assert!(example_key_frag.0 > 0);
            }
        }
    }
}
