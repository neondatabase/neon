use std::ops::Range;

// NOTE the `im` crate has 20x more downloads and also has
// persistent/immutable BTree. But it's bugged so rpds is a
// better choice <https://github.com/neondatabase/neon/issues/3395>
use rpds::RedBlackTreeMapSync;

/// Data structure that can efficiently:
/// - find the latest layer by lsn.end at a given key
/// - iterate the latest layers in a key range
/// - insert layers in non-decreasing lsn.start order
///
/// For a detailed explanation and justification of this approach, see:
/// <https://neon.tech/blog/persistent-structures-in-neons-wal-indexing>
///
/// NOTE The struct is parameterized over Value for easier
///      testing, but in practice it's some sort of layer.
pub struct LayerCoverage<Value> {
    /// For every change in coverage (as we sweep the key space)
    /// we store (lsn.end, value).
    ///
    /// NOTE We use an immutable/persistent tree so that we can keep historic
    ///      versions of this coverage without cloning the whole thing and
    ///      incurring quadratic memory cost. See HistoricLayerCoverage.
    ///
    /// NOTE We use the Sync version of the map because we want Self to
    ///      be Sync. Using nonsync might be faster, if we can work with
    ///      that.
    nodes: RedBlackTreeMapSync<i128, Option<(u64, Value)>>,
}

impl<T: Clone> Default for LayerCoverage<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Value: Clone> LayerCoverage<Value> {
    pub fn new() -> Self {
        Self {
            nodes: RedBlackTreeMapSync::default(),
        }
    }

    /// Helper function to subdivide the key range without changing any values
    ///
    /// This operation has no semantic effect by itself. It only helps us pin in
    /// place the part of the coverage we don't want to change when inserting.
    ///
    /// As an analogy, think of a polygon. If you add a vertex along one of the
    /// segments, the polygon is still the same, but it behaves differently when
    /// we move or delete one of the other points.
    ///
    /// Complexity: O(log N)
    fn add_node(&mut self, key: i128) {
        let value = match self.nodes.range(..=key).last() {
            Some((_, Some(v))) => Some(v.clone()),
            Some((_, None)) => None,
            None => None,
        };
        self.nodes.insert_mut(key, value);
    }

    /// Insert a layer.
    ///
    /// Complexity: worst case O(N), in practice O(log N). See NOTE in implementation.
    pub fn insert(&mut self, key: Range<i128>, lsn: Range<u64>, value: Value) {
        // Add nodes at endpoints
        //
        // NOTE The order of lines is important. We add nodes at the start
        // and end of the key range **before updating any nodes** in order
        // to pin down the current coverage outside of the relevant key range.
        // Only the coverage inside the layer's key range should change.
        self.add_node(key.start);
        self.add_node(key.end);

        // Raise the height where necessary
        //
        // NOTE This loop is worst case O(N), but amortized O(log N) in the special
        // case when rectangles have no height. In practice I don't think we'll see
        // the kind of layer intersections needed to trigger O(N) behavior. The worst
        // case is N/2 horizontal layers overlapped with N/2 vertical layers in a
        // grid pattern.
        let mut to_update = Vec::new();
        let mut to_remove = Vec::new();
        let mut prev_covered = false;
        for (k, node) in self.nodes.range(key) {
            let needs_cover = match node {
                None => true,
                Some((h, _)) => h < &lsn.end,
            };
            if needs_cover {
                match prev_covered {
                    true => to_remove.push(*k),
                    false => to_update.push(*k),
                }
            }
            prev_covered = needs_cover;
        }
        // TODO check if the nodes inserted at key.start and key.end are safe
        //      to remove. It's fine to keep them but they could be redundant.
        for k in to_update {
            self.nodes.insert_mut(k, Some((lsn.end, value.clone())));
        }
        for k in to_remove {
            self.nodes.remove_mut(&k);
        }
    }

    /// Get the latest (by lsn.end) layer at a given key
    ///
    /// Complexity: O(log N)
    pub fn query(&self, key: i128) -> Option<Value> {
        self.nodes
            .range(..=key)
            .next_back()?
            .1
            .as_ref()
            .map(|(_, v)| v.clone())
    }

    /// Iterate the changes in layer coverage in a given range. You will likely
    /// want to start with self.query(key.start), and then follow up with self.range
    ///
    /// Complexity: O(log N + result_size)
    pub fn range(&self, key: Range<i128>) -> impl '_ + Iterator<Item = (i128, Option<Value>)> {
        self.nodes
            .range(key)
            .map(|(k, v)| (*k, v.as_ref().map(|x| x.1.clone())))
    }

    /// Returns an iterator which includes all coverage changes for layers that intersect
    /// with the provided range.
    pub fn range_overlaps(
        &self,
        key_range: &Range<i128>,
    ) -> impl Iterator<Item = (i128, Option<Value>)> + '_
    where
        Value: Eq,
    {
        let first_change = self.query(key_range.start);
        match first_change {
            Some(change) => {
                // If the start of the range is covered, we have to deal with two cases:
                // 1. Start of the range is aligned with the start of a layer.
                // In this case the return of `self.range` will contain the layer which aligns with the start of the key range.
                // We advance said iterator to avoid duplicating the first change.
                // 2. Start of the range is not aligned with the start of a layer.
                let range = key_range.start..key_range.end;
                let mut range_coverage = self.range(range).peekable();
                if range_coverage
                    .peek()
                    .is_some_and(|c| c.1.as_ref() == Some(&change))
                {
                    range_coverage.next();
                }
                itertools::Either::Left(
                    std::iter::once((key_range.start, Some(change))).chain(range_coverage),
                )
            }
            None => {
                let range = key_range.start..key_range.end;
                let coverage = self.range(range);
                itertools::Either::Right(coverage)
            }
        }
    }
    /// O(1) clone
    pub fn clone(&self) -> Self {
        Self {
            nodes: self.nodes.clone(),
        }
    }
}

/// Image and delta coverage at a specific LSN.
pub struct LayerCoverageTuple<Value> {
    pub image_coverage: LayerCoverage<Value>,
    pub delta_coverage: LayerCoverage<Value>,
}

impl<T: Clone> Default for LayerCoverageTuple<T> {
    fn default() -> Self {
        Self {
            image_coverage: LayerCoverage::default(),
            delta_coverage: LayerCoverage::default(),
        }
    }
}

impl<Value: Clone> LayerCoverageTuple<Value> {
    pub fn clone(&self) -> Self {
        Self {
            image_coverage: self.image_coverage.clone(),
            delta_coverage: self.delta_coverage.clone(),
        }
    }
}
