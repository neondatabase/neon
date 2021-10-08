use std::{cmp::Ordering, ops::RangeBounds};

/// Ordered map datastructure implemented in a Vec.
/// Append only - can only add keys that are larger than the
/// current max key.
#[derive(Clone, Debug)]
pub struct VecMap<K, V>(Vec<(K, V)>);

impl<K, V> Default for VecMap<K, V> {
    fn default() -> Self {
        VecMap(Default::default())
    }
}

#[derive(Debug)]
pub struct InvalidKey;

impl<K: Ord, V> VecMap<K, V> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_slice(&self) -> &[(K, V)] {
        self.0.as_slice()
    }

    /// This function may panic if given a range where the lower bound is
    /// greater than the upper bound.
    pub fn slice_range<R: RangeBounds<K>>(&self, range: R) -> &[(K, V)] {
        use std::ops::Bound::*;

        let binary_search = |k: &K| self.0.binary_search_by_key(&k, extract_key);

        let start_idx = match range.start_bound() {
            Unbounded => 0,
            Included(k) => binary_search(k).unwrap_or_else(std::convert::identity),
            Excluded(k) => match binary_search(k) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            },
        };

        let end_idx = match range.end_bound() {
            Unbounded => self.0.len(),
            Included(k) => match binary_search(k) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            },
            Excluded(k) => binary_search(k).unwrap_or_else(std::convert::identity),
        };

        &self.0[start_idx..end_idx]
    }

    /// Add a key value pair to the map.
    /// If [`key`] is less than or equal to the current maximum key
    /// the pair will not be added and InvalidKey error will be returned.
    pub fn append(&mut self, key: K, value: V) -> Result<(), InvalidKey> {
        if let Some((last_key, _last_value)) = self.0.last() {
            if &key <= last_key {
                return Err(InvalidKey);
            }
        }

        self.0.push((key, value));
        Ok(())
    }

    /// Update the maximum key value pair or add a new key value pair to the map.
    /// If [`key`] is less than the current maximum key no updates or additions
    /// will occur and InvalidKey error will be returned.
    pub fn append_or_update_last(&mut self, key: K, mut value: V) -> Result<Option<V>, InvalidKey> {
        if let Some((last_key, last_value)) = self.0.last_mut() {
            match key.cmp(last_key) {
                Ordering::Less => return Err(InvalidKey),
                Ordering::Equal => {
                    std::mem::swap(last_value, &mut value);
                    return Ok(Some(value));
                }
                Ordering::Greater => {}
            }
        }

        self.0.push((key, value));
        Ok(None)
    }

    /// Split the map into two.
    ///
    /// The left map contains everything before [`cutoff`] (exclusive).
    /// Right map contains [`cutoff`] and everything after (inclusive).
    pub fn split_at(&self, cutoff: &K) -> (Self, Self)
    where
        K: Clone,
        V: Clone,
    {
        let split_idx = self
            .0
            .binary_search_by_key(&cutoff, extract_key)
            .unwrap_or_else(std::convert::identity);

        (
            VecMap(self.0[..split_idx].to_vec()),
            VecMap(self.0[split_idx..].to_vec()),
        )
    }

    /// Move items from [`other`] to the end of [`self`], leaving [`other`] empty.
    /// If any keys in [`other`] is less than or equal to any key in [`self`],
    /// [`InvalidKey`] error will be returned and no mutation will occur.
    pub fn extend(&mut self, other: &mut Self) -> Result<(), InvalidKey> {
        let self_last_opt = self.0.last().map(extract_key);
        let other_first_opt = other.0.last().map(extract_key);

        if let (Some(self_last), Some(other_first)) = (self_last_opt, other_first_opt) {
            if self_last >= other_first {
                return Err(InvalidKey);
            }
        }

        self.0.append(&mut other.0);

        Ok(())
    }
}

fn extract_key<K, V>(entry: &(K, V)) -> &K {
    &entry.0
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, ops::Bound};

    use super::VecMap;

    #[test]
    fn unbounded_range() {
        let mut vec = VecMap::default();
        vec.append(0, ()).unwrap();

        assert_eq!(vec.slice_range(0..0), &[]);
    }

    #[test]
    #[should_panic]
    fn invalid_ordering_range() {
        let mut vec = VecMap::default();
        vec.append(0, ()).unwrap();

        #[allow(clippy::reversed_empty_ranges)]
        vec.slice_range(1..0);
    }

    #[test]
    fn range_tests() {
        let mut vec = VecMap::default();
        vec.append(0, ()).unwrap();
        vec.append(2, ()).unwrap();
        vec.append(4, ()).unwrap();

        assert_eq!(vec.slice_range(0..0), &[]);
        assert_eq!(vec.slice_range(0..1), &[(0, ())]);
        assert_eq!(vec.slice_range(0..2), &[(0, ())]);
        assert_eq!(vec.slice_range(0..3), &[(0, ()), (2, ())]);

        assert_eq!(vec.slice_range(..0), &[]);
        assert_eq!(vec.slice_range(..1), &[(0, ())]);

        assert_eq!(vec.slice_range(..3), &[(0, ()), (2, ())]);
        assert_eq!(vec.slice_range(..3), &[(0, ()), (2, ())]);

        assert_eq!(vec.slice_range(0..=0), &[(0, ())]);
        assert_eq!(vec.slice_range(0..=1), &[(0, ())]);
        assert_eq!(vec.slice_range(0..=2), &[(0, ()), (2, ())]);
        assert_eq!(vec.slice_range(0..=3), &[(0, ()), (2, ())]);

        assert_eq!(vec.slice_range(..=0), &[(0, ())]);
        assert_eq!(vec.slice_range(..=1), &[(0, ())]);
        assert_eq!(vec.slice_range(..=2), &[(0, ()), (2, ())]);
        assert_eq!(vec.slice_range(..=3), &[(0, ()), (2, ())]);
    }

    struct BoundIter {
        min: i32,
        max: i32,

        next: Option<Bound<i32>>,
    }

    impl BoundIter {
        fn new(min: i32, max: i32) -> Self {
            Self {
                min,
                max,

                next: Some(Bound::Unbounded),
            }
        }
    }

    impl Iterator for BoundIter {
        type Item = Bound<i32>;

        fn next(&mut self) -> Option<Self::Item> {
            let cur = self.next?;

            self.next = match &cur {
                Bound::Unbounded => Some(Bound::Included(self.min)),
                Bound::Included(x) => {
                    if *x >= self.max {
                        Some(Bound::Excluded(self.min))
                    } else {
                        Some(Bound::Included(x + 1))
                    }
                }
                Bound::Excluded(x) => {
                    if *x >= self.max {
                        None
                    } else {
                        Some(Bound::Excluded(x + 1))
                    }
                }
            };

            Some(cur)
        }
    }

    #[test]
    fn range_exhaustive() {
        let map: BTreeMap<i32, ()> = (1..=7).step_by(2).map(|x| (x, ())).collect();
        let mut vec = VecMap::default();
        for &key in map.keys() {
            vec.append(key, ()).unwrap();
        }

        const RANGE_MIN: i32 = 0;
        const RANGE_MAX: i32 = 8;
        for lower_bound in BoundIter::new(RANGE_MIN, RANGE_MAX) {
            let ub_min = match lower_bound {
                Bound::Unbounded => RANGE_MIN,
                Bound::Included(x) => x,
                Bound::Excluded(x) => x + 1,
            };
            for upper_bound in BoundIter::new(ub_min, RANGE_MAX) {
                let map_range: Vec<(i32, ())> = map
                    .range((lower_bound, upper_bound))
                    .map(|(&x, _)| (x, ()))
                    .collect();
                let vec_slice = vec.slice_range((lower_bound, upper_bound));

                assert_eq!(map_range, vec_slice);
            }
        }
    }

    #[test]
    fn extend() {
        let mut left = VecMap::default();
        left.append(0, ()).unwrap();
        assert_eq!(left.as_slice(), &[(0, ())]);

        let mut empty = VecMap::default();
        left.extend(&mut empty).unwrap();
        assert_eq!(left.as_slice(), &[(0, ())]);
        assert_eq!(empty.as_slice(), &[]);

        let mut right = VecMap::default();
        right.append(1, ()).unwrap();

        left.extend(&mut right).unwrap();

        assert_eq!(left.as_slice(), &[(0, ()), (1, ())]);
        assert_eq!(right.as_slice(), &[]);

        let mut zero_map = VecMap::default();
        zero_map.append(0, ()).unwrap();

        left.extend(&mut zero_map).unwrap_err();
        assert_eq!(left.as_slice(), &[(0, ()), (1, ())]);
        assert_eq!(zero_map.as_slice(), &[(0, ())]);

        let mut one_map = VecMap::default();
        one_map.append(1, ()).unwrap();

        left.extend(&mut one_map).unwrap_err();
        assert_eq!(left.as_slice(), &[(0, ()), (1, ())]);
        assert_eq!(one_map.as_slice(), &[(1, ())]);
    }
}
