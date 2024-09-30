use std::{alloc::Layout, cmp::Ordering, ops::RangeBounds};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VecMapOrdering {
    Greater,
    GreaterOrEqual,
}

/// Ordered map datastructure implemented in a Vec.
///
/// Append only - can only add keys that are larger than the
/// current max key.
/// Ordering can be adjusted using [`VecMapOrdering`]
/// during `VecMap` construction.
#[derive(Clone, Debug)]
pub struct VecMap<K, V> {
    data: Vec<(K, V)>,
    ordering: VecMapOrdering,
}

impl<K, V> Default for VecMap<K, V> {
    fn default() -> Self {
        VecMap {
            data: Default::default(),
            ordering: VecMapOrdering::Greater,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum VecMapError {
    #[error("Key violates ordering constraint")]
    InvalidKey,
    #[error("Mismatched ordering constraints")]
    ExtendOrderingError,
}

impl<K: Ord, V> VecMap<K, V> {
    pub fn new(ordering: VecMapOrdering) -> Self {
        Self {
            data: Vec::new(),
            ordering,
        }
    }

    pub fn with_capacity(capacity: usize, ordering: VecMapOrdering) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            ordering,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn as_slice(&self) -> &[(K, V)] {
        self.data.as_slice()
    }

    /// This function may panic if given a range where the lower bound is
    /// greater than the upper bound.
    pub fn slice_range<R: RangeBounds<K>>(&self, range: R) -> &[(K, V)] {
        use std::ops::Bound::*;

        let binary_search = |k: &K| self.data.binary_search_by_key(&k, extract_key);

        let start_idx = match range.start_bound() {
            Unbounded => 0,
            Included(k) => binary_search(k).unwrap_or_else(std::convert::identity),
            Excluded(k) => match binary_search(k) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            },
        };

        let end_idx = match range.end_bound() {
            Unbounded => self.data.len(),
            Included(k) => match binary_search(k) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            },
            Excluded(k) => binary_search(k).unwrap_or_else(std::convert::identity),
        };

        &self.data[start_idx..end_idx]
    }

    /// Add a key value pair to the map.
    /// If `key` is not respective of the `self` ordering the
    /// pair will not be added and `InvalidKey` error will be returned.
    pub fn append(&mut self, key: K, value: V) -> Result<usize, VecMapError> {
        self.validate_key_order(&key)?;

        let delta_size = self.instrument_vec_op(|vec| vec.push((key, value)));
        Ok(delta_size)
    }

    /// Update the maximum key value pair or add a new key value pair to the map.
    /// If `key` is not respective of the `self` ordering no updates or additions
    /// will occur and `InvalidKey` error will be returned.
    pub fn append_or_update_last(
        &mut self,
        key: K,
        mut value: V,
    ) -> Result<(Option<V>, usize), VecMapError> {
        if let Some((last_key, last_value)) = self.data.last_mut() {
            match key.cmp(last_key) {
                Ordering::Less => return Err(VecMapError::InvalidKey),
                Ordering::Equal => {
                    std::mem::swap(last_value, &mut value);
                    const DELTA_SIZE: usize = 0;
                    return Ok((Some(value), DELTA_SIZE));
                }
                Ordering::Greater => {}
            }
        }

        let delta_size = self.instrument_vec_op(|vec| vec.push((key, value)));
        Ok((None, delta_size))
    }

    /// Move items from `other` to the end of `self`, leaving `other` empty.
    /// If the `other` ordering is different from `self` ordering
    /// `ExtendOrderingError` error will be returned.
    /// If any keys in `other` is not respective of the ordering defined in
    /// `self`, `InvalidKey` error will be returned and no mutation will occur.
    pub fn extend(&mut self, other: &mut Self) -> Result<usize, VecMapError> {
        if self.ordering != other.ordering {
            return Err(VecMapError::ExtendOrderingError);
        }

        let other_first_opt = other.data.last().map(extract_key);
        if let Some(other_first) = other_first_opt {
            self.validate_key_order(other_first)?;
        }

        let delta_size = self.instrument_vec_op(|vec| vec.append(&mut other.data));
        Ok(delta_size)
    }

    /// Validate the current last key in `self` and key being
    /// inserted against the order defined in `self`.
    fn validate_key_order(&self, key: &K) -> Result<(), VecMapError> {
        if let Some(last_key) = self.data.last().map(extract_key) {
            match (&self.ordering, &key.cmp(last_key)) {
                (VecMapOrdering::Greater, Ordering::Less | Ordering::Equal) => {
                    return Err(VecMapError::InvalidKey);
                }
                (VecMapOrdering::Greater, Ordering::Greater) => {}
                (VecMapOrdering::GreaterOrEqual, Ordering::Less) => {
                    return Err(VecMapError::InvalidKey);
                }
                (VecMapOrdering::GreaterOrEqual, Ordering::Equal | Ordering::Greater) => {}
            }
        }

        Ok(())
    }

    /// Instrument an operation on the underlying [`Vec`].
    /// Will panic if the operation decreases capacity.
    /// Returns the increase in memory usage caused by the op.
    fn instrument_vec_op(&mut self, op: impl FnOnce(&mut Vec<(K, V)>)) -> usize {
        let old_cap = self.data.capacity();
        op(&mut self.data);
        let new_cap = self.data.capacity();

        match old_cap.cmp(&new_cap) {
            Ordering::Less => {
                let old_size = Layout::array::<(K, V)>(old_cap).unwrap().size();
                let new_size = Layout::array::<(K, V)>(new_cap).unwrap().size();
                new_size - old_size
            }
            Ordering::Equal => 0,
            Ordering::Greater => panic!("VecMap capacity shouldn't ever decrease"),
        }
    }

    /// Similar to `from_iter` defined in `FromIter` trait except
    /// that it accepts an [`VecMapOrdering`]
    pub fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I, ordering: VecMapOrdering) -> Self {
        let iter = iter.into_iter();
        let initial_capacity = {
            match iter.size_hint() {
                (lower_bound, None) => lower_bound,
                (_, Some(upper_bound)) => upper_bound,
            }
        };

        let mut vec_map = VecMap::with_capacity(initial_capacity, ordering);
        for (key, value) in iter {
            vec_map
                .append(key, value)
                .expect("The passed collection needs to be sorted!");
        }

        vec_map
    }
}

impl<K: Ord, V> IntoIterator for VecMap<K, V> {
    type Item = (K, V);
    type IntoIter = std::vec::IntoIter<(K, V)>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

fn extract_key<K, V>(entry: &(K, V)) -> &K {
    &entry.0
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, ops::Bound};

    use super::{VecMap, VecMapOrdering};

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

        let mut map_greater_or_equal = VecMap::new(VecMapOrdering::GreaterOrEqual);
        map_greater_or_equal.append(2, ()).unwrap();
        map_greater_or_equal.append(2, ()).unwrap();

        left.extend(&mut map_greater_or_equal).unwrap_err();
        assert_eq!(left.as_slice(), &[(0, ()), (1, ())]);
        assert_eq!(map_greater_or_equal.as_slice(), &[(2, ()), (2, ())]);
    }

    #[test]
    fn extend_with_ordering() {
        let mut left = VecMap::new(VecMapOrdering::GreaterOrEqual);
        left.append(0, ()).unwrap();
        assert_eq!(left.as_slice(), &[(0, ())]);

        let mut greater_right = VecMap::new(VecMapOrdering::Greater);
        greater_right.append(0, ()).unwrap();
        left.extend(&mut greater_right).unwrap_err();
        assert_eq!(left.as_slice(), &[(0, ())]);

        let mut greater_or_equal_right = VecMap::new(VecMapOrdering::GreaterOrEqual);
        greater_or_equal_right.append(2, ()).unwrap();
        greater_or_equal_right.append(2, ()).unwrap();
        left.extend(&mut greater_or_equal_right).unwrap();
        assert_eq!(left.as_slice(), &[(0, ()), (2, ()), (2, ())]);
    }

    #[test]
    fn vec_map_from_sorted() {
        let vec = vec![(1, ()), (2, ()), (3, ()), (6, ())];
        let vec_map = VecMap::from_iter(vec, VecMapOrdering::Greater);
        assert_eq!(vec_map.as_slice(), &[(1, ()), (2, ()), (3, ()), (6, ())]);

        let vec = vec![(1, ()), (2, ()), (3, ()), (3, ()), (6, ()), (6, ())];
        let vec_map = VecMap::from_iter(vec, VecMapOrdering::GreaterOrEqual);
        assert_eq!(
            vec_map.as_slice(),
            &[(1, ()), (2, ()), (3, ()), (3, ()), (6, ()), (6, ())]
        );
    }

    #[test]
    #[should_panic]
    fn vec_map_from_unsorted_greater() {
        let vec = vec![(1, ()), (2, ()), (2, ()), (3, ()), (6, ())];
        let _ = VecMap::from_iter(vec, VecMapOrdering::Greater);
    }

    #[test]
    #[should_panic]
    fn vec_map_from_unsorted_greater_or_equal() {
        let vec = vec![(1, ()), (2, ()), (3, ()), (6, ()), (5, ())];
        let _ = VecMap::from_iter(vec, VecMapOrdering::GreaterOrEqual);
    }
}
