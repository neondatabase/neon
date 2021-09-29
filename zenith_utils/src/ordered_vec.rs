use std::{
    collections::BTreeMap,
    ops::{Bound, RangeBounds},
};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderedVec<K, V>(Vec<(K, V)>);

impl<K, V> Default for OrderedVec<K, V> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<K: Ord + Copy, V> OrderedVec<K, V> {
    pub fn iter(&self) -> std::slice::Iter<'_, (K, V)> {
        self.0.iter()
    }

    pub fn range<R: RangeBounds<K>>(&self, range: R) -> &[(K, V)] {
        match (range.start_bound(), range.end_bound()) {
            (Bound::Excluded(l), Bound::Excluded(u)) if l == u => panic!("Invalid excluded"),
            // TODO check for l <= x with or patterns
            _ => {}
        }

        let start_idx = match range.start_bound() {
            Bound::Included(key) => match self.0.binary_search_by_key(key, extract_key) {
                Ok(idx) => idx,
                Err(idx) => idx,
            },
            Bound::Excluded(key) => match self.0.binary_search_by_key(key, extract_key) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            },
            Bound::Unbounded => 0,
        };

        let end_idx = match range.end_bound() {
            Bound::Included(key) => match self.0.binary_search_by_key(key, extract_key) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            },
            Bound::Excluded(key) => match self.0.binary_search_by_key(key, extract_key) {
                Ok(idx) => idx,
                Err(idx) => idx,
            },
            Bound::Unbounded => self.0.len(),
        };

        &self.0[start_idx..end_idx]
    }

    pub fn append(&mut self, key: K, value: V) {
        if let Some((last_key, _last_value)) = self.0.last() {
            debug_assert!(last_key < &key);
        }

        self.0.push((key, value));
    }

    pub fn append_update(&mut self, key: K, value: V) {
        if let Some((last_key, this_value)) = self.0.last_mut() {
            use std::cmp::Ordering;
            match (*last_key).cmp(&key) {
                Ordering::Less => {}
                Ordering::Equal => {
                    *this_value = value;
                    return;
                }
                Ordering::Greater => {
                    panic!();
                }
            }
        }

        self.0.push((key, value));
    }

    pub fn extend(&mut self, other: OrderedVec<K, V>) {
        if let (Some((last, _)), Some((first, _))) = (self.0.last(), other.0.first()) {
            assert!(last < first);
        }

        self.0.extend(other.0);
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<K: Ord, V> From<BTreeMap<K, V>> for OrderedVec<K, V> {
    fn from(map: BTreeMap<K, V>) -> Self {
        let vec: Vec<(K, V)> = map.into_iter().collect();

        // TODO probably change this
        for windows in vec.windows(2) {
            let (k1, _data1) = &windows[0];
            let (k2, _data2) = &windows[1];
            debug_assert!(k1 < k2);
        }

        OrderedVec(vec)
    }
}

fn extract_key<K: Copy, V>(pair: &(K, V)) -> K {
    pair.0
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        ops::{Bound, RangeBounds},
    };

    use super::OrderedVec;

    #[test]
    #[should_panic]
    fn invalid_range() {
        let mut map = BTreeMap::new();
        map.insert(0, ());

        let vec: OrderedVec<i32, ()> = OrderedVec::from(map);
        struct InvalidRange;
        impl RangeBounds<i32> for InvalidRange {
            fn start_bound(&self) -> Bound<&i32> {
                Bound::Excluded(&0)
            }

            fn end_bound(&self) -> Bound<&i32> {
                Bound::Excluded(&0)
            }
        }

        vec.range(InvalidRange);
    }

    #[test]
    fn range_tests() {
        let mut map = BTreeMap::new();
        map.insert(0, ());
        map.insert(2, ());
        map.insert(4, ());
        let vec = OrderedVec::from(map);

        assert_eq!(vec.range(0..0), &[]);
        assert_eq!(vec.range(0..1), &[(0, ())]);
        assert_eq!(vec.range(0..2), &[(0, ())]);
        assert_eq!(vec.range(0..3), &[(0, ()), (2, ())]);

        assert_eq!(vec.range(..0), &[]);
        assert_eq!(vec.range(..1), &[(0, ())]);

        assert_eq!(vec.range(..3), &[(0, ()), (2, ())]);
        assert_eq!(vec.range(..3), &[(0, ()), (2, ())]);

        assert_eq!(vec.range(0..=0), &[(0, ())]);
        assert_eq!(vec.range(0..=1), &[(0, ())]);
        assert_eq!(vec.range(0..=2), &[(0, ()), (2, ())]);
        assert_eq!(vec.range(0..=3), &[(0, ()), (2, ())]);

        assert_eq!(vec.range(..=0), &[(0, ())]);
        assert_eq!(vec.range(..=1), &[(0, ())]);
        assert_eq!(vec.range(..=2), &[(0, ()), (2, ())]);
        assert_eq!(vec.range(..=3), &[(0, ()), (2, ())]);
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
        let vec = OrderedVec::from(map.clone());

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
                let vec_slice = vec.range((lower_bound, upper_bound));

                assert_eq!(map_range, vec_slice);
            }
        }
    }
}
