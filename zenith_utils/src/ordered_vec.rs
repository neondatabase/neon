use std::{
    collections::BTreeMap,
    ops::{Bound, RangeBounds},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
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
                Ok(idx) => idx,
                Err(idx) => idx + 1,
            },
            Bound::Excluded(key) => match self.0.binary_search_by_key(key, extract_key) {
                Ok(idx) => idx + 1,
                Err(idx) => idx + 1,
            },
            Bound::Unbounded => self.0.len(),
        };

        &self.0[start_idx..end_idx]
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
