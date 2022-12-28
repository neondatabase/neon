use std::ops::Range;

use super::coverage::Coverage;

pub struct LatestLayerMap<Value> {
    image_coverage: Coverage<Value>,
    delta_coverage: Coverage<Value>,
}

impl<T: Clone> Default for LatestLayerMap<T> {
    fn default() -> Self {
        Self {
            image_coverage: Coverage::default(),
            delta_coverage: Coverage::default(),
        }
    }
}

impl<Value: Clone> LatestLayerMap<Value> {
    pub fn insert(
        self: &mut Self,
        key: Range<i128>,
        lsn: Range<u64>,
        value: Value,
        is_image: bool,
    ) {
        if is_image {
            self.image_coverage.insert(key, lsn, value);
        } else {
            self.delta_coverage.insert(key.clone(), lsn.clone(), value);
        }
    }

    pub fn query(self: &Self, key: i128) -> (Option<Value>, Option<Value>) {
        let delta = self.delta_coverage.query(key);
        let image = self.image_coverage.query(key);
        (delta, image)
    }

    pub fn image_coverage(
        self: &Self,
        key: Range<i128>,
    ) -> impl '_ + Iterator<Item = (i128, Option<Value>)> {
        self.image_coverage.range(key)
    }

    pub fn delta_coverage(
        self: &Self,
        key: Range<i128>,
    ) -> impl '_ + Iterator<Item = (i128, Option<Value>)> {
        self.delta_coverage.range(key)
    }

    pub fn clone(self: &Self) -> Self {
        Self {
            image_coverage: self.image_coverage.clone(),
            delta_coverage: self.delta_coverage.clone(),
        }
    }
}
