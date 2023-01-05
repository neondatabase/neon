use std::ops::Range;

// TODO the `im` crate has 20x more downloads and also has
// persistent/immutable BTree. It also runs a bit faster but
// results are not the same on some tests.
use rpds::RedBlackTreeMapSync;

pub struct Coverage<Value> {
    /// Mapping key to the latest layer (if any) until the next key.
    /// We use the Sync version of the map because we want Self to
    /// be Sync. Using nonsync might be faster, if we can work with
    /// that.
    head: RedBlackTreeMapSync<i128, Option<(u64, Value)>>,
}

impl<T: Clone> Default for Coverage<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Value: Clone> Coverage<Value> {
    pub fn new() -> Self {
        Self {
            head: RedBlackTreeMapSync::default(),
        }
    }

    /// Helper function to subdivide the key range without changing any values
    fn add_node(self: &mut Self, key: i128) {
        let value = match self.head.range(..=key).last() {
            Some((_, Some(v))) => Some(v.clone()),
            Some((_, None)) => None,
            None => None,
        };
        self.head.insert_mut(key, value);
    }

    pub fn insert(self: &mut Self, key: Range<i128>, lsn: Range<u64>, value: Value) {
        // NOTE The order of the following lines is important!!

        // Add nodes at endpoints
        self.add_node(key.start);
        self.add_node(key.end);

        // Raise the height where necessary
        //
        // NOTE This loop is worst case O(N), but amortized O(log N) in the special
        // case when rectangles have no height. In practice I don't think we'll see
        // the kind of layer intersections needed to trigger O(N) behavior. If we
        // do it can be fixed using lazy propagation.
        let mut to_update = Vec::new();
        let mut to_remove = Vec::new();
        let mut prev_covered = false;
        for (k, node) in self.head.range(key.clone()) {
            let needs_cover = match node {
                None => true,
                Some((h, _)) => h < &lsn.end,
            };
            if needs_cover {
                match prev_covered {
                    true => to_remove.push(k.clone()),
                    false => to_update.push(k.clone()),
                }
            }
            prev_covered = needs_cover;
        }
        if !prev_covered {
            to_remove.push(key.end);
        }
        for k in to_update {
            self.head
                .insert_mut(k.clone(), Some((lsn.end.clone(), value.clone())));
        }
        for k in to_remove {
            self.head.remove_mut(&k);
        }
    }

    pub fn query(self: &Self, key: i128) -> Option<Value> {
        self.head
            .range(..=key)
            .rev()
            .next()?
            .1
            .as_ref()
            .map(|(_, v)| v.clone())
    }

    pub fn range(
        self: &Self,
        key: Range<i128>,
    ) -> impl '_ + Iterator<Item = (i128, Option<Value>)> {
        self.head
            .range(key)
            .map(|(k, v)| (k.clone(), v.as_ref().map(|x| x.1.clone())))
    }

    pub fn iter(self: &Self) -> impl '_ + Iterator<Item = (i128, Option<Value>)> {
        self.head
            .iter()
            .map(|(k, v)| (k.clone(), v.as_ref().map(|x| x.1.clone())))
    }

    pub fn clone(self: &Self) -> Self {
        Self {
            head: self.head.clone(),
        }
    }
}
