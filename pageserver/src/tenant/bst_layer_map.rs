use std::collections::BTreeMap;

// TODO the `im` crate has 20x more downloads and also has
// persistent/immutable BTree. See if it's better.
use rpds::RedBlackTreeMapSync;

/// Layer map implemented using persistent/immutable binary search tree.
/// It supports historical queries, but no retroactive inserts. For that
/// see RetroactiveLayerMap.
///
/// Layer type is abstracted as Value to make unit testing easier.
pub struct PersistentLayerMap<Value> {
    /// Mapping key to the latest layer (if any) until the next key.
    /// We use the Sync version of the map because we want Self to
    /// be Sync.
    head: RedBlackTreeMapSync<i128, Option<Value>>,

    /// All previous states of `self.head`
    ///
    /// TODO: Sorted Vec + binary search could be slightly faster.
    historic: BTreeMap<u64, RedBlackTreeMapSync<i128, Option<Value>>>,
}

impl<Value: std::fmt::Debug> std::fmt::Debug for PersistentLayerMap<Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let head_vec: Vec<_> = self.head.iter().collect();
        write!(f, "PersistentLayerMap: head: {:?}", head_vec)
    }
}

impl<T: Clone> Default for PersistentLayerMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Value: Clone> PersistentLayerMap<Value> {
    pub fn new() -> Self {
        Self {
            head: RedBlackTreeMapSync::default(),
            historic: BTreeMap::default(),
        }
    }

    pub fn insert(self: &mut Self, key_begin: i128, key_end: i128, lsn: u64, value: Value) {
        // TODO check for off-by-one errors

        // It's only a persistent map, not a retroactive one
        if let Some(last_entry) = self.historic.iter().rev().next() {
            let last_lsn = last_entry.0;
            if lsn == *last_lsn {
                // TODO there are edge cases to take care of
            }
            if lsn < *last_lsn {
                panic!("unexpected retroactive insert");
            }
        }

        // NOTE The order of the following lines is important!!

        // Preserve information after right endpoint
        let value_at_end = match self.head.range(0..key_end).last() {
            Some((_, Some(v))) => Some(v.clone()),
            Some((_, None)) => None,
            None => None,
        };
        self.head.insert_mut(key_end, value_at_end);

        // Insert the left endpoint
        self.head.insert_mut(key_begin, Some(value.clone()));

        // Cover the inside of the interval
        let to_remove: Vec<_> = self
            .head
            .range((key_begin + 1)..key_end)
            .map(|(k, _)| k.clone())
            .collect();
        for key in to_remove {
            self.head.remove_mut(&key);
        }

        // Remember history. Clone is O(1)
        self.historic.insert(lsn, self.head.clone());
    }

    pub fn query(self: &Self, key: i128, lsn: u64) -> Option<&Value> {
        // TODO check for off-by-one errors

        let version = self.historic.range(0..=lsn).rev().next()?.1;
        version.range(0..=key).rev().next()?.1.as_ref()
    }

    // TODO Add API for delta layers with lsn range.
    //      The easy solution is to only store images, and then from every
    //      image point to deltas on top of it. There might be something
    //      nicer but we have this solution as backup.
}

/// Basic test for the immutable bst library, just to show usage.
#[test]
fn test_immutable_bst_dependency() {
    let map = RedBlackTreeMapSync::<i32, i32>::default();

    let mut v1 = map.clone();
    let v2 = map.insert(1, 5);

    // We can query current and past versions of key 1
    assert_eq!(v1.get(&1), None);
    assert_eq!(v2.get(&1), Some(&5));

    // We can mutate old state, but it creates a branch.
    // It doesn't retroactively change future versions.
    v1.insert_mut(2, 6);
    assert_eq!(v1.get(&2), Some(&6));
    assert_eq!(v2.get(&2), None);
}

/// This is the most basic test that demonstrates intended usage.
#[test]
fn test_persistent_simple() {
    let mut map = PersistentLayerMap::<String>::new();
    map.insert(0, 5, 100, "Layer 1".to_string());
    dbg!(&map);
    map.insert(3, 9, 110, "Layer 2".to_string());
    dbg!(&map);
    map.insert(5, 6, 120, "Layer 3".to_string());
    dbg!(&map);

    // After Layer 1 insertion
    assert_eq!(map.query(1, 105), Some(&"Layer 1".to_string()));
    assert_eq!(map.query(4, 105), Some(&"Layer 1".to_string()));

    // After Layer 2 insertion
    assert_eq!(map.query(4, 115), Some(&"Layer 2".to_string()));
    assert_eq!(map.query(8, 115), Some(&"Layer 2".to_string()));
    assert_eq!(map.query(11, 115), None);

    // After Layer 3 insertion
    assert_eq!(map.query(4, 125), Some(&"Layer 2".to_string()));
    assert_eq!(map.query(5, 125), Some(&"Layer 3".to_string()));
    assert_eq!(map.query(7, 125), Some(&"Layer 2".to_string()));
}
