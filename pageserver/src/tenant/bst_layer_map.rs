use std::collections::BTreeMap;
use std::sync::Arc;

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

    // TODO add lsn_end argument
    // TODO also return the lsn of the next image
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
        // TODO use lsn_end to decide which ones to cover
        //      NOTE currently insertion is amortized O(log N), and
        //           this would make it worst case O(N), amortized
        //           O(N), but in practice still pretty cheap. The
        //           problem is solveable with lazy propagation but
        //           that requires writing our own tree. It's premature
        //           optimization.
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

    pub fn trim(self: &mut Self, begin: &u64) {
        self.historic.split_off(begin);
    }
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

/// Layer map that supports:
/// - efficient historical queries
/// - efficient append only updates
/// - tombstones and similar methods for non-latest updates
/// - compaction/rebuilding to remove tombstones
///
/// See this for better retroactive techniques we can try
/// https://www.youtube.com/watch?v=WqCWghETNDc&t=581s
///
/// Layer type is abstracted as Value to make unit testing easier.
pub struct RetroactiveLayerMap<Value> {
    /// Using Arc and Vec allows us to hack around the lack of retroactive
    /// insert/delete functionality in PersistentLayerMap:
    /// - For normal append-only updates, we insert Arc::new(vec![value]).
    /// - For retroactive deletion (during gc) we empty the vector. The use
    ///   of Arc gives us a useful indirection layer so that the delete would
    ///   effectively retroactively update future versions, instead of creating
    ///   a new branch.
    /// - For retroactive updates (during compaction), we find all layers below
    ///   the layer we're inserting, and append to their Vec-s. This is O(N), but
    ///   also amortized O(log N). Here's why: We don't insert image layers
    ///   retroactively, only deltas. And after an image gets covered by K (currently
    ///   K = 3) deltas, we do compaction.
    ///
    /// This complexity might be a limitation, or a feature. Here's how it might
    /// actually help: It gives us the option to store the entire reconstruction
    /// result in a single colocated Vec, and get the initial image and all necessary
    /// deltas in one query.
    map: PersistentLayerMap<Arc<Vec<Value>>>,

    /// We buffer insertion into the PersistentLayerMap to decrease the number of rebuilds.
    buffer: BTreeMap<u64, Vec<(i128, i128, Value)>>,

    /// All current layers. This is not used for search. Only to make rebuilds easier.
    layers: BTreeMap<u64, Vec<(i128, i128, Value)>>,
}

impl<Value: std::fmt::Debug> std::fmt::Debug for RetroactiveLayerMap<Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RetroactiveLayerMap: head: {:?}", self.map)
    }
}

impl<Value: Clone> RetroactiveLayerMap<Value> {
    pub fn new() -> Self {
        Self {
            map: PersistentLayerMap::<Arc<Vec<Value>>>::new(),
            buffer: BTreeMap::new(),
            layers: BTreeMap::new(),
        }
    }

    pub fn insert(self: &mut Self, key_begin: i128, key_end: i128, lsn: u64, value: Value) {
        self.buffer
            .entry(lsn)
            .and_modify(|vec| vec.push((key_begin, key_end, value.clone())))
            .or_insert(vec![(key_begin, key_end, value.clone())]);
    }

    pub fn rebuild(self: &mut Self) {
        // Find the first LSN that needs to be rebuilt
        let rebuild_since: u64 = match self.buffer.iter().next() {
            Some((lsn, _)) => lsn.clone(),
            None => return, // No need to rebuild if buffer is empty
        };

        // Move buffer elements into self.layers
        self.buffer.retain(|lsn, layers| {
            self.layers
                .entry(*lsn)
                .and_modify(|vec| vec.append(layers))
                .or_insert(layers.clone());
            false
        });

        // Rebuild
        self.map.trim(&rebuild_since);
        for (lsn, layers) in self.layers.range(rebuild_since..) {
            for (key_begin, key_end, value) in layers {
                let wrapped = Arc::new(vec![value.clone()]);
                self.map.insert(*key_begin, *key_end, *lsn, wrapped);
            }
        }
    }

    pub fn query(self: &Self, key: i128, lsn: u64) -> Option<Value> {
        if !self.buffer.is_empty() {
            panic!("rebuild pls")
        }

        match self.map.query(key, lsn) {
            Some(vec) => match vec.len().cmp(&1) {
                std::cmp::Ordering::Less => todo!(),
                std::cmp::Ordering::Equal => Some(vec[0].clone()),
                std::cmp::Ordering::Greater => todo!(),
            },
            None => None,
        }
    }
}

#[test]
fn test_retroactive_simple() {
    let mut map = RetroactiveLayerMap::new();

    // Append some images in increasing LSN order
    map.insert(0, 5, 100, "Image 1".to_string());
    map.insert(3, 9, 110, "Image 2".to_string());
    map.insert(5, 6, 120, "Image 3".to_string());

    // Add a delta layer out of order
    map.insert(2, 5, 105, "Delta 1".to_string());

    // Rebuild so we can start querying
    map.rebuild();

    // Query
    assert_eq!(map.query(4, 90), None);
    assert_eq!(map.query(4, 102), Some("Image 1".to_string()));
    assert_eq!(map.query(4, 107), Some("Delta 1".to_string()));
    assert_eq!(map.query(4, 115), Some("Image 2".to_string()));
}
