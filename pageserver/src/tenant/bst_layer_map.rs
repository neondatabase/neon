use std::collections::BTreeMap;
use std::ops::Range;
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
    head: RedBlackTreeMapSync<i128, Option<(u64, Value)>>,

    /// All previous states of `self.head`
    ///
    /// TODO: Sorted Vec + binary search could be slightly faster.
    historic: BTreeMap<u64, RedBlackTreeMapSync<i128, Option<(u64, Value)>>>,
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

    /// Helper function to subdivide the key range without changing any values
    fn add_node(self: &mut Self, key: i128) {
        let value = match self.head.range(0..=key).last() {
            Some((_, Some(v))) => Some(v.clone()),
            Some((_, None)) => None,
            None => None,
        };
        self.head.insert_mut(key, value);
    }

    pub fn insert(self: &mut Self, key: Range<i128>, lsn: Range<u64>, value: Value) {
        // TODO check for off-by-one errors

        // It's only a persistent map, not a retroactive one
        if let Some(last_entry) = self.historic.iter().rev().next() {
            let last_lsn = last_entry.0;
            if lsn.start == *last_lsn {
                // TODO there are edge cases to take care of
            }
            if lsn.start < *last_lsn {
                panic!("unexpected retroactive insert");
            }
        }

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

        // Remember history. Clone is O(1)
        self.historic.insert(lsn.start, self.head.clone());
    }

    pub fn query(self: &Self, key: i128, lsn: u64) -> Option<Value> {
        // TODO check for off-by-one errors

        let version = self.historic.range(0..=lsn).rev().next()?.1;
        version
            .range(0..=key)
            .rev()
            .next()?
            .1
            .as_ref()
            .map(|(_, v)| v.clone())
    }

    pub fn trim(self: &mut Self, begin: &u64) {
        self.historic.split_off(begin);
        self.head = self
            .historic
            .iter()
            .rev()
            .next()
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
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
/// All layers in this test have height 1.
#[test]
fn test_persistent_simple() {
    let mut map = PersistentLayerMap::<String>::new();
    map.insert(0..5, 100..101, "Layer 1".to_string());
    map.insert(3..9, 110..111, "Layer 2".to_string());
    map.insert(5..6, 120..121, "Layer 3".to_string());

    // After Layer 1 insertion
    assert_eq!(map.query(1, 105), Some("Layer 1".to_string()));
    assert_eq!(map.query(4, 105), Some("Layer 1".to_string()));

    // After Layer 2 insertion
    assert_eq!(map.query(4, 115), Some("Layer 2".to_string()));
    assert_eq!(map.query(8, 115), Some("Layer 2".to_string()));
    assert_eq!(map.query(11, 115), None);

    // After Layer 3 insertion
    assert_eq!(map.query(4, 125), Some("Layer 2".to_string()));
    assert_eq!(map.query(5, 125), Some("Layer 3".to_string()));
    assert_eq!(map.query(7, 125), Some("Layer 2".to_string()));
}

/// Cover simple off-by-one edge cases
#[test]
fn test_off_by_one() {
    let mut map = PersistentLayerMap::<String>::new();
    map.insert(3..5, 100..110, "Layer 1".to_string());

    // Check different LSNs
    assert_eq!(map.query(4, 99), None);
    assert_eq!(map.query(4, 100), Some("Layer 1".to_string()));

    // Check different keys
    assert_eq!(map.query(2, 105), None);
    assert_eq!(map.query(3, 105), Some("Layer 1".to_string()));
    assert_eq!(map.query(4, 105), Some("Layer 1".to_string()));
    assert_eq!(map.query(5, 105), None);
}

/// Cover edge cases where layers begin or end on the same key
#[test]
fn test_key_collision() {
    let mut map = PersistentLayerMap::<String>::new();

    map.insert(3..5, 100..110, "Layer 10".to_string());
    map.insert(5..8, 100..110, "Layer 11".to_string());

    map.insert(3..4, 200..210, "Layer 20".to_string());

    // Check after layer 11
    assert_eq!(map.query(2, 105), None);
    assert_eq!(map.query(3, 105), Some("Layer 10".to_string()));
    assert_eq!(map.query(5, 105), Some("Layer 11".to_string()));
    assert_eq!(map.query(7, 105), Some("Layer 11".to_string()));
    assert_eq!(map.query(8, 105), None);

    // Check after layer 20
    assert_eq!(map.query(2, 205), None);
    assert_eq!(map.query(3, 205), Some("Layer 20".to_string()));
    assert_eq!(map.query(5, 205), Some("Layer 11".to_string()));
    assert_eq!(map.query(7, 205), Some("Layer 11".to_string()));
    assert_eq!(map.query(8, 205), None);
}

/// Test when rectangles have nontrivial height and possibly overlap
#[test]
fn test_persistent_overlapping() {
    let mut map = PersistentLayerMap::<String>::new();

    // Add 3 key-disjoint layers with varying LSN ranges
    map.insert(1..2, 100..200, "Layer 1".to_string());
    map.insert(4..5, 110..200, "Layer 2".to_string());
    map.insert(7..8, 120..300, "Layer 3".to_string());

    // Add wide and short layer
    map.insert(0..9, 130..199, "Layer 4".to_string());

    // Add wide layer taller than some
    map.insert(0..9, 140..201, "Layer 5".to_string());

    // Add wide layer taller than all
    map.insert(0..9, 150..301, "Layer 6".to_string());

    // After layer 4 insertion
    assert_eq!(map.query(0, 135), Some("Layer 4".to_string()));
    assert_eq!(map.query(1, 135), Some("Layer 1".to_string()));
    assert_eq!(map.query(2, 135), Some("Layer 4".to_string()));
    assert_eq!(map.query(4, 135), Some("Layer 2".to_string()));
    assert_eq!(map.query(5, 135), Some("Layer 4".to_string()));
    assert_eq!(map.query(7, 135), Some("Layer 3".to_string()));
    assert_eq!(map.query(8, 135), Some("Layer 4".to_string()));

    // After layer 5 insertion
    assert_eq!(map.query(0, 145), Some("Layer 5".to_string()));
    assert_eq!(map.query(1, 145), Some("Layer 5".to_string()));
    assert_eq!(map.query(2, 145), Some("Layer 5".to_string()));
    assert_eq!(map.query(4, 145), Some("Layer 5".to_string()));
    assert_eq!(map.query(5, 145), Some("Layer 5".to_string()));
    assert_eq!(map.query(7, 145), Some("Layer 3".to_string()));
    assert_eq!(map.query(8, 145), Some("Layer 5".to_string()));

    // After layer 6 insertion
    assert_eq!(map.query(0, 155), Some("Layer 6".to_string()));
    assert_eq!(map.query(1, 155), Some("Layer 6".to_string()));
    assert_eq!(map.query(2, 155), Some("Layer 6".to_string()));
    assert_eq!(map.query(4, 155), Some("Layer 6".to_string()));
    assert_eq!(map.query(5, 155), Some("Layer 6".to_string()));
    assert_eq!(map.query(7, 155), Some("Layer 6".to_string()));
    assert_eq!(map.query(8, 155), Some("Layer 6".to_string()));
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
    /// A value of None means we want to delete this item.
    buffer: BTreeMap<(u64, u64, i128, i128), Option<Value>>,

    /// All current layers. This is not used for search. Only to make rebuilds easier.
    layers: BTreeMap<(u64, u64, i128, i128), Value>,
}

impl<Value: std::fmt::Debug> std::fmt::Debug for RetroactiveLayerMap<Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RetroactiveLayerMap: head: {:?}", self.map)
    }
}

impl<T: Clone> Default for RetroactiveLayerMap<T> {
    fn default() -> Self {
        Self::new()
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

    pub fn insert(self: &mut Self, key: Range<i128>, lsn: Range<u64>, value: Value) {
        self.buffer.insert(
            (lsn.start, lsn.end, key.start, key.end),
            Some(value.clone()),
        );
    }

    pub fn remove(self: &mut Self, key: Range<i128>, lsn: Range<u64>) {
        self.buffer
            .insert((lsn.start, lsn.end, key.start, key.end), None);
    }

    pub fn rebuild(self: &mut Self) {
        // Find the first LSN that needs to be rebuilt
        let rebuild_since: u64 = match self.buffer.iter().next() {
            Some(((lsn_start, _, _, _), _)) => lsn_start.clone(),
            None => return, // No need to rebuild if buffer is empty
        };

        // Apply buffered updates to self.layers
        self.buffer.retain(|rect, layer| {
            match layer {
                Some(l) => {
                    let existing = self.layers.insert(rect.clone(), l.clone());
                    if existing.is_some() {
                        panic!("can't overwrite layer");
                    }
                }
                None => {
                    let existing = self.layers.remove(rect);
                    if existing.is_none() {
                        panic!("invalid layer deletion");
                    }
                }
            };
            false
        });

        // Rebuild
        self.map.trim(&rebuild_since);
        for ((lsn_start, lsn_end, key_start, key_end), layer) in
            self.layers.range((rebuild_since, 0, 0, 0)..)
        {
            let wrapped = Arc::new(vec![layer.clone()]);
            self.map
                .insert(*key_start..*key_end, *lsn_start..*lsn_end, wrapped);
        }
    }

    pub fn clear(self: &mut Self) {
        self.map.trim(&0);
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
    map.insert(0..5, 100..101, "Image 1".to_string());
    map.insert(3..9, 110..111, "Image 2".to_string());
    map.insert(4..6, 120..121, "Image 3".to_string());
    map.insert(8..9, 120..121, "Image 4".to_string());

    // Add a delta layer out of order
    map.insert(2..5, 105..106, "Delta 1".to_string());

    // Rebuild so we can start querying
    map.rebuild();

    // Query key 4
    assert_eq!(map.query(4, 90), None);
    assert_eq!(map.query(4, 102), Some("Image 1".to_string()));
    assert_eq!(map.query(4, 107), Some("Delta 1".to_string()));
    assert_eq!(map.query(4, 115), Some("Image 2".to_string()));
    assert_eq!(map.query(4, 125), Some("Image 3".to_string()));

    // Remove Image 3
    map.remove(4..6, 120..121);
    map.rebuild();

    // Check deletion worked
    assert_eq!(map.query(4, 125), Some("Image 2".to_string()));
    assert_eq!(map.query(8, 125), Some("Image 4".to_string()));
}
