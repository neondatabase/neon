use std::collections::BTreeMap;
use std::ops::Range;

use super::latest_layer_map::LatestLayerMap;

pub struct PersistentLayerMap<Value> {
    /// The latest-only solution
    head: LatestLayerMap<Value>,

    /// All previous states
    historic: BTreeMap<u64, LatestLayerMap<Value>>,
}

impl<T: Clone> Default for PersistentLayerMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Value: Clone> PersistentLayerMap<Value> {
    pub fn new() -> Self {
        Self {
            head: LatestLayerMap::default(),
            historic: BTreeMap::default(),
        }
    }

    pub fn insert(
        self: &mut Self,
        key: Range<i128>,
        lsn: Range<u64>,
        value: Value,
        is_image: bool,
    ) {
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

        self.head.insert(key, lsn.clone(), value, is_image);

        // Remember history. Clone is O(1)
        self.historic.insert(lsn.start, self.head.clone());
    }

    pub fn query(self: &Self, key: i128, lsn: u64) -> (Option<Value>, Option<Value>) {
        let version = match self.historic.range(..=lsn).rev().next() {
            Some((_, v)) => v,
            None => return (None, None),
        };
        version.query(key)
    }

    pub fn get_version(self: &Self, lsn: u64) -> Option<&LatestLayerMap<Value>> {
        match self.historic.range(..=lsn).rev().next() {
            Some((_, v)) => Some(v),
            None => None,
        }
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

/// This is the most basic test that demonstrates intended usage.
/// All layers in this test have height 1.
#[test]
fn test_persistent_simple() {
    let mut map = PersistentLayerMap::<String>::new();
    map.insert(0..5, 100..101, "Layer 1".to_string(), true);
    map.insert(3..9, 110..111, "Layer 2".to_string(), true);
    map.insert(5..6, 120..121, "Layer 3".to_string(), true);

    // After Layer 1 insertion
    assert_eq!(map.query(1, 105).1, Some("Layer 1".to_string()));
    assert_eq!(map.query(4, 105).1, Some("Layer 1".to_string()));

    // After Layer 2 insertion
    assert_eq!(map.query(4, 115).1, Some("Layer 2".to_string()));
    assert_eq!(map.query(8, 115).1, Some("Layer 2".to_string()));
    assert_eq!(map.query(11, 115).1, None);

    // After Layer 3 insertion
    assert_eq!(map.query(4, 125).1, Some("Layer 2".to_string()));
    assert_eq!(map.query(5, 125).1, Some("Layer 3".to_string()));
    assert_eq!(map.query(7, 125).1, Some("Layer 2".to_string()));
}

/// Cover simple off-by-one edge cases
#[test]
fn test_off_by_one() {
    let mut map = PersistentLayerMap::<String>::new();
    map.insert(3..5, 100..110, "Layer 1".to_string(), true);

    // Check different LSNs
    assert_eq!(map.query(4, 99).1, None);
    assert_eq!(map.query(4, 100).1, Some("Layer 1".to_string()));
    assert_eq!(map.query(4, 110).1, Some("Layer 1".to_string()));

    // Check different keys
    assert_eq!(map.query(2, 105).1, None);
    assert_eq!(map.query(3, 105).1, Some("Layer 1".to_string()));
    assert_eq!(map.query(4, 105).1, Some("Layer 1".to_string()));
    assert_eq!(map.query(5, 105).1, None);
}

/// Cover edge cases where layers begin or end on the same key
#[test]
fn test_key_collision() {
    let mut map = PersistentLayerMap::<String>::new();

    map.insert(3..5, 100..110, "Layer 10".to_string(), true);
    map.insert(5..8, 100..110, "Layer 11".to_string(), true);

    map.insert(3..4, 200..210, "Layer 20".to_string(), true);

    // Check after layer 11
    assert_eq!(map.query(2, 105).1, None);
    assert_eq!(map.query(3, 105).1, Some("Layer 10".to_string()));
    assert_eq!(map.query(5, 105).1, Some("Layer 11".to_string()));
    assert_eq!(map.query(7, 105).1, Some("Layer 11".to_string()));
    assert_eq!(map.query(8, 105).1, None);

    // Check after layer 20
    assert_eq!(map.query(2, 205).1, None);
    assert_eq!(map.query(3, 205).1, Some("Layer 20".to_string()));
    assert_eq!(map.query(5, 205).1, Some("Layer 11".to_string()));
    assert_eq!(map.query(7, 205).1, Some("Layer 11".to_string()));
    assert_eq!(map.query(8, 205).1, None);
}

/// Test when rectangles have nontrivial height and possibly overlap
#[test]
fn test_persistent_overlapping() {
    let mut map = PersistentLayerMap::<String>::new();

    // Add 3 key-disjoint layers with varying LSN ranges
    map.insert(1..2, 100..200, "Layer 1".to_string(), true);
    map.insert(4..5, 110..200, "Layer 2".to_string(), true);
    map.insert(7..8, 120..300, "Layer 3".to_string(), true);

    // Add wide and short layer
    map.insert(0..9, 130..199, "Layer 4".to_string(), true);

    // Add wide layer taller than some
    map.insert(0..9, 140..201, "Layer 5".to_string(), true);

    // Add wide layer taller than all
    map.insert(0..9, 150..301, "Layer 6".to_string(), true);

    // After layer 4 insertion
    assert_eq!(map.query(0, 135).1, Some("Layer 4".to_string()));
    assert_eq!(map.query(1, 135).1, Some("Layer 1".to_string()));
    assert_eq!(map.query(2, 135).1, Some("Layer 4".to_string()));
    assert_eq!(map.query(4, 135).1, Some("Layer 2".to_string()));
    assert_eq!(map.query(5, 135).1, Some("Layer 4".to_string()));
    assert_eq!(map.query(7, 135).1, Some("Layer 3".to_string()));
    assert_eq!(map.query(8, 135).1, Some("Layer 4".to_string()));

    // After layer 5 insertion
    assert_eq!(map.query(0, 145).1, Some("Layer 5".to_string()));
    assert_eq!(map.query(1, 145).1, Some("Layer 5".to_string()));
    assert_eq!(map.query(2, 145).1, Some("Layer 5".to_string()));
    assert_eq!(map.query(4, 145).1, Some("Layer 5".to_string()));
    assert_eq!(map.query(5, 145).1, Some("Layer 5".to_string()));
    assert_eq!(map.query(7, 145).1, Some("Layer 3".to_string()));
    assert_eq!(map.query(8, 145).1, Some("Layer 5".to_string()));

    // After layer 6 insertion
    assert_eq!(map.query(0, 155).1, Some("Layer 6".to_string()));
    assert_eq!(map.query(1, 155).1, Some("Layer 6".to_string()));
    assert_eq!(map.query(2, 155).1, Some("Layer 6".to_string()));
    assert_eq!(map.query(4, 155).1, Some("Layer 6".to_string()));
    assert_eq!(map.query(5, 155).1, Some("Layer 6".to_string()));
    assert_eq!(map.query(7, 155).1, Some("Layer 6".to_string()));
    assert_eq!(map.query(8, 155).1, Some("Layer 6".to_string()));
}

/// Wrapper for PersistentLayerMap that allows us to hack around the lack
/// of support for retroactive insertion by rebuilding the map since the
/// change.
///
/// Why is this needed? We most often insert new layers with newer LSNs,
/// but during compaction we create layers with non-latest LSN, and during
/// GC we delete historic layers.
///
/// Even though rebuilding is an expensive (N log N) solution to the problem,
/// it's not critical since we do something equally expensive just to decide
/// whether or not to create new image layers.
///
/// If this becomes an actual bottleneck, one solution would be to build a
/// segment tree that holds PersistentLayerMaps. Though this would mean that
/// we take an additional log(N) performance hit for queries, which will probably
/// still be more critical.
///
/// See this for more on persistent and retroactive techniques:
/// https://www.youtube.com/watch?v=WqCWghETNDc&t=581s
pub struct RetroactiveLayerMap<Value> {
    /// A persistent layer map that we rebuild when we need to retroactively update
    map: PersistentLayerMap<Value>,

    /// We buffer insertion into the PersistentLayerMap to decrease the number of rebuilds.
    ///
    /// We implicitly assume that layers are identified by their key and lsn range.
    /// A value of None means we want to delete this item.
    buffer: BTreeMap<(u64, u64, i128, i128, bool), Option<Value>>,

    /// All current layers. This is not used for search. Only to make rebuilds easier.
    ///
    /// We implicitly assume that layers are identified by their key and lsn range.
    layers: BTreeMap<(u64, u64, i128, i128, bool), Value>,
}

impl<T: std::fmt::Debug> std::fmt::Debug for RetroactiveLayerMap<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetroactiveLayerMap")
            .field("buffer", &self.buffer)
            .field("layers", &self.layers)
            .finish()
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
            map: PersistentLayerMap::<Value>::new(),
            buffer: BTreeMap::new(),
            layers: BTreeMap::new(),
        }
    }

    pub fn insert(
        self: &mut Self,
        key: Range<i128>,
        lsn: Range<u64>,
        value: Value,
        is_image: bool,
    ) {
        self.buffer.insert(
            (lsn.start, lsn.end, key.start, key.end, is_image),
            Some(value.clone()),
        );
    }

    pub fn remove(self: &mut Self, key: Range<i128>, lsn: Range<u64>, is_image: bool) {
        self.buffer
            .insert((lsn.start, lsn.end, key.start, key.end, is_image), None);
    }

    pub fn rebuild(self: &mut Self) {
        // Find the first LSN that needs to be rebuilt
        let rebuild_since: u64 = match self.buffer.iter().next() {
            Some(((lsn_start, _, _, _, _), _)) => lsn_start.clone(),
            None => return, // No need to rebuild if buffer is empty
        };

        // Apply buffered updates to self.layers
        self.buffer.retain(|rect, layer| {
            match layer {
                Some(l) => {
                    let existing = self.layers.insert(rect.clone(), l.clone());
                    if existing.is_some() {
                        // TODO this happened once. Investigate.
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
        for ((lsn_start, lsn_end, key_start, key_end, is_image), layer) in
            self.layers.range((rebuild_since, 0, 0, 0, false)..)
        {
            self.map.insert(
                *key_start..*key_end,
                *lsn_start..*lsn_end,
                layer.clone(),
                *is_image,
            );
        }
    }

    pub fn clear(self: &mut Self) {
        self.map.trim(&0);
    }

    /// Iterate all the layers
    pub fn iter(self: &Self) -> impl '_ + Iterator<Item = Value> {
        // NOTE we can actually perform this without rebuilding,
        //      but it's not necessary for now.
        if !self.buffer.is_empty() {
            panic!("rebuild pls")
        }

        self.layers.iter().map(|(_, v)| v.clone())
    }

    /// Return a reference to a queryable map, assuming all updates
    /// have already been processed using self.rebuild()
    pub fn get(self: &Self) -> anyhow::Result<&PersistentLayerMap<Value>> {
        // NOTE we error here instead of implicitly rebuilding because
        //      rebuilding is somewhat expensive.
        // TODO maybe implicitly rebuild and log/sentry an error?
        if !self.buffer.is_empty() {
            anyhow::bail!("rebuild required")
        }

        Ok(&self.map)
    }

}

#[test]
fn test_retroactive_regression_1() {
    let mut map = RetroactiveLayerMap::new();

    map.insert(
        0..21267647932558653966460912964485513215,
        23761336..23761457,
        "sdfsdfs".to_string(),
        false,
    );

    map.rebuild();

    assert_eq!(map.get().unwrap().query(100, 23761457).0, Some("sdfsdfs".to_string()));
}

#[test]
fn test_retroactive_simple() {
    let mut map = RetroactiveLayerMap::new();

    // Append some images in increasing LSN order
    map.insert(0..5, 100..101, "Image 1".to_string(), true);
    map.insert(3..9, 110..111, "Image 2".to_string(), true);
    map.insert(4..6, 120..121, "Image 3".to_string(), true);
    map.insert(8..9, 120..121, "Image 4".to_string(), true);

    // Add a delta layer out of order
    map.insert(2..5, 105..106, "Delta 1".to_string(), true);

    // Rebuild so we can start querying
    map.rebuild();

    // Query key 4
    assert_eq!(map.get().unwrap().query(4, 90).1, None);
    assert_eq!(map.get().unwrap().query(4, 102).1, Some("Image 1".to_string()));
    assert_eq!(map.get().unwrap().query(4, 107).1, Some("Delta 1".to_string()));
    assert_eq!(map.get().unwrap().query(4, 115).1, Some("Image 2".to_string()));
    assert_eq!(map.get().unwrap().query(4, 125).1, Some("Image 3".to_string()));

    // Remove Image 3
    map.remove(4..6, 120..121, true);
    map.rebuild();

    // Check deletion worked
    assert_eq!(map.get().unwrap().query(4, 125).1, Some("Image 2".to_string()));
    assert_eq!(map.get().unwrap().query(8, 125).1, Some("Image 4".to_string()));
}
