use std::collections::BTreeMap;
use std::ops::Range;

use super::layer_coverage::LayerCoverageTuple;

/// Efficiently queryable layer coverage for each LSN.
///
/// Allows answering layer map queries very efficiently,
/// but doesn't allow retroactive insertion, which is
/// sometimes necessary. See BufferedHistoricLayerCoverage.
pub struct HistoricLayerCoverage<Value> {
    /// The latest state
    head: LayerCoverageTuple<Value>,

    /// All previous states
    historic: BTreeMap<u64, LayerCoverageTuple<Value>>,
}

impl<T: Clone> Default for HistoricLayerCoverage<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Value: Clone> HistoricLayerCoverage<Value> {
    pub fn new() -> Self {
        Self {
            head: LayerCoverageTuple::default(),
            historic: BTreeMap::default(),
        }
    }

    /// Add a layer
    ///
    /// Panics if new layer has older lsn.start than an existing layer.
    /// See BufferedHistoricLayerCoverage for a more general insertion method.
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

        // Insert into data structure
        if is_image {
            self.head.image_coverage.insert(key, lsn.clone(), value);
        } else {
            self.head
                .delta_coverage
                .insert(key.clone(), lsn.clone(), value);
        }

        // Remember history. Clone is O(1)
        self.historic.insert(lsn.start, self.head.clone());
    }

    /// Query at a particular LSN, inclusive
    pub fn get_version(self: &Self, lsn: u64) -> Option<&LayerCoverageTuple<Value>> {
        match self.historic.range(..=lsn).rev().next() {
            Some((_, v)) => Some(v),
            None => None,
        }
    }

    /// Remove all entries after a certain LSN
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
    let mut map = HistoricLayerCoverage::<String>::new();
    map.insert(0..5, 100..101, "Layer 1".to_string(), true);
    map.insert(3..9, 110..111, "Layer 2".to_string(), true);
    map.insert(5..6, 120..121, "Layer 3".to_string(), true);

    // After Layer 1 insertion
    let version = map.get_version(105).unwrap();
    assert_eq!(version.image_coverage.query(1), Some("Layer 1".to_string()));
    assert_eq!(version.image_coverage.query(4), Some("Layer 1".to_string()));

    // After Layer 2 insertion
    let version = map.get_version(115).unwrap();
    assert_eq!(version.image_coverage.query(4), Some("Layer 2".to_string()));
    assert_eq!(version.image_coverage.query(8), Some("Layer 2".to_string()));
    assert_eq!(version.image_coverage.query(11), None);

    // After Layer 3 insertion
    let version = map.get_version(125).unwrap();
    assert_eq!(version.image_coverage.query(4), Some("Layer 2".to_string()));
    assert_eq!(version.image_coverage.query(5), Some("Layer 3".to_string()));
    assert_eq!(version.image_coverage.query(7), Some("Layer 2".to_string()));
}

/// Cover simple off-by-one edge cases
#[test]
fn test_off_by_one() {
    let mut map = HistoricLayerCoverage::<String>::new();
    map.insert(3..5, 100..110, "Layer 1".to_string(), true);

    // Check different LSNs
    let version = map.get_version(99);
    assert!(version.is_none());
    let version = map.get_version(100).unwrap();
    assert_eq!(version.image_coverage.query(4), Some("Layer 1".to_string()));
    let version = map.get_version(110).unwrap();
    assert_eq!(version.image_coverage.query(4), Some("Layer 1".to_string()));

    // Check different keys
    let version = map.get_version(105).unwrap();
    assert_eq!(version.image_coverage.query(2), None);
    assert_eq!(version.image_coverage.query(3), Some("Layer 1".to_string()));
    assert_eq!(version.image_coverage.query(4), Some("Layer 1".to_string()));
    assert_eq!(version.image_coverage.query(5), None);
}

/// Cover edge cases where layers begin or end on the same key
#[test]
fn test_key_collision() {
    let mut map = HistoricLayerCoverage::<String>::new();

    map.insert(3..5, 100..110, "Layer 10".to_string(), true);
    map.insert(5..8, 100..110, "Layer 11".to_string(), true);

    map.insert(3..4, 200..210, "Layer 20".to_string(), true);

    // Check after layer 11
    let version = map.get_version(105).unwrap();
    assert_eq!(version.image_coverage.query(2), None);
    assert_eq!(
        version.image_coverage.query(3),
        Some("Layer 10".to_string())
    );
    assert_eq!(
        version.image_coverage.query(5),
        Some("Layer 11".to_string())
    );
    assert_eq!(
        version.image_coverage.query(7),
        Some("Layer 11".to_string())
    );
    assert_eq!(version.image_coverage.query(8), None);

    // Check after layer 20
    let version = map.get_version(205).unwrap();
    assert_eq!(version.image_coverage.query(2), None);
    assert_eq!(
        version.image_coverage.query(3),
        Some("Layer 20".to_string())
    );
    assert_eq!(
        version.image_coverage.query(5),
        Some("Layer 11".to_string())
    );
    assert_eq!(
        version.image_coverage.query(7),
        Some("Layer 11".to_string())
    );
    assert_eq!(version.image_coverage.query(8), None);
}

/// Test when rectangles have nontrivial height and possibly overlap
#[test]
fn test_persistent_overlapping() {
    let mut map = HistoricLayerCoverage::<String>::new();

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
    let version = map.get_version(135).unwrap();
    assert_eq!(version.image_coverage.query(0), Some("Layer 4".to_string()));
    assert_eq!(version.image_coverage.query(1), Some("Layer 1".to_string()));
    assert_eq!(version.image_coverage.query(2), Some("Layer 4".to_string()));
    assert_eq!(version.image_coverage.query(4), Some("Layer 2".to_string()));
    assert_eq!(version.image_coverage.query(5), Some("Layer 4".to_string()));
    assert_eq!(version.image_coverage.query(7), Some("Layer 3".to_string()));
    assert_eq!(version.image_coverage.query(8), Some("Layer 4".to_string()));

    // After layer 5 insertion
    let version = map.get_version(145).unwrap();
    assert_eq!(version.image_coverage.query(0), Some("Layer 5".to_string()));
    assert_eq!(version.image_coverage.query(1), Some("Layer 5".to_string()));
    assert_eq!(version.image_coverage.query(2), Some("Layer 5".to_string()));
    assert_eq!(version.image_coverage.query(4), Some("Layer 5".to_string()));
    assert_eq!(version.image_coverage.query(5), Some("Layer 5".to_string()));
    assert_eq!(version.image_coverage.query(7), Some("Layer 3".to_string()));
    assert_eq!(version.image_coverage.query(8), Some("Layer 5".to_string()));

    // After layer 6 insertion
    let version = map.get_version(155).unwrap();
    assert_eq!(version.image_coverage.query(0), Some("Layer 6".to_string()));
    assert_eq!(version.image_coverage.query(1), Some("Layer 6".to_string()));
    assert_eq!(version.image_coverage.query(2), Some("Layer 6".to_string()));
    assert_eq!(version.image_coverage.query(4), Some("Layer 6".to_string()));
    assert_eq!(version.image_coverage.query(5), Some("Layer 6".to_string()));
    assert_eq!(version.image_coverage.query(7), Some("Layer 6".to_string()));
    assert_eq!(version.image_coverage.query(8), Some("Layer 6".to_string()));
}

/// Wrapper for HistoricLayerCoverage that allows us to hack around the lack
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
pub struct BufferedHistoricLayerCoverage<Value> {
    /// A persistent layer map that we rebuild when we need to retroactively update
    historic_coverage: HistoricLayerCoverage<Value>,

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

impl<T: std::fmt::Debug> std::fmt::Debug for BufferedHistoricLayerCoverage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetroactiveLayerMap")
            .field("buffer", &self.buffer)
            .field("layers", &self.layers)
            .finish()
    }
}

impl<T: Clone> Default for BufferedHistoricLayerCoverage<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Value: Clone> BufferedHistoricLayerCoverage<Value> {
    pub fn new() -> Self {
        Self {
            historic_coverage: HistoricLayerCoverage::<Value>::new(),
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
                        // panic!("can't overwrite layer");
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
        self.historic_coverage.trim(&rebuild_since);
        for ((lsn_start, lsn_end, key_start, key_end, is_image), layer) in
            self.layers.range((rebuild_since, 0, 0, 0, false)..)
        {
            self.historic_coverage.insert(
                *key_start..*key_end,
                *lsn_start..*lsn_end,
                layer.clone(),
                *is_image,
            );
        }
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
    pub fn get(self: &Self) -> anyhow::Result<&HistoricLayerCoverage<Value>> {
        // NOTE we error here instead of implicitly rebuilding because
        //      rebuilding is somewhat expensive.
        // TODO maybe implicitly rebuild and log/sentry an error?
        if !self.buffer.is_empty() {
            anyhow::bail!("rebuild required")
        }

        Ok(&self.historic_coverage)
    }
}

#[test]
fn test_retroactive_regression_1() {
    let mut map = BufferedHistoricLayerCoverage::new();

    map.insert(
        0..21267647932558653966460912964485513215,
        23761336..23761457,
        "sdfsdfs".to_string(),
        false,
    );

    map.rebuild();

    let version = map.get().unwrap().get_version(23761457).unwrap();
    assert_eq!(
        version.delta_coverage.query(100),
        Some("sdfsdfs".to_string())
    );
}

#[test]
fn test_retroactive_simple() {
    let mut map = BufferedHistoricLayerCoverage::new();

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
    let version = map.get().unwrap().get_version(90);
    assert!(version.is_none());
    let version = map.get().unwrap().get_version(102).unwrap();
    assert_eq!(version.image_coverage.query(4), Some("Image 1".to_string()));
    let version = map.get().unwrap().get_version(107).unwrap();
    assert_eq!(version.image_coverage.query(4), Some("Delta 1".to_string()));
    let version = map.get().unwrap().get_version(115).unwrap();
    assert_eq!(version.image_coverage.query(4), Some("Image 2".to_string()));
    let version = map.get().unwrap().get_version(125).unwrap();
    assert_eq!(version.image_coverage.query(4), Some("Image 3".to_string()));

    // Remove Image 3
    map.remove(4..6, 120..121, true);
    map.rebuild();

    // Check deletion worked
    let version = map.get().unwrap().get_version(125).unwrap();
    assert_eq!(version.image_coverage.query(4), Some("Image 2".to_string()));
    assert_eq!(version.image_coverage.query(8), Some("Image 4".to_string()));
}
