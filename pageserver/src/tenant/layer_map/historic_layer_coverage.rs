use std::collections::BTreeMap;
use std::ops::Range;

use tracing::info;

use super::layer_coverage::LayerCoverageTuple;

/// Layers in this module are identified and indexed by this data.
///
/// This is a helper struct to enable sorting layers by lsn.start.
///
/// These three values are enough to uniquely identify a layer, since
/// a layer is obligated to contain all contents within range, so two
/// deltas (or images) with the same range have identical content.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct LayerKey {
    // TODO I use i128 and u64 because it was easy for prototyping,
    //      testing, and benchmarking. If we can use the Lsn and Key
    //      types without overhead that would be preferable.
    pub key: Range<i128>,
    pub lsn: Range<u64>,
    pub is_image: bool,
}

impl PartialOrd for LayerKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LayerKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // NOTE we really care about comparing by lsn.start first
        self.lsn
            .start
            .cmp(&other.lsn.start)
            .then(self.lsn.end.cmp(&other.lsn.end))
            .then(self.key.start.cmp(&other.key.start))
            .then(self.key.end.cmp(&other.key.end))
            .then(self.is_image.cmp(&other.is_image))
    }
}

impl<'a, L: crate::tenant::storage_layer::Layer + ?Sized> From<&'a L> for LayerKey {
    fn from(layer: &'a L) -> Self {
        let kr = layer.get_key_range();
        let lr = layer.get_lsn_range();
        LayerKey {
            key: kr.start.to_i128()..kr.end.to_i128(),
            lsn: lr.start.0..lr.end.0,
            is_image: !layer.is_incremental(),
        }
    }
}

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
    pub fn insert(&mut self, layer_key: LayerKey, value: Value) {
        // It's only a persistent map, not a retroactive one
        if let Some(last_entry) = self.historic.iter().next_back() {
            let last_lsn = last_entry.0;
            if layer_key.lsn.start < *last_lsn {
                panic!("unexpected retroactive insert");
            }
        }

        // Insert into data structure
        let target = if layer_key.is_image {
            &mut self.head.image_coverage
        } else {
            &mut self.head.delta_coverage
        };

        target.insert(layer_key.key, layer_key.lsn.clone(), value);

        // Remember history. Clone is O(1)
        self.historic.insert(layer_key.lsn.start, self.head.clone());
    }

    /// Query at a particular LSN, inclusive
    pub fn get_version(&self, lsn: u64) -> Option<&LayerCoverageTuple<Value>> {
        match self.historic.range(..=lsn).next_back() {
            Some((_, v)) => Some(v),
            None => None,
        }
    }

    /// Remove all entries after a certain LSN (inclusive)
    pub fn trim(&mut self, begin: &u64) {
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
    map.insert(
        LayerKey {
            key: 0..5,
            lsn: 100..101,
            is_image: true,
        },
        "Layer 1".to_string(),
    );
    map.insert(
        LayerKey {
            key: 3..9,
            lsn: 110..111,
            is_image: true,
        },
        "Layer 2".to_string(),
    );
    map.insert(
        LayerKey {
            key: 5..6,
            lsn: 120..121,
            is_image: true,
        },
        "Layer 3".to_string(),
    );

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
    map.insert(
        LayerKey {
            key: 3..5,
            lsn: 100..110,
            is_image: true,
        },
        "Layer 1".to_string(),
    );

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

    map.insert(
        LayerKey {
            key: 3..5,
            lsn: 100..110,
            is_image: true,
        },
        "Layer 10".to_string(),
    );
    map.insert(
        LayerKey {
            key: 5..8,
            lsn: 100..110,
            is_image: true,
        },
        "Layer 11".to_string(),
    );
    map.insert(
        LayerKey {
            key: 3..4,
            lsn: 200..210,
            is_image: true,
        },
        "Layer 20".to_string(),
    );

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
    map.insert(
        LayerKey {
            key: 1..2,
            lsn: 100..200,
            is_image: true,
        },
        "Layer 1".to_string(),
    );
    map.insert(
        LayerKey {
            key: 4..5,
            lsn: 110..200,
            is_image: true,
        },
        "Layer 2".to_string(),
    );
    map.insert(
        LayerKey {
            key: 7..8,
            lsn: 120..300,
            is_image: true,
        },
        "Layer 3".to_string(),
    );

    // Add wide and short layer
    map.insert(
        LayerKey {
            key: 0..9,
            lsn: 130..199,
            is_image: true,
        },
        "Layer 4".to_string(),
    );

    // Add wide layer taller than some
    map.insert(
        LayerKey {
            key: 0..9,
            lsn: 140..201,
            is_image: true,
        },
        "Layer 5".to_string(),
    );

    // Add wide layer taller than all
    map.insert(
        LayerKey {
            key: 0..9,
            lsn: 150..301,
            is_image: true,
        },
        "Layer 6".to_string(),
    );

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
/// TODO It's not expensive but it's not great to hold a layer map write lock
///      for that long.
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
    buffer: BTreeMap<LayerKey, Option<Value>>,

    /// All current layers. This is not used for search. Only to make rebuilds easier.
    layers: BTreeMap<LayerKey, Value>,
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

    pub fn insert(&mut self, layer_key: LayerKey, value: Value) {
        self.buffer.insert(layer_key, Some(value));
    }

    pub fn remove(&mut self, layer_key: LayerKey) {
        self.buffer.insert(layer_key, None);
    }

    /// Replaces a previous layer with a new layer value.
    ///
    /// The replacement is conditional on:
    /// - there is an existing `LayerKey` record
    /// - there is no buffered removal for the given `LayerKey`
    /// - the given closure returns true for the current `Value`
    ///
    /// The closure is used to compare the latest value (buffered insert, or existing layer)
    /// against some expectation. This allows to use `Arc::ptr_eq` or similar which would be
    /// inaccessible via `PartialEq` trait.
    ///
    /// Returns a `Replacement` value describing the outcome; only the case of
    /// `Replacement::Replaced` modifies the map and requires a rebuild.
    pub fn replace<F>(
        &mut self,
        layer_key: &LayerKey,
        new: Value,
        check_expected: F,
    ) -> Replacement<Value>
    where
        F: FnOnce(&Value) -> bool,
    {
        let (slot, in_buffered) = match self.buffer.get(layer_key) {
            Some(inner @ Some(_)) => {
                // we compare against the buffered version, because there will be a later
                // rebuild before querying
                (inner.as_ref(), true)
            }
            Some(None) => {
                // buffer has removal for this key; it will not be equivalent by any check_expected.
                return Replacement::RemovalBuffered;
            }
            None => {
                // no pending modification for the key, check layers
                (self.layers.get(layer_key), false)
            }
        };

        match slot {
            Some(existing) if !check_expected(existing) => {
                // unfortunate clone here, but otherwise the nll borrowck grows the region of
                // 'a to cover the whole function, and we could not mutate in the other
                // Some(existing) branch
                Replacement::Unexpected(existing.clone())
            }
            None => Replacement::NotFound,
            Some(_existing) => {
                self.insert(layer_key.to_owned(), new);
                Replacement::Replaced { in_buffered }
            }
        }
    }

    pub fn rebuild(&mut self) {
        // Find the first LSN that needs to be rebuilt
        let rebuild_since: u64 = match self.buffer.iter().next() {
            Some((LayerKey { lsn, .. }, _)) => lsn.start,
            None => return, // No need to rebuild if buffer is empty
        };

        // Apply buffered updates to self.layers
        let num_updates = self.buffer.len();
        self.buffer.retain(|layer_key, layer| {
            match layer {
                Some(l) => {
                    self.layers.insert(layer_key.clone(), l.clone());
                }
                None => {
                    self.layers.remove(layer_key);
                }
            };
            false
        });

        // Rebuild
        let mut num_inserted = 0;
        self.historic_coverage.trim(&rebuild_since);
        for (layer_key, layer) in self.layers.range(
            LayerKey {
                lsn: rebuild_since..0,
                key: 0..0,
                is_image: false,
            }..,
        ) {
            self.historic_coverage
                .insert(layer_key.clone(), layer.clone());
            num_inserted += 1;
        }

        // TODO maybe only warn if ratio is at least 10
        info!(
            "Rebuilt layer map. Did {} insertions to process a batch of {} updates.",
            num_inserted, num_updates,
        )
    }

    /// Iterate all the layers
    pub fn iter(&self) -> impl '_ + Iterator<Item = Value> {
        // NOTE we can actually perform this without rebuilding,
        //      but it's not necessary for now.
        if !self.buffer.is_empty() {
            panic!("rebuild pls")
        }

        self.layers.values().cloned()
    }

    /// Return a reference to a queryable map, assuming all updates
    /// have already been processed using self.rebuild()
    pub fn get(&self) -> anyhow::Result<&HistoricLayerCoverage<Value>> {
        // NOTE we error here instead of implicitly rebuilding because
        //      rebuilding is somewhat expensive.
        // TODO maybe implicitly rebuild and log/sentry an error?
        if !self.buffer.is_empty() {
            anyhow::bail!("rebuild required")
        }

        Ok(&self.historic_coverage)
    }
}

/// Outcome of the replace operation.
#[derive(Debug)]
pub enum Replacement<Value> {
    /// Previous value was replaced with the new value.
    Replaced {
        /// Replacement happened for a scheduled insert.
        in_buffered: bool,
    },
    /// Key was not found buffered updates or existing layers.
    NotFound,
    /// Key has been scheduled for removal, it was not replaced.
    RemovalBuffered,
    /// Previous value was rejected by the closure.
    Unexpected(Value),
}

#[test]
fn test_retroactive_regression_1() {
    let mut map = BufferedHistoricLayerCoverage::new();

    map.insert(
        LayerKey {
            key: 0..21267647932558653966460912964485513215,
            lsn: 23761336..23761457,
            is_image: false,
        },
        "sdfsdfs".to_string(),
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
    map.insert(
        LayerKey {
            key: 0..5,
            lsn: 100..101,
            is_image: true,
        },
        "Image 1".to_string(),
    );
    map.insert(
        LayerKey {
            key: 3..9,
            lsn: 110..111,
            is_image: true,
        },
        "Image 2".to_string(),
    );
    map.insert(
        LayerKey {
            key: 4..6,
            lsn: 120..121,
            is_image: true,
        },
        "Image 3".to_string(),
    );
    map.insert(
        LayerKey {
            key: 8..9,
            lsn: 120..121,
            is_image: true,
        },
        "Image 4".to_string(),
    );

    // Add a delta layer out of order
    map.insert(
        LayerKey {
            key: 2..5,
            lsn: 105..106,
            is_image: false,
        },
        "Delta 1".to_string(),
    );

    // Rebuild so we can start querying
    map.rebuild();

    {
        let map = map.get().expect("rebuilt");

        let version = map.get_version(90);
        assert!(version.is_none());
        let version = map.get_version(102).unwrap();
        assert_eq!(version.image_coverage.query(4), Some("Image 1".to_string()));

        let version = map.get_version(107).unwrap();
        assert_eq!(version.image_coverage.query(4), Some("Image 1".to_string()));
        assert_eq!(version.delta_coverage.query(4), Some("Delta 1".to_string()));

        let version = map.get_version(115).unwrap();
        assert_eq!(version.image_coverage.query(4), Some("Image 2".to_string()));

        let version = map.get_version(125).unwrap();
        assert_eq!(version.image_coverage.query(4), Some("Image 3".to_string()));
    }

    // Remove Image 3
    map.remove(LayerKey {
        key: 4..6,
        lsn: 120..121,
        is_image: true,
    });
    map.rebuild();

    {
        // Check deletion worked
        let map = map.get().expect("rebuilt");
        let version = map.get_version(125).unwrap();
        assert_eq!(version.image_coverage.query(4), Some("Image 2".to_string()));
        assert_eq!(version.image_coverage.query(8), Some("Image 4".to_string()));
    }
}

#[test]
fn test_retroactive_replacement() {
    let mut map = BufferedHistoricLayerCoverage::new();

    let keys = [
        LayerKey {
            key: 0..5,
            lsn: 100..101,
            is_image: true,
        },
        LayerKey {
            key: 3..9,
            lsn: 110..111,
            is_image: true,
        },
        LayerKey {
            key: 4..6,
            lsn: 120..121,
            is_image: true,
        },
    ];

    let layers = [
        "Image 1".to_string(),
        "Image 2".to_string(),
        "Image 3".to_string(),
    ];

    for (key, layer) in keys.iter().zip(layers.iter()) {
        map.insert(key.to_owned(), layer.to_owned());
    }

    // rebuild is not necessary here, because replace works for both buffered updates and existing
    // layers.

    for (key, orig_layer) in keys.iter().zip(layers.iter()) {
        let replacement = format!("Remote {orig_layer}");

        // evict
        let ret = map.replace(key, replacement.clone(), |l| l == orig_layer);
        assert!(
            matches!(ret, Replacement::Replaced { .. }),
            "replace {orig_layer}: {ret:?}"
        );
        map.rebuild();

        let at = key.lsn.end + 1;

        let version = map.get().expect("rebuilt").get_version(at).unwrap();
        assert_eq!(
            version.image_coverage.query(4).as_deref(),
            Some(replacement.as_str()),
            "query for 4 at version {at} after eviction",
        );

        // download
        let ret = map.replace(key, orig_layer.clone(), |l| l == &replacement);
        assert!(
            matches!(ret, Replacement::Replaced { .. }),
            "replace {orig_layer} back: {ret:?}"
        );
        map.rebuild();
        let version = map.get().expect("rebuilt").get_version(at).unwrap();
        assert_eq!(
            version.image_coverage.query(4).as_deref(),
            Some(orig_layer.as_str()),
            "query for 4 at version {at} after download",
        );
    }
}

#[test]
fn missing_key_is_not_inserted_with_replace() {
    let mut map = BufferedHistoricLayerCoverage::new();
    let key = LayerKey {
        key: 0..5,
        lsn: 100..101,
        is_image: true,
    };

    let ret = map.replace(&key, "should not replace", |_| true);
    assert!(matches!(ret, Replacement::NotFound), "{ret:?}");
    map.rebuild();
    assert!(map
        .get()
        .expect("no changes to rebuild")
        .get_version(102)
        .is_none());
}

#[test]
fn replacing_buffered_insert_and_remove() {
    let mut map = BufferedHistoricLayerCoverage::new();
    let key = LayerKey {
        key: 0..5,
        lsn: 100..101,
        is_image: true,
    };

    map.insert(key.clone(), "Image 1");
    let ret = map.replace(&key, "Remote Image 1", |&l| l == "Image 1");
    assert!(
        matches!(ret, Replacement::Replaced { in_buffered: true }),
        "{ret:?}"
    );
    map.rebuild();

    assert_eq!(
        map.get()
            .expect("rebuilt")
            .get_version(102)
            .unwrap()
            .image_coverage
            .query(4),
        Some("Remote Image 1")
    );

    map.remove(key.clone());
    let ret = map.replace(&key, "should not replace", |_| true);
    assert!(
        matches!(ret, Replacement::RemovalBuffered),
        "cannot replace after scheduled remove: {ret:?}"
    );

    map.rebuild();

    let ret = map.replace(&key, "should not replace", |_| true);
    assert!(
        matches!(ret, Replacement::NotFound),
        "cannot replace after remove + rebuild: {ret:?}"
    );

    let at_version = map.get().expect("rebuilt").get_version(102);
    assert!(at_version.is_none());
}
