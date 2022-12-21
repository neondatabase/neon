//!
//! The layer map tracks what layers exist in a timeline.
//!
//! When the timeline is first accessed, the server lists of all layer files
//! in the timelines/<timeline_id> directory, and populates this map with
//! ImageLayer and DeltaLayer structs corresponding to each file. When the first
//! new WAL record is received, we create an InMemoryLayer to hold the incoming
//! records. Now and then, in the checkpoint() function, the in-memory layer is
//! are frozen, and it is split up into new image and delta layers and the
//! corresponding files are written to disk.
//!

use crate::metrics::NUM_ONDISK_LAYERS;
use crate::repository::Key;
use crate::tenant::inmemory_layer::InMemoryLayer;
use crate::tenant::storage_layer::range_overlaps;
use crate::tenant::storage_layer::Layer;
use anyhow::Result;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::sync::Arc;
use utils::lsn::Lsn;

struct LayerRef<L: ?Sized> {
    layer: Arc<L>,
}

impl<L: ?Sized> LayerRef<L> {
    pub fn new(layer: Arc<L>) -> Self {
        LayerRef { layer }
    }
}

impl<L: ?Sized> PartialEq for LayerRef<L> {
    fn eq(&self, other: &LayerRef<L>) -> bool {
        Arc::ptr_eq(&self.layer, &other.layer)
    }
}

impl<L: ?Sized> Eq for LayerRef<L> {}

impl<L: ?Sized> Hash for LayerRef<L> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::into_raw(Arc::clone(&self.layer)).hash(state)
    }
}

///
/// LayerMap tracks what layers exist on a timeline.
///
pub struct LayerMap<L: ?Sized> {
    //
    // 'open_layer' holds the current InMemoryLayer that is accepting new
    // records. If it is None, 'next_open_layer_at' will be set instead, indicating
    // where the start LSN of the next InMemoryLayer that is to be created.
    //
    pub open_layer: Option<Arc<InMemoryLayer>>,
    pub next_open_layer_at: Option<Lsn>,

    ///
    /// Frozen layers, if any. Frozen layers are in-memory layers that
    /// are no longer added to, but haven't been written out to disk
    /// yet. They contain WAL older than the current 'open_layer' or
    /// 'next_open_layer_at', but newer than any historic layer.
    /// The frozen layers are in order from oldest to newest, so that
    /// the newest one is in the 'back' of the VecDeque, and the oldest
    /// in the 'front'.
    ///
    pub frozen_layers: VecDeque<Arc<InMemoryLayer>>,

    /// L1 layers are kept here grouped by layer end
    l1_layers: BTreeMap<i128, BTreeMap<Lsn, Arc<L>>>,

    /// L0 layers have key range Key::MIN..Key::MAX, and locating them using B-Tree search is very inefficient.
    /// So L0 layers are held in l0_delta_layers vector, in addition to the B-tree.
    l0_delta_layers: BTreeMap<Lsn, Arc<L>>,

    /// All historic layers (including L0)
    historic_layers: HashSet<LayerRef<L>>,
}

impl<L: ?Sized> Default for LayerMap<L> {
    fn default() -> Self {
        Self {
            open_layer: None,
            next_open_layer_at: None,
            frozen_layers: VecDeque::default(),
            historic_layers: HashSet::default(),
            l0_delta_layers: BTreeMap::default(),
            l1_layers: BTreeMap::default(),
        }
    }
}

/// Return value of LayerMap::search
pub struct SearchResult<L: ?Sized> {
    pub layer: Arc<L>,
    pub lsn_floor: Lsn,
}

impl<L> LayerMap<L>
where
    L: ?Sized + Layer,
{
    fn find_latest_layer(
        &self,
        key: Key,
        end_lsn: Lsn,
        image_only: bool,
    ) -> Option<SearchResult<L>> {
        let mut candidate: Option<Arc<L>> = None;
        let mut image_lsn: Option<Lsn> = None;
        let int_key = key.to_i128();
        if !image_only {
            candidate = self
                .l0_delta_layers
                .range(..end_lsn)
                .next_back()
                .map(|p| Arc::clone(p.1));
        }
        for (_key, layers) in self.l1_layers.range(int_key..) {
            let mut lsn_iter = layers.iter();
            while let Some((_lsn, layer)) = lsn_iter.next_back() {
                if image_only && layer.is_incremental() {
                    continue;
                }
                if !layer.get_key_range().contains(&key) {
                    continue;
                }
                let lsn_range = layer.get_lsn_range();
                if lsn_range.start >= end_lsn {
                    continue;
                }
                if !layer.is_incremental() && lsn_range.end >= end_lsn {
                    // found exact match
                    return Some(SearchResult {
                        layer: Arc::clone(layer),
                        lsn_floor: lsn_range.start,
                    });
                }
                candidate = Some(Arc::clone(layer));
                if !layer.is_incremental() {
                    image_lsn = Some(lsn_range.end);
                }
                if image_lsn.is_some() {
                    break;
                }
            }
            break;
        }
        if candidate.is_none() {
            tracing::info!("Failed to locate key {} end_lsn={}", key, end_lsn);
        }
        candidate.map(|latest_layer| {
            let is_incremental = latest_layer.is_incremental();
            let start_lsn = latest_layer.get_lsn_range().start;
            SearchResult {
                layer: latest_layer,
                lsn_floor: if is_incremental {
                    std::cmp::max(image_lsn.unwrap_or(Lsn(0)) + 1, start_lsn)
                } else {
                    start_lsn
                },
            }
        })
    }

    ///
    /// Find the latest layer that covers the given 'key', with lsn <
    /// 'end_lsn'.
    ///
    /// Returns the layer, if any, and an 'lsn_floor' value that
    /// indicates which portion of the layer the caller should
    /// check. 'lsn_floor' is normally the start-LSN of the layer, but
    /// can be greater if there is an overlapping layer that might
    /// contain the version, even if it's missing from the returned
    /// layer.
    ///
    pub fn search(&self, key: Key, end_lsn: Lsn) -> Result<Option<SearchResult<L>>> {
        Ok(self.find_latest_layer(key, end_lsn, false))
    }

    // When inserting new point, copy from successor point all layers which contain this point
    fn copy_layers(&self, end_key: i128) -> BTreeMap<Lsn, Arc<L>> {
        let mut new_layers = BTreeMap::new();
        for (_key, layers) in self.l1_layers.range(end_key..) {
            for (lsn, layer) in layers {
                if layer.get_key_range().start.to_i128() < end_key {
                    new_layers.insert(*lsn, Arc::clone(layer));
                }
            }
            break;
        }
        new_layers
    }

    ///
    /// Insert an on-disk layer
    ///
    pub fn insert_historic(&mut self, layer: Arc<L>) {
        let key_range = layer.get_key_range();
        let lsn_range = layer.get_lsn_range();
        self.historic_layers.insert(LayerRef::new(layer.clone()));
        if key_range == (Key::MIN..Key::MAX) {
            self.l0_delta_layers.insert(lsn_range.start, layer);
        } else {
            let start_key = key_range.start.to_i128();
            let end_key = key_range.end.to_i128();
            let mut last_key = -1i128;
            for (key, layers) in self.l1_layers.range_mut(start_key..end_key) {
                layers.insert(lsn_range.end, layer.clone());
                last_key = *key;
            }
            if last_key != end_key - 1 {
                let mut layers = self.copy_layers(end_key);
                layers.insert(lsn_range.end, layer);
                self.l1_layers.insert(end_key - 1, layers);
            }
        }
        NUM_ONDISK_LAYERS.inc();
    }

    ///
    /// Remove an on-disk layer from the map.
    ///
    /// This should be called when the corresponding file on disk has been deleted.
    ///
    pub fn remove_historic(&mut self, layer: Arc<L>) {
        let key_range = layer.get_key_range();
        let lsn_range = layer.get_lsn_range();
        assert!(self.historic_layers.remove(&LayerRef::new(layer)));
        if key_range == (Key::MIN..Key::MAX) {
            assert!(self.l0_delta_layers.remove(&lsn_range.start).is_some());
        } else {
            let start_key = key_range.start.to_i128();
            let end_key = key_range.end.to_i128();
            for (_key, layers) in self.l1_layers.range_mut(start_key..end_key) {
                assert!(layers.remove(&lsn_range.end).is_some());
            }
        }
        NUM_ONDISK_LAYERS.dec();
    }

    /// Is there a newer image layer for given key- and LSN-range?
    ///
    /// This is used for garbage collection, to determine if an old layer can
    /// be deleted.
    pub fn image_layer_exists(
        &self,
        key_range: &Range<Key>,
        lsn_range: &Range<Lsn>,
    ) -> Result<bool> {
        let start_key = key_range.start.to_i128();
        let end_key = key_range.end.to_i128();
        for (key, layers) in self.l1_layers.range(start_key..) {
            let mut lsn_iter = layers.iter();
            while let Some((_lsn, layer)) = lsn_iter.next_back() {
                if !layer.is_incremental()
                    && range_overlaps(key_range, &layer.get_key_range())
                    && range_overlaps(lsn_range, &layer.get_lsn_range())
                {
                    return Ok(true);
                }
            }
            if *key >= end_key {
                break;
            }
        }
        return Ok(false);
    }

    pub fn iter_historic_layers(&self) -> impl '_ + Iterator<Item = Arc<L>> {
        self.historic_layers.iter().map(|r| r.layer.clone())
    }

    /// Find the last image layer that covers 'key', ignoring any image layers
    /// newer than 'lsn'.
    fn find_latest_image(&self, key: Key, lsn: Lsn) -> Option<Arc<L>> {
        self.find_latest_layer(key, Lsn(lsn.0 + 1), true)
            .map(|r| r.layer)
    }

    ///
    /// Divide the whole given range of keys into sub-ranges based on the latest
    /// image layer that covers each range. (This is used when creating  new
    /// image layers)
    ///
    // FIXME: clippy complains that the result type is very complex. She's probably
    // right...
    #[allow(clippy::type_complexity)]
    pub fn image_coverage(
        &self,
        key_range: &Range<Key>,
        lsn: Lsn,
    ) -> Result<Vec<(Range<Key>, Option<Arc<L>>)>> {
        let mut points = vec![key_range.start, key_range.end];
        let start_key = key_range.start.to_i128();
        let end_key = key_range.end.to_i128();
        for (key, layers) in self.l1_layers.range(start_key..) {
            for (_lsn, layer) in layers.iter() {
                if layer.get_lsn_range().start > lsn {
                    continue;
                }
                let range = layer.get_key_range();
                if key_range.contains(&range.start) {
                    points.push(range.start);
                }
                if key_range.contains(&range.end) {
                    points.push(range.end);
                }
            }
            if *key >= end_key {
                break;
            }
        }
        points.sort();
        points.dedup();

        // Ok, we now have a list of "interesting" points in the key space

        // For each range between the points, find the latest image
        let mut start = *points.first().unwrap();
        let mut ranges = Vec::new();
        for end in points[1..].iter() {
            let img = self.find_latest_image(start, lsn);

            ranges.push((start..*end, img));

            start = *end;
        }
        Ok(ranges)
    }

    /// Count how many L1 delta layers there are that overlap with the
    /// given key and LSN range.
    pub fn count_deltas(&self, key_range: &Range<Key>, lsn_range: &Range<Lsn>) -> Result<usize> {
        if lsn_range.start >= lsn_range.end {
            return Ok(0);
        }
        let start_key = key_range.start.to_i128();
        let end_key = key_range.end.to_i128();
        let mut all_layers = HashSet::new();
        for (key, layers) in self.l1_layers.range(start_key..) {
            for (_lsn, layer) in layers.iter() {
                if layer.is_incremental()
                    && range_overlaps(key_range, &layer.get_key_range())
                    && range_overlaps(lsn_range, &layer.get_lsn_range())
                {
                    all_layers.insert(LayerRef::new(Arc::clone(layer)));
                }
            }
            if *key >= end_key {
                break;
            }
        }
        let mut result = all_layers.len();
        for (_key, layer) in &self.l0_delta_layers {
            if range_overlaps(lsn_range, &layer.get_lsn_range()) {
                result += 1;
            }
        }
        Ok(result)
    }

    /// Return all L0 delta layers
    pub fn get_level0_deltas(&self) -> Result<Vec<Arc<L>>> {
        Ok(self.l0_delta_layers.values().cloned().collect())
    }

    /// debugging function to print out the contents of the layer map
    #[allow(unused)]
    pub fn dump(&self, verbose: bool) -> Result<()> {
        println!("Begin dump LayerMap");

        println!("open_layer:");
        if let Some(open_layer) = &self.open_layer {
            open_layer.dump(verbose)?;
        }

        println!("frozen_layers:");
        for frozen_layer in self.frozen_layers.iter() {
            frozen_layer.dump(verbose)?;
        }

        println!("historic_layers:");
        for r in self.historic_layers.iter() {
            r.layer.dump(verbose)?;
        }
        println!("End dump LayerMap");
        Ok(())
    }
}
