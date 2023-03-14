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
//! Design overview:
//!
//! The `search` method of the layer map is on the read critical path, so we've
//! built an efficient data structure for fast reads, stored in `LayerMap::historic`.
//! Other read methods are less critical but still impact performance of background tasks.
//!
//! This data structure relies on a persistent/immutable binary search tree. See the
//! following lecture for an introduction https://www.youtube.com/watch?v=WqCWghETNDc&t=581s
//! Summary: A persistent/immutable BST (and persistent data structures in general) allows
//! you to modify the tree in such a way that each modification creates a new "version"
//! of the tree. When you modify it, you get a new version, but all previous versions are
//! still accessible too. So if someone is still holding a reference to an older version,
//! they continue to see the tree as it was then. The persistent BST stores all the
//! different versions in an efficient way.
//!
//! Our persistent BST maintains a map of which layer file "covers" each key. It has only
//! one dimension, the key. See `layer_coverage.rs`. We use the persistent/immutable property
//! to handle the LSN dimension.
//!
//! To build the layer map, we insert each layer to the persistent BST in LSN.start order,
//! starting from the oldest one. After each insertion, we grab a reference to that "version"
//! of the tree, and store it in another tree, a BtreeMap keyed by the LSN. See
//! `historic_layer_coverage.rs`.
//!
//! To search for a particular key-LSN pair, you first look up the right "version" in the
//! BTreeMap. Then you search that version of the BST with the key.
//!
//! The persistent BST keeps all the versions, but there is no way to change the old versions
//! afterwards. We can add layers as long as they have larger LSNs than any previous layer in
//! the map, but if we need to remove a layer, or insert anything with an older LSN, we need
//! to throw away most of the persistent BST and build a new one, starting from the oldest
//! LSN. See `LayerMap::flush_updates()`.
//!

mod historic_layer_coverage;
mod layer_coverage;

use crate::context::RequestContext;
use crate::keyspace::KeyPartitioning;
use crate::metrics::NUM_ONDISK_LAYERS;
use crate::repository::Key;
use crate::tenant::storage_layer::InMemoryLayer;
use crate::tenant::storage_layer::Layer;
use anyhow::Result;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use utils::lsn::Lsn;

use historic_layer_coverage::BufferedHistoricLayerCoverage;
pub use historic_layer_coverage::Replacement;

use super::storage_layer::range_eq;

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

    /// Index of the historic layers optimized for search
    historic: BufferedHistoricLayerCoverage<Arc<L>>,

    /// L0 layers have key range Key::MIN..Key::MAX, and locating them using R-Tree search is very inefficient.
    /// So L0 layers are held in l0_delta_layers vector, in addition to the R-tree.
    l0_delta_layers: Vec<Arc<L>>,
}

impl<L: ?Sized> Default for LayerMap<L> {
    fn default() -> Self {
        Self {
            open_layer: None,
            next_open_layer_at: None,
            frozen_layers: VecDeque::default(),
            l0_delta_layers: Vec::default(),
            historic: BufferedHistoricLayerCoverage::default(),
        }
    }
}

/// The primary update API for the layer map.
///
/// Batching historic layer insertions and removals is good for
/// performance and this struct helps us do that correctly.
#[must_use]
pub struct BatchedUpdates<'a, L: ?Sized + Layer> {
    // While we hold this exclusive reference to the layer map the type checker
    // will prevent us from accidentally reading any unflushed updates.
    layer_map: &'a mut LayerMap<L>,
}

/// Provide ability to batch more updates while hiding the read
/// API so we don't accidentally read without flushing.
impl<L> BatchedUpdates<'_, L>
where
    L: ?Sized + Layer,
{
    ///
    /// Insert an on-disk layer.
    ///
    pub fn insert_historic(&mut self, layer: Arc<L>) {
        self.layer_map.insert_historic_noflush(layer)
    }

    ///
    /// Remove an on-disk layer from the map.
    ///
    /// This should be called when the corresponding file on disk has been deleted.
    ///
    pub fn remove_historic(&mut self, layer: Arc<L>) {
        self.layer_map.remove_historic_noflush(layer)
    }

    /// Replaces existing layer iff it is the `expected`.
    ///
    /// If the expected layer has been removed it will not be inserted by this function.
    ///
    /// Returned `Replacement` describes succeeding in replacement or the reason why it could not
    /// be done.
    ///
    /// TODO replacement can be done without buffering and rebuilding layer map updates.
    ///      One way to do that is to add a layer of indirection for returned values, so
    ///      that we can replace values only by updating a hashmap.
    pub fn replace_historic(
        &mut self,
        expected: &Arc<L>,
        new: Arc<L>,
    ) -> anyhow::Result<Replacement<Arc<L>>> {
        fail::fail_point!("layermap-replace-notfound", |_| Ok(Replacement::NotFound));

        self.layer_map.replace_historic_noflush(expected, new)
    }

    // We will flush on drop anyway, but this method makes it
    // more explicit that there is some work being done.
    /// Apply all updates
    pub fn flush(self) {
        // Flush happens on drop
    }
}

// Ideally the flush() method should be called explicitly for more
// controlled execution. But if we forget we'd rather flush on drop
// than panic later or read without flushing.
//
// TODO maybe warn if flush hasn't explicitly been called
impl<L> Drop for BatchedUpdates<'_, L>
where
    L: ?Sized + Layer,
{
    fn drop(&mut self) {
        self.layer_map.flush_updates();
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
    ///
    /// Find the latest layer (by lsn.end) that covers the given
    /// 'key', with lsn.start < 'end_lsn'.
    ///
    /// The caller of this function is the page reconstruction
    /// algorithm looking for the next relevant delta layer, or
    /// the terminal image layer. The caller will pass the lsn_floor
    /// value as end_lsn in the next call to search.
    ///
    /// If there's an image layer exactly below the given end_lsn,
    /// search should return that layer regardless if there are
    /// overlapping deltas.
    ///
    /// If the latest layer is a delta and there is an overlapping
    /// image with it below, the lsn_floor returned should be right
    /// above that image so we don't skip it in the search. Otherwise
    /// the lsn_floor returned should be the bottom of the delta layer
    /// because we should make as much progress down the lsn axis
    /// as possible. It's fine if this way we skip some overlapping
    /// deltas, because the delta we returned would contain the same
    /// wal content.
    ///
    /// TODO: This API is convoluted and inefficient. If the caller
    /// makes N search calls, we'll end up finding the same latest
    /// image layer N times. We should either cache the latest image
    /// layer result, or simplify the api to `get_latest_image` and
    /// `get_latest_delta`, and only call `get_latest_image` once.
    ///
    /// NOTE: This only searches the 'historic' layers, *not* the
    /// 'open' and 'frozen' layers!
    ///
    pub fn search(&self, key: Key, end_lsn: Lsn) -> Option<SearchResult<L>> {
        let version = self.historic.get().unwrap().get_version(end_lsn.0 - 1)?;
        let latest_delta = version.delta_coverage.query(key.to_i128());
        let latest_image = version.image_coverage.query(key.to_i128());

        match (latest_delta, latest_image) {
            (None, None) => None,
            (None, Some(image)) => {
                let lsn_floor = image.get_lsn_range().start;
                Some(SearchResult {
                    layer: image,
                    lsn_floor,
                })
            }
            (Some(delta), None) => {
                let lsn_floor = delta.get_lsn_range().start;
                Some(SearchResult {
                    layer: delta,
                    lsn_floor,
                })
            }
            (Some(delta), Some(image)) => {
                let img_lsn = image.get_lsn_range().start;
                let image_is_newer = image.get_lsn_range().end >= delta.get_lsn_range().end;
                let image_exact_match = img_lsn + 1 == end_lsn;
                if image_is_newer || image_exact_match {
                    Some(SearchResult {
                        layer: image,
                        lsn_floor: img_lsn,
                    })
                } else {
                    let lsn_floor =
                        std::cmp::max(delta.get_lsn_range().start, image.get_lsn_range().start + 1);
                    Some(SearchResult {
                        layer: delta,
                        lsn_floor,
                    })
                }
            }
        }
    }

    /// Start a batch of updates, applied on drop
    pub fn batch_update(&mut self) -> BatchedUpdates<'_, L> {
        BatchedUpdates { layer_map: self }
    }

    ///
    /// Insert an on-disk layer
    ///
    /// Helper function for BatchedUpdates::insert_historic
    ///
    pub(self) fn insert_historic_noflush(&mut self, layer: Arc<L>) {
        self.historic.insert(
            historic_layer_coverage::LayerKey::from(&*layer),
            Arc::clone(&layer),
        );

        if Self::is_l0(&layer) {
            self.l0_delta_layers.push(layer);
        }

        NUM_ONDISK_LAYERS.inc();
    }

    ///
    /// Remove an on-disk layer from the map.
    ///
    /// Helper function for BatchedUpdates::remove_historic
    ///
    pub fn remove_historic_noflush(&mut self, layer: Arc<L>) {
        self.historic
            .remove(historic_layer_coverage::LayerKey::from(&*layer));

        if Self::is_l0(&layer) {
            let len_before = self.l0_delta_layers.len();
            self.l0_delta_layers
                .retain(|other| !Self::compare_arced_layers(other, &layer));
            // this assertion is related to use of Arc::ptr_eq in Self::compare_arced_layers,
            // there's a chance that the comparison fails at runtime due to it comparing (pointer,
            // vtable) pairs.
            assert_eq!(
                self.l0_delta_layers.len(),
                len_before - 1,
                "failed to locate removed historic layer from l0_delta_layers"
            );
        }

        NUM_ONDISK_LAYERS.dec();
    }

    pub(self) fn replace_historic_noflush(
        &mut self,
        expected: &Arc<L>,
        new: Arc<L>,
    ) -> anyhow::Result<Replacement<Arc<L>>> {
        let key = historic_layer_coverage::LayerKey::from(&**expected);
        let other = historic_layer_coverage::LayerKey::from(&*new);

        let expected_l0 = Self::is_l0(expected);
        let new_l0 = Self::is_l0(&new);

        anyhow::ensure!(
            key == other,
            "expected and new must have equal LayerKeys: {key:?} != {other:?}"
        );

        anyhow::ensure!(
            expected_l0 == new_l0,
            "expected and new must both be l0 deltas or neither should be: {expected_l0} != {new_l0}"
        );

        let l0_index = if expected_l0 {
            // find the index in case replace worked, we need to replace that as well
            let pos = self
                .l0_delta_layers
                .iter()
                .position(|slot| Self::compare_arced_layers(slot, expected));

            if pos.is_none() {
                return Ok(Replacement::NotFound);
            }
            pos
        } else {
            None
        };

        let replaced = self.historic.replace(&key, new.clone(), |existing| {
            Self::compare_arced_layers(existing, expected)
        });

        if let Replacement::Replaced { .. } = &replaced {
            if let Some(index) = l0_index {
                self.l0_delta_layers[index] = new;
            }
        }

        Ok(replaced)
    }

    /// Helper function for BatchedUpdates::drop.
    pub(self) fn flush_updates(&mut self) {
        self.historic.rebuild();
    }

    /// Is there a newer image layer for given key- and LSN-range? Or a set
    /// of image layers within the specified lsn range that cover the entire
    /// specified key range?
    ///
    /// This is used for garbage collection, to determine if an old layer can
    /// be deleted.
    pub fn image_layer_exists(&self, key: &Range<Key>, lsn: &Range<Lsn>) -> Result<bool> {
        if key.is_empty() {
            // Vacuously true. There's a newer image for all 0 of the kerys in the range.
            return Ok(true);
        }

        let version = match self.historic.get().unwrap().get_version(lsn.end.0 - 1) {
            Some(v) => v,
            None => return Ok(false),
        };

        let start = key.start.to_i128();
        let end = key.end.to_i128();

        let layer_covers = |layer: Option<Arc<L>>| match layer {
            Some(layer) => layer.get_lsn_range().start >= lsn.start,
            None => false,
        };

        // Check the start is covered
        if !layer_covers(version.image_coverage.query(start)) {
            return Ok(false);
        }

        // Check after all changes of coverage
        for (_, change_val) in version.image_coverage.range(start..end) {
            if !layer_covers(change_val) {
                return Ok(false);
            }
        }

        Ok(true)
    }

    pub fn iter_historic_layers(&self) -> impl '_ + Iterator<Item = Arc<L>> {
        self.historic.iter()
    }

    ///
    /// Divide the whole given range of keys into sub-ranges based on the latest
    /// image layer that covers each range at the specified lsn (inclusive).
    /// This is used when creating  new image layers.
    ///
    // FIXME: clippy complains that the result type is very complex. She's probably
    // right...
    #[allow(clippy::type_complexity)]
    pub fn image_coverage(
        &self,
        key_range: &Range<Key>,
        lsn: Lsn,
    ) -> Result<Vec<(Range<Key>, Option<Arc<L>>)>> {
        let version = match self.historic.get().unwrap().get_version(lsn.0) {
            Some(v) => v,
            None => return Ok(vec![]),
        };

        let start = key_range.start.to_i128();
        let end = key_range.end.to_i128();

        // Initialize loop variables
        let mut coverage: Vec<(Range<Key>, Option<Arc<L>>)> = vec![];
        let mut current_key = start;
        let mut current_val = version.image_coverage.query(start);

        // Loop through the change events and push intervals
        for (change_key, change_val) in version.image_coverage.range(start..end) {
            let kr = Key::from_i128(current_key)..Key::from_i128(change_key);
            coverage.push((kr, current_val.take()));
            current_key = change_key;
            current_val = change_val.clone();
        }

        // Add the final interval
        let kr = Key::from_i128(current_key)..Key::from_i128(end);
        coverage.push((kr, current_val.take()));

        Ok(coverage)
    }

    pub fn is_l0(layer: &L) -> bool {
        range_eq(&layer.get_key_range(), &(Key::MIN..Key::MAX))
    }

    /// This function determines which layers are counted in `count_deltas`:
    /// layers that should count towards deciding whether or not to reimage
    /// a certain partition range.
    ///
    /// There are two kinds of layers we currently consider reimage-worthy:
    ///
    /// Case 1: Non-L0 layers are currently reimage-worthy by default.
    /// TODO Some of these layers are very sparse and cover the entire key
    ///      range. Replacing 256MB of data (or less!) with terabytes of
    ///      images doesn't seem wise. We need a better heuristic, possibly
    ///      based on some of these factors:
    ///      a) whether this layer has any wal in this partition range
    ///      b) the size of the layer
    ///      c) the number of images needed to cover it
    ///      d) the estimated time until we'll have to reimage over it for GC
    ///
    /// Case 2: Since L0 layers by definition cover the entire key space, we consider
    /// them reimage-worthy only when the entire key space can be covered by very few
    /// images (currently 1).
    /// TODO The optimal number should probably be slightly higher than 1, but to
    ///      implement that we need to plumb a lot more context into this function
    ///      than just the current partition_range.
    pub fn is_reimage_worthy(layer: &L, partition_range: &Range<Key>) -> bool {
        // Case 1
        if !Self::is_l0(layer) {
            return true;
        }

        // Case 2
        if range_eq(partition_range, &(Key::MIN..Key::MAX)) {
            return true;
        }

        false
    }

    /// Count the height of the tallest stack of reimage-worthy deltas
    /// in this 2d region.
    ///
    /// If `limit` is provided we don't try to count above that number.
    ///
    /// This number is used to compute the largest number of deltas that
    /// we'll need to visit for any page reconstruction in this region.
    /// We use this heuristic to decide whether to create an image layer.
    pub fn count_deltas(
        &self,
        key: &Range<Key>,
        lsn: &Range<Lsn>,
        limit: Option<usize>,
    ) -> Result<usize> {
        // We get the delta coverage of the region, and for each part of the coverage
        // we recurse right underneath the delta. The recursion depth is limited by
        // the largest result this function could return, which is in practice between
        // 3 and 10 (since we usually try to create an image when the number gets larger).

        if lsn.is_empty() || key.is_empty() || limit == Some(0) {
            return Ok(0);
        }

        let version = match self.historic.get().unwrap().get_version(lsn.end.0 - 1) {
            Some(v) => v,
            None => return Ok(0),
        };

        let start = key.start.to_i128();
        let end = key.end.to_i128();

        // Initialize loop variables
        let mut max_stacked_deltas = 0;
        let mut current_key = start;
        let mut current_val = version.delta_coverage.query(start);

        // Loop through the delta coverage and recurse on each part
        for (change_key, change_val) in version.delta_coverage.range(start..end) {
            // If there's a relevant delta in this part, add 1 and recurse down
            if let Some(val) = current_val {
                if val.get_lsn_range().end > lsn.start {
                    let kr = Key::from_i128(current_key)..Key::from_i128(change_key);
                    let lr = lsn.start..val.get_lsn_range().start;
                    if !kr.is_empty() {
                        let base_count = Self::is_reimage_worthy(&val, key) as usize;
                        let new_limit = limit.map(|l| l - base_count);
                        let max_stacked_deltas_underneath =
                            self.count_deltas(&kr, &lr, new_limit)?;
                        max_stacked_deltas = std::cmp::max(
                            max_stacked_deltas,
                            base_count + max_stacked_deltas_underneath,
                        );
                    }
                }
            }

            current_key = change_key;
            current_val = change_val.clone();
        }

        // Consider the last part
        if let Some(val) = current_val {
            if val.get_lsn_range().end > lsn.start {
                let kr = Key::from_i128(current_key)..Key::from_i128(end);
                let lr = lsn.start..val.get_lsn_range().start;

                if !kr.is_empty() {
                    let base_count = Self::is_reimage_worthy(&val, key) as usize;
                    let new_limit = limit.map(|l| l - base_count);
                    let max_stacked_deltas_underneath = self.count_deltas(&kr, &lr, new_limit)?;
                    max_stacked_deltas = std::cmp::max(
                        max_stacked_deltas,
                        base_count + max_stacked_deltas_underneath,
                    );
                }
            }
        }

        Ok(max_stacked_deltas)
    }

    /// Count how many reimage-worthy layers we need to visit for given key-lsn pair.
    ///
    /// The `partition_range` argument is used as context for the reimage-worthiness decision.
    ///
    /// Used as a helper for correctness checks only. Performance not critical.
    pub fn get_difficulty(&self, lsn: Lsn, key: Key, partition_range: &Range<Key>) -> usize {
        match self.search(key, lsn) {
            Some(search_result) => {
                if search_result.layer.is_incremental() {
                    (Self::is_reimage_worthy(&search_result.layer, partition_range) as usize)
                        + self.get_difficulty(search_result.lsn_floor, key, partition_range)
                } else {
                    0
                }
            }
            None => 0,
        }
    }

    /// Used for correctness checking. Results are expected to be identical to
    /// self.get_difficulty_map. Assumes self.search is correct.
    pub fn get_difficulty_map_bruteforce(
        &self,
        lsn: Lsn,
        partitioning: &KeyPartitioning,
    ) -> Vec<usize> {
        // Looking at the difficulty as a function of key, it could only increase
        // when a delta layer starts or an image layer ends. Therefore it's sufficient
        // to check the difficulties at:
        // - the key.start for each non-empty part range
        // - the key.start for each delta
        // - the key.end for each image
        let keys_iter: Box<dyn Iterator<Item = Key>> = {
            let mut keys: Vec<Key> = self
                .iter_historic_layers()
                .map(|layer| {
                    if layer.is_incremental() {
                        layer.get_key_range().start
                    } else {
                        layer.get_key_range().end
                    }
                })
                .collect();
            keys.sort();
            Box::new(keys.into_iter())
        };
        let mut keys_iter = keys_iter.peekable();

        // Iter the partition and keys together and query all the necessary
        // keys, computing the max difficulty for each part.
        partitioning
            .parts
            .iter()
            .map(|part| {
                let mut difficulty = 0;
                // Partition ranges are assumed to be sorted and disjoint
                // TODO assert it
                for range in &part.ranges {
                    if !range.is_empty() {
                        difficulty =
                            std::cmp::max(difficulty, self.get_difficulty(lsn, range.start, range));
                    }
                    while let Some(key) = keys_iter.peek() {
                        if key >= &range.end {
                            break;
                        }
                        let key = keys_iter.next().unwrap();
                        if key < range.start {
                            continue;
                        }
                        difficulty =
                            std::cmp::max(difficulty, self.get_difficulty(lsn, key, range));
                    }
                }
                difficulty
            })
            .collect()
    }

    /// For each part of a keyspace partitioning, return the maximum number of layers
    /// that would be needed for page reconstruction in that part at the given LSN.
    ///
    /// If `limit` is provided we don't try to count above that number.
    ///
    /// This method is used to decide where to create new image layers. Computing the
    /// result for the entire partitioning at once allows this function to be more
    /// efficient, and further optimization is possible by using iterators instead,
    /// to allow early return.
    ///
    /// TODO actually use this method instead of count_deltas. Currently we only use
    ///      it for benchmarks.
    pub fn get_difficulty_map(
        &self,
        lsn: Lsn,
        partitioning: &KeyPartitioning,
        limit: Option<usize>,
    ) -> Vec<usize> {
        // TODO This is a naive implementation. Perf improvements to do:
        // 1. Instead of calling self.image_coverage and self.count_deltas,
        //    iterate the image and delta coverage only once.
        partitioning
            .parts
            .iter()
            .map(|part| {
                let mut difficulty = 0;
                for range in &part.ranges {
                    if limit == Some(difficulty) {
                        break;
                    }
                    for (img_range, last_img) in self
                        .image_coverage(range, lsn)
                        .expect("why would this err?")
                    {
                        if limit == Some(difficulty) {
                            break;
                        }
                        let img_lsn = if let Some(last_img) = last_img {
                            last_img.get_lsn_range().end
                        } else {
                            Lsn(0)
                        };

                        if img_lsn < lsn {
                            let num_deltas = self
                                .count_deltas(&img_range, &(img_lsn..lsn), limit)
                                .expect("why would this err lol?");
                            difficulty = std::cmp::max(difficulty, num_deltas);
                        }
                    }
                }
                difficulty
            })
            .collect()
    }

    /// Return all L0 delta layers
    pub fn get_level0_deltas(&self) -> Result<Vec<Arc<L>>> {
        Ok(self.l0_delta_layers.clone())
    }

    /// debugging function to print out the contents of the layer map
    #[allow(unused)]
    pub fn dump(&self, verbose: bool, ctx: &RequestContext) -> Result<()> {
        println!("Begin dump LayerMap");

        println!("open_layer:");
        if let Some(open_layer) = &self.open_layer {
            open_layer.dump(verbose, ctx)?;
        }

        println!("frozen_layers:");
        for frozen_layer in self.frozen_layers.iter() {
            frozen_layer.dump(verbose, ctx)?;
        }

        println!("historic_layers:");
        for layer in self.iter_historic_layers() {
            layer.dump(verbose, ctx)?;
        }
        println!("End dump LayerMap");
        Ok(())
    }

    /// Similar to `Arc::ptr_eq`, but only compares the object pointers, not vtables.
    ///
    /// Returns `true` if the two `Arc` point to the same layer, false otherwise.
    #[inline(always)]
    pub fn compare_arced_layers(left: &Arc<L>, right: &Arc<L>) -> bool {
        // "dyn Trait" objects are "fat pointers" in that they have two components:
        // - pointer to the object
        // - pointer to the vtable
        //
        // rust does not provide a guarantee that these vtables are unique, but however
        // `Arc::ptr_eq` as of writing (at least up to 1.67) uses a comparison where both the
        // pointer and the vtable need to be equal.
        //
        // See: https://github.com/rust-lang/rust/issues/103763
        //
        // A future version of rust will most likely use this form below, where we cast each
        // pointer into a pointer to unit, which drops the inaccessible vtable pointer, making it
        // not affect the comparison.
        //
        // See: https://github.com/rust-lang/rust/pull/106450
        let left = Arc::as_ptr(left) as *const ();
        let right = Arc::as_ptr(right) as *const ();

        left == right
    }
}

#[cfg(test)]
mod tests {
    use super::{LayerMap, Replacement};
    use crate::tenant::storage_layer::{Layer, LayerDescriptor, LayerFileName};
    use std::str::FromStr;
    use std::sync::Arc;

    mod l0_delta_layers_updated {

        use super::*;

        #[test]
        fn for_full_range_delta() {
            // l0_delta_layers are used by compaction, and should observe all buffered updates
            l0_delta_layers_updated_scenario(
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000053423C21-0000000053424D69",
                true
            )
        }

        #[test]
        fn for_non_full_range_delta() {
            // has minimal uncovered areas compared to l0_delta_layers_updated_on_insert_replace_remove_for_full_range_delta
            l0_delta_layers_updated_scenario(
                "000000000000000000000000000000000001-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFE__0000000053423C21-0000000053424D69",
                // because not full range
                false
            )
        }

        #[test]
        fn for_image() {
            l0_delta_layers_updated_scenario(
                "000000000000000000000000000000000000-000000000000000000000000000000010000__0000000053424D69",
                // code only checks if it is a full range layer, doesn't care about images, which must
                // mean we should in practice never have full range images
                false
            )
        }

        #[test]
        fn replacing_missing_l0_is_notfound() {
            // original impl had an oversight, and L0 was an anyhow::Error. anyhow::Error should
            // however only happen for precondition failures.

            let layer = "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000053423C21-0000000053424D69";
            let layer = LayerFileName::from_str(layer).unwrap();
            let layer = LayerDescriptor::from(layer);

            // same skeletan construction; see scenario below
            let not_found: Arc<dyn Layer> = Arc::new(layer.clone());
            let new_version: Arc<dyn Layer> = Arc::new(layer);

            let mut map = LayerMap::default();

            let res = map.batch_update().replace_historic(&not_found, new_version);

            assert!(matches!(res, Ok(Replacement::NotFound)), "{res:?}");
        }

        fn l0_delta_layers_updated_scenario(layer_name: &str, expected_l0: bool) {
            let name = LayerFileName::from_str(layer_name).unwrap();
            let skeleton = LayerDescriptor::from(name);

            let remote: Arc<dyn Layer> = Arc::new(skeleton.clone());
            let downloaded: Arc<dyn Layer> = Arc::new(skeleton);

            let mut map = LayerMap::default();

            // two disjoint Arcs in different lifecycle phases. even if it seems they must be the
            // same layer, we use LayerMap::compare_arced_layers as the identity of layers.
            assert!(!LayerMap::compare_arced_layers(&remote, &downloaded));

            let expected_in_counts = (1, usize::from(expected_l0));

            map.batch_update().insert_historic(remote.clone());
            assert_eq!(count_layer_in(&map, &remote), expected_in_counts);

            let replaced = map
                .batch_update()
                .replace_historic(&remote, downloaded.clone())
                .expect("name derived attributes are the same");
            assert!(
                matches!(replaced, Replacement::Replaced { .. }),
                "{replaced:?}"
            );
            assert_eq!(count_layer_in(&map, &downloaded), expected_in_counts);

            map.batch_update().remove_historic(downloaded.clone());
            assert_eq!(count_layer_in(&map, &downloaded), (0, 0));
        }

        fn count_layer_in(map: &LayerMap<dyn Layer>, layer: &Arc<dyn Layer>) -> (usize, usize) {
            let historic = map
                .iter_historic_layers()
                .filter(|x| LayerMap::compare_arced_layers(x, layer))
                .count();
            let l0s = map
                .get_level0_deltas()
                .expect("why does this return a result");
            let l0 = l0s
                .iter()
                .filter(|x| LayerMap::compare_arced_layers(x, layer))
                .count();

            (historic, l0)
        }
    }
}
