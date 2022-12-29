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
use crate::tenant::storage_layer::Layer;
use crate::tenant::storage_layer::{range_eq, range_overlaps};
use amplify_num::i256;
use anyhow::Result;
use num_traits::identities::{One, Zero};
use num_traits::{Bounded, Num, Signed};
use rstar::{RTree, RTreeObject, AABB};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Range;
use std::ops::{Add, Div, Mul, Neg, Rem, Sub};
use std::sync::Arc;
use tracing::*;
use utils::lsn::Lsn;

use super::bst_layer_map::RetroactiveLayerMap;

///
/// LayerMap tracks what layers exist on a timeline.
///
#[derive(Default)]
pub struct LayerMap {
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
    index: RetroactiveLayerMap<Arc<dyn Layer>>,

    /// L0 layers have key range Key::MIN..Key::MAX, and locating them using R-Tree search is very inefficient.
    /// So L0 layers are held in l0_delta_layers vector, in addition to the R-tree.
    l0_delta_layers: Vec<Arc<dyn Layer>>,
}

/// Return value of LayerMap::search
pub struct SearchResult {
    pub layer: Arc<dyn Layer>,
    pub lsn_floor: Lsn,
}

impl LayerMap {
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
    pub fn search(&self, key: Key, end_lsn: Lsn) -> Result<Option<SearchResult>> {
        match self.index.query(key.to_i128(), end_lsn.0 - 1) {
            (None, None) => Ok(None),
            (None, Some(image)) => {
                let lsn_floor = image.get_lsn_range().start;
                Ok(Some(SearchResult {
                    layer: image,
                    lsn_floor,
                }))
            }
            (Some(delta), None) => {
                let lsn_floor = delta.get_lsn_range().start;
                Ok(Some(SearchResult {
                    layer: delta,
                    lsn_floor,
                }))
            }
            (Some(delta), Some(image)) => {
                let img_lsn = image.get_lsn_range().start;
                let image_is_newer = image.get_lsn_range().end > delta.get_lsn_range().end;
                let image_exact_match = Lsn(img_lsn.0 + 1) == end_lsn;
                if image_is_newer || image_exact_match {
                    Ok(Some(SearchResult {
                        layer: image,
                        lsn_floor: img_lsn,
                    }))
                } else {
                    let lsn_floor =
                        std::cmp::max(delta.get_lsn_range().start, image.get_lsn_range().start + 1);
                    Ok(Some(SearchResult {
                        layer: delta,
                        lsn_floor,
                    }))
                }
            }
        }
    }

    ///
    /// Insert an on-disk layer
    ///
    pub fn insert_historic(&mut self, layer: Arc<dyn Layer>) {
        let kr = layer.get_key_range();
        let lr = layer.get_lsn_range();
        self.index.insert(
            kr.start.to_i128()..kr.end.to_i128(),
            lr.start.0..lr.end.0,
            Arc::clone(&layer),
            !layer.is_incremental(),
        );

        if layer.get_key_range() == (Key::MIN..Key::MAX) {
            self.l0_delta_layers.push(layer.clone());
        }

        NUM_ONDISK_LAYERS.inc();
    }

    /// Must be called after a batch of insert_historic calls, before querying
    pub fn rebuild_index(&mut self) {
        self.index.rebuild();
    }

    ///
    /// Remove an on-disk layer from the map.
    ///
    /// This should be called when the corresponding file on disk has been deleted.
    ///
    pub fn remove_historic(&mut self, layer: Arc<dyn Layer>) {
        let kr = layer.get_key_range();
        let lr = layer.get_lsn_range();
        self.index.remove(
            kr.start.to_i128()..kr.end.to_i128(),
            lr.start.0..lr.end.0,
            !layer.is_incremental(),
        );

        if layer.get_key_range() == (Key::MIN..Key::MAX) {
            let len_before = self.l0_delta_layers.len();

            // FIXME: ptr_eq might fail to return true for 'dyn'
            // references.  Clippy complains about this. In practice it
            // seems to work, the assertion below would be triggered
            // otherwise but this ought to be fixed.
            #[allow(clippy::vtable_address_comparisons)]
            self.l0_delta_layers
                .retain(|other| !Arc::ptr_eq(other, &layer));
            assert_eq!(self.l0_delta_layers.len(), len_before - 1);
        }

        NUM_ONDISK_LAYERS.dec();
    }

    /// Is there a newer image layer for given key- and LSN-range?
    ///
    /// This is used for garbage collection, to determine if an old layer can
    /// be deleted.
    pub fn image_layer_exists(&self, key: &Range<Key>, lsn: &Range<Lsn>) -> Result<bool> {
        if key.is_empty() {
            return Ok(true);
        }

        let version = match self.index.get_version(lsn.end.0) {
            Some(v) => v,
            None => return Ok(false),
        };

        let start = key.start.to_i128();
        let end = key.end.to_i128();

        let layer_covers = |layer: Option<Arc<dyn Layer>>| match layer {
            Some(layer) => layer.get_lsn_range().start >= lsn.start,
            None => false,
        };

        // Check the start is covered
        if !layer_covers(version.query(start).1) {
            return Ok(false);
        }

        // Check after all changes of coverage
        for (_, change_val) in version.image_coverage(start..end) {
            if !layer_covers(change_val) {
                return Ok(false);
            }
        }

        return Ok(true);
    }

    pub fn iter_historic_layers(&self) -> impl '_ + Iterator<Item = Arc<dyn Layer>> {
        self.index.iter()
    }

    /// Find the last image layer that covers 'key', ignoring any image layers
    /// newer than 'lsn'.
    fn find_latest_image(&self, key: Key, lsn: Lsn) -> Option<Arc<dyn Layer>> {
        return self.index.query(key.to_i128(), lsn.0).1;
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
    ) -> Result<Vec<(Range<Key>, Option<Arc<dyn Layer>>)>> {
        let version = match self.index.get_version(lsn.0) {
            Some(v) => v,
            None => return Ok(vec![]),
        };

        let start = key_range.start.to_i128();
        let end = key_range.end.to_i128();

        // Initialize loop variables
        let mut coverage: Vec<(Range<Key>, Option<Arc<dyn Layer>>)> = vec![];
        let mut current_key = start.clone();
        let mut current_val = version.query(start).1;

        // Loop through the change events and push intervals
        for (change_key, change_val) in version.image_coverage(start..end) {
            let kr = Key::from_i128(current_key)..Key::from_i128(change_key);
            coverage.push((kr, current_val.take()));
            current_key = change_key.clone();
            current_val = change_val.clone();
        }

        // Add the final interval
        let kr = Key::from_i128(current_key)..Key::from_i128(end);
        coverage.push((kr, current_val.take()));

        return Ok(coverage);
    }

    /// Count the height of the tallest stack of deltas in this 2d region.
    /// This number is used to compute the largest number of deltas that
    /// we'll need to visit for any page reconstruction in this region.
    /// We use this heuristic to decide whether to create an image layer.
    pub fn count_deltas(&self, key: &Range<Key>, lsn: &Range<Lsn>) -> Result<usize> {
        // We get the delta coverage of the region, and for each part of the coverage
        // we recurse right underneath the delta. The recursion depth is limited by
        // the largest result this function could return, which is in practice between
        // 3 and 10 (since we usually try to create an image when the number gets larger).

        let version = match self.index.get_version(lsn.end.0) {
            Some(v) => v,
            None => return Ok(0),
        };

        let start = key.start.to_i128();
        let end = key.end.to_i128();

        // Initialize loop variables
        let mut max_stacked_deltas = 0;
        let mut current_key = start.clone();
        let mut current_val = version.query(start).1;

        // Loop through the delta coverage and recurse on each part
        for (change_key, change_val) in version.delta_coverage(start..end) {
            // If there's a relevant delta in this part, add 1 and recurse down
            if let Some(val) = current_val {
                if val.get_lsn_range().end.0 >= lsn.start.0 {
                    let kr = Key::from_i128(current_key)..Key::from_i128(change_key);
                    let lr = lsn.start..val.get_lsn_range().start;
                    let max_stacked_deltas_underneath = self.count_deltas(&kr, &lr)?;

                    max_stacked_deltas =
                        std::cmp::max(max_stacked_deltas, 1 + max_stacked_deltas_underneath);
                }
            }

            current_key = change_key.clone();
            current_val = change_val.clone();
        }

        // Consider the last part
        if let Some(val) = current_val {
            if val.get_lsn_range().end.0 >= lsn.start.0 {
                let kr = Key::from_i128(current_key)..Key::from_i128(end);
                let lr = lsn.start..val.get_lsn_range().start;
                let max_stacked_deltas_underneath = self.count_deltas(&kr, &lr)?;

                max_stacked_deltas =
                    std::cmp::max(max_stacked_deltas, 1 + max_stacked_deltas_underneath);
            }
        }

        Ok(max_stacked_deltas)
    }

    /// Return all L0 delta layers
    pub fn get_level0_deltas(&self) -> Result<Vec<Arc<dyn Layer>>> {
        Ok(self.l0_delta_layers.clone())
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
        for layer in self.iter_historic_layers() {
            layer.dump(verbose)?;
        }
        println!("End dump LayerMap");
        Ok(())
    }
}

// TODO add layer map tests
