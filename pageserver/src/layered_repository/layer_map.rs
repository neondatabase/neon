//!
//! The layer map tracks what layers exist in a timeline.
//!
//! When the timeline is first accessed, the server lists of all layer files
//! in the timelines/<timelineid> directory, and populates this map with
//! ImageLayer and DeltaLayer structs corresponding to each file. When the first
//! new WAL record is received, we create an InMemoryLayer to hold the incoming
//! records. Now and then, in the checkpoint() function, the in-memory layer is
//! are frozen, and it is split up into new image and delta layers and the
//! corresponding files are written to disk.
//!

use crate::layered_repository::storage_layer::Layer;
use crate::layered_repository::storage_layer::{range_eq, range_overlaps};
use crate::layered_repository::InMemoryLayer;
use crate::repository::Key;
use anyhow::Result;
use lazy_static::lazy_static;
use metrics::{register_int_gauge, IntGauge};
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use tracing::*;
use utils::lsn::Lsn;

lazy_static! {
    static ref NUM_ONDISK_LAYERS: IntGauge =
        register_int_gauge!("pageserver_ondisk_layers", "Number of layers on-disk")
            .expect("failed to define a metric");
}

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
    /// The frozen layer, if any, contains WAL older than the current 'open_layer'
    /// or 'next_open_layer_at', but newer than any historic layer. The frozen
    /// layer is during checkpointing, when an InMemoryLayer is being written out
    /// to disk.
    ///
    pub frozen_layers: VecDeque<Arc<InMemoryLayer>>,

    /// All the historic layers are kept here

    /// TODO: This is a placeholder implementation of a data structure
    /// to hold information about all the layer files on disk and in
    /// S3. Currently, it's just a vector and all operations perform a
    /// linear scan over it.  That obviously becomes slow as the
    /// number of layers grows. I'm imagining that an R-tree or some
    /// other 2D data structure would be the long-term solution here.
    historic_layers: Vec<Arc<dyn Layer>>,
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
    pub fn search(&self, key: Key, end_lsn: Lsn, delta_only: bool) -> Result<Option<SearchResult>> {
        // linear search
        // Find the latest image layer that covers the given key
        let mut latest_img: Option<Arc<dyn Layer>> = None;
        let mut latest_img_lsn: Option<Lsn> = None;
        if !delta_only {
            for l in self.historic_layers.iter() {
                if l.is_incremental() {
                    continue;
                }
                if !l.get_key_range().contains(&key) {
                    continue;
                }
                let img_lsn = l.get_lsn_range().start;

                if img_lsn >= end_lsn {
                    // too new
                    continue;
                }
                if Lsn(img_lsn.0 + 1) == end_lsn {
                    // found exact match
                    return Ok(Some(SearchResult {
                        layer: Arc::clone(l),
                        lsn_floor: img_lsn,
                    }));
                }
                if img_lsn > latest_img_lsn.unwrap_or(Lsn(0)) {
                    latest_img = Some(Arc::clone(l));
                    latest_img_lsn = Some(img_lsn);
                }
            }
        }
        // Search the delta layers
        let mut latest_delta: Option<Arc<dyn Layer>> = None;
        for l in self.historic_layers.iter() {
            if !l.is_incremental() {
                continue;
            }
            if !l.get_key_range().contains(&key) {
                continue;
            }

            if l.get_lsn_range().start >= end_lsn {
                // too new
                continue;
            }

            if l.get_lsn_range().end >= end_lsn {
                // this layer contains the requested point in the key/lsn space.
                // No need to search any further
                trace!(
                    "found layer {} for request on {} at {}",
                    l.filename().display(),
                    key,
                    end_lsn
                );
                latest_delta.replace(Arc::clone(l));
                break;
            }
            // this layer's end LSN is smaller than the requested point. If there's
            // nothing newer, this is what we need to return. Remember this.
            if let Some(ref old_candidate) = latest_delta {
                if l.get_lsn_range().end > old_candidate.get_lsn_range().end {
                    latest_delta.replace(Arc::clone(l));
                }
            } else {
                latest_delta.replace(Arc::clone(l));
            }
        }
        if let Some(l) = latest_delta {
            trace!(
                "found (old) layer {} for request on {} at {}",
                l.filename().display(),
                key,
                end_lsn
            );
            let lsn_floor = std::cmp::max(
                Lsn(latest_img_lsn.unwrap_or(Lsn(0)).0 + 1),
                l.get_lsn_range().start,
            );
            Ok(Some(SearchResult {
                lsn_floor,
                layer: l,
            }))
        } else if let Some(l) = latest_img {
            trace!(
                "found img layer and no deltas for request on {} at {}",
                key,
                end_lsn
            );
            Ok(Some(SearchResult {
                lsn_floor: latest_img_lsn.unwrap(),
                layer: l,
            }))
        } else {
            trace!("no layer found for request on {} at {}", key, end_lsn);
            Ok(None)
        }
    }

    ///
    /// Insert an on-disk layer
    ///
    pub fn insert_historic(&mut self, layer: Arc<dyn Layer>) {
        self.historic_layers.push(layer);
        NUM_ONDISK_LAYERS.inc();
    }

    ///
    /// Remove an on-disk layer from the map.
    ///
    /// This should be called when the corresponding file on disk has been deleted.
    ///
    #[allow(dead_code)]
    pub fn remove_historic(&mut self, layer: Arc<dyn Layer>) {
        let len_before = self.historic_layers.len();

        // FIXME: ptr_eq might fail to return true for 'dyn'
        // references.  Clippy complains about this. In practice it
        // seems to work, the assertion below would be triggered
        // otherwise but this ought to be fixed.
        #[allow(clippy::vtable_address_comparisons)]
        self.historic_layers
            .retain(|other| !Arc::ptr_eq(other, &layer));

        assert_eq!(self.historic_layers.len(), len_before - 1);
        NUM_ONDISK_LAYERS.dec();
    }

    /// Is there a newer image layer for given key-range?
    ///
    /// This is used for garbage collection, to determine if an old layer can
    /// be deleted.
    /// We ignore layers newer than disk_consistent_lsn because they will be removed at restart
    /// We also only look at historic layers
    //#[allow(dead_code)]
    pub fn newer_image_layer_exists(
        &self,
        key_range: &Range<Key>,
        lsn: Lsn,
        disk_consistent_lsn: Lsn,
    ) -> Result<bool> {
        let mut range_remain = key_range.clone();

        loop {
            let mut made_progress = false;
            for l in self.historic_layers.iter() {
                if l.is_incremental() {
                    continue;
                }
                let img_lsn = l.get_lsn_range().start;
                if !l.is_incremental()
                    && l.get_key_range().contains(&range_remain.start)
                    && img_lsn > lsn
                    && img_lsn < disk_consistent_lsn
                {
                    made_progress = true;
                    let img_key_end = l.get_key_range().end;

                    if img_key_end >= range_remain.end {
                        return Ok(true);
                    }
                    range_remain.start = img_key_end;
                }
            }

            if !made_progress {
                return Ok(false);
            }
        }
    }

    pub fn iter_historic_layers(&self) -> std::slice::Iter<Arc<dyn Layer>> {
        self.historic_layers.iter()
    }

    /// Find the last image layer that covers 'key', ignoring any image layers
    /// newer than 'lsn'.
    fn find_latest_image(&self, key: Key, lsn: Lsn) -> Option<Arc<dyn Layer>> {
        let mut candidate_lsn = Lsn(0);
        let mut candidate = None;
        for l in self.historic_layers.iter() {
            if l.is_incremental() {
                continue;
            }

            if !l.get_key_range().contains(&key) {
                continue;
            }

            let this_lsn = l.get_lsn_range().start;
            if this_lsn > lsn {
                continue;
            }
            if this_lsn < candidate_lsn {
                // our previous candidate was better
                continue;
            }
            candidate_lsn = this_lsn;
            candidate = Some(Arc::clone(l));
        }

        candidate
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
        let mut points = vec![key_range.start];
        for l in self.historic_layers.iter() {
            if l.get_lsn_range().start > lsn {
                continue;
            }
            let range = l.get_key_range();
            if key_range.contains(&range.start) {
                points.push(l.get_key_range().start);
            }
            if key_range.contains(&range.end) {
                points.push(l.get_key_range().end);
            }
        }
        points.push(key_range.end);

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
        let mut result = 0;
        for l in self.historic_layers.iter() {
            if !l.is_incremental() {
                continue;
            }
            if !range_overlaps(&l.get_lsn_range(), lsn_range) {
                continue;
            }
            if !range_overlaps(&l.get_key_range(), key_range) {
                continue;
            }

            // We ignore level0 delta layers. Unless the whole keyspace fits
            // into one partition
            if !range_eq(key_range, &(Key::MIN..Key::MAX))
                && range_eq(&l.get_key_range(), &(Key::MIN..Key::MAX))
            {
                continue;
            }

            result += 1;
        }
        Ok(result)
    }

    /// Return all L0 delta layers
    pub fn get_level0_deltas(&self) -> Result<Vec<Arc<dyn Layer>>> {
        let mut deltas = Vec::new();
        for l in self.historic_layers.iter() {
            if !l.is_incremental() {
                continue;
            }
            if l.get_key_range() != (Key::MIN..Key::MAX) {
                continue;
            }
            deltas.push(Arc::clone(l));
        }
        Ok(deltas)
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
        for layer in self.historic_layers.iter() {
            layer.dump(verbose)?;
        }
        println!("End dump LayerMap");
        Ok(())
    }
}
