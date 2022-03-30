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
use num_traits::identities::{One, Zero};
use num_traits::{Bounded, Num, Signed};
use rstar::{RTree, RTreeObject, AABB};
use std::collections::VecDeque;
use std::ops::Range;
use std::ops::{Add, Div, Mul, Neg, Rem, Sub};
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
    /// Frozen layers, if any. Frozen layers are in-memory layers that
    /// are no longer added to, but haven't been written out to disk
    /// yet. They contain WAL older than the current 'open_layer' or
    /// 'next_open_layer_at', but newer than any historic layer.
    /// The frozen layers are in order from oldest to newest, so that
    /// the newest one is in the 'back' of the VecDeque, and the oldest
    /// in the 'front'.
    ///
    pub frozen_layers: VecDeque<Arc<InMemoryLayer>>,

    /// All the historic layers are kept here
    historic_layers: RTree<LayerEnvelope>,

    /// L0 layers has key range: (Key::MIN..Key::MAX) and locating them using R-Tree search is very inefficient.
    /// So l) layers are also pushed in l0_layers vector.
    l0_layers: Vec<Arc<dyn Layer>>,
}

struct LayerEnvelope {
    layer: Arc<dyn Layer>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Debug)]
struct IntKey(i128);

impl Bounded for IntKey {
    fn min_value() -> Self {
        IntKey(i128::MIN)
    }
    fn max_value() -> Self {
        IntKey(i128::MAX)
    }
}

impl Signed for IntKey {
    fn is_positive(&self) -> bool {
        self.0 > 0
    }
    fn is_negative(&self) -> bool {
        self.0 < 0
    }
    fn signum(&self) -> Self {
        IntKey(self.0.signum())
    }
    fn abs(&self) -> Self {
        IntKey(self.0.abs())
    }
    fn abs_sub(&self, other: &Self) -> Self {
        IntKey(self.0.abs_sub(&other.0))
    }
}

impl Neg for IntKey {
    type Output = Self;
    fn neg(self) -> Self::Output {
        IntKey(-self.0)
    }
}

impl Rem for IntKey {
    type Output = Self;
    fn rem(self, rhs: Self) -> Self::Output {
        IntKey(self.0 % rhs.0)
    }
}

impl Div for IntKey {
    type Output = Self;
    fn div(self, rhs: Self) -> Self::Output {
        IntKey(self.0 / rhs.0)
    }
}

impl Add for IntKey {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        IntKey(self.0 + rhs.0)
    }
}

impl Sub for IntKey {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self::Output {
        IntKey(self.0 - rhs.0)
    }
}

impl Mul for IntKey {
    type Output = Self;
    fn mul(self, rhs: Self) -> Self::Output {
        IntKey(self.0.wrapping_mul(rhs.0))
    }
}

impl One for IntKey {
    fn one() -> Self {
        IntKey(1)
    }
}

impl Zero for IntKey {
    fn zero() -> Self {
        IntKey(0)
    }
    fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl Num for IntKey {
    type FromStrRadixErr = <i128 as Num>::FromStrRadixErr;
    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        Ok(IntKey(i128::from_str_radix(str, radix)?))
    }
}

impl PartialEq for LayerEnvelope {
    fn eq(&self, other: &Self) -> bool {
        // FIXME: ptr_eq might fail to return true for 'dyn'
        // references.  Clippy complains about this. In practice it
        // seems to work, the assertion below would be triggered
        // otherwise but this ought to be fixed.
        #[allow(clippy::vtable_address_comparisons)]
        Arc::ptr_eq(&self.layer, &other.layer)
    }
}

impl RTreeObject for LayerEnvelope {
    type Envelope = AABB<[IntKey; 2]>;
    fn envelope(&self) -> Self::Envelope {
        let key_range = self.layer.get_key_range();
        let lsn_range = self.layer.get_lsn_range();
        AABB::from_corners(
            [
                IntKey(key_range.start.to_i128()),
                IntKey(lsn_range.start.0 as i128),
            ],
            [
                IntKey(key_range.end.to_i128() - 1),
                IntKey(lsn_range.end.0 as i128 - 1),
            ], // end is exlusive
        )
    }
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
        // linear search
        // Find the latest image layer that covers the given key
        let mut latest_img: Option<Arc<dyn Layer>> = None;
        let mut latest_img_lsn: Option<Lsn> = None;
        let envelope = AABB::from_corners(
            [IntKey(key.to_i128()), IntKey(0i128)],
            [IntKey(key.to_i128()), IntKey(end_lsn.0 as i128 - 1)],
        );
        for e in self
            .historic_layers
            .locate_in_envelope_intersecting(&envelope)
        {
            let l = &e.layer;
            if l.is_incremental() {
                continue;
            }
            assert!(l.get_key_range().contains(&key));
            let img_lsn = l.get_lsn_range().start;
            assert!(img_lsn < end_lsn);
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

        // Search the delta layers
        let mut latest_delta: Option<Arc<dyn Layer>> = None;
        for e in self
            .historic_layers
            .locate_in_envelope_intersecting(&envelope)
        {
            let l = &e.layer;
            if !l.is_incremental() {
                continue;
            }
            assert!(l.get_key_range().contains(&key));
            assert!(l.get_lsn_range().start < end_lsn);
            if l.get_lsn_range().end >= end_lsn {
                // this layer contains the requested point in the key/lsn space.
                // No need to search any further
                trace!(
                    "found layer {} for request on {key} at {end_lsn}",
                    l.filename().display(),
                );
                latest_delta.replace(Arc::clone(l));
                break;
            }
            // this layer's end LSN is smaller than the requested point. If there's
            // nothing newer, this is what we need to return. Remember this.
            if let Some(old_candidate) = &latest_delta {
                if l.get_lsn_range().end > old_candidate.get_lsn_range().end {
                    latest_delta.replace(Arc::clone(l));
                }
            } else {
                latest_delta.replace(Arc::clone(l));
            }
        }
        if let Some(l) = latest_delta {
            trace!(
                "found (old) layer {} for request on {key} at {end_lsn}",
                l.filename().display(),
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
            trace!("found img layer and no deltas for request on {key} at {end_lsn}");
            Ok(Some(SearchResult {
                lsn_floor: latest_img_lsn.unwrap(),
                layer: l,
            }))
        } else {
            trace!("no layer found for request on {key} at {end_lsn}");
            Ok(None)
        }
    }

    ///
    /// Insert an on-disk layer
    ///
    pub fn insert_historic(&mut self, layer: Arc<dyn Layer>) {
        if layer.get_key_range() == (Key::MIN..Key::MAX) {
            self.l0_layers.push(layer.clone());
        }
        self.historic_layers.insert(LayerEnvelope { layer });
        NUM_ONDISK_LAYERS.inc();
    }

    ///
    /// Remove an on-disk layer from the map.
    ///
    /// This should be called when the corresponding file on disk has been deleted.
    ///
    pub fn remove_historic(&mut self, layer: Arc<dyn Layer>) {
        if layer.get_key_range() == (Key::MIN..Key::MAX) {
            let len_before = self.l0_layers.len();

            // FIXME: ptr_eq might fail to return true for 'dyn'
            // references.  Clippy complains about this. In practice it
            // seems to work, the assertion below would be triggered
            // otherwise but this ought to be fixed.
            #[allow(clippy::vtable_address_comparisons)]
            self.l0_layers.retain(|other| !Arc::ptr_eq(other, &layer));
            assert_eq!(self.l0_layers.len(), len_before - 1);
        }
        assert!(self
            .historic_layers
            .remove(&LayerEnvelope { layer })
            .is_some());
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
            let envelope = AABB::from_corners(
                [IntKey(range_remain.start.to_i128()), IntKey(lsn.0 as i128)],
                [
                    IntKey(range_remain.end.to_i128() - 1),
                    IntKey(disk_consistent_lsn.0 as i128),
                ],
            );
            for e in self
                .historic_layers
                .locate_in_envelope_intersecting(&envelope)
            {
                let l = &e.layer;
                if l.is_incremental() {
                    continue;
                }
                let img_lsn = l.get_lsn_range().start;
                if l.get_key_range().contains(&range_remain.start)
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

    pub fn iter_historic_layers(&self) -> impl Iterator<Item = &Arc<dyn Layer>> {
        self.historic_layers.iter().map(|e| e.layer.clone())
    }

    /// Find the last image layer that covers 'key', ignoring any image layers
    /// newer than 'lsn'.
    fn find_latest_image(&self, key: Key, lsn: Lsn) -> Option<Arc<dyn Layer>> {
        let mut candidate_lsn = Lsn(0);
        let mut candidate = None;
        let envelope = AABB::from_corners(
            [IntKey(key.to_i128()), IntKey(0)],
            [IntKey(key.to_i128()), IntKey(lsn.0 as i128)],
        );
        for e in self
            .historic_layers
            .locate_in_envelope_intersecting(&envelope)
        {
            let l = &e.layer;
            if l.is_incremental() {
                continue;
            }

            assert!(l.get_key_range().contains(&key));
            let this_lsn = l.get_lsn_range().start;
            assert!(this_lsn <= lsn);
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
        let envelope = AABB::from_corners(
            [IntKey(key_range.start.to_i128()), IntKey(0)],
            [IntKey(key_range.end.to_i128()), IntKey(lsn.0 as i128)],
        );
        for e in self
            .historic_layers
            .locate_in_envelope_intersecting(&envelope)
        {
            let l = &e.layer;
            assert!(l.get_lsn_range().start <= lsn);
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
        let envelope = AABB::from_corners(
            [
                IntKey(key_range.start.to_i128()),
                IntKey(lsn_range.start.0 as i128),
            ],
            [
                IntKey(key_range.end.to_i128() - 1),
                IntKey(lsn_range.end.0 as i128 - 1),
            ],
        );
        for e in self
            .historic_layers
            .locate_in_envelope_intersecting(&envelope)
        {
            let l = &e.layer;
            if !l.is_incremental() {
                continue;
            }
            assert!(range_overlaps(&l.get_lsn_range(), lsn_range));
            assert!(range_overlaps(&l.get_key_range(), key_range));

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
        Ok(self.l0_layers.clone())
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
        for e in self.historic_layers.iter() {
            e.layer.dump(verbose)?;
        }
        println!("End dump LayerMap");
        Ok(())
    }
}
