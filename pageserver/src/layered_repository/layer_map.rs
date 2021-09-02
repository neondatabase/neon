//!
//! The layer map tracks what layers exist for all the relations in a timeline.
//!
//! When the timeline is first accessed, the server lists of all layer files
//! in the timelines/<timelineid> directory, and populates this map with
//! ImageLayer and DeltaLayer structs corresponding to each file. When new WAL
//! is received, we create InMemoryLayers to hold the incoming records. Now and
//! then, in the checkpoint() function, the in-memory layers are frozen, forming
//! new image and delta layers and corresponding files are written to disk.
//!

use crate::layered_repository::storage_layer::{Layer, SegmentTag};
use crate::layered_repository::InMemoryLayer;
use crate::relish::*;
use anyhow::Result;
use lazy_static::lazy_static;
use log::*;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::ops::Bound::Included;
use std::sync::Arc;
use zenith_metrics::{register_int_gauge, IntGauge};
use zenith_utils::lsn::Lsn;

lazy_static! {
    static ref NUM_INMEMORY_LAYERS: IntGauge =
        register_int_gauge!("pageserver_inmemory_layers", "Number of layers in memory")
            .expect("failed to define a metric");
    static ref NUM_ONDISK_LAYERS: IntGauge =
        register_int_gauge!("pageserver_ondisk_layers", "Number of layers on-disk")
            .expect("failed to define a metric");
}

///
/// LayerMap tracks what layers exist on a timeline.
///
pub struct LayerMap {
    /// All the layers keyed by segment tag
    segs: HashMap<SegmentTag, SegEntry>,

    /// All in-memory layers, ordered by 'oldest_pending_lsn' of each layer.
    /// This allows easy access to the in-memory layer that contains the
    /// oldest WAL record.
    open_segs: BinaryHeap<OpenSegEntry>,

    /// Generation number, used to distinguish newly inserted entries in the
    /// binary heap from older entries during checkpoint.
    current_generation: u64,
}

///
/// Per-segment entry in the LayerMap.segs hash map
///
/// The last layer that is open for writes is always an InMemoryLayer,
/// and is kept in a separate field, because there can be only one for
/// each segment. The older layers, stored on disk, are kept in a
/// BTreeMap keyed by the layer's start LSN.
struct SegEntry {
    pub open: Option<Arc<InMemoryLayer>>,
    pub historic: BTreeMap<Lsn, Arc<dyn Layer>>,
}

/// Entry held LayerMap.open_segs, with boilerplate comparison
/// routines to implement a min-heap ordered by 'oldest_pending_lsn'
///
/// Each entry also carries a generation number. It can be used to distinguish
/// entries with the same 'oldest_pending_lsn'.
struct OpenSegEntry {
    pub oldest_pending_lsn: Lsn,
    pub layer: Arc<InMemoryLayer>,
    pub generation: u64,
}
impl Ord for OpenSegEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap, and we want a min-heap. Reverse the ordering here
        // to get that.
        other.oldest_pending_lsn.cmp(&self.oldest_pending_lsn)
    }
}
impl PartialOrd for OpenSegEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // BinaryHeap is a max-heap, and we want a min-heap. Reverse the ordering here
        // to get that. Entries with identical oldest_pending_lsn are ordered by generation
        Some(
            other
                .oldest_pending_lsn
                .cmp(&self.oldest_pending_lsn)
                .then_with(|| other.generation.cmp(&self.generation)),
        )
    }
}
impl PartialEq for OpenSegEntry {
    fn eq(&self, other: &Self) -> bool {
        self.oldest_pending_lsn.eq(&other.oldest_pending_lsn)
    }
}
impl Eq for OpenSegEntry {}

impl LayerMap {
    ///
    /// Look up a layer using the given segment tag and LSN. This differs from a
    /// plain key-value lookup in that if there is any layer that covers the
    /// given LSN, or precedes the given LSN, it is returned. In other words,
    /// you don't need to know the exact start LSN of the layer.
    ///
    pub fn get(&self, tag: &SegmentTag, lsn: Lsn) -> Option<Arc<dyn Layer>> {
        let segentry = self.segs.get(tag)?;

        if let Some(open) = &segentry.open {
            if open.get_start_lsn() <= lsn {
                let x: Arc<dyn Layer> = Arc::clone(&open) as _;
                return Some(x);
            }
        }

        if let Some((_k, v)) = segentry
            .historic
            .range((Included(Lsn(0)), Included(lsn)))
            .next_back()
        {
            let x: Arc<dyn Layer> = Arc::clone(&v) as _;
            Some(x)
        } else {
            None
        }
    }

    ///
    /// Get the open layer for given segment for writing. Or None if no open
    /// layer exists.
    ///
    pub fn get_open(&self, tag: &SegmentTag) -> Option<Arc<InMemoryLayer>> {
        let segentry = self.segs.get(tag)?;

        if let Some(open) = &segentry.open {
            Some(Arc::clone(open))
        } else {
            None
        }
    }

    ///
    /// Insert an open in-memory layer
    ///
    pub fn insert_open(&mut self, layer: Arc<InMemoryLayer>) {
        let tag = layer.get_seg_tag();

        if let Some(segentry) = self.segs.get_mut(&tag) {
            if let Some(_old) = &segentry.open {
                // FIXME: shouldn't exist, but check
            }
            segentry.open = Some(Arc::clone(&layer));
        } else {
            let segentry = SegEntry {
                open: Some(Arc::clone(&layer)),
                historic: BTreeMap::new(),
            };
            self.segs.insert(tag, segentry);
        }

        let opensegentry = OpenSegEntry {
            oldest_pending_lsn: layer.get_oldest_pending_lsn(),
            layer: layer,
            generation: self.current_generation,
        };
        self.open_segs.push(opensegentry);

        NUM_INMEMORY_LAYERS.inc();
    }

    /// Remove the oldest in-memory layer
    pub fn pop_oldest_open(&mut self) {
        let opensegentry = self.open_segs.pop().unwrap();
        let segtag = opensegentry.layer.get_seg_tag();

        let mut segentry = self.segs.get_mut(&segtag).unwrap();
        segentry.open = None;
        NUM_INMEMORY_LAYERS.dec();
    }

    ///
    /// Insert an on-disk layer
    ///
    pub fn insert_historic(&mut self, layer: Arc<dyn Layer>) {
        let tag = layer.get_seg_tag();
        let start_lsn = layer.get_start_lsn();

        if let Some(segentry) = self.segs.get_mut(&tag) {
            segentry.historic.insert(start_lsn, layer);
        } else {
            let mut historic = BTreeMap::new();
            historic.insert(start_lsn, layer);

            let segentry = SegEntry {
                open: None,
                historic,
            };
            self.segs.insert(tag, segentry);
        }
        NUM_ONDISK_LAYERS.inc();
    }

    ///
    /// Remove an on-disk layer from the map.
    ///
    /// This should be called when the corresponding file on disk has been deleted.
    ///
    pub fn remove_historic(&mut self, layer: &dyn Layer) {
        let tag = layer.get_seg_tag();
        let start_lsn = layer.get_start_lsn();

        if let Some(segentry) = self.segs.get_mut(&tag) {
            segentry.historic.remove(&start_lsn);
        }
        NUM_ONDISK_LAYERS.dec();
    }

    // List relations that exist at the lsn
    pub fn list_rels(&self, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelTag>> {
        let mut rels: HashSet<RelTag> = HashSet::new();

        for (seg, segentry) in self.segs.iter() {
            if let RelishTag::Relation(reltag) = seg.rel {
                if (spcnode == 0 || reltag.spcnode == spcnode)
                    && (dbnode == 0 || reltag.dbnode == dbnode)
                {
                    // Add only if it exists at the requested LSN.
                    if let Some(open) = &segentry.open {
                        if open.get_end_lsn() > lsn {
                            rels.insert(reltag);
                        }
                    } else if let Some((_k, _v)) = segentry
                        .historic
                        .range((Included(Lsn(0)), Included(lsn)))
                        .next_back()
                    {
                        rels.insert(reltag);
                    }
                }
            }
        }
        Ok(rels)
    }

    // List non-relation relishes that exist at the lsn
    pub fn list_nonrels(&self, lsn: Lsn) -> Result<HashSet<RelishTag>> {
        let mut rels: HashSet<RelishTag> = HashSet::new();

        // Scan the timeline directory to get all rels in this timeline.
        for (seg, segentry) in self.segs.iter() {
            if let RelishTag::Relation(_) = seg.rel {
            } else {
                // Add only if it exists at the requested LSN.
                if let Some(open) = &segentry.open {
                    if open.get_end_lsn() > lsn {
                        rels.insert(seg.rel);
                    }
                } else if let Some((_k, _v)) = segentry
                    .historic
                    .range((Included(Lsn(0)), Included(lsn)))
                    .next_back()
                {
                    rels.insert(seg.rel);
                }
            }
        }
        Ok(rels)
    }

    /// Is there a newer image layer for given segment?
    ///
    /// This is used for garbage collection, to determine if an old layer can
    /// be deleted. We ignore in-memory layers because they are not durable
    /// on disk, and delta layers because they depend on an older layer.
    pub fn newer_image_layer_exists(&self, seg: SegmentTag, lsn: Lsn) -> bool {
        if let Some(segentry) = self.segs.get(&seg) {
            for (newer_lsn, layer) in segentry
                .historic
                .range((Included(lsn), Included(Lsn(u64::MAX))))
            {
                // Ignore delta layers.
                if layer.is_incremental() {
                    continue;
                }
                if layer.get_end_lsn() > lsn {
                    trace!(
                        "found later layer for {}, {} {}-{}",
                        seg,
                        lsn,
                        newer_lsn,
                        layer.get_end_lsn()
                    );
                    return true;
                } else {
                    trace!("found singleton layer for {}, {} {}", seg, lsn, newer_lsn);
                    continue;
                }
            }
        }
        trace!("no later layer found for {}, {}", seg, lsn);
        false
    }

    /// Return the oldest in-memory layer, along with its generation number.
    pub fn peek_oldest_open(&self) -> Option<(Arc<InMemoryLayer>, u64)> {
        if let Some(opensegentry) = self.open_segs.peek() {
            Some((Arc::clone(&opensegentry.layer), opensegentry.generation))
        } else {
            None
        }
    }

    /// Increment the generation number used to stamp open in-memory layers. Layers
    /// added with `insert_open` after this call will be associated with the new
    /// generation. Returns the new generation number.
    pub fn increment_generation(&mut self) -> u64 {
        self.current_generation += 1;
        self.current_generation
    }

    pub fn iter_historic_layers(&self) -> HistoricLayerIter {
        HistoricLayerIter {
            segiter: self.segs.iter(),
            iter: None,
        }
    }
}

impl Default for LayerMap {
    fn default() -> Self {
        LayerMap {
            segs: HashMap::new(),
            open_segs: BinaryHeap::new(),
            current_generation: 0,
        }
    }
}

pub struct HistoricLayerIter<'a> {
    segiter: std::collections::hash_map::Iter<'a, SegmentTag, SegEntry>,
    iter: Option<std::collections::btree_map::Iter<'a, Lsn, Arc<dyn Layer>>>,
}

impl<'a> Iterator for HistoricLayerIter<'a> {
    type Item = Arc<dyn Layer>;

    fn next(&mut self) -> std::option::Option<<Self as std::iter::Iterator>::Item> {
        loop {
            if let Some(x) = &mut self.iter {
                if let Some(x) = x.next() {
                    return Some(Arc::clone(&*x.1));
                }
            }
            if let Some(seg) = self.segiter.next() {
                self.iter = Some(seg.1.historic.iter());
                continue;
            } else {
                return None;
            }
        }
    }
}
