//!
//! The layer map tracks what layers exist for all the relations in a timeline.
//!
//! When the timeline is first accessed, the server lists of all snapshot files
//! in the timelines/<timelineid> directory, and populates this map with
//! SnapshotLayers corresponding to each file. When new WAL is received,
//! we create InMemoryLayers to hold the incoming records. Now and then,
//! in the checkpoint() function, the in-memory layers are frozen, forming
//! new snapshot layers and corresponding files are written to disk.
//!

use crate::layered_repository::storage_layer::{Layer, SegmentTag};
use crate::layered_repository::{InMemoryLayer, SnapshotLayer};
use crate::relish::*;
use anyhow::Result;
use lazy_static::lazy_static;
use log::*;
use std::collections::HashSet;
use std::collections::{BinaryHeap, BTreeMap, HashMap};
use std::ops::Bound::Included;
use std::cmp::Ordering;
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
/// LayerMap tracks what layers exist on a timeline. The last layer that is
/// open for writes is always an InMemoryLayer, and is tracked separately
/// because there can be only one for each segment. The older layers,
/// stored on disk, are kept in a BTreeMap keyed by the layer's start LSN.
///
pub struct LayerMap {
    segs: HashMap<SegmentTag, SegEntry>,

    // FIXME: explain this
    open_segs: BinaryHeap<OpenSegEntry>,
}

struct SegEntry {
    pub open: Option<Arc<InMemoryLayer>>,
    pub historic: BTreeMap<Lsn, Arc<SnapshotLayer>>,
}

struct OpenSegEntry {
    pub oldest_pending_lsn: Lsn,
    pub layer: Arc<InMemoryLayer>,
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
        // to get that.
        other.oldest_pending_lsn.partial_cmp(&self.oldest_pending_lsn)
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
    /// Look up using the given segment tag and LSN. This differs from a plain
    /// key-value lookup in that if there is any layer that covers the
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
        };
        self.open_segs.push(opensegentry);

        NUM_INMEMORY_LAYERS.inc();
    }

    // replace given open layer with other layers.
    pub fn pop_oldest(&mut self) {
        let opensegentry = self.open_segs.pop().unwrap();
        let segtag = opensegentry.layer.get_seg_tag();

        let mut segentry = self.segs.get_mut(&segtag).unwrap();
        segentry.open = None;
        NUM_INMEMORY_LAYERS.dec();
    }

    ///
    /// Insert an on-disk layer
    ///
    pub fn insert_historic(&mut self, layer: Arc<SnapshotLayer>) {
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
    pub fn remove_historic(&mut self, layer: &SnapshotLayer) {
        let tag = layer.get_seg_tag();
        let start_lsn = layer.get_start_lsn();

        if let Some(segentry) = self.segs.get_mut(&tag) {
            segentry.historic.remove(&start_lsn);
        }
        NUM_ONDISK_LAYERS.dec();
    }

    pub fn list_rels(&self, spcnode: u32, dbnode: u32) -> Result<HashSet<RelTag>> {
        let mut rels: HashSet<RelTag> = HashSet::new();

        for (seg, _entry) in self.segs.iter() {
            if let RelishTag::Relation(reltag) = seg.rel {
                // FIXME: skip if it was dropped before the requested LSN. But there is no
                // LSN argument

                if (spcnode == 0 || reltag.spcnode == spcnode)
                    && (dbnode == 0 || reltag.dbnode == dbnode)
                {
                    rels.insert(reltag);
                }
            }
        }
        Ok(rels)
    }

    pub fn list_nonrels(&self, _lsn: Lsn) -> Result<HashSet<RelishTag>> {
        let mut rels: HashSet<RelishTag> = HashSet::new();

        // Scan the timeline directory to get all rels in this timeline.
        for (seg, _entry) in self.segs.iter() {
            // FIXME: skip if it was dropped before the requested LSN.

            if let RelishTag::Relation(_) = seg.rel {
            } else {
                rels.insert(seg.rel);
            }
        }
        Ok(rels)
    }

    /// Is there a newer layer for given segment?
    pub fn newer_layer_exists(&self, seg: SegmentTag, lsn: Lsn) -> bool {
        if let Some(segentry) = self.segs.get(&seg) {
            if let Some(_open) = &segentry.open {
                return true;
            }

            for (newer_lsn, layer) in segentry
                .historic
                .range((Included(lsn), Included(Lsn(u64::MAX))))
            {
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

    pub fn get_oldest_open_layer(&mut self) -> Option<Arc<InMemoryLayer>> {
        if let Some(opensegentry) = self.open_segs.peek() {
            Some(Arc::clone(&opensegentry.layer))
        } else {
            None
        }
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
        }
    }
}

pub struct HistoricLayerIter<'a> {
    segiter: std::collections::hash_map::Iter<'a, SegmentTag, SegEntry>,
    iter: Option<std::collections::btree_map::Iter<'a, Lsn, Arc<SnapshotLayer>>>,
}

impl<'a> Iterator for HistoricLayerIter<'a> {
    type Item = Arc<SnapshotLayer>;

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
