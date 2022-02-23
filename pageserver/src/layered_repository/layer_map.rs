//!
//! The layer map tracks what layers exist for all the relishes in a timeline.
//!
//! When the timeline is first accessed, the server lists of all layer files
//! in the timelines/<timelineid> directory, and populates this map with
//! ImageLayer and DeltaLayer structs corresponding to each file. When the first
//! new WAL record is received, we create an InMemoryLayer to hold the incoming
//! records. Now and then, in the checkpoint() function, the in-memory layer is
//! are frozen, and it is split up into new image and delta layers and the
//! corresponding files are written to disk.
//!

use crate::layered_repository::interval_tree::{IntervalItem, IntervalIter, IntervalTree};
use crate::layered_repository::storage_layer::{Layer, SegmentTag};
use crate::layered_repository::InMemoryLayer;
use crate::relish::*;
use anyhow::Result;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::Arc;
use zenith_metrics::{register_int_gauge, IntGauge};
use zenith_utils::lsn::Lsn;

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
    pub frozen_layer: Option<Arc<InMemoryLayer>>,

    /// All the layers keyed by segment tag
    historic_layers: HashMap<SegmentTag, SegEntry>,
}

impl LayerMap {
    ///
    /// Look up a layer using the given segment tag and LSN. This differs from a
    /// plain key-value lookup in that if there is any layer that covers the
    /// given LSN, or precedes the given LSN, it is returned. In other words,
    /// you don't need to know the exact start LSN of the layer.
    ///
    pub fn get(&self, tag: &SegmentTag, lsn: Lsn) -> Option<Arc<dyn Layer>> {
        // Check the open and frozen in-memory layers first
        if let Some(open_layer) = &self.open_layer {
            if lsn >= open_layer.get_start_lsn() && open_layer.covers_seg(*tag) {
                return Some(open_layer.clone());
            }
        }
        if let Some(frozen_layer) = &self.frozen_layer {
            if lsn >= frozen_layer.get_start_lsn() && frozen_layer.covers_seg(*tag) {
                return Some(frozen_layer.clone());
            }
        }

        let segentry = self.historic_layers.get(tag)?;
        segentry.get(lsn)
    }

    ///
    /// Insert an on-disk layer
    ///
    pub fn insert_historic(&mut self, layer: Arc<dyn Layer>) {
        // TODO: We currently only support singleton historic ranges.
        // In other words, a historic layer's segment range must be
        // exactly one segment.
        let tag = layer.get_seg_range().get_singleton().unwrap();

        let segentry = self.historic_layers.entry(tag).or_default();
        segentry.insert_historic(layer);

        NUM_ONDISK_LAYERS.inc();
    }

    ///
    /// Remove an on-disk layer from the map.
    ///
    /// This should be called when the corresponding file on disk has been deleted.
    ///
    pub fn remove_historic(&mut self, layer: Arc<dyn Layer>) {
        // TODO: We currently only support singleton historic ranges.
        // In other words, a historic layer's segment range must be
        // exactly one segment.
        let tag = layer.get_seg_range().get_singleton().unwrap();

        if let Some(segentry) = self.historic_layers.get_mut(&tag) {
            segentry.historic.remove(&layer);
        }
        NUM_ONDISK_LAYERS.dec();
    }

    fn filter_fn(query: Option<RelTag>, seg: SegmentTag) -> bool {
        match seg.rel {
            RelishTag::Relation(reltag) => {
                if let Some(request_rel) = query {
                    (request_rel.spcnode == 0 || reltag.spcnode == request_rel.spcnode)
                        && (request_rel.dbnode == 0 || reltag.dbnode == request_rel.dbnode)
                } else {
                    // FIXME: If query is None, shouldn't we return true?
                    false
                }
            }
            _ => query == None,
        }
    }

    // List relations along with a flag that marks if they exist at the given lsn.
    // spcnode 0 and dbnode 0 have special meanings and mean all tablespaces/databases.
    // Pass Tag if we're only interested in some relations.
    pub fn list_relishes(&self, tag: Option<RelTag>, lsn: Lsn) -> Result<HashMap<RelishTag, bool>> {
        let mut rels: HashMap<RelishTag, bool> = HashMap::new();

        for (seg, segentry) in self.historic_layers.iter() {
            if Self::filter_fn(tag, *seg) {
                if let Some(exists) = segentry.exists_at_lsn(*seg, lsn)? {
                    rels.insert(seg.rel, exists);
                }
            }
        }

        if let Some(frozen_layer) = &self.frozen_layer {
            if frozen_layer.get_start_lsn() <= lsn {
                for seg in frozen_layer.list_covered_segs()? {
                    if Self::filter_fn(tag, seg) {
                        rels.insert(seg.rel, frozen_layer.get_seg_exists(seg, lsn)?);
                    }
                }
            }
        }
        if let Some(open_layer) = &self.open_layer {
            if open_layer.get_start_lsn() <= lsn {
                for seg in open_layer.list_covered_segs()? {
                    if Self::filter_fn(tag, seg) {
                        rels.insert(seg.rel, open_layer.get_seg_exists(seg, lsn)?);
                    }
                }
            }
        }

        Ok(rels)
    }

    /// Is there a newer image layer for given segment?
    ///
    /// This is used for garbage collection, to determine if an old layer can
    /// be deleted.
    /// We ignore segments newer than disk_consistent_lsn because they will be removed at restart
    pub fn newer_image_layer_exists(
        &self,
        seg: SegmentTag,
        lsn: Lsn,
        disk_consistent_lsn: Lsn,
    ) -> bool {
        if let Some(segentry) = self.historic_layers.get(&seg) {
            segentry.newer_image_layer_exists(lsn, disk_consistent_lsn)
        } else {
            false
        }
    }

    /// Is there any layer for given segment that is alive at the lsn?
    ///
    /// This is a public wrapper for SegEntry fucntion,
    /// used for garbage collection, to determine if some alive layer
    /// exists at the lsn. If so, we shouldn't delete a newer dropped layer
    /// to avoid incorrectly making it visible.
    pub fn layer_exists_at_lsn(&self, seg: SegmentTag, lsn: Lsn) -> Result<bool> {
        Ok(if let Some(segentry) = self.historic_layers.get(&seg) {
            segentry.exists_at_lsn(seg, lsn)?.unwrap_or(false)
        } else {
            false
        })
    }

    pub fn iter_historic_layers(&self) -> HistoricLayerIter {
        HistoricLayerIter {
            seg_iter: self.historic_layers.iter(),
            iter: None,
        }
    }

    /// debugging function to print out the contents of the layer map
    #[allow(unused)]
    pub fn dump(&self) -> Result<()> {
        println!("Begin dump LayerMap");
        for (seg, segentry) in self.historic_layers.iter() {
            for layer in segentry.historic.iter() {
                layer.dump()?;
            }
        }
        println!("End dump LayerMap");
        Ok(())
    }
}

impl IntervalItem for dyn Layer {
    type Key = Lsn;

    fn start_key(&self) -> Lsn {
        self.get_start_lsn()
    }
    fn end_key(&self) -> Lsn {
        self.get_end_lsn()
    }
}

///
/// Per-segment entry in the LayerMap::historic_layers hash map.
///
#[derive(Default)]
struct SegEntry {
    historic: IntervalTree<dyn Layer>,
}

impl SegEntry {
    /// Does the segment exist at given LSN?
    /// Return None if object is not found in this SegEntry.
    fn exists_at_lsn(&self, seg: SegmentTag, lsn: Lsn) -> Result<Option<bool>> {
        if let Some(layer) = self.get(lsn) {
            Ok(Some(layer.get_seg_exists(seg, lsn)?))
        } else {
            Ok(None)
        }
    }

    pub fn get(&self, lsn: Lsn) -> Option<Arc<dyn Layer>> {
        self.historic.search(lsn)
    }

    pub fn newer_image_layer_exists(&self, lsn: Lsn, disk_consistent_lsn: Lsn) -> bool {
        // We only check on-disk layers, because
        // in-memory layers are not durable

        // The end-LSN is exclusive, while disk_consistent_lsn is
        // inclusive. For example, if disk_consistent_lsn is 100, it is
        // OK for a delta layer to have end LSN 101, but if the end LSN
        // is 102, then it might not have been fully flushed to disk
        // before crash.
        self.historic
            .iter_newer(lsn)
            .any(|layer| !layer.is_incremental() && layer.get_end_lsn() <= disk_consistent_lsn + 1)
    }

    pub fn insert_historic(&mut self, layer: Arc<dyn Layer>) {
        self.historic.insert(layer);
    }
}

/// Iterator returned by LayerMap::iter_historic_layers()
pub struct HistoricLayerIter<'a> {
    seg_iter: std::collections::hash_map::Iter<'a, SegmentTag, SegEntry>,
    iter: Option<IntervalIter<'a, dyn Layer>>,
}

impl<'a> Iterator for HistoricLayerIter<'a> {
    type Item = Arc<dyn Layer>;

    fn next(&mut self) -> std::option::Option<<Self as std::iter::Iterator>::Item> {
        loop {
            if let Some(x) = &mut self.iter {
                if let Some(x) = x.next() {
                    return Some(Arc::clone(&x));
                }
            }
            if let Some((_tag, segentry)) = self.seg_iter.next() {
                self.iter = Some(segentry.historic.iter());
                continue;
            } else {
                return None;
            }
        }
    }
}
