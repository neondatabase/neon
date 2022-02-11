//!
//! The layer map tracks what layers exist for all the relishes in a timeline.
//!
//! When the timeline is first accessed, the server lists of all layer files
//! in the timelines/<timelineid> directory, and populates this map with
//! ImageLayer and DeltaLayer structs corresponding to each file. When new WAL
//! is received, we create InMemoryLayers to hold the incoming records. Now and
//! then, in the checkpoint() function, the in-memory layers are frozen, forming
//! new image and delta layers and corresponding files are written to disk.
//!

use crate::layered_repository::interval_tree::{IntervalItem, IntervalIter, IntervalTree};
use crate::layered_repository::storage_layer::{Layer, SegmentTag};
use crate::layered_repository::InMemoryLayer;
use crate::relish::*;
use anyhow::Result;
use lazy_static::lazy_static;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use zenith_metrics::{register_int_gauge, IntGauge};
use zenith_utils::lsn::Lsn;

use super::global_layer_map::{LayerId, GLOBAL_LAYER_MAP};

lazy_static! {
    static ref NUM_INMEMORY_LAYERS: IntGauge =
        register_int_gauge!("pageserver_inmemory_layers", "Number of layers in memory")
            .expect("failed to define a metric");
    static ref NUM_ONDISK_LAYERS: IntGauge =
        register_int_gauge!("pageserver_ondisk_layers", "Number of layers on-disk")
            .expect("failed to define a metric");
}

//
// Struct used for caching most recent blocky relation size.
//
struct RelishSize {
    last_segno: u32,
    lsn: Lsn,
    is_dropped: bool,
}

///
/// LayerMap tracks what layers exist on a timeline.
///
#[derive(Default)]
pub struct LayerMap {
    /// All the layers keyed by segment tag
    segs: HashMap<SegmentTag, SegEntry>,

    /// Cache for fast location of last relation segment
    last_seg: HashMap<RelishTag, RelishSize>,

    /// All in-memory layers, ordered by 'oldest_lsn' and generation
    /// of each layer. This allows easy access to the in-memory layer that
    /// contains the oldest WAL record.
    open_layers: BinaryHeap<OpenLayerEntry>,

    /// Generation number, used to distinguish newly inserted entries in the
    /// binary heap from older entries during checkpoint.
    current_generation: u64,
}

impl LayerMap {
    ///
    /// Look up a layer using the given segment tag and LSN. This differs from a
    /// plain key-value lookup in that if there is any layer that covers the
    /// given LSN, or precedes the given LSN, it is returned. In other words,
    /// you don't need to know the exact start LSN of the layer.
    ///
    pub fn get(&self, tag: &SegmentTag, lsn: Lsn) -> Option<Arc<dyn Layer>> {
        let segentry = self.segs.get(tag)?;

        segentry.get(lsn)
    }

    ///
    /// Get the open layer for given segment for writing. Or None if no open
    /// layer exists.
    ///
    pub fn get_open(&self, tag: &SegmentTag) -> Option<Arc<InMemoryLayer>> {
        let segentry = self.segs.get(tag)?;

        segentry
            .open_layer_id
            .and_then(|layer_id| GLOBAL_LAYER_MAP.read().unwrap().get(&layer_id))
    }

    ///
    /// Insert an open in-memory layer
    ///
    pub fn insert_open(&mut self, layer: Arc<InMemoryLayer>) {
        let segentry = self.segs.entry(layer.get_seg_tag()).or_default();

        let layer_id = segentry.update_open(Arc::clone(&layer));

        let oldest_lsn = layer.get_oldest_lsn();

        // After a crash and restart, 'oldest_lsn' of the oldest in-memory
        // layer becomes the WAL streaming starting point, so it better not point
        // in the middle of a WAL record.
        assert!(oldest_lsn.is_aligned());

        // Also add it to the binary heap
        let open_layer_entry = OpenLayerEntry {
            oldest_lsn: layer.get_oldest_lsn(),
            layer_id,
            generation: self.current_generation,
        };
        self.open_layers.push(open_layer_entry);

        NUM_INMEMORY_LAYERS.inc();
    }

    /// Remove an open in-memory layer
    pub fn remove_open(&mut self, layer_id: LayerId) {
        // Note: we don't try to remove the entry from the binary heap.
        // It will be removed lazily by peek_oldest_open() when it's made it to
        // the top of the heap.

        let layer_opt = {
            let mut global_map = GLOBAL_LAYER_MAP.write().unwrap();
            let layer_opt = global_map.get(&layer_id);
            global_map.remove(&layer_id);
            // TODO it's bad that a ref can still exist after being evicted from cache
            layer_opt
        };

        if let Some(layer) = layer_opt {
            let mut segentry = self.segs.get_mut(&layer.get_seg_tag()).unwrap();

            if segentry.open_layer_id == Some(layer_id) {
                // Also remove it from the SegEntry of this segment
                segentry.open_layer_id = None;
            } else {
                // We could have already updated segentry.open for
                // dropped (non-writeable) layer. This is fine.
                assert!(!layer.is_writeable());
                assert!(layer.is_dropped());
            }

            NUM_INMEMORY_LAYERS.dec();
        }
    }

    ///
    /// Insert an on-disk layer
    ///
    pub fn insert_historic(&mut self, layer: Arc<dyn Layer>) {
        let tag = layer.get_seg_tag();
        let segentry = self.segs.entry(tag).or_default();
        let lsn = layer.get_end_lsn();
        let is_dropped = layer.is_dropped();
        let last_segno = tag.segno;
        let mut e = self.last_seg.entry(tag.rel).or_insert(RelishSize {
            lsn,
            last_segno,
            is_dropped,
        });
        if (is_dropped && e.last_segno > last_segno && (e.lsn <= lsn || e.is_dropped))
            || (!is_dropped && e.last_segno < last_segno && (e.lsn <= lsn || !e.is_dropped))
        {
            e.last_segno = last_segno;
            e.lsn = lsn;
            e.is_dropped = is_dropped;
        }
        segentry.insert_historic(layer);

        NUM_ONDISK_LAYERS.inc();
    }

    ///
    /// Remove an on-disk layer from the map.
    ///
    /// This should be called when the corresponding file on disk has been deleted.
    ///
    pub fn remove_historic(&mut self, layer: Arc<dyn Layer>) {
        let tag = layer.get_seg_tag();

        if let Some(segentry) = self.segs.get_mut(&tag) {
            segentry.historic.remove(&layer);
        }
        NUM_ONDISK_LAYERS.dec();
    }

    ///
    /// Update last segment cache.
    /// This method is not actually removing something (it is responsibility of
    /// remove_historic/remove_open), it just update cache to let get_last_segno
    /// return correct number.
    ///
    pub fn drop_layer(&mut self, tag: &SegmentTag, lsn: Lsn) {
        if tag.segno != 0 {
            self.last_seg.insert(
                tag.rel,
                RelishSize {
                    lsn,
                    last_segno: tag.segno - 1,
                    is_dropped: false,
                },
            );
        } else {
            self.last_seg.remove(&tag.rel);
        }
    }

    ///
    /// Get last segment number
    ///
    pub fn get_last_segno(&self, rel: RelishTag, lsn: Lsn) -> Option<u32> {
        if let Some(entry) = self.last_seg.get(&rel) {
            if lsn >= entry.lsn {
                return Some(entry.last_segno);
            }
        }
        None
    }

    // List relations along with a flag that marks if they exist at the given lsn.
    // spcnode 0 and dbnode 0 have special meanings and mean all tabespaces/databases.
    // Pass Tag if we're only interested in some relations.
    pub fn list_relishes(&self, tag: Option<RelTag>, lsn: Lsn) -> Result<HashMap<RelishTag, bool>> {
        let mut rels: HashMap<RelishTag, bool> = HashMap::new();

        for (seg, segentry) in self.segs.iter() {
            match seg.rel {
                RelishTag::Relation(reltag) => {
                    if let Some(request_rel) = tag {
                        if (request_rel.spcnode == 0 || reltag.spcnode == request_rel.spcnode)
                            && (request_rel.dbnode == 0 || reltag.dbnode == request_rel.dbnode)
                        {
                            if let Some(exists) = segentry.exists_at_lsn(lsn)? {
                                rels.insert(seg.rel, exists);
                            }
                        }
                    }
                }
                _ => {
                    if tag == None {
                        if let Some(exists) = segentry.exists_at_lsn(lsn)? {
                            rels.insert(seg.rel, exists);
                        }
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
        if let Some(segentry) = self.segs.get(&seg) {
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
        Ok(if let Some(segentry) = self.segs.get(&seg) {
            segentry.exists_at_lsn(lsn)?.unwrap_or(false)
        } else {
            false
        })
    }

    /// Return the oldest in-memory layer, along with its generation number.
    pub fn peek_oldest_open(&mut self) -> Option<(LayerId, Arc<InMemoryLayer>, u64)> {
        let global_map = GLOBAL_LAYER_MAP.read().unwrap();

        while let Some(oldest_entry) = self.open_layers.peek() {
            if let Some(layer) = global_map.get(&oldest_entry.layer_id) {
                return Some((oldest_entry.layer_id, layer, oldest_entry.generation));
            } else {
                self.open_layers.pop();
            }
        }
        None
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
            seg_iter: self.segs.iter(),
            iter: None,
        }
    }

    /// debugging function to print out the contents of the layer map
    #[allow(unused)]
    pub fn dump(&self) -> Result<()> {
        println!("Begin dump LayerMap");
        for (seg, segentry) in self.segs.iter() {
            if let Some(open) = &segentry.open_layer_id {
                if let Some(layer) = GLOBAL_LAYER_MAP.read().unwrap().get(open) {
                    layer.dump()?;
                } else {
                    println!("layer not found in global map");
                }
            }

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
/// Per-segment entry in the LayerMap::segs hash map. Holds all the layers
/// associated with the segment.
///
/// The last layer that is open for writes is always an InMemoryLayer,
/// and is kept in a separate field, because there can be only one for
/// each segment. The older layers, stored on disk, are kept in an
/// IntervalTree.
#[derive(Default)]
struct SegEntry {
    open_layer_id: Option<LayerId>,
    historic: IntervalTree<dyn Layer>,
}

impl SegEntry {
    /// Does the segment exist at given LSN?
    /// Return None if object is not found in this SegEntry.
    fn exists_at_lsn(&self, lsn: Lsn) -> Result<Option<bool>> {
        if let Some(layer) = self.get(lsn) {
            Ok(Some(layer.get_seg_exists(lsn)?))
        } else {
            Ok(None)
        }
    }

    pub fn get(&self, lsn: Lsn) -> Option<Arc<dyn Layer>> {
        if let Some(open_layer_id) = &self.open_layer_id {
            let open_layer = GLOBAL_LAYER_MAP.read().unwrap().get(open_layer_id)?;
            if open_layer.get_start_lsn() <= lsn {
                return Some(open_layer);
            }
        }

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

    // Set new open layer for a SegEntry.
    // It's ok to rewrite previous open layer,
    // but only if it is not writeable anymore.
    pub fn update_open(&mut self, layer: Arc<InMemoryLayer>) -> LayerId {
        if let Some(prev_open_layer_id) = &self.open_layer_id {
            if let Some(prev_open_layer) = GLOBAL_LAYER_MAP.read().unwrap().get(prev_open_layer_id)
            {
                assert!(!prev_open_layer.is_writeable());
            }
        }
        let open_layer_id = GLOBAL_LAYER_MAP.write().unwrap().insert(layer);
        self.open_layer_id = Some(open_layer_id);
        open_layer_id
    }

    pub fn insert_historic(&mut self, layer: Arc<dyn Layer>) {
        self.historic.insert(layer);
    }
}

/// Entry held in LayerMap::open_layers, with boilerplate comparison routines
/// to implement a min-heap ordered by 'oldest_lsn' and 'generation'
///
/// The generation number associated with each entry can be used to distinguish
/// recently-added entries (i.e after last call to increment_generation()) from older
/// entries with the same 'oldest_lsn'.
struct OpenLayerEntry {
    oldest_lsn: Lsn, // copy of layer.get_oldest_lsn()
    generation: u64,
    layer_id: LayerId,
}
impl Ord for OpenLayerEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap, and we want a min-heap. Reverse the ordering here
        // to get that. Entries with identical oldest_lsn are ordered by generation
        other
            .oldest_lsn
            .cmp(&self.oldest_lsn)
            .then_with(|| other.generation.cmp(&self.generation))
    }
}
impl PartialOrd for OpenLayerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for OpenLayerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for OpenLayerEntry {}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PageServerConf;
    use std::str::FromStr;
    use zenith_utils::zid::{ZTenantId, ZTimelineId};

    /// Arbitrary relation tag, for testing.
    const TESTREL_A: RelishTag = RelishTag::Relation(RelTag {
        spcnode: 0,
        dbnode: 111,
        relnode: 1000,
        forknum: 0,
    });

    lazy_static! {
        static ref DUMMY_TIMELINEID: ZTimelineId =
            ZTimelineId::from_str("00000000000000000000000000000000").unwrap();
        static ref DUMMY_TENANTID: ZTenantId =
            ZTenantId::from_str("00000000000000000000000000000000").unwrap();
    }

    /// Construct a dummy InMemoryLayer for testing
    fn dummy_inmem_layer(
        conf: &'static PageServerConf,
        segno: u32,
        start_lsn: Lsn,
        oldest_lsn: Lsn,
    ) -> Arc<InMemoryLayer> {
        Arc::new(
            InMemoryLayer::create(
                conf,
                *DUMMY_TIMELINEID,
                *DUMMY_TENANTID,
                SegmentTag {
                    rel: TESTREL_A,
                    segno,
                },
                start_lsn,
                oldest_lsn,
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_open_layers() -> Result<()> {
        let conf = PageServerConf::dummy_conf(PageServerConf::test_repo_dir("dummy_inmem_layer"));
        let conf = Box::leak(Box::new(conf));
        std::fs::create_dir_all(conf.timeline_path(&DUMMY_TIMELINEID, &DUMMY_TENANTID))?;

        let mut layers = LayerMap::default();

        let gen1 = layers.increment_generation();
        layers.insert_open(dummy_inmem_layer(conf, 0, Lsn(0x100), Lsn(0x100)));
        layers.insert_open(dummy_inmem_layer(conf, 1, Lsn(0x100), Lsn(0x200)));
        layers.insert_open(dummy_inmem_layer(conf, 2, Lsn(0x100), Lsn(0x120)));
        layers.insert_open(dummy_inmem_layer(conf, 3, Lsn(0x100), Lsn(0x110)));

        let gen2 = layers.increment_generation();
        layers.insert_open(dummy_inmem_layer(conf, 4, Lsn(0x100), Lsn(0x110)));
        layers.insert_open(dummy_inmem_layer(conf, 5, Lsn(0x100), Lsn(0x100)));

        // A helper function (closure) to pop the next oldest open entry from the layer map,
        // and assert that it is what we'd expect
        let mut assert_pop_layer = |expected_segno: u32, expected_generation: u64| {
            let (layer_id, l, generation) = layers.peek_oldest_open().unwrap();
            assert!(l.get_seg_tag().segno == expected_segno);
            assert!(generation == expected_generation);
            layers.remove_open(layer_id);
        };

        assert_pop_layer(0, gen1); // 0x100
        assert_pop_layer(5, gen2); // 0x100
        assert_pop_layer(3, gen1); // 0x110
        assert_pop_layer(4, gen2); // 0x110
        assert_pop_layer(2, gen1); // 0x120
        assert_pop_layer(1, gen1); // 0x200

        Ok(())
    }
}
