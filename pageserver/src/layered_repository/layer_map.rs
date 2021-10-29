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



//
// Global layer registry:
//
// Every layer is inserted into the global registry, and assigned an ID
//
// The global registry tracks memory usage and usage count for each layer
//
//
// In addition to that, there is a per-timeline LayerMap, used for lookups
//
// 



use crate::layered_repository::{schedule_job, GlobalJob};
use crate::layered_repository::interval_tree::{IntervalItem, IntervalIter, IntervalTree};
use crate::layered_repository::storage_layer::{Layer, SegmentTag};
use crate::layered_repository::InMemoryLayer;
use crate::relish::*;
use anyhow::Result;
use lazy_static::lazy_static;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Mutex};
use tracing::*;
use zenith_metrics::{register_int_gauge, IntGauge};
use zenith_utils::lsn::Lsn;

lazy_static! {
    static ref NUM_INMEMORY_LAYERS: IntGauge =
        register_int_gauge!("pageserver_inmemory_layers", "Number of layers in memory")
            .expect("failed to define a metric");
    static ref NUM_ONDISK_LAYERS: IntGauge =
        register_int_gauge!("pageserver_ondisk_layers", "Number of layers on-disk")
            .expect("failed to define a metric");

    // Global layer map
    static ref LAYERS: Mutex<GlobalLayerMap> = Mutex::new(GlobalLayerMap::new());
}

const MAX_OPEN_LAYERS: usize = 10;

const MAX_LOADED_LAYERS: usize = 100;

struct GlobalLayerEntry {
    tag: u64, // to fix ABA problem
    layer: Option<Arc<dyn Layer>>,
    usage_count: u32,
}

struct GlobalLayerMap {
    open_layers: Vec<GlobalLayerEntry>,
    clock_arm: u32,

    num_open_layers: usize,
    eviction_scheduled: bool,
    
    historic_layers: Vec<GlobalLayerEntry>,
}

impl GlobalLayerMap {
    pub fn new() -> GlobalLayerMap {
        GlobalLayerMap {
            open_layers: Vec::new(),
            clock_arm: 0,
            historic_layers: Vec::new(),
            eviction_scheduled: false,
            num_open_layers: 0,
        }
    }

    pub fn get(&mut self, layer_id: LayerId) -> Option<Arc<dyn Layer>> {
        let e = if layer_id.is_historic() {
            let idx = (layer_id.index - 1) as usize;
            &mut self.historic_layers[idx]
        } else {
            let idx = ((-layer_id.index) - 1) as usize;
            &mut self.open_layers[idx]
        };
        if e.usage_count < 5 {
            e.usage_count += 1;
        }

        e.layer.clone()
    }

    pub fn insert_open(&mut self, layer: Arc<InMemoryLayer>) -> LayerId {
        let index = -(self.open_layers.len() as isize + 1);

        let entry = GlobalLayerEntry {
            layer: Some(layer),
            usage_count: 1,
            tag: 1,
        };
        let tag = entry.tag;
        self.open_layers.push(entry);
        self.num_open_layers += 1;

        NUM_INMEMORY_LAYERS.inc();
        
        if !self.eviction_scheduled && self.num_open_layers >= MAX_OPEN_LAYERS {
            info!("scheduling global eviction");
            schedule_job(GlobalJob::EvictSomeLayer);
            self.eviction_scheduled = true;
        }

        LayerId {
            index,
            tag,
        }
    }

    pub fn insert_historic(&mut self, layer: Arc<dyn Layer>) -> LayerId {
        let index = self.historic_layers.len() as isize + 1;

        let entry = GlobalLayerEntry {
            layer: Some(layer),
            usage_count: 1,
            tag: 1,
        };
        let tag = entry.tag;
        self.historic_layers.push(entry);

        NUM_ONDISK_LAYERS.inc();

        LayerId {
            index,
            tag,
        }
    }

    pub fn remove(&mut self, layer_id: LayerId) -> Option<Arc<dyn Layer>> {
        let old_layer;

        if layer_id.is_historic() {
            let idx = (layer_id.index - 1) as usize;
            old_layer = self.historic_layers[idx].layer.take();
            if old_layer.is_some() {
                NUM_ONDISK_LAYERS.dec();
            }
        } else {
            let idx = ((-layer_id.index) - 1) as usize;
            old_layer = self.open_layers[idx].layer.take();
            if old_layer.is_some() {
                NUM_INMEMORY_LAYERS.dec();
                self.num_open_layers -= 1;
            }
        }
        old_layer
    }

    pub fn find_victim(&mut self) -> Option<LayerId> {
        // run the clock algorithm among all open layers
        for _ in 0..self.open_layers.len() * 5 {
            self.clock_arm += 1;
            if self.clock_arm >= self.open_layers.len() as u32 {
                self.clock_arm = 0;
            }
            let next = self.clock_arm as usize;

            if self.open_layers[next].usage_count == 0 {
                return Some(LayerId {
                    index: -((next + 1) as isize),
                    tag: self.open_layers[next].tag,
                });
            } else {
                self.open_layers[next].usage_count -= 1;
            }
        }
        None
    }
}

pub fn find_victim() -> Option<LayerId> {
    let mut l = LAYERS.lock().unwrap();

    if l.num_open_layers >= MAX_OPEN_LAYERS {
        if let Some(x) = l.find_victim() {
            info!("found victim out of {} open layers", l.num_open_layers);
            Some(x)
        } else {
            info!("no victim found at {} open layers", l.num_open_layers);
            None
        }
    } else {
        info!("no victim needed at {} open layers", l.num_open_layers);
        l.eviction_scheduled = false;
        None
    }
}

pub fn get_layer(layer_id: LayerId) -> Option<Arc<dyn Layer>> {
    LAYERS.lock().unwrap().get(layer_id)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct LayerId {
    index: isize,
    tag: u64
}

impl LayerId {
    pub fn is_historic(&self) -> bool {
        self.index > 0
    }
}

///
/// LayerMap tracks what layers exist on a timeline.
///
#[derive(Default)]
pub struct LayerMap {
    /// All the layers keyed by segment tag
    segs: HashMap<SegmentTag, SegEntry>,

    /// All in-memory layers, ordered by 'oldest_pending_lsn' and generation
    /// of each layer. This allows easy access to the in-memory layer that
    /// contains the oldest WAL record.
    open_layers: BinaryHeap<OpenLayerHeapEntry>,

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

    pub fn get_with_id(&self, layer_id: LayerId) -> Option<Arc<dyn Layer>> {
        // TODO: check that it belongs to this tenant+timeline
        LAYERS.lock().unwrap().get(layer_id)
    }

    ///
    /// Get the open layer for given segment for writing. Or None if no open
    /// layer exists.
    ///
    pub fn get_open(&self, tag: &SegmentTag) -> Option<Arc<dyn Layer>> {
        let segentry = self.segs.get(tag)?;

        let (layer_id, _start_lsn) = segentry.open?;
        LAYERS.lock().unwrap().get(layer_id)
    }

    ///
    /// Insert an open in-memory layer
    ///
    pub fn insert_open(&mut self, layer: Arc<InMemoryLayer>) {

        let layer_id = LAYERS.lock().unwrap().insert_open(Arc::clone(&layer));
        
        let segentry = self.segs.entry(layer.get_seg_tag()).or_default();

        segentry.update_open(layer_id, layer.get_start_lsn());

        let oldest_pending_lsn = layer.get_oldest_pending_lsn();

        // After a crash and restart, 'oldest_pending_lsn' of the oldest in-memory
        // layer becomes the WAL streaming starting point, so it better not point
        // in the middle of a WAL record.
        assert!(oldest_pending_lsn.is_aligned());

        // Also add it to the binary heap
        let open_layer_entry = OpenLayerHeapEntry {
            oldest_pending_lsn: layer.get_oldest_pending_lsn(),
            layer_id,
            generation: self.current_generation,
        };
        self.open_layers.push(open_layer_entry);
    }

    /// Remove a layer
    pub fn remove(&mut self, layer_id: LayerId) {
        if let Some(layer) = LAYERS.lock().unwrap().remove(layer_id) {
            // Also remove it from the SegEntry of this segment
            if layer_id.is_historic() {
                let tag = layer.get_seg_tag();

                if let Some(segentry) = self.segs.get_mut(&tag) {
                    segentry.historic.remove(&HistoricLayerIntervalTreeEntry::new(layer_id, layer));
                }
            } else {
                let segtag = layer.get_seg_tag();
                let mut segentry = self.segs.get_mut(&segtag).unwrap();
                if let Some(open) = segentry.open {
                    if open.0 == layer_id {
                        segentry.open = None;
                    }
                } else {
                    // We could have already updated segentry.open for
                    // dropped (non-writeable) layer. This is fine.
                    //assert!(!layer.is_writeable());
                    //assert!(layer.is_dropped());
                }
            }
        }
    }

    ///
    /// Insert an on-disk layer
    ///
    pub fn insert_historic(&mut self, layer: Arc<dyn Layer>) -> LayerId {

        let layer_id = LAYERS.lock().unwrap().insert_historic(Arc::clone(&layer));

        let segentry = self.segs.entry(layer.get_seg_tag()).or_default();
        segentry.insert_historic(layer_id, layer);

        layer_id
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
    pub fn newer_image_layer_exists(&self, seg: SegmentTag, lsn: Lsn) -> bool {
        if let Some(segentry) = self.segs.get(&seg) {
            segentry.newer_image_layer_exists(lsn)
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
    pub fn peek_oldest_open(&mut self) -> Option<(LayerId, Arc<dyn Layer>, u64)> {

        while let Some(oldest_entry) = self.open_layers.peek() {
            if let Some(layer) = LAYERS.lock().unwrap().get(oldest_entry.layer_id) {
                return Some((oldest_entry.layer_id,
                             layer,
                             oldest_entry.generation));
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

    /*
    /// debugging function to print out the contents of the layer map
    #[allow(unused)]
    pub fn dump(&self) -> Result<()> {
        println!("Begin dump LayerMap");
        for (seg, segentry) in self.segs.iter() {
            if let Some(open) = &segentry.open {
                open.dump()?;
            }

            for layer in segentry.historic.iter() {
                layer.dump()?;
            }
        }
        println!("End dump LayerMap");
        Ok(())
    }
*/
}

#[derive(Clone)]
struct HistoricLayerIntervalTreeEntry {
    layer_id: LayerId,
    start_lsn: Lsn,
    end_lsn: Lsn,
}

impl HistoricLayerIntervalTreeEntry {
    fn new(layer_id: LayerId, layer: Arc<dyn Layer>) -> HistoricLayerIntervalTreeEntry{
        HistoricLayerIntervalTreeEntry {
            layer_id,
            start_lsn: layer.get_start_lsn(),
            end_lsn: layer.get_end_lsn(),
        }
    }
}

impl PartialEq for HistoricLayerIntervalTreeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.layer_id == other.layer_id
    }
}
impl IntervalItem for HistoricLayerIntervalTreeEntry {
    type Key = Lsn;

    fn start_key(&self) -> Lsn {
        self.start_lsn
    }
    fn end_key(&self) -> Lsn {
        self.end_lsn
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
    open: Option<(LayerId, Lsn)>,
    historic: IntervalTree<HistoricLayerIntervalTreeEntry>,
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
        if let Some(open) = &self.open {
            if let Some(layer) = LAYERS.lock().unwrap().get(open.0) {
                if layer.get_start_lsn() <= lsn {
                    return Some(layer);
                }
            }
        }

        if let Some(historic) = self.historic.search(lsn) {
            Some(LAYERS.lock().unwrap().get(historic.layer_id).unwrap())
        } else {
            None
        }
    }

    pub fn newer_image_layer_exists(&self, lsn: Lsn) -> bool {
        // We only check on-disk layers, because
        // in-memory layers are not durable

        self.historic
            .iter_newer(lsn)
            .any(|e| {
                let layer = LAYERS.lock().unwrap().get(e.layer_id).unwrap();
                !layer.is_incremental()
            }
            )
    }

    // Set new open layer for a SegEntry.
    // It's ok to rewrite previous open layer,
    // but only if it is not writeable anymore.
    pub fn update_open(&mut self, layer_id: LayerId, start_lsn: Lsn) {
        if let Some(_prev_open) = &self.open {
            //assert!(!prev_open.is_writeable());
        }
        self.open = Some((layer_id, start_lsn));
    }

    pub fn insert_historic(&mut self, layer_id: LayerId, layer: Arc<dyn Layer>) {
        self.historic.insert(&HistoricLayerIntervalTreeEntry::new(layer_id, layer));
    }
}

/// Entry held in LayerMap::open_layers, with boilerplate comparison routines
/// to implement a min-heap ordered by 'oldest_pending_lsn' and 'generation'
///
/// The generation number associated with each entry can be used to distinguish
/// recently-added entries (i.e after last call to increment_generation()) from older
/// entries with the same 'oldest_pending_lsn'.
struct OpenLayerHeapEntry {
    pub oldest_pending_lsn: Lsn, // copy of layer.get_oldest_pending_lsn()
    pub generation: u64,
    pub layer_id: LayerId,
}
impl Ord for OpenLayerHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap, and we want a min-heap. Reverse the ordering here
        // to get that. Entries with identical oldest_pending_lsn are ordered by generation
        other
            .oldest_pending_lsn
            .cmp(&self.oldest_pending_lsn)
            .then_with(|| other.generation.cmp(&self.generation))
    }
}
impl PartialOrd for OpenLayerHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for OpenLayerHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for OpenLayerHeapEntry {}

/// Iterator returned by LayerMap::iter_historic_layers()
pub struct HistoricLayerIter<'a> {
    seg_iter: std::collections::hash_map::Iter<'a, SegmentTag, SegEntry>,
    iter: Option<IntervalIter<'a, HistoricLayerIntervalTreeEntry>>,
}

impl<'a> Iterator for HistoricLayerIter<'a> {
    type Item = (LayerId, Arc<dyn Layer>);

    fn next(&mut self) -> std::option::Option<<Self as std::iter::Iterator>::Item> {
        loop {
            if let Some(x) = &mut self.iter {
                if let Some(x) = x.next() {
                    let layer = LAYERS.lock().unwrap().get(x.layer_id).unwrap();
                    return Some((x.layer_id, layer));
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
    use crate::PageServerConf;
    use std::str::FromStr;
    use zenith_utils::zid::{ZTenantId, ZTimelineId};

    /// Arbitrary relation tag, for testing.
    const TESTREL_A: RelishTag = RelishTag::Relation(RelTag {
        spcnode: 0,
        dbnode: 111,
        relnode: 1000,
        forknum: 0,
    });

    /// Construct a dummy InMemoryLayer for testing
    fn dummy_inmem_layer(
        conf: &'static PageServerConf,
        segno: u32,
        start_lsn: Lsn,
        oldest_pending_lsn: Lsn,
    ) -> Arc<InMemoryLayer> {
        Arc::new(
            InMemoryLayer::create(
                conf,
                ZTimelineId::from_str("00000000000000000000000000000000").unwrap(),
                ZTenantId::from_str("00000000000000000000000000000000").unwrap(),
                SegmentTag {
                    rel: TESTREL_A,
                    segno,
                },
                start_lsn,
                oldest_pending_lsn,
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_open_layers() -> Result<()> {
        let conf = PageServerConf::dummy_conf(PageServerConf::test_repo_dir("dummy_inmem_layer"));
        let conf = Box::leak(Box::new(conf));

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
            layers.remove(layer_id);
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
