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
use log::*;
use std::collections::HashSet;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::Included;
use std::sync::Arc;
use zenith_utils::lsn::Lsn;

///
/// LayerMap tracks what layers exist or a timeline. The last layer that is
/// open for writes is always an InMemoryLayer, and is tracked separately
/// because there can be only one for each segment. The older layers,
/// stored on disk, are kept in a BTreeMap keyed by the layer's start LSN.
///
pub struct LayerMap {
    segs: HashMap<SegmentTag, SegEntry>,
}

struct SegEntry {
    pub open: Option<Arc<InMemoryLayer>>,
    pub historic: BTreeMap<Lsn, Arc<SnapshotLayer>>,
}

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
            segentry.open = Some(layer);
        } else {
            let segentry = SegEntry {
                open: Some(layer),
                historic: BTreeMap::new(),
            };
            self.segs.insert(tag, segentry);
        }
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

    pub fn iter_open_layers(&mut self) -> OpenLayerIter {
        OpenLayerIter {
            last: None,
            segiter: self.segs.iter_mut(),
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
        }
    }
}

pub struct OpenLayerIter<'a> {
    last: Option<&'a mut SegEntry>,

    segiter: std::collections::hash_map::IterMut<'a, SegmentTag, SegEntry>,
}

impl<'a> OpenLayerIter<'a> {
    pub fn replace(&mut self, replacement: Option<Arc<InMemoryLayer>>) {
        let segentry = self.last.as_mut().unwrap();
        segentry.open = replacement;
    }

    pub fn insert_historic(&mut self, new_layer: Arc<SnapshotLayer>) {
        let start_lsn = new_layer.get_start_lsn();

        let segentry = self.last.as_mut().unwrap();
        segentry.historic.insert(start_lsn, new_layer);
    }
}

impl<'a> Iterator for OpenLayerIter<'a> {
    type Item = Arc<InMemoryLayer>;

    fn next(&mut self) -> std::option::Option<<Self as std::iter::Iterator>::Item> {
        while let Some((_seg, entry)) = self.segiter.next() {
            if let Some(open) = &entry.open {
                let op = Arc::clone(&open);
                self.last = Some(entry);
                return Some(op);
            }
        }
        self.last = None;
        None
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
