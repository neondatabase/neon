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

use crate::layered_repository::storage_layer::Layer;
use crate::relish::*;
use anyhow::Result;
use log::*;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::ops::Bound::Included;
use std::sync::Arc;
use zenith_utils::lsn::Lsn;

/// LayerMap is a BTreeMap keyed by RelishTag and the layer's start LSN.
/// It provides a couple of convenience functions over a plain BTreeMap
pub struct LayerMap {
    pub inner: BTreeMap<(RelishTag, Lsn), Arc<dyn Layer>>,
}

impl LayerMap {
    ///
    /// Look up using the given rel tag and LSN. This differs from a plain
    /// key-value lookup in that if there is any layer that covers the
    /// given LSN, or precedes the given LSN, it is returned. In other words,
    /// you don't need to know the exact start LSN of the layer.
    ///
    pub fn get(&self, tag: RelishTag, lsn: Lsn) -> Option<Arc<dyn Layer>> {
        let startkey = (tag, Lsn(0));
        let endkey = (tag, lsn);

        if let Some((_k, v)) = self
            .inner
            .range((Included(startkey), Included(endkey)))
            .next_back()
        {
            Some(Arc::clone(v))
        } else {
            None
        }
    }

    pub fn insert(&mut self, layer: Arc<dyn Layer>) {
        let rel = layer.get_relish_tag();
        let start_lsn = layer.get_start_lsn();

        self.inner.insert((rel, start_lsn), Arc::clone(&layer));
    }

    pub fn remove(&mut self, layer: &dyn Layer) {
        let rel = layer.get_relish_tag();
        let start_lsn = layer.get_start_lsn();

        self.inner.remove(&(rel, start_lsn));
    }

    pub fn list_rels(&self, spcnode: u32, dbnode: u32) -> Result<HashSet<RelTag>> {
        let mut rels: HashSet<RelTag> = HashSet::new();

        // Scan the timeline directory to get all rels in this timeline.
        for ((rel, _lsn), _l) in self.inner.iter() {
            if let RelishTag::Relation(reltag) = rel {
                // FIXME: skip if it was dropped before the requested LSN. But there is no
                // LSN argument

                if (spcnode == 0 || reltag.spcnode == spcnode)
                    && (dbnode == 0 || reltag.dbnode == dbnode)
                {
                    rels.insert(*reltag);
                }
            }
        }
        Ok(rels)
    }

    pub fn list_nonrels(&self, _lsn: Lsn) -> Result<HashSet<RelishTag>> {
        let mut rels: HashSet<RelishTag> = HashSet::new();

        // Scan the timeline directory to get all rels in this timeline.
        for ((rel, _lsn), _l) in self.inner.iter() {
            // FIXME: skip if it was dropped before the requested LSN.

            if let RelishTag::Relation(_) = rel {
            } else {
                rels.insert(*rel);
            }
        }
        Ok(rels)
    }

    /// Is there a newer layer for given relation?
    pub fn newer_layer_exists(&self, rel: RelishTag, lsn: Lsn) -> bool {
        let startkey = (rel, lsn);
        let endkey = (rel, Lsn(u64::MAX));

        for ((_rel, newer_lsn), layer) in self.inner.range((Included(startkey), Included(endkey))) {
            if layer.get_end_lsn() > lsn {
                trace!(
                    "found later layer for rel {}, {} {}-{}",
                    rel,
                    lsn,
                    newer_lsn,
                    layer.get_end_lsn()
                );
                return true;
            } else {
                trace!(
                    "found singleton layer for rel {}, {} {}",
                    rel,
                    lsn,
                    newer_lsn
                );
                continue;
            }
        }
        trace!("no later layer found for rel {}, {}", rel, lsn);
        false
    }
}

impl Default for LayerMap {
    fn default() -> Self {
        LayerMap {
            inner: BTreeMap::new(),
        }
    }
}
