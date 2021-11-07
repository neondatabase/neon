//!
//! Global registry of open layers.
//!
//! Whenever a new in-memory layer is created to hold incoming WAL, it is registered
//! in [`GLOBAL_LAYER_MAP`]. This view across all in-memory layers in the system allows
//! global optimization of memory resources. We keep track of the total memory used by
//! all the in-memory layers [`GLOBAL_OPEN_MEM_USAGE`], and whenever that becomes too
//! high, we evict an in-memory layer to disk. See [`find_victim_if_needed`].
//!
//! Each layer is assigned a unique ID when it's registered in the global registry.
//! The ID can be used to relocate the layer later, without having to hold locks.
//!
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use tracing::error;

use super::inmemory_layer::InMemoryLayer;

use lazy_static::lazy_static;

const MAX_USAGE_COUNT: u8 = 5;

lazy_static! {
    pub static ref GLOBAL_LAYER_MAP: RwLock<OpenLayers> = RwLock::new(OpenLayers::default());
}

///
/// How much memory is being used by all the open layers? This is used to trigger
/// freezing and evicting an open layer to disk.
///
/// This is only a rough approximation, it leaves out a lot of things like malloc()
/// overhead. But as long there is enough "slop" and it's not set too close to the RAM
/// size on the system, it's good enough.
pub static GLOBAL_OPEN_MEM_USAGE: AtomicUsize = AtomicUsize::new(0);

// TODO these types can probably be smaller
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct LayerId {
    index: usize,
    tag: u64, // to avoid ABA problem
}

enum SlotData {
    /// The second element is the (approximate) amount of memory used by the layer.
    Occupied(Arc<InMemoryLayer>, usize),
    /// Vacant slots form a linked list, the value is the index
    /// of the next vacant slot in the list.
    Vacant(Option<usize>),
}

struct Slot {
    tag: u64,
    data: SlotData,
    /// Tracks how frequently this layer has been accessed recently. TODO: This is
    /// intended for a clock algorithm, but currently the eviction is based purely
    /// on memory usage, not how recently a layer has been accessed. So currently
    /// this is unused.
    usage_count: AtomicU8,
}

#[derive(Default)]
pub struct OpenLayers {
    slots: Vec<Slot>,
    num_occupied: usize,

    // Head of free-slot list.
    next_empty_slot_idx: Option<usize>,
}

impl OpenLayers {
    pub fn insert(&mut self, layer: Arc<InMemoryLayer>) -> LayerId {
        let slot_idx = match self.next_empty_slot_idx {
            Some(slot_idx) => slot_idx,
            None => {
                let idx = self.slots.len();
                self.slots.push(Slot {
                    tag: 0,
                    data: SlotData::Vacant(None),
                    usage_count: AtomicU8::new(0),
                });
                idx
            }
        };
        let slots_len = self.slots.len();

        let slot = &mut self.slots[slot_idx];

        match slot.data {
            SlotData::Occupied(_, _) => {
                panic!("an occupied slot was in the free list");
            }
            SlotData::Vacant(next_empty_slot_idx) => {
                self.next_empty_slot_idx = next_empty_slot_idx;
            }
        }

        let layer_id = LayerId {
            index: slot_idx,
            tag: slot.tag,
        };

        layer.register_layer_id(layer_id);

        slot.data = SlotData::Occupied(layer, 0);
        slot.usage_count.store(1, Ordering::Relaxed);

        self.num_occupied += 1;
        assert!(self.num_occupied <= slots_len);

        layer_id
    }

    pub fn get(&self, layer_id: &LayerId) -> Option<Arc<InMemoryLayer>> {
        let slot = self.slots.get(layer_id.index)?; // TODO should out of bounds indexes just panic?
        if slot.tag != layer_id.tag {
            return None;
        }

        if let SlotData::Occupied(layer, _mem_usage) = &slot.data {
            let _ = slot.usage_count.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |old_usage_count| {
                    if old_usage_count < MAX_USAGE_COUNT {
                        Some(old_usage_count + 1)
                    } else {
                        None
                    }
                },
            );
            Some(Arc::clone(layer))
        } else {
            None
        }
    }

    /// Find a victim layer to evict, if the total memory usage of all open layers
    /// is larger than 'limit'
    pub fn find_victim_if_needed(&self, limit: usize) -> Option<(LayerId, Arc<InMemoryLayer>)> {
        let mem_usage = GLOBAL_OPEN_MEM_USAGE.load(Ordering::Relaxed);

        if mem_usage > limit {
            self.find_victim()
        } else {
            None
        }
    }

    /// Find a layer to evict, to release memory.
    fn find_victim(&self) -> Option<(LayerId, Arc<InMemoryLayer>)> {
        if self.num_occupied == 0 {
            return None;
        }

        // Currently, we choose the in-memory layer that uses most memory as the victim.
        //
        // TODO: An optimal algorithm would laso take into account how frequently or recently
        // each layer has been accessed, but we don't do that now. (To be pedentic, an optimal
        // algorithm would take into account when a layer is *next* going to be accessed, but
        // the best we can do in practice is to guess based on history.)
        let mut best_slot = 0;
        let mut best_mem_usage = 0;

        for i in 0..self.slots.len() {
            let slot = &self.slots[i];

            if let SlotData::Occupied(_data, mem_usage) = &slot.data {
                if *mem_usage > best_mem_usage {
                    best_slot = i;
                    best_mem_usage = *mem_usage;
                }
            }
        }

        if best_mem_usage > 0 {
            let slot = &self.slots[best_slot];

            if let SlotData::Occupied(data, _mem_usage) = &slot.data {
                return Some((
                    LayerId {
                        index: best_slot,
                        tag: slot.tag,
                    },
                    Arc::clone(data),
                ));
            }
        }

        None
    }

    // TODO this won't be a public API in the future
    pub fn remove(&mut self, layer_id: &LayerId) {
        let slot = &mut self.slots[layer_id.index];

        if slot.tag != layer_id.tag {
            return;
        }

        match &slot.data {
            SlotData::Occupied(layer, mem_usage) => {
                layer.unregister();
                GLOBAL_OPEN_MEM_USAGE.fetch_sub(*mem_usage, Ordering::Relaxed);
            }
            SlotData::Vacant(_) => unimplemented!(),
        }

        slot.data = SlotData::Vacant(self.next_empty_slot_idx);
        self.next_empty_slot_idx = Some(layer_id.index);

        assert!(self.num_occupied > 0);
        self.num_occupied -= 1;

        slot.tag = slot.tag.wrapping_add(1);
    }

    /// In-memory layers call this function whenever the memory consumption of layer
    /// changes significantly.
    pub fn update_layer_memory_stats(&mut self, layer_id: LayerId, mem_usage: usize) {
        let slot = &mut self.slots[layer_id.index];

        if slot.tag != layer_id.tag {
            return;
        }

        match &mut slot.data {
            SlotData::Occupied(_layer, m) => {
                *m = mem_usage;
            }
            SlotData::Vacant(_) => {
                // Shouldn't happen.
                error!("update_layer_memory_stats called for a layer that was already evicted");
            }
        }
    }
}
