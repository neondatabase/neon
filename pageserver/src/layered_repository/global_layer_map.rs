//!
//! Global registry of open layers.
//!
//! Whenever a new in-memory layer is created to hold incoming WAL, it is registered
//! in [`GLOBAL_LAYER_MAP`], so that we can keep track of the total number of in-memory layers
//! in the system, and know when we need to evict some to release memory.
//!
//! Each layer is assigned a unique ID when it's registered in the global registry.
//! The ID can be used to relocate the layer later, without having to hold locks.

use std::sync::{Arc, RwLock};

use super::inmemory_layer::InMemoryLayer;

use lazy_static::lazy_static;

lazy_static! {
    pub static ref GLOBAL_LAYER_MAP: RwLock<OpenLayers> = RwLock::new(OpenLayers::default());
}

// TODO these types can probably be smaller
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct LayerId {
    index: usize,
    tag: u64, // to avoid ABA problem
}

enum SlotData {
    Occupied(Arc<InMemoryLayer>),
    /// Vacant slots form a linked list, the value is the index
    /// of the next vacant slot in the list.
    Vacant(Option<usize>),
}

struct Slot {
    tag: u64,
    data: SlotData,
}

#[derive(Default)]
pub struct OpenLayers {
    slots: Vec<Slot>,

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
                });
                idx
            }
        };

        let slot = &mut self.slots[slot_idx];

        match slot.data {
            SlotData::Occupied(_) => unimplemented!(),
            SlotData::Vacant(next_empty_slot_idx) => {
                self.next_empty_slot_idx = next_empty_slot_idx;
            }
        }

        slot.data = SlotData::Occupied(layer);

        LayerId {
            index: slot_idx,
            tag: slot.tag,
        }
    }

    pub fn get(&self, slot_id: &LayerId) -> Option<Arc<InMemoryLayer>> {
        let slot = self.slots.get(slot_id.index)?; // TODO should out of bounds indexes just panic?
        if slot.tag != slot_id.tag {
            return None;
        }

        if let SlotData::Occupied(layer) = &slot.data {
            Some(Arc::clone(layer))
        } else {
            None
        }
    }

    // TODO this won't be a public API in the future
    pub fn remove(&mut self, slot_id: &LayerId) {
        let slot = &mut self.slots[slot_id.index];

        if slot.tag != slot_id.tag {
            return;
        }

        match &slot.data {
            SlotData::Occupied(_layer) => {
                // TODO evict the layer
            }
            SlotData::Vacant(_) => unimplemented!(),
        }

        slot.data = SlotData::Vacant(self.next_empty_slot_idx);
        self.next_empty_slot_idx = Some(slot_id.index);

        slot.tag = slot.tag.wrapping_add(1);
    }
}
