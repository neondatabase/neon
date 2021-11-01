use std::sync::{Arc, RwLock};

use super::inmemory_layer::InMemoryLayer;

use lazy_static::lazy_static;

lazy_static! {
    pub static ref GLOBAL_CACHE: RwLock<Cache> = RwLock::new(Cache::default());
}

#[derive(PartialEq, Eq, Clone, Copy)]
pub struct SlotId {
    index: usize,
    version: u64,
}

enum SlotData {
    Occupied(Arc<InMemoryLayer>),
    Vacant(Option<usize>),
}

struct Slot {
    version: u64,
    data: SlotData,
}

// TODO Cache isn't really the right name
#[derive(Default)]
pub struct Cache {
    slots: Vec<Slot>,
    next_empty_slot_id: Option<usize>,
}

impl Cache {
    pub fn insert(&mut self, layer: Arc<InMemoryLayer>) -> SlotId {
        let slot_id = match self.next_empty_slot_id {
            Some(slot_id) => slot_id,
            None => {
                let id = self.slots.len();
                self.slots.push(Slot {
                    version: 0,
                    data: SlotData::Vacant(None),
                });
                id
            }
        };

        let slot = &mut self.slots[slot_id];

        match slot.data {
            SlotData::Occupied(_) => unimplemented!(),
            SlotData::Vacant(next_empty_slot_id) => {
                self.next_empty_slot_id = next_empty_slot_id;
            }
        }

        slot.data = SlotData::Occupied(layer);

        SlotId {
            index: slot_id,
            version: slot.version,
        }
    }

    pub fn get(&self, slot_id: &SlotId) -> Option<Arc<InMemoryLayer>> {
        let slot = self.slots.get(slot_id.index)?; // TODO should out of bounds indexes just panic?
        if slot.version != slot_id.version {
            return None;
        }

        if let SlotData::Occupied(layer) = &slot.data {
            Some(Arc::clone(layer))
        } else {
            None
        }
    }

    // TODO this won't be a public API in the future
    pub fn remove(&mut self, slot_id: &SlotId) {
        let slot = &mut self.slots[slot_id.index];

        if slot.version != slot_id.version {
            return;
        }

        match &slot.data {
            SlotData::Occupied(_layer) => {
                // TODO evict the layer
            }
            SlotData::Vacant(_) => unimplemented!(),
        }

        slot.data = SlotData::Vacant(self.next_empty_slot_id);
        self.next_empty_slot_id = Some(slot_id.index);

        slot.version = slot.version.wrapping_add(1);
    }
}
