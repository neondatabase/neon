//!
//! Global registry of open layers.
//!
//! Whenever a new in-memory layer is created to hold incoming WAL, it is registered
//! in [`GLOBAL_LAYER_MAP`], so that we can keep track of the total number of
//! in-memory layers in the system, and know when we need to evict some to release
//! memory.
//!
//! Each layer is assigned a unique ID when it's registered in the global registry.
//! The ID can be used to relocate the layer later, without having to hold locks.
//!

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

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
    Occupied(Arc<InMemoryLayer>),
    /// Vacant slots form a linked list, the value is the index
    /// of the next vacant slot in the list.
    Vacant(Option<usize>),
}

struct Slot {
    tag: u64,
    data: SlotData,
    usage_count: AtomicU8, // for clock algorithm
}

#[derive(Default)]
pub struct OpenLayers {
    slots: Vec<Slot>,
    num_occupied: usize,
    next_victim: AtomicUsize,

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
            SlotData::Occupied(_) => {
                panic!("an occupied slot was in the free list");
            }
            SlotData::Vacant(next_empty_slot_idx) => {
                self.next_empty_slot_idx = next_empty_slot_idx;
            }
        }

        slot.data = SlotData::Occupied(layer);
        slot.usage_count.store(1, Ordering::Relaxed);

        self.num_occupied += 1;
        assert!(self.num_occupied <= slots_len);

        LayerId {
            index: slot_idx,
            tag: slot.tag,
        }
    }

    pub fn get(&self, layer_id: &LayerId) -> Option<Arc<InMemoryLayer>> {
        let slot = self.slots.get(layer_id.index)?; // TODO should out of bounds indexes just panic?
        if slot.tag != layer_id.tag {
            return None;
        }

        if let SlotData::Occupied(layer) = &slot.data {
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

    pub fn find_victim(&self) -> Option<(LayerId, Arc<InMemoryLayer>)> {
        if self.num_occupied == 0 {
            return None;
        }

        // Run the clock algorithm.
        //
        // FIXME: It's theoretically possible that a constant stream of get() requests
        // comes in faster than we advance the clock hand, so that this never finishes.
        loop {
            // FIXME: Because we interpret the clock hand variable modulo slots.len(), the
            // hand effectively jumps to a more or less random place whenever the array is
            // expanded. That's relatively harmless, it just leads to a non-optimal choice
            // of victim. Also, in a server that runs for long enough, the array should reach
            // a steady-state size and not grow anymore.
            let next_victim = self.next_victim.fetch_add(1, Ordering::Relaxed) % self.slots.len();

            let slot = &self.slots[next_victim];

            if let SlotData::Occupied(data) = &slot.data {
                fn update_fn(old_usage_count: u8) -> Option<u8> {
                    if old_usage_count > 0 {
                        Some(old_usage_count - 1)
                    } else {
                        None
                    }
                }

                if slot
                    .usage_count
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, update_fn)
                    .is_err()
                {
                    // Found a slot with usage_count == 0. Return it.
                    return Some((
                        LayerId {
                            index: next_victim,
                            tag: slot.tag,
                        },
                        Arc::clone(data),
                    ));
                }
            }
        }
    }

    // TODO this won't be a public API in the future
    pub fn remove(&mut self, layer_id: &LayerId) {
        let slot = &mut self.slots[layer_id.index];

        if slot.tag != layer_id.tag {
            return;
        }

        match &slot.data {
            SlotData::Occupied(_layer) => {
                // TODO evict the layer
            }
            SlotData::Vacant(_) => unimplemented!(),
        }

        slot.data = SlotData::Vacant(self.next_empty_slot_idx);
        self.next_empty_slot_idx = Some(layer_id.index);

        assert!(self.num_occupied > 0);
        self.num_occupied -= 1;

        slot.tag = slot.tag.wrapping_add(1);
    }
}
