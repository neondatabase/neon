//! This is similar to crossbeam_epoch crate, but works in shared memory

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;

const NUM_SLOTS: usize = 1000;

/// This is the struct that is stored in shmem
///
/// bit 0: is it pinned or not?
/// rest of the bits are the epoch counter.
pub struct EpochShared {
    global_epoch: AtomicU64,
    participants: [CachePadded<AtomicU64>; NUM_SLOTS],

    broadcast_lock: spin::Mutex<()>,
}

impl EpochShared {
    pub fn new() -> EpochShared {
        EpochShared {
            global_epoch: AtomicU64::new(2),
            participants: [const { CachePadded::new(AtomicU64::new(2)) }; NUM_SLOTS],
            broadcast_lock: spin::Mutex::new(()),
        }
    }

    pub fn register(&self) -> LocalHandle {
        LocalHandle {
            global: self,
            last_slot: AtomicUsize::new(0), // todo: choose more intelligently
        }
    }

    fn release_pin(&self, slot: usize, _epoch: u64) {
        let global_epoch = self.global_epoch.load(Ordering::Relaxed);
        self.participants[slot].store(global_epoch, Ordering::Relaxed);
    }

    fn pin_internal(&self, slot_hint: usize) -> (usize, u64) {
        // pick a slot
        let mut slot = slot_hint;
        let epoch = loop {
            let old = self.participants[slot].fetch_or(1, Ordering::Relaxed);
            if old & 1 == 0 {
                // Got this slot
                break old;
            }

            // the slot was busy by another thread / process. try a different slot
            slot += 1;
            if slot == NUM_SLOTS {
                slot = 0;
            }
            continue;
        };
        (slot, epoch)
    }

    pub(crate) fn advance(&self) -> u64 {
        // Advance the global epoch
        let old_epoch = self.global_epoch.fetch_add(2, Ordering::Relaxed);
        // Anyone that release their pin after this will update their slot.
        old_epoch + 2
    }

    pub(crate) fn broadcast(&self) {
        let Some(_guard) = self.broadcast_lock.try_lock() else {
            return;
        };

        let epoch = self.global_epoch.load(Ordering::Relaxed);
        let old_epoch = epoch.wrapping_sub(2);

        // Update all free slots.
        for i in 0..NUM_SLOTS {
            // TODO: check result, as a sanity check. It should either be the old epoch, or pinned
            let _ = self.participants[i].compare_exchange(
                old_epoch,
                epoch,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }

        // FIXME: memory fence here, since we used Relaxed?
    }

    pub(crate) fn get_oldest(&self) -> u64 {
        // Read all slots.
        let now = self.global_epoch.load(Ordering::Relaxed);
        let mut oldest = now;
        for i in 0..NUM_SLOTS {
            let this_epoch = self.participants[i].load(Ordering::Relaxed);
            let delta = now.wrapping_sub(this_epoch);
            if delta > u64::MAX / 2 {
                // this is very recent
            } else if delta > now.wrapping_sub(oldest) {
                oldest = this_epoch;
            }
        }
        oldest
    }

    pub(crate) fn get_current(&self) -> u64 {
        self.global_epoch.load(Ordering::Relaxed)
    }
}

pub(crate) struct EpochPin<'e> {
    slot: usize,
    pub(crate) epoch: u64,

    handle: &'e LocalHandle<'e>,
}

impl<'e> Drop for EpochPin<'e> {
    fn drop(&mut self) {
        self.handle.global.release_pin(self.slot, self.epoch);
    }
}

pub struct LocalHandle<'g> {
    global: &'g EpochShared,

    last_slot: AtomicUsize,
}

impl<'g> LocalHandle<'g> {
    pub fn pin(&self) -> EpochPin {
        let (slot, epoch) = self
            .global
            .pin_internal(self.last_slot.load(Ordering::Relaxed));
        self.last_slot.store(slot, Ordering::Relaxed);
        EpochPin {
            handle: self,
            epoch,
            slot,
        }
    }
}
