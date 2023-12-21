use std::{cmp::Ordering, collections::BinaryHeap, fmt::Debug, sync::{Arc, atomic::{AtomicU64, AtomicU32}}, ops::DerefMut};

use parking_lot::Mutex;
use tracing::{debug, trace};

use crate::executor::ThreadContext;

pub struct Timing {
    /// Current world's time.
    current_time: AtomicU64,
    /// Pending timers.
    queue: Mutex<BinaryHeap<Pending>>,
    /// Global nonce.
    nonce: AtomicU32,
}

impl Default for Timing {
    fn default() -> Self {
        Self::new()
    }
}

impl Timing {
    pub fn new() -> Timing {
        Timing {
            current_time: AtomicU64::new(0),
            queue: Mutex::new(BinaryHeap::new()),
            nonce: AtomicU32::new(0),
        }
    }

    /// Return the current world's time.
    pub fn now(&self) -> u64 {
        self.current_time.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Tick-tock the global clock. Return the event ready to be processed
    /// or move the clock forward and then return the event.
    pub(crate) fn step(&self) -> Option<Arc<ThreadContext>> {
        let mut queue = self.queue.lock();

        if queue.is_empty() {
            // no future events
            return None;
        }

        if !self.is_event_ready(queue.deref_mut()) {
            let next_time = queue.peek().unwrap().time;
            self.current_time.store(next_time, std::sync::atomic::Ordering::SeqCst);
            trace!("rewind time to {}", next_time);
            assert!(self.is_event_ready(queue.deref_mut()));
        }

        Some(queue.pop().unwrap().wake_context)
    }

    pub(crate) fn schedule_wakeup(&self, ms: u64, wake_context: Arc<ThreadContext>) {
        self.nonce.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let nonce = self.nonce.load(std::sync::atomic::Ordering::SeqCst);
        self.queue.lock().push(Pending {
            time: self.now() + ms,
            nonce,
            wake_context,
        })
    }

    /// Return true if there is a ready event.
    fn is_event_ready(&self, queue: &mut BinaryHeap<Pending>) -> bool {
        queue
            .peek()
            .map_or(false, |x| x.time <= self.now())
    }

    pub(crate) fn clear(&mut self) {
        self.queue.lock().clear();
    }
}

struct Pending {
    time: u64,
    nonce: u32,
    wake_context: Arc<ThreadContext>,
}

// BinaryHeap is a max-heap, and we want a min-heap. Reverse the ordering here
// to get that.
impl PartialOrd for Pending {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Pending {
    fn cmp(&self, other: &Self) -> Ordering {
        (other.time, other.nonce).cmp(&(self.time, self.nonce))
    }
}

impl PartialEq for Pending {
    fn eq(&self, other: &Self) -> bool {
        (other.time, other.nonce) == (self.time, self.nonce)
    }
}

impl Eq for Pending {}
