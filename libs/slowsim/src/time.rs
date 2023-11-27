use std::{cmp::Ordering, collections::BinaryHeap, fmt::Debug, sync::Arc};

use super::{chan::Chan, network::VirtualConnection};

pub struct Timing {
    /// Current world's time.
    current_time: u64,
    /// Pending timers.
    timers: BinaryHeap<Pending>,
    /// Global nonce.
    nonce: u32,
}

impl Default for Timing {
    fn default() -> Self {
        Self::new()
    }
}

impl Timing {
    pub fn new() -> Timing {
        Timing {
            current_time: 0,
            timers: BinaryHeap::new(),
            nonce: 0,
        }
    }

    /// Return the current world's time.
    pub fn now(&self) -> u64 {
        self.current_time
    }

    /// Tick-tock the global clock. Return the event ready to be processed
    /// or move the clock forward and then return the event.
    pub fn step(&mut self) -> Option<Pending> {
        if self.timers.is_empty() {
            // no future events
            return None;
        }

        if !self.is_event_ready() {
            let next_time = self.timers.peek().unwrap().time;
            self.current_time = next_time;
            assert!(self.is_event_ready());
        }

        self.timers.pop()
    }

    /// TODO: write docs
    pub fn schedule_future(&mut self, ms: u64, event: Box<dyn Event + Send + Sync>) {
        self.nonce += 1;
        let nonce = self.nonce;
        self.timers.push(Pending {
            time: self.current_time + ms,
            nonce,
            event,
        })
    }

    /// Return true if there is a ready event.
    fn is_event_ready(&self) -> bool {
        self.timers
            .peek()
            .map_or(false, |x| x.time <= self.current_time)
    }

    pub fn clear(&mut self) {
        self.timers.clear();
    }
}

pub struct Pending {
    pub time: u64,
    pub nonce: u32,
    pub event: Box<dyn Event + Send + Sync>,
}

impl Pending {
    pub fn process(&self) {
        self.event.process();
    }
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

pub trait Event: Debug {
    fn process(&self);
}

pub struct SendMessageEvent<T: Debug + Clone> {
    chan: Chan<T>,
    msg: T,
}

impl<T: Debug + Clone> SendMessageEvent<T> {
    pub fn new(chan: Chan<T>, msg: T) -> Box<SendMessageEvent<T>> {
        Box::new(SendMessageEvent { chan, msg })
    }
}

impl<T: Debug + Clone> Event for SendMessageEvent<T> {
    fn process(&self) {
        self.chan.send(self.msg.clone());
    }
}

impl<T: Debug + Clone> Debug for SendMessageEvent<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: add more context about receiver channel
        f.debug_struct("SendMessageEvent")
            .field("msg", &self.msg)
            .finish()
    }
}

pub struct NetworkEvent(pub Arc<VirtualConnection>);

impl Event for NetworkEvent {
    fn process(&self) {
        self.0.process();
    }
}

impl Debug for NetworkEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Network")
            .field("conn", &self.0.connection_id)
            .field("node[0]", &self.0.nodes[0].id)
            .field("node[1]", &self.0.nodes[1].id)
            .finish()
    }
}

#[derive(Copy, Clone, Debug)]
pub struct EmptyEvent;

impl Event for EmptyEvent {
    fn process(&self) {}
}
