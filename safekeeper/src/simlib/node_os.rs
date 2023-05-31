use std::sync::Arc;

use rand::Rng;

use super::{
    chan::Chan,
    network::TCP,
    time::SendMessageEvent,
    world::{Node, NodeEvent, NodeId, World},
};

/// Abstraction with all functions (aka syscalls) available to the node.
#[derive(Clone)]
pub struct NodeOs {
    world: Arc<World>,
    internal: Arc<Node>,
}

impl NodeOs {
    pub fn new(world: Arc<World>, internal: Arc<Node>) -> NodeOs {
        NodeOs { world, internal }
    }

    /// Get the node id.
    pub fn id(&self) -> NodeId {
        self.internal.id
    }

    /// Returns a writable pipe. All incoming messages should be polled
    /// with [`network_epoll`]. Always successful.
    pub fn open_tcp(&self, dst: NodeId) -> TCP {
        self.world.open_tcp(&self.internal, dst)
    }

    /// Returns a channel to receive timers and events from the network.
    pub fn epoll(&self) -> Chan<NodeEvent> {
        self.internal.network_chan()
    }

    /// Sleep for a given number of milliseconds.
    /// Currently matches the global virtual time, TODO may be good to
    /// introduce a separate clocks for each node.
    pub fn sleep(&self, ms: u64) {
        let chan: Chan<()> = Chan::new();
        self.world
            .schedule(ms, SendMessageEvent::new(chan.clone(), ()));
        chan.recv();
    }

    /// Generate a random number in range [0, max).
    pub fn random(&self, max: u64) -> u64 {
        self.internal.rng.lock().gen_range(0..max)
    }
}
