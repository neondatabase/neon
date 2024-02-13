use std::sync::Arc;

use rand::Rng;

use crate::proto::NodeEvent;

use super::{
    chan::Chan,
    network::TCP,
    world::{Node, NodeId, World},
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

    /// Opens a bidirectional connection with the other node. Always successful.
    pub fn open_tcp(&self, dst: NodeId) -> TCP {
        self.world.open_tcp(dst)
    }

    /// Returns a channel to receive node events (socket Accept and internal messages).
    pub fn node_events(&self) -> Chan<NodeEvent> {
        self.internal.node_events()
    }

    /// Get current time.
    pub fn now(&self) -> u64 {
        self.world.now()
    }

    /// Generate a random number in range [0, max).
    pub fn random(&self, max: u64) -> u64 {
        self.internal.rng.lock().gen_range(0..max)
    }

    /// Append a new event to the world event log.
    pub fn log_event(&self, data: String) {
        self.internal.log_event(data)
    }
}
