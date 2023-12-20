use std::sync::Arc;

use rand::Rng;

use super::{
    chan::Chan,
    network::TCP,
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

    /// Opens a bidirectional connection with the other node. Always successful.
    pub fn open_tcp(&self, dst: NodeId) -> TCP {
        self.world.open_tcp(dst)
    }

    /// Returns a channel to receive node events (socket Accept and internal messages).
    pub fn node_events(&self) -> Chan<NodeEvent> {
        self.internal.network_chan()
    }

    /// Generate a random number in range [0, max).
    pub fn random(&self, max: u64) -> u64 {
        self.internal.rng.lock().gen_range(0..max)
    }

    /// Set the result for the current node.
    pub fn set_result(&self, code: i32, result: String) {
        *self.internal.result.lock() = (code, result);
    }

    pub fn log_event(&self, data: String) {
        self.world.add_event(self.id(), data)
    }

    pub fn exit(&self, reason: String) {
        self.internal.set_crash_token();
        panic!("exit: {}", reason);
    }
}
