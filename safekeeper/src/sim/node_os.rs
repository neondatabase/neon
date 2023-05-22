use std::sync::Arc;

use super::{
    chan::Chan,
    tcp::Tcp,
    world::{Node, NodeEvent, NodeId, World},
};

/// Abstraction with all functions (aka syscalls) available to the node.
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
    pub fn open_tcp(&self, dst: NodeId) -> Tcp {
        self.world.open_tcp(&self.internal, dst)
    }

    /// Returns a channel to receive timers and events from the network.
    pub fn epoll(&self) -> Chan<NodeEvent> {
        self.internal.network_chan()
    }
}
