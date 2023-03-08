use std::sync::Arc;

use super::{world::{World, NodeId, Node, NetworkEvent}, tcp::Tcp, chan::Chan};

/// Abstraction with all functions (aka syscalls) available to the node.
pub struct NodeOs {
    world: Arc<World>,
    internal: Arc<Node>,
}

impl NodeOs {
    pub fn new(world: Arc<World>, internal: Arc<Node>) -> NodeOs {
        NodeOs{
            world,
            internal,
        }
    }

    /// Returns a writable pipe. All incoming messages should be polled
    /// with [`network_epoll`]. Always successful.
    pub fn open_tcp(&self, dst: NodeId) -> Tcp {
        self.world.open_tcp(self.internal, dst)
    }

    /// Returns a channel to receive events from the network.
    pub fn network_epoll(&self) -> Chan<NetworkEvent> {
        self.internal.network_chan()
    }
}
