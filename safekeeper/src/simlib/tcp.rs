use std::{
    sync::{Arc},
};



use super::{
    proto::AnyMessage,
    world::{Node, NodeEvent},
};

/// Simplistic simulation of a bidirectional network stream without reordering (TCP).
/// There are almost no errors, writes are always successful (but may end up in void).
/// Reads are implemented as a messages in a shared queue, refer to [`NodeOs::network_epoll`]
/// for details.
pub struct Tcp {
    // TODO: replace with internal TCP buffer, to add random delays and close support
    dst: Arc<Node>,
}

impl Tcp {
    pub fn new(dst: Arc<Node>) -> Tcp {
        Tcp { dst }
    }

    /// Send a message to the other side. It's guaranteed that it will not arrive
    /// before the arrival of all messages sent earlier.
    pub fn send(&self, msg: AnyMessage) {
        // TODO: send to the internal TCP buffer
        self.dst.network_chan().send(NodeEvent::Message(msg));
    }
}
