use std::sync::{atomic::AtomicI32, Arc};

use super::{tcp::Tcp, sync::Mutex, chan::Chan, proto::AnyMessage};

pub type NodeId = u32;

/// Full world simulation state, shared between all nodes.
pub struct World {
    nodes: Mutex<Vec<Arc<Node>>>,
}

impl World {
    pub fn new() -> World {
        World{
            nodes: Mutex::new(Vec::new()),
        }
    }

    /// Create a new node.
    pub fn new_node(&self) -> Arc<Node> {
        // TODO: verify
        let mut nodes = self.nodes.lock();
        let id = nodes.len() as NodeId;
        let node = Arc::new(Node{
            id,
            network: Chan::new(),
        });
        nodes.push(node.clone());
        node
    }

    /// Get an internal node state by id.
    pub fn get_node(&self, id: NodeId) -> Option<Arc<Node>> {
        let nodes = self.nodes.lock();
        let num = id as usize;
        if num < nodes.len() {
            Some(nodes[num].clone())
        } else {
            None
        }
    }

    /// Returns a writable end of a TCP connection, to send src->dst messages.
    pub fn open_tcp(&self, src: Arc<Node>, dst: NodeId) -> Tcp {
        // TODO: replace unwrap() with /dev/null socket.
        let dst = self.get_node(dst).unwrap();

        Tcp::new(dst)
    }
}

/// Internal node state.
pub struct Node {
    pub id: NodeId,
    network: Chan<NetworkEvent>,
    status: Mutex<NodeStatus>,
    world: Arc<World>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeStatus {
    NotStarted,
    Running,
    Parked,
    Finished,
    Failed,
}

impl Node {
    pub fn new(id: NodeId, world: Arc<World>) -> Node {
        Node{
            id,
            network: Chan::new(),
            status: Mutex::new(NodeStatus::NotStarted), 
        }
    }

    pub fn launch(&self, f: impl FnOnce() + Send + 'static) {
        let node = self.clone();
        std::thread::spawn(move || {
            let status = node.status.lock();
            if *status != NodeStatus::NotStarted {
                // unhandled panic, clearly a caller bug, should never happen
                panic!("node {} is already running", node.id);
            }
            *status = NodeStatus::Running;
            drop(status);

            // TODO:
        });
    }

    /// Returns a channel to receive events from the network.
    pub fn network_chan(&self) -> Chan<NetworkEvent> {
        self.network.clone()
    }
}

#[derive(Clone)]
pub enum NetworkEvent {
    Accept,
    Message(AnyMessage),
    // TODO: close?
}
