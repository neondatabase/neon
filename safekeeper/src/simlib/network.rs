use std::{sync::Arc, collections::VecDeque, fmt::{Debug, self}, ops::DerefMut};

use super::{
    proto::AnyMessage,
    world::{Node, NodeEvent, World}, time::NetworkEvent, sync::Mutex,
};

// 0 - from node(0) to node(1)
// 1 - from node(1) to node(0)
type MessageDirection = u8;

/// Virtual connection between two nodes.
/// Node 0 is the creator of the connection.
pub struct VirtualConnection {
    /// Connection id, used for logging and debugging.
    pub connection_id: u64,
    pub world: Arc<World>,
    pub nodes: [Arc<Node>; 2],
    state: Mutex<ConnectionState>,
}

struct ConnectionState {
    buffers: [NetworkBuffer; 2],
}

impl VirtualConnection {
    pub fn new(id: u64, world: Arc<World>, src: Arc<Node>, dst: Arc<Node>) -> Arc<Self> {
        let conn = Arc::new(Self {
            connection_id: id,
            world,
            nodes: [src.clone(), dst.clone()],
            state: Mutex::new(ConnectionState {
                buffers: [NetworkBuffer::new(), NetworkBuffer::new()],
            }),
        });

        // TODO: add connection to the dst node
        // conn.nodes[1].network_chan().send(NodeEvent::Connection(conn.clone()));

        conn
    }

    /// Transmit some of the messages from the buffer to the nodes.
    pub fn process(self: &Arc<Self>) {
        let now = self.world.now();

        let mut state = self.state.lock();

        for direction in 0..2 {
            self.process_direction(state.deref_mut(), now, direction as MessageDirection, &self.nodes[direction], &self.nodes[direction^1]);
        }
    }

    /// Process messages in the buffer in the given direction.
    fn process_direction(self: &Arc<Self>, state: &mut ConnectionState, now: u64, direction: MessageDirection, from_node: &Arc<Node>, to_node: &Arc<Node>) {
        let buffer = &mut state.buffers[direction as usize];
        
        while !buffer.buf.is_empty() && buffer.buf.front().unwrap().0 <= now {
            let msg = buffer.buf.pop_front().unwrap().1;
            let callback = TCP::new(self.clone(), direction^1);

            println!("NET(time={}): {:?} delivered, {}=>{}", now, msg, from_node.id, to_node.id);
            to_node.network_chan().send(NodeEvent::Message((msg, callback)));
        }
    }

    /// Send a message to the buffer.
    pub fn send(self: &Arc<Self>, direction: MessageDirection, msg: AnyMessage) {
        let now = self.world.now();
        let mut state = self.state.lock();
        let buffer = &mut state.buffers[direction as usize];

        // Send a message 1ms into the future.
        buffer.buf.push_back((now+1, msg));
        self.world.schedule(1, self.as_event());

        // TODO: more involved logic, random delays, connection breaks, etc.
    }

    /// Get an event suitable for scheduling.
    fn as_event(self: &Arc<Self>) -> Box<NetworkEvent> {
        Box::new(NetworkEvent(self.clone()))
    }
}

struct NetworkBuffer {
    // Messages paired with time of delivery
    buf: VecDeque<(u64, AnyMessage)>,
}

impl NetworkBuffer {
    fn new() -> Self {
        Self {
            buf: VecDeque::new(),
        }
    }
}

/// Simplistic simulation of a bidirectional network stream without reordering (TCP).
/// There are almost no errors, writes are always successful (but may end up in void).
/// Reads are implemented as a messages in a shared queue, refer to [`NodeOs::network_epoll`]
/// for details.
/// 
/// TCP struct is just a one side of a connection. To create a connection, use [`NodeOs::open_tcp`].
#[derive(Clone)]
pub struct TCP {
    conn: Arc<VirtualConnection>,
    dir: MessageDirection,
}

impl Debug for TCP {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TCP #{} {{{}=>{}}}", self.conn.connection_id, self.conn.nodes[self.dir as usize].id, self.conn.nodes[1 - self.dir as usize].id)
    }
}

impl TCP {
    pub fn new(conn: Arc<VirtualConnection>, dir: MessageDirection) -> TCP {
        TCP { conn, dir }
    }

    /// Send a message to the other side. It's guaranteed that it will not arrive
    /// before the arrival of all messages sent earlier.
    pub fn send(&self, msg: AnyMessage) {
        self.conn.send(self.dir, msg);
    }
}
