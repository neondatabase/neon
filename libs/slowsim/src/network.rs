use std::{
    collections::VecDeque,
    fmt::{self, Debug},
    ops::DerefMut,
    sync::Arc,
};

use rand::{rngs::StdRng, Rng};
use tracing::debug;

use super::{
    chan::Chan,
    proto::AnyMessage,
    sync::Mutex,
    time::NetworkEvent,
    world::{Node, NodeEvent, World},
};

#[derive(Clone, Debug)]
pub struct Delay {
    pub min: u64,
    pub max: u64,
    pub fail_prob: f64, // [0; 1]
}

impl Delay {
    /// No delay, no failures.
    pub fn empty() -> Delay {
        Delay {
            min: 0,
            max: 0,
            fail_prob: 0.0,
        }
    }

    /// Fixed delay.
    pub fn fixed(ms: u64) -> Delay {
        Delay {
            min: ms,
            max: ms,
            fail_prob: 0.0,
        }
    }

    /// Generate a random delay in range [min, max]. Return None if the
    /// message should be dropped.
    pub fn delay(&self, rng: &mut StdRng) -> Option<u64> {
        if rng.gen_bool(self.fail_prob) {
            return None;
        }
        Some(rng.gen_range(self.min..=self.max))
    }
}

#[derive(Clone, Debug)]
pub struct NetworkOptions {
    /// Connection will be automatically closed after this timeout.
    pub keepalive_timeout: Option<u64>,
    pub connect_delay: Delay,
    pub send_delay: Delay,
}

// 0 - from node(0) to node(1)
// 1 - from node(1) to node(0)
type MessageDirection = u8;

/// Virtual connection between two nodes.
/// Node 0 is the creator of the connection (client),
/// and node 1 is the acceptor (server).
pub struct VirtualConnection {
    /// Connection id, used for logging and debugging and C API.
    pub connection_id: u64,
    pub world: Arc<World>,
    pub nodes: [Arc<Node>; 2],
    dst_sockets: [Chan<NodeEvent>; 2],
    state: Mutex<ConnectionState>,
    options: Arc<NetworkOptions>,
}

struct ConnectionState {
    buffers: [NetworkBuffer; 2],
    rng: StdRng,
}

impl VirtualConnection {
    pub fn new(
        id: u64,
        world: Arc<World>,
        src_sink: Chan<NodeEvent>,
        dst_sink: Chan<NodeEvent>,
        src: Arc<Node>,
        dst: Arc<Node>,
        options: Arc<NetworkOptions>,
    ) -> Arc<Self> {
        let now = world.now();
        let rng = world.new_rng();

        let conn = Arc::new(Self {
            connection_id: id,
            world,
            dst_sockets: [src_sink, dst_sink],
            nodes: [src, dst],
            state: Mutex::new(ConnectionState {
                buffers: [NetworkBuffer::new(None), NetworkBuffer::new(Some(now))],
                rng,
            }),
            options,
        });

        conn.world.add_conn(conn.clone());

        conn.schedule_timeout();
        conn.send_connect();

        // TODO: add connection to the dst node
        // conn.dst_sockets[1].send(NodeEvent::Connection(conn.clone()));

        conn
    }

    /// Notify the future about the possible timeout.
    fn schedule_timeout(self: &Arc<Self>) {
        if let Some(timeout) = self.options.keepalive_timeout {
            self.world.schedule(timeout, self.as_event());
        }
    }

    /// Transmit some of the messages from the buffer to the nodes.
    pub fn process(self: &Arc<Self>) {
        let now = self.world.now();

        let mut state = self.state.lock();

        for direction in 0..2 {
            self.process_direction(
                state.deref_mut(),
                now,
                direction as MessageDirection,
                &self.dst_sockets[direction ^ 1],
            );
        }

        // Close the one side of the connection by timeout if the node
        // has not received any messages for a long time.
        if let Some(timeout) = self.options.keepalive_timeout {
            let mut to_close = [false, false];
            for direction in 0..2 {
                let node_idx = direction ^ 1;
                let node = &self.nodes[node_idx];

                let buffer = &mut state.buffers[direction];
                if buffer.recv_closed {
                    continue;
                }
                if let Some(last_recv) = buffer.last_recv {
                    if now - last_recv >= timeout {
                        debug!(
                            "NET: connection {} timed out at node {}",
                            self.connection_id, node.id
                        );
                        to_close[node_idx] = true;
                    }
                }
            }
            drop(state);

            for node_idx in 0..2 {
                if to_close[node_idx] {
                    self.close(node_idx);
                }
            }
        }
    }

    /// Process messages in the buffer in the given direction.
    fn process_direction(
        self: &Arc<Self>,
        state: &mut ConnectionState,
        now: u64,
        direction: MessageDirection,
        to_socket: &Chan<NodeEvent>,
    ) {
        let buffer = &mut state.buffers[direction as usize];
        if buffer.recv_closed {
            assert!(buffer.buf.is_empty());
        }

        while !buffer.buf.is_empty() && buffer.buf.front().unwrap().0 <= now {
            let msg = buffer.buf.pop_front().unwrap().1;
            let callback = TCP::new(self.clone(), direction ^ 1);

            // debug!(
            //     "NET: {:?} delivered, {}=>{}",
            //     msg, from_node.id, to_node.id
            // );
            buffer.last_recv = Some(now);
            self.schedule_timeout();

            if let AnyMessage::InternalConnect = msg {
                to_socket.send(NodeEvent::Accept(callback));
            } else {
                to_socket.send(NodeEvent::Message((msg, callback)));
            }
        }
    }

    /// Send a message to the buffer.
    pub fn send(self: &Arc<Self>, direction: MessageDirection, msg: AnyMessage) {
        let now = self.world.now();
        let mut state = self.state.lock();

        let (delay, close) = if let Some(ms) = self.options.send_delay.delay(&mut state.rng) {
            (ms, false)
        } else {
            (0, true)
        };

        let buffer = &mut state.buffers[direction as usize];
        if buffer.send_closed {
            debug!(
                "NET: TCP #{} dropped message {:?} (broken pipe)",
                self.connection_id, msg
            );
            return;
        }

        if close {
            debug!(
                "NET: TCP #{} dropped message {:?} (pipe just broke)",
                self.connection_id, msg
            );
            buffer.send_closed = true;
            return;
        }

        if buffer.recv_closed {
            debug!(
                "NET: TCP #{} dropped message {:?} (recv closed)",
                self.connection_id, msg
            );
            return;
        }

        // Send a message into the future.
        buffer.buf.push_back((now + delay, msg));
        self.world.schedule(delay, self.as_event());
    }

    /// Send the handshake (Accept) to the server.
    fn send_connect(self: &Arc<Self>) {
        let now = self.world.now();
        let mut state = self.state.lock();
        let delay = self.options.connect_delay.delay(&mut state.rng);
        let buffer = &mut state.buffers[0];
        assert!(buffer.buf.is_empty());
        assert!(!buffer.recv_closed);
        assert!(!buffer.send_closed);
        assert!(buffer.last_recv.is_none());

        let delay = if let Some(ms) = delay {
            ms
        } else {
            debug!("NET: TCP #{} dropped connect", self.connection_id);
            buffer.send_closed = true;
            return;
        };

        // Send a message into the future.
        buffer
            .buf
            .push_back((now + delay, AnyMessage::InternalConnect));
        self.world.schedule(delay, self.as_event());
    }

    fn internal_recv(self: &Arc<Self>, node_idx: usize) -> NodeEvent {
        // Only src node can receive messages.
        assert!(node_idx == 0);
        return self.dst_sockets[node_idx].recv();
    }

    /// Close the connection. Only one side of the connection will be closed,
    /// and no further messages will be delivered. The other side will not be notified.
    pub fn close(self: &Arc<Self>, node_idx: usize) {
        let node = &self.nodes[node_idx];

        let mut state = self.state.lock();
        let recv_buffer = &mut state.buffers[1 ^ node_idx];
        if recv_buffer.recv_closed {
            debug!(
                "NET: TCP #{} closed twice at node {}",
                self.connection_id, node.id
            );
            return;
        }

        debug!(
            "NET: TCP #{} closed at node {}",
            self.connection_id, node.id
        );
        recv_buffer.recv_closed = true;
        for msg in recv_buffer.buf.drain(..) {
            debug!(
                "NET: TCP #{} dropped message {:?} (closed)",
                self.connection_id, msg
            );
        }

        let send_buffer = &mut state.buffers[node_idx];
        send_buffer.send_closed = true;
        drop(state);

        // TODO: notify the other side?

        self.dst_sockets[node_idx].send(NodeEvent::Closed(TCP::new(self.clone(), node_idx as u8)));
    }

    /// Get an event suitable for scheduling.
    fn as_event(self: &Arc<Self>) -> Box<NetworkEvent> {
        Box::new(NetworkEvent(self.clone()))
    }

    pub fn deallocate(&self) {
        self.dst_sockets[0].clear();
        self.dst_sockets[1].clear();
    }
}

struct NetworkBuffer {
    /// Messages paired with time of delivery
    buf: VecDeque<(u64, AnyMessage)>,
    /// True if the connection is closed on the receiving side,
    /// i.e. no more messages from the buffer will be delivered.
    recv_closed: bool,
    /// True if the connection is closed on the sending side,
    /// i.e. no more messages will be added to the buffer.
    send_closed: bool,
    /// Last time a message was delivered from the buffer.
    /// If None, it means that the server is the receiver and
    /// it has not yet aware of this connection (i.e. has not
    /// received the Accept).
    last_recv: Option<u64>,
}

impl NetworkBuffer {
    fn new(last_recv: Option<u64>) -> Self {
        Self {
            buf: VecDeque::new(),
            recv_closed: false,
            send_closed: false,
            last_recv,
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
        write!(
            f,
            "TCP #{} {{{}=>{}}}",
            self.conn.connection_id,
            self.conn.nodes[self.dir as usize].id,
            self.conn.nodes[1 - self.dir as usize].id
        )
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

    /// Receive a message. Blocks until a message is available. Can be used only
    /// with sockets opened with [`NodeOs::open_tcp_nopoll`].
    pub fn recv(&self) -> NodeEvent {
        // TODO: handle closed connection
        self.conn.internal_recv(self.dir as usize)
    }

    pub fn id(&self) -> i64 {
        let positive: i64 = (self.conn.connection_id + 1) as i64;
        if self.dir == 0 {
            positive
        } else {
            -positive
        }
    }

    pub fn connection_id(&self) -> u64 {
        self.conn.connection_id
    }

    pub fn close(&self) {
        self.conn.close(self.dir as usize);
    }
}
