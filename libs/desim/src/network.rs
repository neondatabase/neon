use std::{
    cmp::Ordering,
    collections::{BinaryHeap, VecDeque},
    fmt::{self, Debug},
    ops::DerefMut,
    sync::{mpsc, Arc},
};

use parking_lot::{
    lock_api::{MappedMutexGuard, MutexGuard},
    Mutex, RawMutex,
};
use rand::rngs::StdRng;
use tracing::debug;

use crate::{
    executor::{self, ThreadContext},
    options::NetworkOptions,
    proto::NetEvent,
    proto::NodeEvent,
};

use super::{chan::Chan, proto::AnyMessage};

pub struct NetworkTask {
    options: Arc<NetworkOptions>,
    connections: Mutex<Vec<VirtualConnection>>,
    /// min-heap of connections having something to deliver.
    events: Mutex<BinaryHeap<Event>>,
    task_context: Arc<ThreadContext>,
}

impl NetworkTask {
    pub fn start_new(options: Arc<NetworkOptions>, tx: mpsc::Sender<Arc<NetworkTask>>) {
        let ctx = executor::get_thread_ctx();
        let task = Arc::new(Self {
            options,
            connections: Mutex::new(Vec::new()),
            events: Mutex::new(BinaryHeap::new()),
            task_context: ctx,
        });

        // send the task upstream
        tx.send(task.clone()).unwrap();

        // start the task
        task.start();
    }

    pub fn start_new_connection(self: &Arc<Self>, rng: StdRng, dst_accept: Chan<NodeEvent>) -> TCP {
        let now = executor::now();
        let connection_id = self.connections.lock().len();

        let vc = VirtualConnection {
            connection_id,
            dst_accept,
            dst_sockets: [Chan::new(), Chan::new()],
            state: Mutex::new(ConnectionState {
                buffers: [NetworkBuffer::new(None), NetworkBuffer::new(Some(now))],
                rng,
            }),
        };
        vc.schedule_timeout(self);
        vc.send_connect(self);

        let recv_chan = vc.dst_sockets[0].clone();
        self.connections.lock().push(vc);

        TCP {
            net: self.clone(),
            conn_id: connection_id,
            dir: 0,
            recv_chan,
        }
    }
}

// private functions
impl NetworkTask {
    /// Schedule to wakeup network task (self) `after_ms` later to deliver
    /// messages of connection `id`.
    fn schedule(&self, id: usize, after_ms: u64) {
        self.events.lock().push(Event {
            time: executor::now() + after_ms,
            conn_id: id,
        });
        self.task_context.schedule_wakeup(after_ms);
    }

    /// Get locked connection `id`.
    fn get(&self, id: usize) -> MappedMutexGuard<'_, RawMutex, VirtualConnection> {
        MutexGuard::map(self.connections.lock(), |connections| {
            connections.get_mut(id).unwrap()
        })
    }

    fn collect_pending_events(&self, now: u64, vec: &mut Vec<Event>) {
        vec.clear();
        let mut events = self.events.lock();
        while let Some(event) = events.peek() {
            if event.time > now {
                break;
            }
            let event = events.pop().unwrap();
            vec.push(event);
        }
    }

    fn start(self: &Arc<Self>) {
        debug!("started network task");

        let mut events = Vec::new();
        loop {
            let now = executor::now();
            self.collect_pending_events(now, &mut events);

            for event in events.drain(..) {
                let conn = self.get(event.conn_id);
                conn.process(self);
            }

            // block until wakeup
            executor::yield_me(-1);
        }
    }
}

// 0 - from node(0) to node(1)
// 1 - from node(1) to node(0)
type MessageDirection = u8;

fn sender_str(dir: MessageDirection) -> &'static str {
    match dir {
        0 => "client",
        1 => "server",
        _ => unreachable!(),
    }
}

fn receiver_str(dir: MessageDirection) -> &'static str {
    match dir {
        0 => "server",
        1 => "client",
        _ => unreachable!(),
    }
}

/// Virtual connection between two nodes.
/// Node 0 is the creator of the connection (client),
/// and node 1 is the acceptor (server).
struct VirtualConnection {
    connection_id: usize,
    /// one-off chan, used to deliver Accept message to dst
    dst_accept: Chan<NodeEvent>,
    /// message sinks
    dst_sockets: [Chan<NetEvent>; 2],
    state: Mutex<ConnectionState>,
}

struct ConnectionState {
    buffers: [NetworkBuffer; 2],
    rng: StdRng,
}

impl VirtualConnection {
    /// Notify the future about the possible timeout.
    fn schedule_timeout(&self, net: &NetworkTask) {
        if let Some(timeout) = net.options.keepalive_timeout {
            net.schedule(self.connection_id, timeout);
        }
    }

    /// Send the handshake (Accept) to the server.
    fn send_connect(&self, net: &NetworkTask) {
        let now = executor::now();
        let mut state = self.state.lock();
        let delay = net.options.connect_delay.delay(&mut state.rng);
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
        net.schedule(self.connection_id, delay);
    }

    /// Transmit some of the messages from the buffer to the nodes.
    fn process(&self, net: &Arc<NetworkTask>) {
        let now = executor::now();

        let mut state = self.state.lock();

        for direction in 0..2 {
            self.process_direction(
                net,
                state.deref_mut(),
                now,
                direction as MessageDirection,
                &self.dst_sockets[direction ^ 1],
            );
        }

        // Close the one side of the connection by timeout if the node
        // has not received any messages for a long time.
        if let Some(timeout) = net.options.keepalive_timeout {
            let mut to_close = [false, false];
            for direction in 0..2 {
                let buffer = &mut state.buffers[direction];
                if buffer.recv_closed {
                    continue;
                }
                if let Some(last_recv) = buffer.last_recv {
                    if now - last_recv >= timeout {
                        debug!(
                            "NET: connection {} timed out at {}",
                            self.connection_id,
                            receiver_str(direction as MessageDirection)
                        );
                        let node_idx = direction ^ 1;
                        to_close[node_idx] = true;
                    }
                }
            }
            drop(state);

            for (node_idx, should_close) in to_close.iter().enumerate() {
                if *should_close {
                    self.close(node_idx);
                }
            }
        }
    }

    /// Process messages in the buffer in the given direction.
    fn process_direction(
        &self,
        net: &Arc<NetworkTask>,
        state: &mut ConnectionState,
        now: u64,
        direction: MessageDirection,
        to_socket: &Chan<NetEvent>,
    ) {
        let buffer = &mut state.buffers[direction as usize];
        if buffer.recv_closed {
            assert!(buffer.buf.is_empty());
        }

        while !buffer.buf.is_empty() && buffer.buf.front().unwrap().0 <= now {
            let msg = buffer.buf.pop_front().unwrap().1;

            buffer.last_recv = Some(now);
            self.schedule_timeout(net);

            if let AnyMessage::InternalConnect = msg {
                // TODO: assert to_socket is the server
                let server_to_client = TCP {
                    net: net.clone(),
                    conn_id: self.connection_id,
                    dir: direction ^ 1,
                    recv_chan: to_socket.clone(),
                };
                // special case, we need to deliver new connection to a separate channel
                self.dst_accept.send(NodeEvent::Accept(server_to_client));
            } else {
                to_socket.send(NetEvent::Message(msg));
            }
        }
    }

    /// Try to send a message to the buffer, optionally dropping it and
    /// determining delivery timestamp.
    fn send(&self, net: &NetworkTask, direction: MessageDirection, msg: AnyMessage) {
        let now = executor::now();
        let mut state = self.state.lock();

        let (delay, close) = if let Some(ms) = net.options.send_delay.delay(&mut state.rng) {
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
        net.schedule(self.connection_id, delay);
    }

    /// Close the connection. Only one side of the connection will be closed,
    /// and no further messages will be delivered. The other side will not be notified.
    fn close(&self, node_idx: usize) {
        let mut state = self.state.lock();
        let recv_buffer = &mut state.buffers[1 ^ node_idx];
        if recv_buffer.recv_closed {
            debug!(
                "NET: TCP #{} closed twice at {}",
                self.connection_id,
                sender_str(node_idx as MessageDirection),
            );
            return;
        }

        debug!(
            "NET: TCP #{} closed at {}",
            self.connection_id,
            sender_str(node_idx as MessageDirection),
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

        self.dst_sockets[node_idx].send(NetEvent::Closed);
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

/// Single end of a bidirectional network stream without reordering (TCP-like).
/// Reads are implemented using channels, writes go to the buffer inside VirtualConnection.
pub struct TCP {
    net: Arc<NetworkTask>,
    conn_id: usize,
    dir: MessageDirection,
    recv_chan: Chan<NetEvent>,
}

impl Debug for TCP {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TCP #{} ({})", self.conn_id, sender_str(self.dir),)
    }
}

impl TCP {
    /// Send a message to the other side. It's guaranteed that it will not arrive
    /// before the arrival of all messages sent earlier.
    pub fn send(&self, msg: AnyMessage) {
        let conn = self.net.get(self.conn_id);
        conn.send(&self.net, self.dir, msg);
    }

    /// Get a channel to receive incoming messages.
    pub fn recv_chan(&self) -> Chan<NetEvent> {
        self.recv_chan.clone()
    }

    pub fn connection_id(&self) -> usize {
        self.conn_id
    }

    pub fn close(&self) {
        let conn = self.net.get(self.conn_id);
        conn.close(self.dir as usize);
    }
}
struct Event {
    time: u64,
    conn_id: usize,
}

// BinaryHeap is a max-heap, and we want a min-heap. Reverse the ordering here
// to get that.
impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Self) -> Ordering {
        (other.time, other.conn_id).cmp(&(self.time, self.conn_id))
    }
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        (other.time, other.conn_id) == (self.time, self.conn_id)
    }
}

impl Eq for Event {}
