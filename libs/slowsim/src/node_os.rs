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

    pub fn now(&self) -> u64 {
        self.world.now()
    }

    /// Returns a writable pipe. All incoming messages should be polled
    /// with [`network_epoll`]. Always successful.
    pub fn open_tcp(&self, dst: NodeId) -> TCP {
        self.world.open_tcp(&self.internal, dst)
    }

    /// Returns a readable and writable pipe. All incoming messages should
    /// be read from [`TCP`] object.
    pub fn open_tcp_nopoll(&self, dst: NodeId) -> TCP {
        self.world.open_tcp_nopoll(&self.internal, dst)
    }

    /// Returns a channel to receive timers and events from the network.
    pub fn epoll(&self) -> Chan<NodeEvent> {
        self.internal.network_chan()
    }

    /// Returns next event from the epoll channel with timeout.
    /// Returns `None` if timeout is reached.
    /// -1 – wait forever.
    /// 0 - poll, return immediately.
    /// >0 - wait for timeout milliseconds.
    pub fn epoll_recv(&self, timeout: i64) -> Option<NodeEvent> {
        let epoll = self.epoll();

        let ready_event = loop {
            let event = epoll.try_recv();
            if let Some(NodeEvent::WakeTimeout(_)) = event {
                continue;
            }
            break event;
        };

        if let Some(event) = ready_event {
            // return event if it's ready
            return Some(event);
        }

        if timeout == 0 {
            // poll, return immediately
            return None;
        }

        // or wait for timeout

        let rand_nonce = self.random(u64::MAX);
        if timeout > 0 {
            self.world.schedule(
                timeout as u64,
                SendMessageEvent::new(epoll.clone(), NodeEvent::WakeTimeout(rand_nonce)),
            );
        }

        loop {
            match epoll.recv() {
                NodeEvent::WakeTimeout(nonce) if nonce == rand_nonce => {
                    return None;
                }
                NodeEvent::WakeTimeout(_) => {}
                event => {
                    return Some(event);
                }
            }
        }
    }

    /// Same as epoll_recv, but does not remove the event from the queue.
    pub fn epoll_peek(&self, timeout: i64) -> Option<NodeEvent> {
        let epoll = self.epoll();

        let ready_event = loop {
            let event = epoll.try_peek();
            if let Some(NodeEvent::WakeTimeout(_)) = event {
                assert!(epoll.try_recv().is_some());
                continue;
            }
            break event;
        };

        if let Some(event) = ready_event {
            // return event if it's ready
            return Some(event);
        }

        if timeout == 0 {
            // poll, return immediately
            return None;
        }

        // or wait for timeout

        let rand_nonce = self.random(u64::MAX);
        if timeout > 0 {
            self.world.schedule(
                timeout as u64,
                SendMessageEvent::new(epoll.clone(), NodeEvent::WakeTimeout(rand_nonce)),
            );
        }

        loop {
            match epoll.peek() {
                NodeEvent::WakeTimeout(nonce) if nonce == rand_nonce => {
                    assert!(epoll.try_recv().is_some());
                    return None;
                }
                NodeEvent::WakeTimeout(_) => {
                    assert!(epoll.try_recv().is_some());
                }
                event => {
                    return Some(event);
                }
            }
        }
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
