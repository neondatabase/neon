use parking_lot::Mutex;
use rand::{rngs::StdRng, SeedableRng};
use std::{
    ops::DerefMut,
    sync::{mpsc, Arc},
};

use crate::{
    executor::{ExternalHandle, Runtime},
    network::NetworkTask,
    options::NetworkOptions,
    proto::{NodeEvent, SimEvent},
    time::Timing,
};

use super::{chan::Chan, network::TCP, node_os::NodeOs};

pub type NodeId = u32;

/// World contains simulation state.
pub struct World {
    nodes: Mutex<Vec<Arc<Node>>>,
    /// Random number generator.
    rng: Mutex<StdRng>,
    /// Internal event log.
    events: Mutex<Vec<SimEvent>>,
    /// Separate task that processes all network messages.
    network_task: Arc<NetworkTask>,
    /// Runtime for running threads and moving time.
    runtime: Mutex<Runtime>,
    /// To get current time.
    timing: Arc<Timing>,
}

impl World {
    pub fn new(seed: u64, options: Arc<NetworkOptions>) -> World {
        let timing = Arc::new(Timing::new());
        let mut runtime = Runtime::new(timing.clone());

        let (tx, rx) = mpsc::channel();

        runtime.spawn(move || {
            // create and start network background thread, and send it back via the channel
            NetworkTask::start_new(options, tx)
        });

        // wait for the network task to start
        while runtime.step() {}

        let network_task = rx.recv().unwrap();

        World {
            nodes: Mutex::new(Vec::new()),
            rng: Mutex::new(StdRng::seed_from_u64(seed)),
            events: Mutex::new(Vec::new()),
            network_task,
            runtime: Mutex::new(runtime),
            timing,
        }
    }

    pub fn step(&self) -> bool {
        self.runtime.lock().step()
    }

    pub fn get_thread_step_count(&self) -> u64 {
        self.runtime.lock().step_counter
    }

    /// Create a new random number generator.
    pub fn new_rng(&self) -> StdRng {
        let mut rng = self.rng.lock();
        StdRng::from_rng(rng.deref_mut()).unwrap()
    }

    /// Create a new node.
    pub fn new_node(self: &Arc<Self>) -> Arc<Node> {
        let mut nodes = self.nodes.lock();
        let id = nodes.len() as NodeId;
        let node = Arc::new(Node::new(id, self.clone(), self.new_rng()));
        nodes.push(node.clone());
        node
    }

    /// Get an internal node state by id.
    fn get_node(&self, id: NodeId) -> Option<Arc<Node>> {
        let nodes = self.nodes.lock();
        let num = id as usize;
        if num < nodes.len() {
            Some(nodes[num].clone())
        } else {
            None
        }
    }

    pub fn stop_all(&self) {
        self.runtime.lock().crash_all_threads();
    }

    /// Returns a writable end of a TCP connection, to send src->dst messages.
    pub fn open_tcp(self: &Arc<World>, dst: NodeId) -> TCP {
        // TODO: replace unwrap() with /dev/null socket.
        let dst = self.get_node(dst).unwrap();
        let dst_accept = dst.node_events.lock().clone();

        let rng = self.new_rng();
        self.network_task.start_new_connection(rng, dst_accept)
    }

    /// Get current time.
    pub fn now(&self) -> u64 {
        self.timing.now()
    }

    /// Get a copy of the internal clock.
    pub fn clock(&self) -> Arc<Timing> {
        self.timing.clone()
    }

    pub fn add_event(&self, node: NodeId, data: String) {
        let time = self.now();
        self.events.lock().push(SimEvent { time, node, data });
    }

    pub fn take_events(&self) -> Vec<SimEvent> {
        let mut events = self.events.lock();
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut events);
        res
    }

    pub fn deallocate(&self) {
        self.stop_all();
        self.timing.clear();
        self.nodes.lock().clear();
    }
}

/// Internal node state.
pub struct Node {
    pub id: NodeId,
    node_events: Mutex<Chan<NodeEvent>>,
    world: Arc<World>,
    pub(crate) rng: Mutex<StdRng>,
}

impl Node {
    pub fn new(id: NodeId, world: Arc<World>, rng: StdRng) -> Node {
        Node {
            id,
            node_events: Mutex::new(Chan::new()),
            world,
            rng: Mutex::new(rng),
        }
    }

    /// Spawn a new thread with this node context.
    pub fn launch(self: &Arc<Self>, f: impl FnOnce(NodeOs) + Send + 'static) -> ExternalHandle {
        let node = self.clone();
        let world = self.world.clone();
        self.world.runtime.lock().spawn(move || {
            f(NodeOs::new(world, node.clone()));
        })
    }

    /// Returns a channel to receive Accepts and internal messages.
    pub fn node_events(&self) -> Chan<NodeEvent> {
        self.node_events.lock().clone()
    }

    /// This will drop all in-flight Accept messages.
    pub fn replug_node_events(&self, chan: Chan<NodeEvent>) {
        *self.node_events.lock() = chan;
    }

    /// Append event to the world's log.
    pub fn log_event(&self, data: String) {
        self.world.add_event(self.id, data)
    }
}
