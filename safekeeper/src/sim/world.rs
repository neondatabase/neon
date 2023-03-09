use std::{sync::{atomic::AtomicI32, Arc}, cell::RefCell};
use rand::{rngs::StdRng, SeedableRng, Rng};

use super::{tcp::Tcp, sync::{Mutex, Park}, chan::Chan, proto::AnyMessage, node_os::NodeOs, wait_group::WaitGroup};

pub type NodeId = u32;

/// Full world simulation state, shared between all nodes.
pub struct World {
    nodes: Mutex<Vec<Arc<Node>>>,

    /// List of parked threads, to be woken up by the world simulation.
    unconditional_parking: Mutex<Vec<Arc<Park>>>,

    /// Counter for running threads. Generally should not be more than 1, if you want
    /// to get a deterministic simulation. 0 means that all threads are parked or finished.
    wait_group: WaitGroup,

    /// Random number generator.
    rng: Mutex<StdRng>,
}

impl World {
    pub fn new() -> World {
        World{
            nodes: Mutex::new(Vec::new()),
            unconditional_parking: Mutex::new(Vec::new()),
            wait_group: WaitGroup::new(),
            rng: Mutex::new(StdRng::seed_from_u64(1337)),
        }
    }

    /// Create a new node.
    pub fn new_node(self: &Arc<Self>) -> Arc<Node> {
        // TODO: verify
        let mut nodes = self.nodes.lock();
        let id = nodes.len() as NodeId;
        let node = Arc::new(Node::new(id, self.clone()));
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
    pub fn open_tcp(&self, src: &Arc<Node>, dst: NodeId) -> Tcp {
        // TODO: replace unwrap() with /dev/null socket.
        let dst = self.get_node(dst).unwrap();

        Tcp::new(dst)
    }

    /// Blocks the current thread until all nodes will park or finish.
    pub fn await_all(&self) {
        self.wait_group.wait();
    }

    pub fn step(&self) -> bool {
        self.await_all();

        let mut parking = self.unconditional_parking.lock();
        if parking.is_empty() {
            // nothing to do, all threads have finished
            return false;
        }

        let chosen_one = self.rng.lock().gen_range(0..parking.len());
        let park = parking.swap_remove(chosen_one);
        drop(parking);

        println!("Waking up park at node {:?}", park.node_id());

        // Wake up the chosen thread. To do that:
        // 1. Increment the counter of running threads.
        // 2. Send a singal to continue the thread.
        self.wait_group.add(1);
        park.internal_world_wake();

        // to have a clean state after each step, wait for all threads to finish
        self.await_all();
        return true;
    }

    /// Print full world state to stdout.
    pub fn debug_print_state(&self) {
        println!("[DEBUG] World state, nodes.len()={:?}, parking.len()={:?}", self.nodes.lock().len(), self.unconditional_parking.lock().len());
        for node in self.nodes.lock().iter() {
            println!("[DEBUG] node id={:?} status={:?}", node.id, node.status.lock());
        }
        for park in self.unconditional_parking.lock().iter() {
            park.debug_print();
        }
    }
}

thread_local! {
    pub static CURRENT_NODE: RefCell<Option<Arc<Node>>> = RefCell::new(None);
}

/// Internal node state.
pub struct Node {
    pub id: NodeId,
    network: Chan<NetworkEvent>,
    status: Mutex<NodeStatus>,
    world: Arc<World>,
    join_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeStatus {
    NotStarted,
    Running,
    Waiting,
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
            world: world.clone(),
            join_handle: Mutex::new(None),
        }
    }

    /// Set a code to run in this node thread.
    pub fn launch(self: &Arc<Self>, f: impl FnOnce(NodeOs) + Send + 'static) {
        let node = self.clone();
        let world = self.world.clone();
        world.wait_group.add(1);
        let join_handle = std::thread::spawn(move || {
            CURRENT_NODE.with(|current_node| {
                *current_node.borrow_mut() = Some(node.clone());
            });

            let wg = world.wait_group.clone();
            scopeguard::defer! {
                wg.done();
            }

            let mut status = node.status.lock();
            if *status != NodeStatus::NotStarted {
                // clearly a caller bug, should never happen
                panic!("node {} is already running", node.id);
            }
            *status = NodeStatus::Running;
            drop(status);

            // park the current thread, [`launch`] will wait until it's parked
            Park::yield_thread();
            // TODO: recover from panic (update state, log the error)
            f(NodeOs::new(world, node.clone()));

            let mut status = node.status.lock();
            *status = NodeStatus::Finished;
            // TODO: log the thread is finished
        });
        *self.join_handle.lock() = Some(join_handle);

        // we need to wait for the thread to park, to assure that threads
        // are parked in deterministic order
        self.world.wait_group.wait();
    }

    /// Returns a channel to receive events from the network.
    pub fn network_chan(&self) -> Chan<NetworkEvent> {
        self.network.clone()
    }

    pub fn internal_parking_start(&self) {
        // Node started parking (waiting for condition), and the current thread
        // is the only one running, so we need to do:
        // 1. Change the node status to Waiting
        // 2. Decrease the running threads counter
        // 3. Block the current thread until it's woken up (outside this function)
        *self.status.lock() = NodeStatus::Waiting;
        self.world.wait_group.done();
    }

    pub fn internal_parking_middle(&self, park: Arc<Park>) {
        // [`park`] entered the unconditional_parking state, and the current thread
        // is the only one running, so we need to do:
        // 1. Change the node status to Parked
        // 2. Park in the world list
        // 3. Decrease the running threads counter
        // 4. Block the current thread until it's woken up (outside this function)
        *self.status.lock() = NodeStatus::Parked;
        self.world.unconditional_parking.lock().push(park);
        self.world.wait_group.done();
    }

    pub fn internal_parking_ahead(&self, park: Arc<Park>) {
        // [`park`] entered the unconditional_parking state, and the current thread
        // wants to transfer control to another thread, so we need to do:
        // 1. Change the node status to Parked
        // 2. Park in the world list
        // 3. Notify the other thread to continue
        // 4. Block the current thread until it's woken up (outside this function)
        *self.status.lock() = NodeStatus::Parked;
        self.world.unconditional_parking.lock().push(park);
    }

    pub fn internal_parking_end(&self) {
        // node finished parking, now it's running again
        *self.status.lock() = NodeStatus::Running;
    }

    /// Get the current node, panics if called from outside of a node thread.
    pub fn current() -> Arc<Node> {
        CURRENT_NODE.with(|current_node| {
            current_node.borrow().clone().unwrap()
        })
    }
}

#[derive(Clone, Debug)]
pub enum NetworkEvent {
    Accept,
    Message(AnyMessage),
    // TODO: close?
}
