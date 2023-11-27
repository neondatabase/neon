use std::{backtrace::Backtrace, sync::Arc};

use tracing::debug;

use super::world::{Node, NodeId, World};

pub type Mutex<T> = parking_lot::Mutex<T>;

/// More deterministic condvar. Determenism comes from the fact that
/// at all times there is at most one running thread.
pub struct Condvar {
    waiters: Mutex<CondvarState>,
}

struct CondvarState {
    waiters: Vec<Arc<Park>>,
}

impl Default for Condvar {
    fn default() -> Self {
        Self::new()
    }
}

impl Condvar {
    pub fn new() -> Condvar {
        Condvar {
            waiters: Mutex::new(CondvarState {
                waiters: Vec::new(),
            }),
        }
    }

    /// Blocks the current thread until this condition variable receives a notification.
    pub fn wait<T>(&self, guard: &mut parking_lot::MutexGuard<'_, T>) {
        let park = Park::new(false);

        // add the waiter to the list
        self.waiters.lock().waiters.push(park.clone());

        parking_lot::MutexGuard::unlocked(guard, || {
            // part the thread, it will be woken up by notify_one or notify_all
            park.park();
        });
    }

    /// Wakes up all blocked threads on this condvar, can be called only from the node thread.
    pub fn notify_all(&self) {
        // TODO: check that it's waked up in random order and yield to the scheduler

        let mut waiters = self.waiters.lock().waiters.drain(..).collect::<Vec<_>>();
        for waiter in waiters.drain(..) {
            // block (park) the current thread, wake the other thread
            waiter.wake();
        }
    }

    /// Wakes up one blocked thread on this condvar. Usually can be called only from the node thread,
    /// because we have a global running threads counter and we transfer it from the current thread
    /// to the woken up thread. But we have a HACK here to allow calling it from the world thread.
    pub fn notify_one(&self) {
        // TODO: wake up random thread

        let to_wake = self.waiters.lock().waiters.pop();

        if Node::is_node_thread() {
            if let Some(waiter) = to_wake {
                // block (park) the current thread, wake the other thread
                waiter.wake();
            } else {
                // block (park) the current thread just in case
                Park::yield_thread()
            }
        } else {
            // HACK: custom notify_one implementation for the world thread
            if let Some(waiter) = to_wake {
                // block (park) the current thread, wake the other thread
                waiter.external_wake();
            }
        }
    }
}

/// A tool to block (park) a current thread until it will be woken up.
pub struct Park {
    lock: Mutex<ParkState>,
    cvar: parking_lot::Condvar,
}

struct ParkState {
    /// False means that thread cannot continue without external signal,
    /// i.e. waiting for some event to happen.
    can_continue: bool,
    /// False means that thread is unconditionally parked and waiting for
    /// world simulation to wake it up. True means that the parking is
    /// finished and the thread can continue.
    finished: bool,
    /// True means that the thread should wake up and panic.
    panic: bool,
    node_id: Option<NodeId>,
    backtrace: Option<Backtrace>,
}

impl Park {
    pub fn new(can_continue: bool) -> Arc<Park> {
        Arc::new(Park {
            lock: Mutex::new(ParkState {
                can_continue,
                finished: false,
                panic: false,
                node_id: None,
                backtrace: None,
            }),
            cvar: parking_lot::Condvar::new(),
        })
    }

    fn init_state(state: &mut ParkState, node: &Arc<Node>) {
        state.node_id = Some(node.id);
        state.backtrace = Some(Backtrace::capture());
    }

    /// Should be called once by the waiting thread. Blocks the thread until wake() is called,
    /// and until the thread is woken up by the world simulation.
    pub fn park(self: &Arc<Self>) {
        let node = Node::current();

        // start blocking
        let mut state = self.lock.lock();
        Self::init_state(&mut state, &node);

        if state.can_continue {
            // unconditional parking

            parking_lot::MutexGuard::unlocked(&mut state, || {
                // first put to world parking, then decrease the running threads counter
                node.internal_parking_middle(self.clone());
            });
        } else {
            parking_lot::MutexGuard::unlocked(&mut state, || {
                // conditional parking, decrease the running threads counter without parking
                node.internal_parking_start(self.clone());
            });

            // wait for condition
            while !state.can_continue {
                self.cvar.wait(&mut state);
            }

            if state.panic {
                panic!("thread was crashed by the simulation");
            }

            // condition is met, we are now running instead of the waker thread.
            // the next thing is to park the thread in the world, then decrease
            // the running threads counter
            node.internal_parking_middle(self.clone());
        }

        self.park_wait_the_world(node, &mut state);
    }

    fn park_wait_the_world(&self, node: Arc<Node>, state: &mut parking_lot::MutexGuard<ParkState>) {
        // condition is met, wait for world simulation to wake us up
        while !state.finished {
            self.cvar.wait(state);
        }

        if state.panic {
            panic!("node {} was crashed by the simulation", node.id);
        }

        // We are the only running thread now, we just need to update the state,
        // and continue the execution.
        node.internal_parking_end();
    }

    /// Hacky way to register parking before the thread is actually blocked.
    fn park_ahead_now() -> Arc<Park> {
        let park = Park::new(true);
        let node = Node::current();
        Self::init_state(&mut park.lock.lock(), &node);
        node.internal_parking_ahead(park.clone());
        park
    }

    /// Will wake up the thread that is currently conditionally parked. Can be called only
    /// from the node thread, because it will block the caller thread. What it will do:
    /// 1. Park the thread that called wake() in the world
    /// 2. Wake up the waiting thread (it will also park in the world)
    /// 3. Block the thread that called wake()
    pub fn wake(&self) {
        // parking the thread that called wake()
        let self_park = Park::park_ahead_now();

        let mut state = self.lock.lock();
        if state.can_continue {
            debug!(
                "WARN wake() called on a thread that is already waked, node {:?}",
                state.node_id
            );
        } else {
            state.can_continue = true;
            // and here we park the waiting thread
            self.cvar.notify_all();
        }
        drop(state);

        // and here we block the thread that called wake() by defer
        let node = Node::current();
        let mut state = self_park.lock.lock();
        self_park.park_wait_the_world(node, &mut state);
    }

    /// Will wake up the thread that is currently conditionally parked. Can be called only
    /// from the world threads. What it will do:
    /// 1. Increase the running threads counter
    /// 2. Wake up the waiting thread (it will park itself in the world)
    pub fn external_wake(&self) {
        let world = World::current();

        let mut state = self.lock.lock();
        if state.can_continue {
            debug!(
                "WARN external_wake() called on a thread that is already waked, node {:?}",
                state.node_id
            );
            return;
        }
        world.internal_parking_wake();
        state.can_continue = true;
        // and here we park the waiting thread
        self.cvar.notify_all();
        drop(state);
    }

    /// Will wake up the thread that is currently unconditionally parked.
    pub fn internal_world_wake(&self) {
        let mut state = self.lock.lock();
        if state.finished {
            debug!(
                "WARN internal_world_wake() called on a thread that is already waked, node {:?}",
                state.node_id
            );
            return;
        }
        state.finished = true;
        self.cvar.notify_all();
    }

    /// Will wake up thread to panic instantly.
    pub fn crash_panic(&self) {
        let mut state = self.lock.lock();
        state.can_continue = true;
        state.finished = true;
        state.panic = true;
        self.cvar.notify_all();
        drop(state);
    }

    /// Print debug info about the parked thread.
    pub fn debug_print(&self) {
        // let state = self.lock.lock();
        // debug!("PARK: node {:?} wake1={} wake2={}", state.node_id, state.can_continue, state.finished);
        // debug!("DEBUG: node {:?} wake1={} wake2={}, trace={:?}", state.node_id, state.can_continue, state.finished, state.backtrace);
    }

    /// It feels that this function can cause deadlocks.
    pub fn node_id(&self) -> Option<NodeId> {
        let state = self.lock.lock();
        state.node_id
    }

    /// Yield the current thread to the world simulation.
    pub fn yield_thread() {
        let park = Park::new(true);
        park.park();
    }
}
