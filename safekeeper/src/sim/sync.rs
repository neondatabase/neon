use std::{sync::Arc, backtrace::Backtrace, io::{self, Write}};

use super::world::{Node, NodeId};

pub type Mutex<T> = parking_lot::Mutex<T>;

/// More deterministic condvar.
pub struct Condvar {
    waiters: Mutex<CondvarState>,
}

struct CondvarState {
    waiters: Vec<Arc<Park>>,
}

impl Condvar {
    pub fn new() -> Condvar {
        Condvar {
            waiters: Mutex::new(CondvarState { waiters: Vec::new() }),
        }
    }

    /// Blocks the current thread until this condition variable receives a notification.
    pub fn wait<'a, T>(&self, guard: &mut parking_lot::MutexGuard<'a, T>) {
        let park = Park::new(false);

        // add the waiter to the list
        self.waiters.lock().waiters.push(park.clone());

        parking_lot::MutexGuard::unlocked(guard, || {
            // part the thread, it will be woken up by notify_one or notify_all
            park.park();
        });
    }

    /// Wakes up all blocked threads on this condvar.
    pub fn notify_all(&self) {
        // TODO: check that it's waked up in random order and yield to the scheduler

        let mut state = self.waiters.lock();
        for waiter in state.waiters.drain(..) {
            waiter.wake();
        }
        drop(state);

        // yield the current thread to the scheduler
        Park::yield_thread();
    }

    /// Wakes up one blocked thread on this condvar.
    pub fn notify_one(&self) {
        // TODO: wake up random thread

        let mut state = self.waiters.lock();
        if let Some(waiter) = state.waiters.pop() {
            waiter.wake();
        }
        drop(state);

        // yield the current thread to the scheduler
        Park::yield_thread();
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
    node_id: Option<NodeId>,
    backtrace: Option<Backtrace>,
}

impl Park {
    pub fn new(can_continue: bool) -> Arc<Park> {
        Arc::new(
            Park {
                lock: Mutex::new(ParkState {
                    can_continue,
                    finished: false,
                    node_id: None,
                    backtrace: None,
                }),
                cvar: parking_lot::Condvar::new(),
            }
        )
    }

    /// Should be called once by the waiting thread. Blocks the thread until wake() is called,
    /// and until the thread is woken up by the world simulation.
    pub fn park(self: &Arc<Self>) {
        let node = Node::current();

        // start parking
        let mut state = self.lock.lock();
        state.node_id = Some(node.id);
        state.backtrace = Some(Backtrace::capture());

        println!("PARKING STARTED: node {:?}", node.id);

        parking_lot::MutexGuard::unlocked(&mut state, || {
            node.internal_parking_start();
        });

        scopeguard::defer! {
            node.internal_parking_end();
        };

        // wait for condition
        while !state.can_continue {
            self.cvar.wait(&mut state);
        }

        println!("PARKING MIDDLE: node {:?}", node.id);

        // condition is met, wait for world simulation to wake us up
        node.internal_parking_middle(self.clone());

        while !state.finished {
            self.cvar.wait(&mut state);
        }

        println!("PARKING ENDED: node {:?}", node.id);

        // finish parking
    }

    /// Will wake up the thread that is currently conditionally parked.
    pub fn wake(&self) {
        let mut state = self.lock.lock();
        if state.can_continue {
            println!("WARN wake() called on a thread that is already waked, node {:?}", state.node_id);
            return;
        }
        state.can_continue = true;
        self.cvar.notify_all();
    }

    /// Will wake up the thread that is currently unconditionally parked.
    pub fn internal_world_wake(&self) {
        let mut state = self.lock.lock();
        if state.finished {
            println!("WARN internal_world_wake() called on a thread that is already waked, node {:?}", state.node_id);
            return;
        }
        state.finished = true;
        self.cvar.notify_all();
    }

    /// Print debug info about the parked thread.
    pub fn debug_print(&self) {
        let mut state = self.lock.lock();
        println!("DEBUG: node {:?} wake1={} wake2={}, trace={:?}", state.node_id, state.can_continue, state.finished, state.backtrace);
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
