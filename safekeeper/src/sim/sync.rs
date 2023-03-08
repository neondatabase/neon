use std::sync::Arc;

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
        let park = Park::new();

        // add the waiter to the list
        self.waiters.lock().waiters.push(park.clone());

        parking_lot::MutexGuard::unlocked(guard, || {
            // part the thread, it will be woken up by notify_one or notify_all
            park.park();
        });
    }

    /// Wakes up all blocked threads on this condvar.
    pub fn notify_all(&self) {
        // TODO: wake up in random order, yield to the scheduler

        let mut state = self.waiters.lock();
        for waiter in state.waiters.drain(..) {
            waiter.wake();
        }
    }

    /// Wakes up one blocked thread on this condvar.
    pub fn notify_one(&self) {
        // TODO: wake up random thread, yield to the scheduler

        let mut state = self.waiters.lock();
        if let Some(waiter) = state.waiters.pop() {
            waiter.wake();
        }
    }
}

/// A tool to block (park) a current thread until it will be woken up.
pub struct Park {
    lock: Mutex<ParkState>,
    cvar: parking_lot::Condvar,
}

struct ParkState {
    finished: bool,
}

impl Park {
    pub fn new() -> Arc<Park> {
        Arc::new(
            Park {
                lock: Mutex::new(ParkState { finished: false }),
                cvar: parking_lot::Condvar::new(),
            }
        )
    }

    /// Should be called once by the waiting thread. Blocks the thread until wake() is called.
    pub fn park(&self) {
        // TODO: update state of the current thread in the world struct

        // start parking
        let mut state = self.lock.lock();
        while !state.finished {
            self.cvar.wait(&mut state);
        }
        // finish parking
    }

    /// Will wake up the thread that is currently parked.
    pub fn wake(&self) {
        let mut state = self.lock.lock();
        state.finished = true;
        self.cvar.notify_all();
    }
}
