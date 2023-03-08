use std::{sync::Arc, backtrace::Backtrace, io::{self, Write}};

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
    dbg_signal: u8,
}

impl Park {
    pub fn new() -> Arc<Park> {
        Arc::new(
            Park {
                lock: Mutex::new(ParkState {
                    finished: false,
                    dbg_signal: 0,
                }),
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

            // check if debug info was requested
            if state.dbg_signal != 0 {
                let bt = Backtrace::capture();
                println!("DEBUG: thread {:?} is parked at {:?}", std::thread::current().id(), bt);
                // TODO: fix bad ordering of output
                io::stdout().flush().unwrap();
                state.dbg_signal = 0;
                // trigger a notification to wake up the caller thread
                self.cvar.notify_all();
            }
        }
        // finish parking
    }

    /// Will wake up the thread that is currently parked.
    pub fn wake(&self) {
        let mut state = self.lock.lock();
        state.finished = true;
        self.cvar.notify_all();
    }

    /// Send a signal to the thread that is currently parked to print debug info.
    pub fn debug_print(&self) {
        let mut state = self.lock.lock();
        state.dbg_signal = 1;
        self.cvar.notify_all();

        while !state.dbg_signal == 0 && !state.finished {
            self.cvar.wait(&mut state);
        }
    }
}
