use std::{collections::VecDeque, sync::Arc};

use parking_lot::{Mutex, MutexGuard};

use crate::executor::{self, PollSome, Waker};

/// FIFO channel with blocking send and receive. Can be cloned and shared between threads.
/// Blocking functions should be used only from threads that are managed by the executor.
pub struct Chan<T> {
    shared: Arc<State<T>>,
}

impl<T> Clone for Chan<T> {
    fn clone(&self) -> Self {
        Chan {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Default for Chan<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Chan<T> {
    pub fn new() -> Chan<T> {
        Chan {
            shared: Arc::new(State {
                queue: Mutex::new(VecDeque::new()),
                waker: Waker::new(),
            }),
        }
    }

    /// Get a message from the front of the queue, block if the queue is empty.
    /// If not called from the executor thread, it can block forever.
    pub fn recv(&self) -> T {
        self.shared.recv()
    }

    /// Panic if the queue is empty.
    pub fn must_recv(&self) -> T {
        self.shared
            .try_recv()
            .expect("message should've been ready")
    }

    /// Get a message from the front of the queue, return None if the queue is empty.
    /// Never blocks.
    pub fn try_recv(&self) -> Option<T> {
        self.shared.try_recv()
    }

    /// Send a message to the back of the queue.
    pub fn send(&self, t: T) {
        self.shared.send(t);
    }
}

struct State<T> {
    queue: Mutex<VecDeque<T>>,
    waker: Waker,
}

impl<T> State<T> {
    fn send(&self, t: T) {
        self.queue.lock().push_back(t);
        self.waker.wake_all();
    }

    fn try_recv(&self) -> Option<T> {
        let mut q = self.queue.lock();
        q.pop_front()
    }

    fn recv(&self) -> T {
        // interrupt the receiver to prevent consuming everything at once
        executor::yield_me(0);

        let mut queue = self.queue.lock();
        if let Some(t) = queue.pop_front() {
            return t;
        }
        loop {
            self.waker.wake_me_later();
            if let Some(t) = queue.pop_front() {
                return t;
            }
            MutexGuard::unlocked(&mut queue, || {
                executor::yield_me(-1);
            });
        }
    }
}

impl<T> PollSome for Chan<T> {
    /// Schedules a wakeup for the current thread.
    fn wake_me(&self) {
        self.shared.waker.wake_me_later();
    }

    /// Checks if chan has any pending messages.
    fn has_some(&self) -> bool {
        !self.shared.queue.lock().is_empty()
    }
}
