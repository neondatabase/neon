use std::{collections::VecDeque, sync::Arc};

use parking_lot::{Mutex, MutexGuard};

use crate::executor::{Waker, self, PollSome};

// use super::sync::{Condvar, Mutex, Park};

/// FIFO channel with blocking send and receive. Can be cloned and shared between threads.
pub struct Chan<T> {
    // shared: Arc<ChanState<T>>,
    shared: Arc<Chan2State<T>>,
}

impl<T> Clone for Chan<T> {
    fn clone(&self) -> Self {
        Chan {
            shared: self.shared.clone(),
        }
    }
}

// struct ChanState<T> {
//     queue: Mutex<VecDeque<T>>,
//     waker: Waker,
// }

// impl<T: Clone> Default for Chan<T> {
//     fn default() -> Self {
//         Self::new()
//     }
// }

// impl<T: Clone> Chan<T> {
//     pub fn new() -> Chan<T> {
//         Chan {
//             shared: Arc::new(ChanState {
//                 queue: Mutex::new(VecDeque::new()),
//                 waker: Waker::new(),
//             }),
//         }
//     }

//     /// Append a message to the end of the queue.
//     /// Can be called from any thread.
//     pub fn send(&self, t: T) {
//         self.shared.queue.lock().push_back(t);
//         self.shared.waker.wake_all();
//     }

//     /// Same as `recv`, but doesn't take the message from the queue.
//     pub fn peek(&self) -> T {
//         // interrupt the receiver to prevent consuming everything at once
//         Park::yield_thread();

//         let mut queue = self.shared.queue.lock();
//         loop {
//             if let Some(t) = queue.front().cloned() {
//                 return t;
//             }
//             self.shared.condvar.wait(&mut queue);
//         }
//     }

//     /// Get a message from the front of the queue, or return `None` if the queue is empty.
//     pub fn try_recv(&self) -> Option<T> {
//         let mut queue = self.shared.queue.lock();
//         queue.pop_front()
//     }

//     /// Clone a message from the front of the queue, or return `None` if the queue is empty.
//     pub fn try_peek(&self) -> Option<T> {
//         let queue = self.shared.queue.lock();
//         queue.front().cloned()
//     }

//     pub fn clear(&self) {
//         let mut queue = self.shared.queue.lock();
//         queue.clear();
//     }
// }

impl<T> Chan<T> {
    pub fn new() -> Chan<T> {
        Chan {
            shared: Arc::new(Chan2State {
                queue: Mutex::new(VecDeque::new()),
                waker: Waker::new(),
            }),
        }
    }

    pub fn recv(&self) -> T {
        self.shared.recv()
    }

    pub fn must_recv(&self) -> T {
        self.shared.try_recv().expect("message should've been ready")
    }

    pub fn send(&self, t: T) {
        self.shared.send(t);
    }
}

pub struct Chan2State<T> {
    queue: Mutex<VecDeque<T>>,
    waker: Waker,
}

impl<T> Chan2State<T> {
    pub fn send(&self, t: T) {
        self.queue.lock().push_back(t);
        self.waker.wake_all();
    }

    pub fn try_recv(&self) -> Option<T> {
        let mut q = self.queue.lock();
        q.pop_front()
    }

    /// Get a message from the front of the queue, block if the queue is empty.
    /// Can be called only from the node thread.
    pub fn recv(&self) -> T {
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
