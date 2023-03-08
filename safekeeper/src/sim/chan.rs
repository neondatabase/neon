use std::{collections::VecDeque, sync::Arc};

use super::sync::{Mutex, Condvar};

/// FIFO channel with blocking send and receive. Can be cloned and shared between threads.
#[derive(Clone)]
pub struct Chan<T: Clone> {
    shared: Arc<ChanState<T>>,
}

struct ChanState<T> {
    queue: Mutex<VecDeque<T>>,
    condvar: Condvar,
}

impl<T: Clone> Chan<T> {
    pub fn new() -> Chan<T> {
        Chan{
            shared: Arc::new(ChanState{
                queue: Mutex::new(VecDeque::new()),
                condvar: Condvar::new(),
            })
        }
    }

    /// Append a message to the end of the queue.
    pub fn send(&self, t: T) {
        self.shared.queue.lock().push_back(t);
        self.shared.condvar.notify_one();
    }

    /// Get a message from the front of the queue, or block if the queue is empty.
    pub fn recv(&self) -> T {
        let mut queue = self.shared.queue.lock();
        loop {
            if let Some(t) = queue.pop_front() {
                return t;
            }
            self.shared.condvar.wait(&mut queue);
        }
    }
}
