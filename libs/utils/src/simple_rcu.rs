//!
//! RCU stands for Read-Copy-Update. It's a synchronization mechanism somewhat
//! similar to a lock, but it allows readers to "hold on" to an old value of RCU
//! without blocking writers, and allows writing a new values without blocking
//! readers. When you update the new value, the new value is immediately visible
//! to new readers, but the update waits until all existing readers have
//! finishe, so that no one sees the old value anymore.
//!
//! This implementation isn't wait-free; it uses an RwLock that is held for a
//! short duration when the value is read or updated.
//!
#![warn(missing_docs)]

use std::ops::Deref;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Weak};
use std::sync::{Mutex, RwLock, RwLockWriteGuard};

///
/// Rcu allows multiple readers to read and hold onto a value without blocking
/// (for very long).  Storing to the Rcu updates the value, making new readers
/// immediately see the new value, but it also waits for all current readers to
/// finish.
///
pub struct Rcu<V> {
    inner: RwLock<RcuInner<V>>,
}

struct RcuInner<V> {
    current_cell: Arc<RcuCell<V>>,
    old_cells: Vec<Weak<RcuCell<V>>>,
}

///
/// RcuCell holds one value. It can be the latest one, or an old one.
///
struct RcuCell<V> {
    value: V,

    /// A dummy channel. We never send anything to this channel. The point is
    /// that when the RcuCell is dropped, any cloned Senders will be notified
    /// that the channel is closed. Updaters can use this to wait out until the
    /// RcuCell has been dropped, i.e. until the old value is no longer in use.
    ///
    /// We never do anything with the receiver, we just need to hold onto it so
    /// that the Senders will be notified when it's dropped. But because it's
    /// not Sync, we need a Mutex on it.
    watch: (SyncSender<()>, Mutex<Receiver<()>>),
}

impl<V> RcuCell<V> {
    fn new(value: V) -> Self {
        let (watch_sender, watch_receiver) = sync_channel(0);
        RcuCell {
            value,
            watch: (watch_sender, Mutex::new(watch_receiver)),
        }
    }
}

impl<V> Rcu<V> {
    /// Create a new `Rcu`, initialized to `starting_val`
    pub fn new(starting_val: V) -> Self {
        let inner = RcuInner {
            current_cell: Arc::new(RcuCell::new(starting_val)),
            old_cells: Vec::new(),
        };
        Self {
            inner: RwLock::new(inner),
        }
    }

    ///
    /// Read current value. Any store() calls will block until the returned
    /// guard object is dropped.
    ///
    pub fn read(&self) -> RcuReadGuard<V> {
        let current_cell = Arc::clone(&self.inner.read().unwrap().current_cell);
        RcuReadGuard { cell: current_cell }
    }

    ///
    /// Lock the current value for updating. Returns a guard object that can be
    /// used to read the current value, and to store a new value.
    ///
    /// Note: holding the write-guard blocks concurrent readers, so you should
    /// finish the update and drop the guard quickly!
    ///
    pub fn write(&self) -> RcuWriteGuard<'_, V> {
        let inner = self.inner.write().unwrap();
        RcuWriteGuard { inner }
    }
}

///
/// Read guard returned by `read`
///
pub struct RcuReadGuard<V> {
    cell: Arc<RcuCell<V>>,
}

impl<V> Deref for RcuReadGuard<V> {
    type Target = V;

    fn deref(&self) -> &V {
        &self.cell.value
    }
}

///
/// Read guard returned by `read`
///
pub struct RcuWriteGuard<'a, V> {
    inner: RwLockWriteGuard<'a, RcuInner<V>>,
}

impl<'a, V> Deref for RcuWriteGuard<'a, V> {
    type Target = V;

    fn deref(&self) -> &V {
        &self.inner.current_cell.value
    }
}

impl<'a, V> RcuWriteGuard<'a, V> {
    ///
    /// Store a new value. The new value will be written to the Rcu immediately,
    /// and will be immediately seen by any `read` calls that start afterwards.
    /// But if there are any readers still holding onto the old value, or any
    /// even older values, this will await until they have been released.
    ///
    /// This will drop the write-guard before it starts waiting for the reads to
    /// finish, so a new write operation can begin before this functio returns.
    ///
    pub fn store(mut self, new_val: V) {
        let new_cell = Arc::new(RcuCell::new(new_val));

        let mut watches = Vec::new();
        {
            let old = std::mem::replace(&mut self.inner.current_cell, new_cell);
            self.inner.old_cells.push(Arc::downgrade(&old));

            // cleanup old cells that no longer have any readers, and collect
            // the watches for any that do.
            self.inner.old_cells.retain(|weak| {
                if let Some(cell) = weak.upgrade() {
                    watches.push(cell.watch.0.clone());
                    true
                } else {
                    false
                }
            });
        }
        drop(self);

        // after all the old_cells are no longer in use, we're done
        for w in watches.iter_mut() {
            // This will block until the Receiver is closed. That happens then
            // the RcuCell is dropped.
            #[allow(clippy::single_match)]
            match w.send(()) {
                Ok(_) => panic!("send() unexpectedly succeeded on dummy channel"),
                Err(_) => {
                    // closed, which means that the cell has been dropped, and
                    // its value is no longer in use
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread::{sleep, spawn};
    use std::time::Duration;

    #[test]
    fn basic() {
        let rcu = Arc::new(Rcu::new(1));
        let log = Arc::new(Mutex::new(Vec::new()));

        let a = rcu.read();
        assert_eq!(*a, 1);
        log.lock().unwrap().push("one");

        let (rcu_clone, log_clone) = (Arc::clone(&rcu), Arc::clone(&log));
        let thread = spawn(move || {
            log_clone.lock().unwrap().push("store two start");
            let write_guard = rcu_clone.write();
            assert_eq!(*write_guard, 1);
            write_guard.store(2);
            log_clone.lock().unwrap().push("store two done");
        });
        // without this sleep the test can pass on accident if the writer is slow
        sleep(Duration::from_secs(1));

        // new read should see the new value
        let b = rcu.read();
        assert_eq!(*b, 2);

        // old guard still sees the old value
        assert_eq!(*a, 1);

        // Release the old guard. This lets the store in the thread to finish.
        log.lock().unwrap().push("release a");
        drop(a);

        thread.join().unwrap();

        assert_eq!(
            log.lock().unwrap().as_slice(),
            &["one", "store two start", "release a", "store two done",]
        );
    }
}
