//!
//! RCU stands for Read-Copy-Update. It's a synchronization mechanism somewhat
//! similar to a lock, but it allows readers to "hold on" to an old value of RCU
//! without blocking writers, and allows writing a new value without blocking
//! readers. When you update the value, the new value is immediately visible
//! to new readers, but the update waits until all existing readers have
//! finished, so that on return, no one sees the old value anymore.
//!
//! This implementation isn't wait-free; it uses an RwLock that is held for a
//! short duration when the value is read or updated.
//!
//! # Examples
//!
//! Read a value and do things with it while holding the guard:
//!
//! ```
//! # let rcu = utils::simple_rcu::Rcu::new(1);
//! {
//!     let read = rcu.read();
//!     println!("the current value is {}", *read);
//!     // exiting the scope drops the read-guard, and allows concurrent writers
//!     // to finish.
//! }
//! ```
//!
//! Increment the value by one, and wait for old readers to finish:
//!
//! ```
//! # async fn dox() {
//! # let rcu = utils::simple_rcu::Rcu::new(1);
//! let write_guard = rcu.lock_for_write();
//!
//! // NB: holding `write_guard` blocks new readers and writers. Keep this section short!
//! let new_value = *write_guard + 1;
//!
//! let waitlist = write_guard.store_and_unlock(new_value); // consumes `write_guard`
//!
//! // Concurrent reads and writes are now possible again. Wait for all the readers
//! // that still observe the old value to finish.
//! waitlist.wait().await;
//! # }
//! ```
//!
#![warn(missing_docs)]

use std::ops::Deref;
use std::sync::{Arc, Weak};
use std::sync::{RwLock, RwLockWriteGuard};

use tokio::sync::watch;

/// Rcu allows multiple readers to read and hold onto a value without blocking
/// (for very long).
///
/// Storing to the Rcu updates the value, making new readers immediately see
/// the new value, but it also waits for all current readers to finish.
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
    /// that when the RcuCell is dropped, any subscribed Receivers will be notified
    /// that the channel is closed. Updaters can use this to wait out until the
    /// RcuCell has been dropped, i.e. until the old value is no longer in use.
    ///
    /// We never send anything to this, we just need to hold onto it so that the
    /// Receivers will be notified when it's dropped.
    watch: watch::Sender<()>,
}

impl<V> RcuCell<V> {
    fn new(value: V) -> Self {
        let (watch_sender, _) = watch::channel(());
        RcuCell {
            value,
            watch: watch_sender,
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
    /// finish the update and drop the guard quickly! Multiple writers can be
    /// waiting on the RcuWriteGuard::store step at the same time, however.
    ///
    pub fn lock_for_write(&self) -> RcuWriteGuard<'_, V> {
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
/// Write guard returned by `write`
///
/// NB: Holding this guard blocks all concurrent `read` and `write` calls, so it should only be
/// held for a short duration!
///
/// Calling [`Self::store_and_unlock`] consumes the guard, making new reads and new writes possible
/// again.
///
pub struct RcuWriteGuard<'a, V> {
    inner: RwLockWriteGuard<'a, RcuInner<V>>,
}

impl<V> Deref for RcuWriteGuard<'_, V> {
    type Target = V;

    fn deref(&self) -> &V {
        &self.inner.current_cell.value
    }
}

impl<V> RcuWriteGuard<'_, V> {
    ///
    /// Store a new value. The new value will be written to the Rcu immediately,
    /// and will be immediately seen by any `read` calls that start afterwards.
    ///
    /// Returns a list of readers that can see old values. You can call `wait()`
    /// on it to wait for them to finish.
    ///
    pub fn store_and_unlock(mut self, new_val: V) -> RcuWaitList {
        let new_cell = Arc::new(RcuCell::new(new_val));

        let mut watches = Vec::new();
        {
            let old = std::mem::replace(&mut self.inner.current_cell, new_cell);
            self.inner.old_cells.push(Arc::downgrade(&old));

            // cleanup old cells that no longer have any readers, and collect
            // the watches for any that do.
            self.inner.old_cells.retain(|weak| {
                if let Some(cell) = weak.upgrade() {
                    watches.push(cell.watch.subscribe());
                    true
                } else {
                    false
                }
            });
        }
        RcuWaitList(watches)
    }
}

///
/// List of readers who can still see old values.
///
pub struct RcuWaitList(Vec<watch::Receiver<()>>);

impl RcuWaitList {
    ///
    /// Wait for old readers to finish.
    ///
    pub async fn wait(mut self) {
        // after all the old_cells are no longer in use, we're done
        for w in self.0.iter_mut() {
            // This will block until the Receiver is closed. That happens when
            // the RcuCell is dropped.
            #[allow(clippy::single_match)]
            match w.changed().await {
                Ok(_) => panic!("changed() unexpectedly succeeded on dummy channel"),
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
    use std::sync::Mutex;
    use std::time::Duration;

    #[tokio::test]
    async fn two_writers() {
        let rcu = Rcu::new(1);

        let read1 = rcu.read();
        assert_eq!(*read1, 1);

        let write2 = rcu.lock_for_write();
        assert_eq!(*write2, 1);
        let wait2 = write2.store_and_unlock(2);

        let read2 = rcu.read();
        assert_eq!(*read2, 2);

        let write3 = rcu.lock_for_write();
        assert_eq!(*write3, 2);
        let wait3 = write3.store_and_unlock(3);

        // new reader can see the new value, and old readers continue to see the old values.
        let read3 = rcu.read();
        assert_eq!(*read3, 3);
        assert_eq!(*read2, 2);
        assert_eq!(*read1, 1);

        let log = Arc::new(Mutex::new(Vec::new()));
        // Wait for the old readers to finish in separate tasks.
        let log_clone = Arc::clone(&log);
        let task2 = tokio::spawn(async move {
            wait2.wait().await;
            log_clone.lock().unwrap().push("wait2 done");
        });
        let log_clone = Arc::clone(&log);
        let task3 = tokio::spawn(async move {
            wait3.wait().await;
            log_clone.lock().unwrap().push("wait3 done");
        });

        // without this sleep the test can pass on accident if the writer is slow
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Release first reader. This allows first write to finish, but calling
        // wait() on the 'task3' would still block.
        log.lock().unwrap().push("dropping read1");
        drop(read1);
        task2.await.unwrap();

        assert!(!task3.is_finished());

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Release second reader, and finish second writer.
        log.lock().unwrap().push("dropping read2");
        drop(read2);
        task3.await.unwrap();

        assert_eq!(
            log.lock().unwrap().as_slice(),
            &[
                "dropping read1",
                "wait2 done",
                "dropping read2",
                "wait3 done"
            ]
        );
    }
}
