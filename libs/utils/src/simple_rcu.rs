#![warn(missing_docs)]

use std::ops::Deref;
use std::sync::{Arc, Weak};
use std::sync::RwLock;
use tokio::sync::watch::Sender;

///
/// Rcu allows multiple readers to read and hold onto a value without blocking (for very long).
/// Storing to the Rcu updates the value, making new readers immediately see the new value,
/// but it also waits for all current readers to finish.
///
pub struct Rcu<V>
{
    inner: RwLock<RcuInner<V>>,
}

struct RcuInner<V> {
    current_cell: Arc<RcuCell<V>>,
    old_cells: Vec<Weak<RcuCell<V>>>,
}

struct RcuCell<V>
{
    value: V,

    /// A dummy channel. We never send anything to this channel. The point is that
    /// when the RcuCell is dropped, any Receivers will be notified that the channel
    /// is closed. Updaters can use this to wait out until the RcuCell has been dropped,
    /// i.e. until the old value is no longer in use.
    watch_sender: Sender<()>,
}

impl<V> RcuCell<V> {
    fn new(value: V) -> Self {
        let (watch_sender, _) = tokio::sync::watch::channel(());
        RcuCell {
            value,
            watch_sender,
        }
    }
}

impl<V> Rcu<V>
{
    /// Create a new `Rcu`, initialized to a particular number
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
    /// Read current value. Any store() calls will block, until the returned guard
    /// object is dropped.
    ///
    pub fn read(&self) -> RcuReadGuard<V> {
        let current_cell = Arc::clone(&self.inner.read().unwrap().current_cell);
        RcuReadGuard { cell: current_cell }
    }

    ///
    /// Store a new value. If there are any readers still holding onto old values,
    /// this will await until they have released them.
    ///
    pub async fn store(&self, new_val: V) {
        let new_cell = Arc::new(RcuCell::new(new_val));

        let mut watches = Vec::new();
        {
            let mut inner = self.inner.write().unwrap();
            let old = std::mem::replace(&mut inner.current_cell, new_cell);
            inner.old_cells.push(Arc::downgrade(&old));

            // cleanup and get old cells
            inner.old_cells.retain(|weak| {
                if let Some(cell) = weak.upgrade() {
                    watches.push(cell.watch_sender.subscribe());
                    true
                } else {
                    false
                }
            });
        }

        // after all the old_cells are no longer in use, we're done
        for w in watches.iter_mut() {
            match w.changed().await {
                Ok(()) => panic!("unexpected value received in dummy channel"),
                Err(_recverr) => {
                    // closed, which means that the cell has been dropped, and
                    // its value is no longer in use
                }
            }
        }
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
