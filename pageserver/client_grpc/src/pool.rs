use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use std::future::Future;
use std::pin::Pin;
use tokio::sync::{Semaphore, SemaphorePermit};

/// Constructs new pool items.
/// TODO: use a proper error type.
type Maker<T> = Box<dyn Fn() -> Pin<Box<dyn Future<Output = anyhow::Result<T>>>> + Send + Sync>;

/// A resource pool. This is used to manage gRPC channels, clients, and stream.
///
/// An item is only handed out to a single user at a time. New items will be created up to the pool
/// limit, if specified.
pub struct Pool<T: PooledItem> {
    /// Creates new pool items.
    maker: Maker<T>,
    /// Idle items in the pool. Returned items are pushed to the front of the queue, so that the
    /// oldest idle items are kept at the back.
    ///
    /// TODO: reap idle items after some time.
    /// TODO: consider prewarming items.
    idle: Arc<Mutex<VecDeque<T>>>,
    /// Limits the max number of items managed by the pool.
    limiter: Semaphore,
}

impl<T: PooledItem> Pool<T> {
    /// Create a new pool with the specified limit.
    pub fn new(maker: Maker<T>, limit: Option<usize>) -> Self {
        Self {
            maker,
            idle: Default::default(),
            limiter: Semaphore::new(limit.unwrap_or(Semaphore::MAX_PERMITS)),
        }
    }

    /// Gets an item from the pool, or creates a new one if necessary. Blocks if the pool is at its
    /// limit. The item is returned to the pool when the guard is dropped.
    pub async fn get(&mut self) -> anyhow::Result<PoolGuard<T>> {
        let permit = self.limiter.acquire().await.expect("never closed");

        // Acquire an idle item from the pool, or create a new one.
        let item = self.idle.lock().unwrap().pop_front();
        let item = match item {
            Some(item) => item,
            // TODO: if an item is returned while we're waiting, use the returned item instead.
            None => (self.maker)().await?,
        };

        Ok(PoolGuard {
            pool: self,
            permit,
            item: Some(item),
        })
    }
}

/// A guard for a pooled item.
pub struct PoolGuard<'a, T: PooledItem> {
    pool: &'a Pool<T>,
    permit: SemaphorePermit<'a>,
    item: Option<T>, // only None during drop
}

impl<T: PooledItem> Deref for PoolGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.item.as_ref().expect("not dropped")
    }
}

impl<T: PooledItem> DerefMut for PoolGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.item.as_mut().expect("not dropped")
    }
}

impl<T: PooledItem> Drop for PoolGuard<'_, T> {
    fn drop(&mut self) {
        // Return the item to the pool.
        self.pool
            .idle
            .lock()
            .unwrap()
            .push_front(self.item.take().expect("only dropped once"));
        // The permit will be returned by its drop handler. Tag it here for visibility.
        _ = self.permit;
    }
}

/// A pooled item.
///
/// TODO: do we even need this?
pub trait PooledItem {}
