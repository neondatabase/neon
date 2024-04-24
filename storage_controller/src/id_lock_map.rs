use std::fmt::Display;
use std::time::Instant;
use std::{collections::HashMap, sync::Arc};

use std::time::Duration;

const LOCK_TIMEOUT_ALERT_THRESHOLD: Duration = Duration::from_secs(3);

/// A wrapper around `OwnedRwLockWriteGuard` that when dropped changes the
/// current holding operation in lock.
pub struct WrappedWriteGuard<T: Display> {
    guard: tokio::sync::OwnedRwLockWriteGuard<Option<T>>,
    start: Instant,
}

impl<T: Display> WrappedWriteGuard<T> {
    pub fn new(guard: tokio::sync::OwnedRwLockWriteGuard<Option<T>>) -> Self {
        Self {
            guard,
            start: Instant::now(),
        }
    }
}

impl<T: Display> Drop for WrappedWriteGuard<T> {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        if duration > LOCK_TIMEOUT_ALERT_THRESHOLD {
            tracing::warn!(
                "Lock on {} was held for {:?}",
                self.guard.as_ref().unwrap(),
                duration
            );
        }
        *self.guard = None;
    }
}

/// A map of locks covering some arbitrary identifiers. Useful if you have a collection of objects but don't
/// want to embed a lock in each one, or if your locking granularity is different to your object granularity.
/// For example, used in the storage controller where the objects are tenant shards, but sometimes locking
/// is needed at a tenant-wide granularity.
pub(crate) struct IdLockMap<T, I>
where
    T: Eq + PartialEq + std::hash::Hash,
{
    /// A synchronous lock for getting/setting the async locks that our callers will wait on.
    entities: std::sync::Mutex<std::collections::HashMap<T, Arc<tokio::sync::RwLock<Option<I>>>>>,
}

impl<T, I> IdLockMap<T, I>
where
    T: Eq + PartialEq + std::hash::Hash,
    I: Display,
{
    pub(crate) fn shared(
        &self,
        key: T,
    ) -> impl std::future::Future<Output = tokio::sync::OwnedRwLockReadGuard<Option<I>>> {
        let mut locked = self.entities.lock().unwrap();
        let entry = locked.entry(key).or_default();
        entry.clone().read_owned()
    }

    pub(crate) fn exclusive(
        &self,
        key: T,
        operation: I,
    ) -> impl std::future::Future<Output = WrappedWriteGuard<I>> {
        let mut locked = self.entities.lock().unwrap();
        let entry = locked.entry(key).or_default().clone();
        async move {
            let mut guard = WrappedWriteGuard::new(entry.clone().write_owned().await);
            *guard.guard = Some(operation);
            guard
        }
    }

    /// Rather than building a lock guard that re-takes the [`Self::entities`] lock, we just do
    /// periodic housekeeping to avoid the map growing indefinitely
    pub(crate) fn housekeeping(&self) {
        let mut locked = self.entities.lock().unwrap();
        locked.retain(|_k, entry| entry.try_write().is_err())
    }
}

impl<T, I> Default for IdLockMap<T, I>
where
    T: Eq + PartialEq + std::hash::Hash,
{
    fn default() -> Self {
        Self {
            entities: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

pub async fn trace_exclusive_lock<
    T: Clone + Display + Eq + PartialEq + std::hash::Hash,
    I: Display + Clone,
>(
    op_locks: &IdLockMap<T, I>,
    key: T,
    operation: I,
) -> WrappedWriteGuard<I> {
    let start = Instant::now();
    let guard = op_locks.exclusive(key.clone(), operation.clone()).await;

    let duration = start.elapsed();
    if duration > LOCK_TIMEOUT_ALERT_THRESHOLD {
        tracing::warn!(
            "Operation {} on key {} has waited {:?} for exclusive lock",
            operation,
            key,
            duration
        );
    }

    guard
}

pub async fn trace_shared_lock<
    T: Clone + Display + Eq + PartialEq + std::hash::Hash,
    I: Display,
>(
    op_locks: &IdLockMap<T, I>,
    key: T,
    operation: I,
) -> tokio::sync::OwnedRwLockReadGuard<Option<I>> {
    let start = Instant::now();
    let guard = op_locks.shared(key.clone()).await;

    let duration = start.elapsed();
    if duration > LOCK_TIMEOUT_ALERT_THRESHOLD {
        tracing::warn!(
            "Operation {} on key {} has waited {:?} for shared lock",
            operation,
            key,
            duration
        );
    }

    guard
}

#[cfg(test)]
mod tests {
    use super::IdLockMap;

    #[derive(Clone, Debug, strum_macros::Display, PartialEq)]
    enum Operations {
        Op1,
        Op2,
    }

    #[tokio::test]
    async fn multiple_shared_locks() {
        let id_lock_map: IdLockMap<i32, Operations> = IdLockMap::default();

        let shared_lock_1 = id_lock_map.shared(1).await;
        let shared_lock_2 = id_lock_map.shared(1).await;

        assert!(shared_lock_1.is_none());
        assert!(shared_lock_2.is_none());
    }

    #[tokio::test]
    async fn exclusive_locks() {
        let id_lock_map = IdLockMap::default();
        let resource_id = 1;

        {
            let _ex_lock = id_lock_map.exclusive(resource_id, Operations::Op1).await;
            assert_eq!(_ex_lock.guard.clone().unwrap(), Operations::Op1);

            let _ex_lock_2 = tokio::time::timeout(
                tokio::time::Duration::from_millis(1),
                id_lock_map.exclusive(resource_id, Operations::Op2),
            )
            .await;
            assert!(_ex_lock_2.is_err());
        }

        let shared_lock_1 = id_lock_map.shared(resource_id).await;
        assert!(shared_lock_1.is_none());
    }
}
