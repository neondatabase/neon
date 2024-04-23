use std::{collections::HashMap, sync::Arc};

use tokio::sync::{OwnedRwLockWriteGuard, RwLock};

/// A wrapper around `OwnedRwLockWriteGuard` that holds the identifier of the operation
/// that is holding the exclusive lock.
pub struct WrappedWriteGuard<T> {
    guard: OwnedRwLockWriteGuard<Option<T>>,
}

impl<T> WrappedWriteGuard<T> {
    pub fn new(guard: OwnedRwLockWriteGuard<Option<T>>) -> Self {
        Self { guard }
    }
}

impl<T> Drop for WrappedWriteGuard<T> {
    fn drop(&mut self) {
        *self.guard = None;
    }
}

/// A map of locks covering some arbitrary identifiers. Useful if you have a collection of objects but don't
/// want to embed a lock in each one, or if your locking granularity is different to your object granularity.
/// For example, used in the storage controller where the objects are tenant shards, but sometimes locking
/// is needed at a tenant-wide granularity.
pub(crate) struct IdLockMap<T>
where
    T: Eq + PartialEq + std::hash::Hash,
{
    /// A synchronous lock for getting/setting the async locks that our callers will wait on.
    entities: std::sync::Mutex<std::collections::HashMap<T, Arc<RwLock<Option<&'static str>>>>>,
}

impl<T> IdLockMap<T>
where
    T: Eq + PartialEq + std::hash::Hash,
{
    pub(crate) fn shared(
        &self,
        key: T,
    ) -> impl std::future::Future<Output = tokio::sync::OwnedRwLockReadGuard<Option<&'static str>>>
    {
        let mut locked = self.entities.lock().unwrap();
        let entry = locked.entry(key).or_default();
        entry.clone().read_owned()
    }

    pub(crate) fn exclusive(
        &self,
        key: T,
        operation: &'static str,
    ) -> impl std::future::Future<Output = WrappedWriteGuard<&'static str>> {
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

impl<T> Default for IdLockMap<T>
where
    T: Eq + PartialEq + std::hash::Hash,
{
    fn default() -> Self {
        Self {
            entities: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IdLockMap;

    #[tokio::test]
    async fn multiple_shared_locks() {
        let id_lock_map = IdLockMap::default();

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
            let _ex_lock = id_lock_map.exclusive(resource_id, "op").await;
            assert_eq!(_ex_lock.guard.unwrap(), "op");

            let _ex_lock_2 = tokio::time::timeout(
                tokio::time::Duration::from_millis(1),
                id_lock_map.exclusive(resource_id, "op_2"),
            )
            .await;
            assert!(_ex_lock_2.is_err());
        }

        let shared_lock_1 = id_lock_map.shared(resource_id).await;
        assert!(shared_lock_1.is_none());
    }
}
