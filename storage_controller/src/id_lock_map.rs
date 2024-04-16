use std::{collections::HashMap, sync::Arc};

use tokio::sync::OwnedRwLockWriteGuard;

/// A wrapper around `OwnedRwLockWriteGuard` that holds the identifier of the operation
/// that is holding the exclusive lock.
pub struct WrappedWriteGuard<T> {
    _inner: OwnedRwLockWriteGuard<T>,
    operation: Arc<std::sync::RwLock<Option<&'static str>>>,
}

impl<T> Drop for WrappedWriteGuard<T> {
    fn drop(&mut self) {
        let mut guard = self.operation.write().unwrap();
        *guard = None;
    }
}

#[derive(Default, Clone)]
struct TrackedOperationLock {
    lock: Arc<tokio::sync::RwLock<()>>,
    operation: Arc<std::sync::RwLock<Option<&'static str>>>,
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
    entities: std::sync::Mutex<std::collections::HashMap<T, TrackedOperationLock>>,
}

impl<T> IdLockMap<T>
where
    T: Eq + PartialEq + std::hash::Hash,
{
    pub(crate) fn shared(
        &self,
        key: T,
    ) -> impl std::future::Future<Output = tokio::sync::OwnedRwLockReadGuard<()>> {
        let mut locked = self.entities.lock().unwrap();
        let entry = locked.entry(key).or_default();
        entry.clone().lock.read_owned()
    }

    pub(crate) fn exclusive(
        &self,
        key: T,
        operation: &'static str,
    ) -> impl std::future::Future<Output = WrappedWriteGuard<()>> {
        let mut locked = self.entities.lock().unwrap();
        let entry = locked.entry(key).or_default().clone();
        *entry.operation.write().unwrap() = Some(operation);
        async move {
            WrappedWriteGuard {
                _inner: entry.lock.clone().write_owned().await,
                operation: entry.operation.clone(),
            }
        }
    }

    pub(crate) fn get_operation(&self, key: T) -> Option<&str> {
        let mut locked = self.entities.lock().unwrap();
        let entry = locked.entry(key).or_default();
        *entry.clone().operation.read().unwrap()
    }

    /// Rather than building a lock guard that re-takes the [`Self::entities`] lock, we just do
    /// periodic housekeeping to avoid the map growing indefinitely
    pub(crate) fn housekeeping(&self) {
        let mut locked = self.entities.lock().unwrap();
        locked.retain(|_k, entry| entry.lock.try_write().is_err())
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

        assert_eq!(id_lock_map.get_operation(1), None);
        id_lock_map.shared(1).await;
        id_lock_map.shared(1).await;
        assert_eq!(id_lock_map.get_operation(1), None);
    }

    #[tokio::test]
    async fn single_exclusive() {
        let id_lock_map = IdLockMap::default();

        {
            assert_eq!(id_lock_map.get_operation(1), None);
            let _ex_lock = id_lock_map.exclusive(1, "op").await;
            assert_eq!(id_lock_map.get_operation(1), Some("op"));
        }
        assert_eq!(id_lock_map.get_operation(1), None);
    }
}
