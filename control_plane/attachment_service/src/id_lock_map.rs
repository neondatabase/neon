use std::{collections::HashMap, sync::Arc};

/// A map of locks covering some arbitrary identifiers. Useful if you have a collection of objects but don't
/// want to embed a lock in each one, or if your locking granularity is different to your object granularity.
/// For example, used in the storage controller where the objects are tenant shards, but sometimes locking
/// is needed at a tenant-wide granularity.
pub(crate) struct IdLockMap<T>
where
    T: Eq + PartialEq + std::hash::Hash,
{
    /// A synchronous lock for getting/setting the async locks that our callers will wait on.
    entities: std::sync::Mutex<std::collections::HashMap<T, Arc<tokio::sync::RwLock<()>>>>,
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
        entry.clone().read_owned()
    }

    pub(crate) fn exclusive(
        &self,
        key: T,
    ) -> impl std::future::Future<Output = tokio::sync::OwnedRwLockWriteGuard<()>> {
        let mut locked = self.entities.lock().unwrap();
        let entry = locked.entry(key).or_default();
        entry.clone().write_owned()
    }

    /// Rather than building a lock guard that re-takes the [`Self::entities`] lock, we just do
    /// periodic housekeeping to avoid the map growing indefinitely
    pub(crate) fn housekeeping(&self) {
        let mut locked = self.entities.lock().unwrap();
        locked.retain(|_k, lock| lock.try_write().is_err())
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
