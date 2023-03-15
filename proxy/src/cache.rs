use std::{any::Any, sync::Arc, time::Instant};

/// A variant of LRU where every entry has a TTL.
pub mod timed_lru;
pub use timed_lru::TimedLru;

/// Useful type aliases.
pub mod types {
    pub type Cached<T> = super::Cached<'static, T>;
}

/// Lookup information for cache entry invalidation.
#[derive(Clone)]
pub struct LookupInfo<K> {
    /// Cache entry creation time.
    /// We use this during invalidation lookups to prevent eviction of a newer
    /// entry sharing the same key (it might've been inserted by a different
    /// task after we got the entry we're trying to invalidate now).
    created_at: Instant,

    /// Search by this key.
    key: K,
}

/// This type incapsulates everything needed for cache entry invalidation.
/// For convenience, we completely erase the types of a cache ref and a key.
/// This lets us store multiple tokens in a homogeneous collection (e.g. Vec).
#[derive(Clone)]
pub struct InvalidationToken<'a> {
    // TODO: allow more than one type of references (e.g. Arc) if it's ever needed.
    cache: &'a (dyn Cache + Sync + Send),
    info: LookupInfo<Arc<dyn Any + Sync + Send>>,
}

impl InvalidationToken<'_> {
    /// Invalidate a corresponding cache entry.
    pub fn invalidate(&self) {
        let info = LookupInfo {
            created_at: self.info.created_at,
            key: self.info.key.as_ref(),
        };
        self.cache.invalidate_entry(info);
    }
}

/// A combination of a cache entry and its invalidation token.
/// Makes it easier to see how those two are connected.
#[derive(Clone)]
pub struct Cached<'a, T> {
    pub token: Option<InvalidationToken<'a>>,
    pub value: Arc<T>,
}

impl<T> Cached<'_, T> {
    /// Place any entry into this wrapper; invalidation will be a no-op.
    pub fn new_uncached(value: T) -> Self {
        Self {
            token: None,
            value: value.into(),
        }
    }

    /// Invalidate a corresponding cache entry.
    pub fn invalidate(&self) {
        if let Some(token) = &self.token {
            token.invalidate();
        }
    }
}

impl<T> std::ops::Deref for Cached<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

/// This trait captures the notion of cache entry invalidation.
/// It doesn't have any associated types because we use dyn-based type erasure.
trait Cache {
    /// Invalidate an entry using a lookup info.
    /// We don't have an empty default impl because it's error-prone.
    fn invalidate_entry(&self, info: LookupInfo<&(dyn Any + Send + Sync)>);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trivial_properties_of_cached() {
        let cached = Cached::new_uncached(0);
        assert_eq!(*cached, 0);
        cached.invalidate();
    }

    #[test]
    fn invalidation_token_type_erasure() {
        let lifetime = std::time::Duration::from_secs(10);
        let foo = TimedLru::<u32, u32>::new("foo", 128, lifetime);
        let bar = TimedLru::<String, usize>::new("bar", 128, lifetime);

        let (_, x) = foo.insert(100.into(), 0.into());
        let (_, y) = bar.insert(String::new().into(), 404.into());

        // Invalidation tokens should be cloneable and homogeneous (same type).
        let tokens = [x.token.clone().unwrap(), y.token.clone().unwrap()];
        for token in tokens {
            token.invalidate();
        }

        // Values are still there.
        assert_eq!(*x, 0);
        assert_eq!(*y, 404);
    }
}
