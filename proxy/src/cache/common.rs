use std::ops::{Deref, DerefMut};

/// A generic trait which exposes types of cache's key and value,
/// as well as the notion of cache entry invalidation.
/// This is useful for [`Cached`].
#[allow(async_fn_in_trait)]
pub trait Cache {
    /// Entry's key.
    type Key;

    /// Entry's value.
    type Value;

    /// Used for entry invalidation.
    type LookupInfo<Key>;

    /// Invalidate an entry using a lookup info.
    /// We don't have an empty default impl because it's error-prone.
    async fn invalidate(&self, _: &Self::LookupInfo<Self::Key>);
}

impl<C: Cache> Cache for &C {
    type Key = C::Key;
    type Value = C::Value;
    type LookupInfo<Key> = C::LookupInfo<Key>;

    async fn invalidate(&self, info: &Self::LookupInfo<Self::Key>) {
        C::invalidate(self, info).await
    }
}

/// Wrapper for convenient entry invalidation.
pub struct Cached<C: Cache, V = <C as Cache>::Value> {
    /// Cache + lookup info.
    pub token: Option<(C, C::LookupInfo<C::Key>)>,

    /// The value itself.
    pub value: V,
}

impl<C: Cache, V> Cached<C, V> {
    /// Place any entry into this wrapper; invalidation will be a no-op.
    pub fn new_uncached(value: V) -> Self {
        Self { token: None, value }
    }

    pub fn take_value(self) -> (Cached<C, ()>, V) {
        (
            Cached {
                token: self.token,
                value: (),
            },
            self.value,
        )
    }

    /// Drop this entry from a cache if it's still there.
    pub async fn invalidate(self) -> V {
        if let Some((cache, info)) = &self.token {
            cache.invalidate(info).await;
        }
        self.value
    }

    /// Tell if this entry is actually cached.
    pub fn cached(&self) -> bool {
        self.token.is_some()
    }
}

impl<C: Cache, V> Deref for Cached<C, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<C: Cache, V> DerefMut for Cached<C, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<K, V, S> Cache for moka::future::Cache<K, V, S>
where
    K: std::hash::Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: std::hash::BuildHasher + Clone + Send + Sync + 'static,
{
    type Key = K;
    type Value = V;
    type LookupInfo<Key> = Key;

    async fn invalidate(&self, key: &Self::LookupInfo<Self::Key>) {
        moka::future::Cache::invalidate(self, key).await
    }
}
