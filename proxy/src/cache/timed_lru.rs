#[cfg(test)]
mod tests;

use super::{Cache, Cached, InvalidationToken, LookupInfo};
use ref_cast::RefCast;
use std::{
    any::Any,
    borrow::Borrow,
    hash::Hash,
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::debug;

// This seems to make more sense than `lru` or `cached`:
//
// * `near/nearcore` ditched `cached` in favor of `lru`
//   (https://github.com/near/nearcore/issues?q=is%3Aissue+lru+is%3Aclosed).
//
// * `lru` methods use an obscure `KeyRef` type in their contraints (which is deliberately excluded from docs).
//   This severely hinders its usage both in terms of creating wrappers and supported key types.
//
// On the other hand, `hashlink` has good download stats and appears to be maintained.
use hashlink::{linked_hash_map::RawEntryMut, LruCache};

/// An implementation of timed LRU cache with fixed capacity.
/// Key properties:
///
/// * Whenever a new entry is inserted, the least recently accessed one is evicted.
///   The cache also keeps track of entry's insertion time (`created_at`) and TTL (`expires_at`).
///
/// * When the entry is about to be retrieved, we check its expiration timestamp.
///   If the entry has expired, we remove it from the cache; Otherwise we bump the
///   expiration timestamp (e.g. +5mins) and change its place in LRU list to prolong
///   its existence.
///
/// * There's an API for immediate invalidation (removal) of a cache entry;
///   It's useful in case we know for sure that the entry is no longer correct.
///   See [`timed_lru::LookupInfo`] & [`timed_lru::Cached`] for more information.
///
/// * Expired entries are kept in the cache, until they are evicted by the LRU policy,
///   or by a successful lookup (i.e. the entry hasn't expired yet).
///   There is no background job to reap the expired records.
///
/// * It's possible for an entry that has not yet expired entry to be evicted
///   before expired items. That's a bit wasteful, but probably fine in practice.
pub struct TimedLru<K, V> {
    /// Cache's name for tracing.
    name: &'static str,

    /// The underlying cache implementation.
    cache: parking_lot::Mutex<LruCache<Key<K>, Entry<V>>>,

    /// Default time-to-live of a single entry.
    ttl: Duration,
}

#[derive(RefCast, Hash, PartialEq, Eq)]
#[repr(transparent)]
struct Query<Q: ?Sized>(Q);

#[derive(Hash, PartialEq, Eq)]
#[repr(transparent)]
struct Key<T: ?Sized>(Arc<T>);

/// It's impossible to implement this without [`Key`] & [`Query`]:
/// * We can't implement std traits for [`Arc`].
/// * Even if we could, it'd conflict with `impl<T> Borrow<T> for T`.
impl<Q, T> Borrow<Query<Q>> for Key<T>
where
    Q: ?Sized,
    T: Borrow<Q>,
{
    #[inline(always)]
    fn borrow(&self) -> &Query<Q> {
        RefCast::ref_cast(self.0.as_ref().borrow())
    }
}

struct Entry<T> {
    created_at: Instant,
    expires_at: Instant,
    value: Arc<T>,
}

impl<K: Hash + Eq, V> TimedLru<K, V> {
    /// Construct a new LRU cache with timed entries.
    pub fn new(name: &'static str, capacity: usize, ttl: Duration) -> Self {
        Self {
            name,
            cache: LruCache::new(capacity).into(),
            ttl,
        }
    }

    /// Get the number of entries in the cache.
    /// Note that this method will not try to evict stale entries.
    pub fn size(&self) -> usize {
        self.cache.lock().len()
    }

    /// Try retrieving an entry by its key, then execute `extract` if it exists.
    #[tracing::instrument(level = "debug", fields(cache = self.name), skip_all)]
    fn get_raw<Q, F, R>(&self, key: &Q, extract: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        F: FnOnce(&Arc<K>, &Entry<V>) -> R,
    {
        let key: &Query<Q> = RefCast::ref_cast(key);
        let now = Instant::now();
        let deadline = now.checked_add(self.ttl).expect("time overflow");

        // Do costly things before taking the lock.
        let mut cache = self.cache.lock();
        let mut raw_entry = match cache.raw_entry_mut().from_key(key) {
            RawEntryMut::Vacant(_) => return None,
            RawEntryMut::Occupied(x) => x,
        };

        // Immeditely drop the entry if it has expired.
        let entry = raw_entry.get();
        if entry.expires_at <= now {
            raw_entry.remove();
            return None;
        }

        let value = extract(&raw_entry.key().0, entry);
        let (created_at, expires_at) = (entry.created_at, entry.expires_at);

        // Update the deadline and the entry's position in the LRU list.
        raw_entry.get_mut().expires_at = deadline;
        raw_entry.to_back();

        drop(cache); // drop lock before logging
        debug!(
            ?created_at,
            old_expires_at = ?expires_at,
            new_expires_at = ?deadline,
            "accessed a cache entry"
        );

        Some(value)
    }

    /// Insert an entry to the cache. If an entry with the same key already
    /// existed, return the previous value and its creation timestamp.
    #[tracing::instrument(level = "debug", fields(cache = self.name), skip_all)]
    fn insert_raw(&self, key: Arc<K>, value: Arc<V>) -> (Option<Arc<V>>, Instant) {
        let created_at = Instant::now();
        let expires_at = created_at.checked_add(self.ttl).expect("time overflow");

        let entry = Entry {
            created_at,
            expires_at,
            value,
        };

        // Do costly things before taking the lock.
        let old = self
            .cache
            .lock()
            .insert(Key(key), entry)
            .map(|entry| entry.value);

        debug!(
            ?created_at,
            ?expires_at,
            replaced = old.is_some(),
            "created a cache entry"
        );

        (old, created_at)
    }
}

/// Convenient wrappers for raw methods.
impl<K: Hash + Eq + Sync + Send + 'static, V> TimedLru<K, V>
where
    Self: Sync + Send,
{
    pub fn get<Q>(&self, key: &Q) -> Option<Cached<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get_raw(key, |key, entry| {
            let info = LookupInfo {
                created_at: entry.created_at,
                key: Arc::clone(key),
            };

            Cached {
                token: Some(Self::invalidation_token(self, info)),
                value: entry.value.clone(),
            }
        })
    }

    pub fn insert(&self, key: Arc<K>, value: Arc<V>) -> (Option<Arc<V>>, Cached<V>) {
        let (old, created_at) = self.insert_raw(key.clone(), value.clone());

        let info = LookupInfo { created_at, key };
        let cached = Cached {
            token: Some(Self::invalidation_token(self, info)),
            value,
        };

        (old, cached)
    }
}

/// Implementation details of the entry invalidation machinery.
impl<K: Hash + Eq + Sync + Send + 'static, V> TimedLru<K, V>
where
    Self: Sync + Send,
{
    /// This is a proper (safe) way to create an invalidation token for [`TimedLru`].
    fn invalidation_token(&self, info: LookupInfo<Arc<K>>) -> InvalidationToken<'_> {
        InvalidationToken {
            cache: self,
            info: LookupInfo {
                created_at: info.created_at,
                key: info.key,
            },
        }
    }
}

/// This implementation depends on [`Self::invalidation_token`].
impl<K: Hash + Eq + 'static, V> Cache for TimedLru<K, V> {
    fn invalidate_entry(&self, info: LookupInfo<&(dyn Any + Sync + Send)>) {
        let info = LookupInfo {
            created_at: info.created_at,
            // NOTE: it's important to downcast to the correct type!
            key: info.key.downcast_ref::<K>().expect("bad key type"),
        };
        self.invalidate_raw(info)
    }
}

impl<K: Hash + Eq, V> TimedLru<K, V> {
    #[tracing::instrument(level = "debug", fields(cache = self.name), skip_all)]
    fn invalidate_raw(&self, info: LookupInfo<&K>) {
        let key: &Query<K> = RefCast::ref_cast(info.key);
        let now = Instant::now();

        // Do costly things before taking the lock.
        let mut cache = self.cache.lock();
        let raw_entry = match cache.raw_entry_mut().from_key(key) {
            RawEntryMut::Vacant(_) => return,
            RawEntryMut::Occupied(x) => x,
        };

        // Remove the entry if it was created prior to lookup timestamp.
        let entry = raw_entry.get();
        let (created_at, expires_at) = (entry.created_at, entry.expires_at);
        let should_remove = created_at <= info.created_at || expires_at <= now;

        if should_remove {
            raw_entry.remove();
        }

        drop(cache); // drop lock before logging
        debug!(
            ?created_at,
            ?expires_at,
            entry_removed = should_remove,
            "processed a cache entry invalidation event"
        );
    }
}
