use std::borrow::Borrow;
use std::hash::Hash;
use std::time::{Duration, Instant};

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
use tracing::debug;

use super::common::Cached;
use super::{timed_lru, Cache};

/// An implementation of timed LRU cache with fixed capacity.
/// Key properties:
///
/// * Whenever a new entry is inserted, the least recently accessed one is evicted.
///   The cache also keeps track of entry's insertion time (`created_at`) and TTL (`expires_at`).
///
/// * If `update_ttl_on_retrieval` is `true`. When the entry is about to be retrieved, we check its expiration timestamp.
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
pub(crate) struct TimedLru<K, V> {
    /// Cache's name for tracing.
    name: &'static str,

    /// The underlying cache implementation.
    cache: parking_lot::Mutex<LruCache<K, Entry<V>>>,

    /// Default time-to-live of a single entry.
    ttl: Duration,

    update_ttl_on_retrieval: bool,
}

impl<K: Hash + Eq, V> Cache for TimedLru<K, V> {
    type Key = K;
    type Value = V;
    type LookupInfo<Key> = LookupInfo<Key>;

    fn invalidate(&self, info: &Self::LookupInfo<K>) {
        self.invalidate_raw(info);
    }
}

struct Entry<T> {
    created_at: Instant,
    expires_at: Instant,
    ttl: Duration,
    update_ttl_on_retrieval: bool,
    value: T,
}

impl<K: Hash + Eq, V> TimedLru<K, V> {
    /// Construct a new LRU cache with timed entries.
    pub(crate) fn new(
        name: &'static str,
        capacity: usize,
        ttl: Duration,
        update_ttl_on_retrieval: bool,
    ) -> Self {
        Self {
            name,
            cache: LruCache::new(capacity).into(),
            ttl,
            update_ttl_on_retrieval,
        }
    }

    /// Drop an entry from the cache if it's outdated.
    #[tracing::instrument(level = "debug", fields(cache = self.name), skip_all)]
    fn invalidate_raw(&self, info: &LookupInfo<K>) {
        let now = Instant::now();

        // Do costly things before taking the lock.
        let mut cache = self.cache.lock();
        let raw_entry = match cache.raw_entry_mut().from_key(&info.key) {
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
            created_at = format_args!("{created_at:?}"),
            expires_at = format_args!("{expires_at:?}"),
            entry_removed = should_remove,
            "processed a cache entry invalidation event"
        );
    }

    /// Try retrieving an entry by its key, then execute `extract` if it exists.
    #[tracing::instrument(level = "debug", fields(cache = self.name), skip_all)]
    fn get_raw<Q, R>(&self, key: &Q, extract: impl FnOnce(&K, &Entry<V>) -> R) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let now = Instant::now();

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

        let value = extract(raw_entry.key(), entry);
        let (created_at, expires_at) = (entry.created_at, entry.expires_at);

        // Update the deadline and the entry's position in the LRU list.
        let deadline = now.checked_add(raw_entry.get().ttl).expect("time overflow");
        if raw_entry.get().update_ttl_on_retrieval {
            raw_entry.get_mut().expires_at = deadline;
        }
        raw_entry.to_back();

        drop(cache); // drop lock before logging
        debug!(
            created_at = format_args!("{created_at:?}"),
            old_expires_at = format_args!("{expires_at:?}"),
            new_expires_at = format_args!("{deadline:?}"),
            "accessed a cache entry"
        );

        Some(value)
    }

    /// Insert an entry to the cache. If an entry with the same key already
    /// existed, return the previous value and its creation timestamp.
    #[tracing::instrument(level = "debug", fields(cache = self.name), skip_all)]
    fn insert_raw(&self, key: K, value: V) -> (Instant, Option<V>) {
        self.insert_raw_ttl(key, value, self.ttl, self.update_ttl_on_retrieval)
    }

    /// Insert an entry to the cache. If an entry with the same key already
    /// existed, return the previous value and its creation timestamp.
    #[tracing::instrument(level = "debug", fields(cache = self.name), skip_all)]
    fn insert_raw_ttl(
        &self,
        key: K,
        value: V,
        ttl: Duration,
        update: bool,
    ) -> (Instant, Option<V>) {
        let created_at = Instant::now();
        let expires_at = created_at.checked_add(ttl).expect("time overflow");

        let entry = Entry {
            created_at,
            expires_at,
            ttl,
            update_ttl_on_retrieval: update,
            value,
        };

        // Do costly things before taking the lock.
        let old = self
            .cache
            .lock()
            .insert(key, entry)
            .map(|entry| entry.value);

        debug!(
            created_at = format_args!("{created_at:?}"),
            expires_at = format_args!("{expires_at:?}"),
            replaced = old.is_some(),
            "created a cache entry"
        );

        (created_at, old)
    }
}

impl<K: Hash + Eq + Clone, V: Clone> TimedLru<K, V> {
    pub(crate) fn insert_ttl(&self, key: K, value: V, ttl: Duration) {
        self.insert_raw_ttl(key, value, ttl, false);
    }

    pub(crate) fn insert_unit(&self, key: K, value: V) -> (Option<V>, Cached<&Self, ()>) {
        let (created_at, old) = self.insert_raw(key.clone(), value);

        let cached = Cached {
            token: Some((self, LookupInfo { created_at, key })),
            value: (),
        };

        (old, cached)
    }
}

impl<K: Hash + Eq, V: Clone> TimedLru<K, V> {
    /// Retrieve a cached entry in convenient wrapper.
    pub(crate) fn get<Q>(&self, key: &Q) -> Option<timed_lru::Cached<&Self>>
    where
        K: Borrow<Q> + Clone,
        Q: Hash + Eq + ?Sized,
    {
        self.get_raw(key, |key, entry| {
            let info = LookupInfo {
                created_at: entry.created_at,
                key: key.clone(),
            };

            Cached {
                token: Some((self, info)),
                value: entry.value.clone(),
            }
        })
    }
}

/// Lookup information for key invalidation.
pub(crate) struct LookupInfo<K> {
    /// Time of creation of a cache [`Entry`].
    /// We use this during invalidation lookups to prevent eviction of a newer
    /// entry sharing the same key (it might've been inserted by a different
    /// task after we got the entry we're trying to invalidate now).
    created_at: Instant,

    /// Search by this key.
    key: K,
}
