use std::{
    borrow::Borrow,
    hash::Hash,
    ops::{Deref, DerefMut},
    time::{Duration, Instant},
};

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

pub trait Cache {
    /// Entry's key.
    type Key;
    /// Entry's value.
    type Value;
    /// Used for entry invalidation.
    type LookupInfo<Key>;

    /// Invalidate an entry using a lookup info.
    fn invalidate(&self, _: &Self::LookupInfo<Self::Key>) {}
}

impl<C: Cache> Cache for &C {
    type Key = C::Key;
    type Value = C::Value;
    type LookupInfo<Key> = C::LookupInfo<Key>;
}

pub use timed_lru::TimedLru;
pub mod timed_lru {
    use super::*;

    /// A timed LRU cache implementation.
    pub struct TimedLru<K, V> {
        /// The underlying cache implementation.
        cache: parking_lot::Mutex<LruCache<K, Entry<V>>>,

        /// Default time-to-live of a single entry.
        ttl: Duration,
    }

    impl<K: Hash + Eq, V> Cache for TimedLru<K, V> {
        type Key = K;
        type Value = V;
        type LookupInfo<Key> = LookupInfo<Key>;

        fn invalidate(&self, info: &Self::LookupInfo<K>) {
            self.invalidate_raw(info)
        }
    }

    struct Entry<T> {
        created_at: Instant,
        expires_at: Instant,
        value: T,
    }

    impl<K: Hash + Eq, V> TimedLru<K, V> {
        /// Construct a new LRU cache with timed entries.
        pub fn new(capacity: usize, ttl: Duration) -> Self {
            Self {
                cache: LruCache::new(capacity).into(),
                ttl,
            }
        }

        fn invalidate_raw(&self, info: &LookupInfo<K>) {
            let mut cache = self.cache.lock();
            let raw_entry = match cache.raw_entry_mut().from_key(&info.key) {
                RawEntryMut::Vacant(_) => return,
                RawEntryMut::Occupied(x) => x,
            };

            // We shouldn't remove the entry if it's newer!
            if raw_entry.get().created_at <= info.created_at {
                raw_entry.remove();
            }
        }

        fn get_raw<Q, R>(&self, key: &Q, extract: impl FnOnce(&K, &Entry<V>) -> R) -> Option<R>
        where
            K: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
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

            let value = extract(raw_entry.key(), entry);

            // Update the deadline and the entry's position in the LRU list.
            raw_entry.get_mut().expires_at = deadline;
            raw_entry.to_back();

            Some(value)
        }

        pub fn insert_raw(&self, key: K, value: V) -> (Instant, Option<V>) {
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
                .insert(key, entry)
                .map(|entry| entry.value);

            (created_at, old)
        }
    }

    impl<K: Hash + Eq + Clone, V: Clone> TimedLru<K, V> {
        pub fn insert(&self, key: K, value: V) -> (Option<V>, Cached<&Self>) {
            let (created_at, old) = self.insert_raw(key.clone(), value.clone());

            let cached = Cached {
                token: Some((self, LookupInfo { created_at, key })),
                value,
            };

            (old, cached)
        }
    }

    impl<K: Hash + Eq, V: Clone> TimedLru<K, V> {
        /// Retrieve a cached entry in convenient wrapper.
        pub fn get<Q>(&self, key: &Q) -> Option<timed_lru::Cached<&Self>>
        where
            K: Borrow<Q> + Clone,
            Q: Hash + Eq + ?Sized,
        {
            self.get_raw(key, |key, entry| {
                let info = LookupInfo {
                    created_at: entry.created_at,
                    key: key.clone(), // low-cost for Arc<T> or &T
                };

                Cached {
                    token: Some((self, info)),
                    value: entry.value.clone(),
                }
            })
        }
    }

    pub struct LookupInfo<K> {
        created_at: Instant,
        key: K,
    }

    /// Wrapper for convenient entry invalidation.
    pub struct Cached<C: Cache> {
        /// Cache + lookup info.
        token: Option<(C, C::LookupInfo<C::Key>)>,

        /// The value itself.
        pub value: C::Value,
    }

    impl<C: Cache> Cached<C> {
        /// Place any entry into this wrapper; invalidation will be a no-op.
        /// Unfortunately, rust doesn't let us implement [`From`] or [`Into`].
        pub fn new_uncached(value: C::Value) -> Self {
            Self { token: None, value }
        }

        /// Drop this entry from a cache if it's still there.
        pub fn invalidate(&self) {
            if let Some((cache, info)) = &self.token {
                cache.invalidate(info);
            }
        }

        /// Tell if this entry is cached.
        pub fn cached(&self) -> bool {
            self.token.is_some()
        }
    }

    impl<C: Cache> Deref for Cached<C> {
        type Target = C::Value;

        fn deref(&self) -> &Self::Target {
            &self.value
        }
    }

    impl<C: Cache> DerefMut for Cached<C> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.value
        }
    }
}
