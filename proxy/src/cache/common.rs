use std::ops::{Deref, DerefMut};
use std::time::{Duration, Instant};

use moka::Expiry;
use moka::notification::RemovalCause;

use crate::control_plane::messages::ControlPlaneErrorMessage;
use crate::metrics::{
    CacheEviction, CacheKind, CacheOutcome, CacheOutcomeGroup, CacheRemovalCause, Metrics,
};

/// Default TTL used when caching errors from control plane.
pub const DEFAULT_ERROR_TTL: Duration = Duration::from_secs(30);

/// A generic trait which exposes types of cache's key and value,
/// as well as the notion of cache entry invalidation.
/// This is useful for [`Cached`].
pub(crate) trait Cache {
    /// Entry's key.
    type Key;

    /// Entry's value.
    type Value;

    /// Invalidate an entry using a lookup info.
    /// We don't have an empty default impl because it's error-prone.
    fn invalidate(&self, _: &Self::Key);
}

impl<C: Cache> Cache for &C {
    type Key = C::Key;
    type Value = C::Value;

    fn invalidate(&self, info: &Self::Key) {
        C::invalidate(self, info);
    }
}

/// Wrapper for convenient entry invalidation.
pub(crate) struct Cached<C: Cache, V = <C as Cache>::Value> {
    /// Cache + lookup info.
    pub(crate) token: Option<(C, C::Key)>,

    /// The value itself.
    pub(crate) value: V,
}

impl<C: Cache, V> Cached<C, V> {
    /// Place any entry into this wrapper; invalidation will be a no-op.
    pub(crate) fn new_uncached(value: V) -> Self {
        Self { token: None, value }
    }

    /// Drop this entry from a cache if it's still there.
    pub(crate) fn invalidate(self) -> V {
        if let Some((cache, info)) = &self.token {
            cache.invalidate(info);
        }
        self.value
    }

    /// Tell if this entry is actually cached.
    pub(crate) fn cached(&self) -> bool {
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

pub type ControlPlaneResult<T> = Result<T, Box<ControlPlaneErrorMessage>>;

#[derive(Clone, Copy)]
pub struct CplaneExpiry {
    pub error: Duration,
}

impl Default for CplaneExpiry {
    fn default() -> Self {
        Self {
            error: DEFAULT_ERROR_TTL,
        }
    }
}

impl CplaneExpiry {
    pub fn expire_early<V>(
        &self,
        value: &ControlPlaneResult<V>,
        updated: Instant,
    ) -> Option<Duration> {
        match value {
            Ok(_) => None,
            Err(err) => Some(self.expire_err_early(err, updated)),
        }
    }

    pub fn expire_err_early(&self, err: &ControlPlaneErrorMessage, updated: Instant) -> Duration {
        err.status
            .as_ref()
            .and_then(|s| s.details.retry_info.as_ref())
            .map_or(self.error, |r| r.retry_at.into_std() - updated)
    }
}

impl<K, V> Expiry<K, ControlPlaneResult<V>> for CplaneExpiry {
    fn expire_after_create(
        &self,
        _key: &K,
        value: &ControlPlaneResult<V>,
        created_at: Instant,
    ) -> Option<Duration> {
        self.expire_early(value, created_at)
    }

    fn expire_after_update(
        &self,
        _key: &K,
        value: &ControlPlaneResult<V>,
        updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        self.expire_early(value, updated_at)
    }
}

pub fn eviction_listener(kind: CacheKind, cause: RemovalCause) {
    let cause = match cause {
        RemovalCause::Expired => CacheRemovalCause::Expired,
        RemovalCause::Explicit => CacheRemovalCause::Explicit,
        RemovalCause::Replaced => CacheRemovalCause::Replaced,
        RemovalCause::Size => CacheRemovalCause::Size,
    };
    Metrics::get()
        .cache
        .evicted_total
        .inc(CacheEviction { cache: kind, cause });
}

#[inline]
pub fn count_cache_outcome<T>(kind: CacheKind, cache_result: Option<T>) -> Option<T> {
    let outcome = if cache_result.is_some() {
        CacheOutcome::Hit
    } else {
        CacheOutcome::Miss
    };
    Metrics::get().cache.request_total.inc(CacheOutcomeGroup {
        cache: kind,
        outcome,
    });
    cache_result
}

#[inline]
pub fn count_cache_insert(kind: CacheKind) {
    Metrics::get().cache.inserted_total.inc(kind);
}
