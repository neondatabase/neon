use std::{
    ops::{Deref, DerefMut},
    time::{Duration, Instant},
};

use crate::control_plane::messages::ControlPlaneErrorMessage;

/// A generic trait which exposes types of cache's key and value,
/// as well as the notion of cache entry invalidation.
/// This is useful for [`Cached`].
pub(crate) trait Cache {
    /// Entry's key.
    type Key;

    /// Entry's value.
    type Value;

    /// Used for entry invalidation.
    type LookupInfo<Key>;

    /// Invalidate an entry using a lookup info.
    /// We don't have an empty default impl because it's error-prone.
    fn invalidate(&self, _: &Self::LookupInfo<Self::Key>);
}

impl<C: Cache> Cache for &C {
    type Key = C::Key;
    type Value = C::Value;
    type LookupInfo<Key> = C::LookupInfo<Key>;

    fn invalidate(&self, info: &Self::LookupInfo<Self::Key>) {
        C::invalidate(self, info);
    }
}

/// Wrapper for convenient entry invalidation.
pub(crate) struct Cached<C: Cache, V = <C as Cache>::Value> {
    /// Cache + lookup info.
    pub(crate) token: Option<(C, C::LookupInfo<C::Key>)>,

    /// The value itself.
    pub(crate) value: V,
}

impl<C: Cache, V> Cached<C, V> {
    /// Place any entry into this wrapper; invalidation will be a no-op.
    pub(crate) fn new_uncached(value: V) -> Self {
        Self { token: None, value }
    }

    pub(crate) fn take_value(self) -> (Cached<C, ()>, V) {
        (
            Cached {
                token: self.token,
                value: (),
            },
            self.value,
        )
    }

    pub(crate) fn map<U>(self, f: impl FnOnce(V) -> U) -> Cached<C, U> {
        Cached {
            token: self.token,
            value: f(self.value),
        }
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
