use tokio::time::Instant;
use x509_cert::der::zeroize::Zeroize as _;

use super::pbkdf2;
use crate::cache::Cached;
use crate::cache::common::{Cache, count_cache_insert, count_cache_outcome, eviction_listener};
use crate::intern::{EndpointIdInt, RoleNameInt};
use crate::metrics::{CacheKind, Metrics};

pub(crate) struct Pbkdf2Cache(moka::sync::Cache<(EndpointIdInt, RoleNameInt), Pbkdf2CacheEntry>);
pub(crate) type CachedPbkdf2<'a> = Cached<&'a Pbkdf2Cache>;

impl Cache for Pbkdf2Cache {
    type Key = (EndpointIdInt, RoleNameInt);
    type Value = Pbkdf2CacheEntry;

    fn invalidate(&self, info: &(EndpointIdInt, RoleNameInt)) {
        self.0.invalidate(info);
    }
}

/// To speed up password hashing for more active customers, we store the tail results of the
/// PBKDF2 algorithm. If the output of PBKDF2 is U1 ^ U2 ^ ⋯ ^ Uc, then we store
/// suffix = U17 ^ U18 ^ ⋯ ^ Uc. We only need to calculate U1 ^ U2 ^ ⋯ ^ U15 ^ U16
/// to determine the final result.
///
/// The suffix alone isn't enough to crack the password. The stored_key is still required.
/// While both are cached in memory, given they're in different locations is makes it much
/// harder to exploit, even if any such memory exploit exists in proxy.
#[derive(Clone)]
pub struct Pbkdf2CacheEntry {
    /// corresponds to [`super::ServerSecret::cached_at`]
    pub(super) cached_from: Instant,
    pub(super) suffix: pbkdf2::Block,
}

impl Drop for Pbkdf2CacheEntry {
    fn drop(&mut self) {
        self.suffix.zeroize();
    }
}

impl Pbkdf2Cache {
    pub fn new() -> Self {
        let builder = moka::sync::Cache::builder()
            .name("pbkdf2")
            .max_capacity(100)
            .time_to_idle(std::time::Duration::from_secs(60));

        Metrics::get().cache.capacity.set(CacheKind::Pbkdf2, 10);

        let builder =
            builder.eviction_listener(|_k, _v, cause| eviction_listener(CacheKind::Pbkdf2, cause));

        Self(builder.build())
    }

    pub fn insert(&self, endpoint: EndpointIdInt, role: RoleNameInt, value: Pbkdf2CacheEntry) {
        count_cache_insert(CacheKind::Pbkdf2);
        self.0.insert((endpoint, role), value);
    }

    fn get(&self, endpoint: EndpointIdInt, role: RoleNameInt) -> Option<Pbkdf2CacheEntry> {
        count_cache_outcome(CacheKind::Pbkdf2, self.0.get(&(endpoint, role)))
    }

    pub fn get_entry(
        &self,
        endpoint: EndpointIdInt,
        role: RoleNameInt,
    ) -> Option<CachedPbkdf2<'_>> {
        self.get(endpoint, role).map(|value| Cached {
            token: Some((self, (endpoint, role))),
            value,
        })
    }
}
