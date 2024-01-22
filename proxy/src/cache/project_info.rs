use std::{
    collections::HashSet,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use dashmap::DashMap;
use hashlink::LruCache;
use parking_lot::Mutex;
use smol_str::SmolStr;
use tokio::time::Instant;
use tracing::info;

use crate::{auth::IpPattern, config::ProjectInfoCacheOptions, console::AuthSecret};

use super::{Cache, Cached};

pub trait ProjectInfoCache {
    fn invalidate_allowed_ips_for_project(&self, project_id: &SmolStr);
    fn invalidate_role_secret_for_project(&self, project_id: &SmolStr, role_name: &SmolStr);
    fn enable_ttl(&self);
    fn disable_ttl(&self);
}

struct Entry<T> {
    created_at: Instant,
    value: T,
}

impl<T> Entry<T> {
    pub fn new(value: T) -> Self {
        Self {
            created_at: Instant::now(),
            value,
        }
    }
}

impl<T> From<T> for Entry<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

fn check_ignore_cache(ignore_cache_since: Option<Instant>, created_at: Instant) -> bool {
    match ignore_cache_since {
        None => false,
        Some(t) => t < created_at,
    }
}

/// Cache for project info.
/// This is used to cache auth data for endpoints.
/// Invalidation is done by console notifications or by TTL (if console notifications are disabled).
///
/// We also store endpoint-to-project mapping in the cache, to be able to access per-endpoint data.
/// One may ask, why the data is stored per project, when on the user request there is only data about the endpoint available?
/// On the cplane side updates are done per project (or per branch), so it's easier to invalidate the whole project cache.
pub struct ProjectInfoCacheImpl {
    ip_cache: Mutex<LruCache<SmolStr, Entry<Arc<Vec<IpPattern>>>>>,
    role_cache: Mutex<LruCache<(SmolStr, SmolStr), Entry<AuthSecret>>>,

    project2ep: DashMap<SmolStr, HashSet<SmolStr>>,

    start_time: Instant,
    ttl: Duration,
    ttl_disabled_since_us: AtomicU64,
}

impl ProjectInfoCache for ProjectInfoCacheImpl {
    fn invalidate_allowed_ips_for_project(&self, project_id: &SmolStr) {
        info!("invalidating allowed ips for project `{}`", project_id);
        let endpoints = self
            .project2ep
            .get(project_id)
            .map(|kv| kv.value().clone())
            .unwrap_or_default();
        for endpoint_id in endpoints {
            self.ip_cache.lock().remove(&endpoint_id);
        }
    }
    fn invalidate_role_secret_for_project(&self, project_id: &SmolStr, role_name: &SmolStr) {
        info!(
            "invalidating role secret for project_id `{}` and role_name `{}`",
            project_id, role_name
        );
        let endpoints = self
            .project2ep
            .get(project_id)
            .map(|kv| kv.value().clone())
            .unwrap_or_default();
        for endpoint_id in endpoints {
            self.role_cache
                .lock()
                .remove(&(endpoint_id, role_name.clone()));
        }
    }
    fn enable_ttl(&self) {
        self.ttl_disabled_since_us
            .store(u64::MAX, std::sync::atomic::Ordering::Relaxed);
    }

    fn disable_ttl(&self) {
        let new_ttl = (self.start_time.elapsed() + self.ttl).as_micros() as u64;
        self.ttl_disabled_since_us
            .store(new_ttl, std::sync::atomic::Ordering::Relaxed);
    }
}

impl ProjectInfoCacheImpl {
    pub fn new(config: ProjectInfoCacheOptions) -> Self {
        Self {
            ip_cache: Mutex::new(LruCache::new(config.size)),
            role_cache: Mutex::new(LruCache::new(config.size * config.max_roles)),
            project2ep: DashMap::new(),
            ttl: config.ttl,
            ttl_disabled_since_us: AtomicU64::new(u64::MAX),
            start_time: Instant::now(),
        }
    }

    pub fn get_role_secret(
        &self,
        endpoint_id: &SmolStr,
        role_name: &SmolStr,
    ) -> Option<Cached<&Self, AuthSecret>> {
        let (valid_since, ignore_cache_since) = self.get_cache_times();
        let (value, ignore_cache) = {
            let mut cache = self.role_cache.lock();
            let secret = cache.get(&(endpoint_id.clone(), role_name.clone()))?;
            if secret.created_at <= valid_since {
                return None;
            }
            (
                secret.value.clone(),
                check_ignore_cache(ignore_cache_since, secret.created_at),
            )
        };
        if !ignore_cache {
            let cached = Cached {
                token: Some((
                    self,
                    CachedLookupInfo::new_role_secret(endpoint_id.clone(), role_name.clone()),
                )),
                value,
            };
            return Some(cached);
        }
        Some(Cached::new_uncached(value))
    }

    pub fn get_allowed_ips(
        &self,
        endpoint_id: &SmolStr,
    ) -> Option<Cached<&Self, Arc<Vec<IpPattern>>>> {
        let (valid_since, ignore_cache_since) = self.get_cache_times();
        let (value, ignore_cache) = {
            let mut cache = self.ip_cache.lock();
            let allowed_ips = cache.get(endpoint_id)?;
            if allowed_ips.created_at <= valid_since {
                return None;
            }
            (
                allowed_ips.value.clone(),
                check_ignore_cache(ignore_cache_since, allowed_ips.created_at),
            )
        };
        if !ignore_cache {
            let cached = Cached {
                token: Some((self, CachedLookupInfo::new_allowed_ips(endpoint_id.clone()))),
                value,
            };
            return Some(cached);
        }
        Some(Cached::new_uncached(value))
    }

    pub fn insert_role_secret(
        &self,
        project_id: &SmolStr,
        endpoint_id: &SmolStr,
        role_name: &SmolStr,
        secret: AuthSecret,
    ) {
        self.insert_project2endpoint(project_id, endpoint_id);
        self.role_cache
            .lock()
            .insert((endpoint_id.clone(), role_name.clone()), secret.into());
    }

    pub fn insert_allowed_ips(
        &self,
        project_id: &SmolStr,
        endpoint_id: &SmolStr,
        allowed_ips: Arc<Vec<IpPattern>>,
    ) {
        self.insert_project2endpoint(project_id, endpoint_id);
        self.ip_cache
            .lock()
            .insert(endpoint_id.clone(), allowed_ips.into());
    }

    fn insert_project2endpoint(&self, project_id: &SmolStr, endpoint_id: &SmolStr) {
        self.project2ep
            .entry(project_id.clone())
            .or_default()
            .insert(endpoint_id.clone());
    }

    fn get_cache_times(&self) -> (Instant, Option<Instant>) {
        let mut valid_since = Instant::now() - self.ttl;
        // Only ignore cache if ttl is disabled.
        let ttl_disabled_since_us = self
            .ttl_disabled_since_us
            .load(std::sync::atomic::Ordering::Relaxed);
        let ignore_cache_since = if ttl_disabled_since_us != u64::MAX {
            let ignore_cache_since = self.start_time + Duration::from_micros(ttl_disabled_since_us);
            // We are fine if entry is not older than ttl or was added before we are getting notifications.
            valid_since = valid_since.min(ignore_cache_since);
            Some(ignore_cache_since)
        } else {
            None
        };
        (valid_since, ignore_cache_since)
    }
}

/// Lookup info for project info cache.
/// This is used to invalidate cache entries.
pub struct CachedLookupInfo {
    /// Search by this key.
    endpoint_id: SmolStr,
    lookup_type: LookupType,
}

impl CachedLookupInfo {
    pub(self) fn new_role_secret(endpoint_id: SmolStr, role_name: SmolStr) -> Self {
        Self {
            endpoint_id,
            lookup_type: LookupType::RoleSecret(role_name),
        }
    }
    pub(self) fn new_allowed_ips(endpoint_id: SmolStr) -> Self {
        Self {
            endpoint_id,
            lookup_type: LookupType::AllowedIps,
        }
    }
}

enum LookupType {
    RoleSecret(SmolStr),
    AllowedIps,
}

impl Cache for ProjectInfoCacheImpl {
    type Key = SmolStr;
    // Value is not really used here, but we need to specify it.
    type Value = SmolStr;

    type LookupInfo<Key> = CachedLookupInfo;

    fn invalidate(&self, key: &Self::LookupInfo<SmolStr>) {
        match &key.lookup_type {
            LookupType::RoleSecret(role_name) => {
                self.role_cache
                    .lock()
                    .remove(&(key.endpoint_id.clone(), role_name.clone()));
            }
            LookupType::AllowedIps => {
                self.ip_cache.lock().remove(&key.endpoint_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{console::AuthSecret, scram::ServerSecret};
    use smol_str::SmolStr;
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn test_project_info_cache_settings() {
        tokio::time::pause();
        let cache = ProjectInfoCacheImpl::new(ProjectInfoCacheOptions {
            size: 2,
            max_roles: 1,
            ttl: Duration::from_secs(1),
        });
        let project_id = "project".into();
        let endpoint_id = "endpoint".into();
        let user1: SmolStr = "user1".into();
        let user2: SmolStr = "user2".into();
        let secret1 = AuthSecret::Scram(ServerSecret::mock(user1.as_str(), [1; 32]));
        let secret2 = AuthSecret::Scram(ServerSecret::mock(user2.as_str(), [2; 32]));
        let allowed_ips = Arc::new(vec![
            "127.0.0.1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
        ]);
        cache.insert_role_secret(&project_id, &endpoint_id, &user1, secret1.clone());
        cache.insert_role_secret(&project_id, &endpoint_id, &user2, secret2.clone());
        cache.insert_allowed_ips(&project_id, &endpoint_id, allowed_ips.clone());

        let cached = cache.get_role_secret(&endpoint_id, &user1).unwrap();
        assert!(cached.cached());
        assert_eq!(cached.value, secret1);
        let cached = cache.get_role_secret(&endpoint_id, &user2).unwrap();
        assert!(cached.cached());
        assert_eq!(cached.value, secret2);

        // Shouldn't add more than 2 roles.
        let user3: SmolStr = "user3".into();
        let secret3 = AuthSecret::Scram(ServerSecret::mock(user3.as_str(), [3; 32]));
        cache.insert_role_secret(&project_id, &endpoint_id, &user3, secret3.clone());
        assert!(cache.get_role_secret(&endpoint_id, &user1).is_none(),);

        let cached = cache.get_allowed_ips(&endpoint_id).unwrap();
        assert!(cached.cached());
        assert_eq!(cached.value, allowed_ips);

        tokio::time::advance(Duration::from_secs(2)).await;
        let cached = cache.get_role_secret(&endpoint_id, &user1);
        assert!(cached.is_none());
        let cached = cache.get_role_secret(&endpoint_id, &user2);
        assert!(cached.is_none());
        let cached = cache.get_allowed_ips(&endpoint_id);
        assert!(cached.is_none());
    }

    #[tokio::test]
    async fn test_project_info_cache_invalidations() {
        tokio::time::pause();
        let cache = Arc::new(ProjectInfoCacheImpl::new(ProjectInfoCacheOptions {
            size: 2,
            max_roles: 2,
            ttl: Duration::from_secs(1),
        }));
        cache.clone().disable_ttl();
        tokio::time::advance(Duration::from_secs(2)).await;

        let project_id = "project".into();
        let endpoint_id = "endpoint".into();
        let user1: SmolStr = "user1".into();
        let user2: SmolStr = "user2".into();
        let secret1 = AuthSecret::Scram(ServerSecret::mock(user1.as_str(), [1; 32]));
        let secret2 = AuthSecret::Scram(ServerSecret::mock(user2.as_str(), [2; 32]));
        let allowed_ips = Arc::new(vec![
            "127.0.0.1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
        ]);
        cache.insert_role_secret(&project_id, &endpoint_id, &user1, secret1.clone());
        cache.insert_role_secret(&project_id, &endpoint_id, &user2, secret2.clone());
        cache.insert_allowed_ips(&project_id, &endpoint_id, allowed_ips.clone());

        tokio::time::advance(Duration::from_secs(2)).await;
        // Nothing should be invalidated.

        let cached = cache.get_role_secret(&endpoint_id, &user1).unwrap();
        // TTL is disabled, so it should be impossible to invalidate this value.
        assert!(!cached.cached());
        assert_eq!(cached.value, secret1);

        cached.invalidate(); // Shouldn't do anything.
        let cached = cache.get_role_secret(&endpoint_id, &user1).unwrap();
        assert_eq!(cached.value, secret1);

        let cached = cache.get_role_secret(&endpoint_id, &user2).unwrap();
        assert!(!cached.cached());
        assert_eq!(cached.value, secret2);

        // The only way to invalidate this value is to invalidate via the api.
        cache.invalidate_role_secret_for_project(&project_id, &user2);
        assert!(cache.get_role_secret(&endpoint_id, &user2).is_none());

        let cached = cache.get_allowed_ips(&endpoint_id).unwrap();
        assert!(!cached.cached());
        assert_eq!(cached.value, allowed_ips);
    }

    #[tokio::test]
    async fn test_disable_ttl_invalidate_added_before() {
        tokio::time::pause();
        let cache = Arc::new(ProjectInfoCacheImpl::new(ProjectInfoCacheOptions {
            size: 2,
            max_roles: 2,
            ttl: Duration::from_secs(1),
        }));

        let project_id = "project".into();
        let endpoint_id = "endpoint".into();
        let user1: SmolStr = "user1".into();
        let user2: SmolStr = "user2".into();
        let secret1 = AuthSecret::Scram(ServerSecret::mock(user1.as_str(), [1; 32]));
        let secret2 = AuthSecret::Scram(ServerSecret::mock(user2.as_str(), [2; 32]));
        let allowed_ips = Arc::new(vec![
            "127.0.0.1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
        ]);
        cache.insert_role_secret(&project_id, &endpoint_id, &user1, secret1.clone());
        cache.clone().disable_ttl();
        tokio::time::advance(Duration::from_millis(100)).await;
        cache.insert_role_secret(&project_id, &endpoint_id, &user2, secret2.clone());

        // Added before ttl was disabled + ttl should be still cached.
        let cached = cache.get_role_secret(&endpoint_id, &user1).unwrap();
        assert!(cached.cached());
        let cached = cache.get_role_secret(&endpoint_id, &user2).unwrap();
        assert!(cached.cached());

        tokio::time::advance(Duration::from_secs(1)).await;
        // Added before ttl was disabled + ttl should expire.
        assert!(cache.get_role_secret(&endpoint_id, &user1).is_none());
        assert!(cache.get_role_secret(&endpoint_id, &user2).is_none());

        // Added after ttl was disabled + ttl should not be cached.
        cache.insert_allowed_ips(&project_id, &endpoint_id, allowed_ips.clone());
        let cached = cache.get_allowed_ips(&endpoint_id).unwrap();
        assert!(!cached.cached());

        tokio::time::advance(Duration::from_secs(1)).await;
        // Added before ttl was disabled + ttl still should expire.
        assert!(cache.get_role_secret(&endpoint_id, &user1).is_none());
        assert!(cache.get_role_secret(&endpoint_id, &user2).is_none());
        // Shouldn't be invalidated.

        let cached = cache.get_allowed_ips(&endpoint_id).unwrap();
        assert!(!cached.cached());
        assert_eq!(cached.value, allowed_ips);
    }
}
