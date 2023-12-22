use std::{
    collections::HashSet,
    convert::Infallible,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, Instant},
};

use dashmap::DashMap;
use rand::{thread_rng, Rng};
use smol_str::SmolStr;
use tracing::{debug, info};

use crate::{config::ProjectInfoCacheOptions, console::AuthSecret};

use super::{Cache, Cached};

pub trait ProjectInfoCache {
    fn invalidate_allowed_ips_for_project(&self, project_id: &SmolStr);
    fn invalidate_role_secret_for_project(&self, project_id: &SmolStr, user: &SmolStr);
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

#[derive(Default)]
struct EndpointInfo {
    secret: std::collections::HashMap<SmolStr, Entry<AuthSecret>>,
    allowed_ips: Option<Entry<Arc<Vec<SmolStr>>>>,
}

impl EndpointInfo {
    pub fn new_with_secret(user: SmolStr, secret: AuthSecret) -> Self {
        let mut secret_map = std::collections::HashMap::new();
        secret_map.insert(user, secret.into());
        Self {
            secret: secret_map,
            allowed_ips: None,
        }
    }
    pub fn new_with_allowed_ips(allowed_ips: Arc<Vec<SmolStr>>) -> Self {
        Self {
            secret: std::collections::HashMap::new(),
            allowed_ips: Some(allowed_ips.into()),
        }
    }
    pub fn get_role_secret(&self, user: &SmolStr, ttl: Option<Duration>) -> Option<AuthSecret> {
        if let Some(secret) = self.secret.get(user) {
            if let Some(ttl) = ttl {
                if secret.created_at.elapsed() > ttl {
                    return None;
                }
            }
            return Some(secret.value.clone());
        }
        None
    }

    pub fn get_allowed_ips(&self, ttl: Option<Duration>) -> Option<Arc<Vec<SmolStr>>> {
        if let Some(allowed_ips) = &self.allowed_ips {
            if let Some(ttl) = ttl {
                if allowed_ips.created_at.elapsed() > ttl {
                    return None;
                }
            }
            return Some(allowed_ips.value.clone());
        }
        None
    }
    pub fn invalidate_allowed_ips(&mut self) {
        self.allowed_ips = None;
    }
    pub fn invalidate_role_secret(&mut self, user: &SmolStr) {
        self.secret.remove(user);
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
    cache: DashMap<SmolStr, EndpointInfo>,

    project2ep: DashMap<SmolStr, HashSet<SmolStr>>,
    config: ProjectInfoCacheOptions,
    ttl_enabled: AtomicBool,
}

impl ProjectInfoCache for ProjectInfoCacheImpl {
    fn invalidate_allowed_ips_for_project(&self, project_id: &SmolStr) {
        let endpoints = self
            .project2ep
            .get(project_id)
            .map(|kv| kv.value().clone())
            .unwrap_or_default();
        for endpoint_id in endpoints {
            if let Some(mut endpoint_info) = self.cache.get_mut(&endpoint_id) {
                endpoint_info.invalidate_allowed_ips();
            }
        }
    }
    fn invalidate_role_secret_for_project(&self, project_id: &SmolStr, user: &SmolStr) {
        let endpoints = self
            .project2ep
            .get(project_id)
            .map(|kv| kv.value().clone())
            .unwrap_or_default();
        for endpoint_id in endpoints {
            if let Some(mut endpoint_info) = self.cache.get_mut(&endpoint_id) {
                endpoint_info.invalidate_role_secret(user);
            }
        }
    }
    fn enable_ttl(&self) {
        self.ttl_enabled
            .store(true, std::sync::atomic::Ordering::Relaxed)
    }

    fn disable_ttl(&self) {
        self.ttl_enabled
            .store(false, std::sync::atomic::Ordering::Relaxed)
    }
}

impl ProjectInfoCacheImpl {
    pub fn new(config: ProjectInfoCacheOptions) -> Self {
        Self {
            cache: DashMap::new(),
            project2ep: DashMap::new(),
            config,
            ttl_enabled: true.into(),
        }
    }

    pub fn get_role_secret(
        &self,
        endpoint_id: &SmolStr,
        user: &SmolStr,
    ) -> Option<Cached<&Self, AuthSecret>> {
        let ttl = self.get_ttl();
        if let Some(endpoint_info) = self.cache.get(endpoint_id) {
            let value = endpoint_info.get_role_secret(user, ttl);
            if let Some(value) = value {
                if ttl.is_some() {
                    let cached = Cached {
                        token: Some((
                            self,
                            CachedLookupInfo::new_role_secret(endpoint_id.clone(), user.clone()),
                        )),
                        value,
                    };
                    return Some(cached);
                }
                return Some(Cached::new_uncached(value));
            }
            None
        } else {
            None
        }
    }
    pub fn get_allowed_ips(
        &self,
        endpoint_id: &SmolStr,
    ) -> Option<Cached<&Self, Arc<Vec<SmolStr>>>> {
        let ttl = self.get_ttl();
        if let Some(endpoint_info) = self.cache.get(endpoint_id) {
            let value = endpoint_info.get_allowed_ips(ttl);
            if let Some(value) = value {
                if ttl.is_some() {
                    let cached = Cached {
                        token: Some((self, CachedLookupInfo::new_allowed_ips(endpoint_id.clone()))),
                        value,
                    };
                    return Some(cached);
                }
                return Some(Cached::new_uncached(value));
            }
            None
        } else {
            None
        }
    }
    pub fn insert_role_secret(
        &self,
        project_id: &SmolStr,
        endpoint_id: &SmolStr,
        user: &SmolStr,
        secret: AuthSecret,
    ) {
        if self.cache.len() >= self.config.size {
            // If there are too many entries, wait until the next gc cycle.
            return;
        }
        self.inser_project2endpoint(project_id, endpoint_id);
        if let Some(mut endpoint_info) = self.cache.get_mut(endpoint_id) {
            if endpoint_info.secret.len() < self.config.max_roles {
                endpoint_info.secret.insert(user.clone(), secret.into());
            }
        } else {
            self.cache.insert(
                endpoint_id.clone(),
                EndpointInfo::new_with_secret(user.clone(), secret),
            );
        }
    }
    pub fn insert_allowed_ips(
        &self,
        project_id: &SmolStr,
        endpoint_id: &SmolStr,
        allowed_ips: Arc<Vec<SmolStr>>,
    ) {
        if self.cache.len() >= self.config.size {
            // If there are too many entries, wait until the next gc cycle.
            return;
        }
        self.inser_project2endpoint(project_id, endpoint_id);
        if let Some(mut endpoint_info) = self.cache.get_mut(endpoint_id) {
            endpoint_info.allowed_ips = Some(allowed_ips.into());
        } else {
            self.cache.insert(
                endpoint_id.clone(),
                EndpointInfo::new_with_allowed_ips(allowed_ips),
            );
        }
    }
    fn inser_project2endpoint(&self, project_id: &SmolStr, endpoint_id: &SmolStr) {
        if let Some(mut endpoints) = self.project2ep.get_mut(project_id) {
            endpoints.insert(endpoint_id.clone());
        } else {
            self.project2ep
                .insert(project_id.clone(), HashSet::from([endpoint_id.clone()]));
        }
    }
    fn get_ttl(&self) -> Option<Duration> {
        if self.ttl_enabled.load(std::sync::atomic::Ordering::Relaxed) {
            Some(self.config.ttl)
        } else {
            None
        }
    }

    pub async fn gc_worker(&self) -> anyhow::Result<Infallible> {
        let epoch = Duration::from_secs(600);
        let mut interval = tokio::time::interval(epoch / (self.cache.shards().len()) as u32);
        loop {
            interval.tick().await;
            self.gc();
        }
    }

    fn gc(&self) {
        if self.cache.len() >= self.config.size {
            // If there are too many entries, wait until the next gc cycle.
            return;
        }
        let shard = thread_rng().gen_range(0..self.project2ep.shards().len());
        debug!(shard, "project_info_cache: performing epoch reclamation");

        // acquire a random shard lock
        let mut removed = 0;
        let shard = self.project2ep.shards()[shard].write();
        for (_, endpoints) in shard.iter() {
            for endpoint in endpoints.get().iter() {
                self.cache.remove(endpoint);
                removed += 1;
            }
        }
        // We can drop this shard only after making sure that all endpoints are removed.
        drop(shard);
        info!("project_info_cache: removed {removed} endpoints");
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
    pub(self) fn new_role_secret(endpoint_id: SmolStr, user: SmolStr) -> Self {
        Self {
            endpoint_id,
            lookup_type: LookupType::RoleSecret(user),
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
            LookupType::RoleSecret(user) => {
                if let Some(mut endpoint_info) = self.cache.get_mut(&key.endpoint_id) {
                    endpoint_info.invalidate_role_secret(user);
                }
            }
            LookupType::AllowedIps => {
                if let Some(mut endpoint_info) = self.cache.get_mut(&key.endpoint_id) {
                    endpoint_info.invalidate_allowed_ips();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{console::AuthSecret, scram::ServerSecret};
    use smol_str::SmolStr;
    use std::sync::Arc;

    #[test]
    fn test_project_info_cache_settings() {
        let cache = ProjectInfoCacheImpl::new(ProjectInfoCacheOptions {
            size: 2,
            max_roles: 2,
            ttl: Duration::from_secs(1),
        });
        let project_id = "project".into();
        let endpoint_id = "endpoint".into();
        let user1: SmolStr = "user1".into();
        let user2: SmolStr = "user2".into();
        let secret1 = AuthSecret::Scram(ServerSecret::mock(user1.as_str(), [1; 32]));
        let secret2 = AuthSecret::Scram(ServerSecret::mock(user2.as_str(), [2; 32]));
        let allowed_ips = Arc::new(vec!["allowed_ip1".into(), "allowed_ip2".into()]);
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
        assert!(cache.get_role_secret(&endpoint_id, &user3).is_none());

        let cached = cache.get_allowed_ips(&endpoint_id).unwrap();
        assert!(cached.cached());
        assert_eq!(cached.value, allowed_ips);

        std::thread::sleep(Duration::from_secs(2));
        let cached = cache.get_role_secret(&endpoint_id, &user1);
        assert!(cached.is_none());
        let cached = cache.get_role_secret(&endpoint_id, &user2);
        assert!(cached.is_none());
        let cached = cache.get_allowed_ips(&endpoint_id);
        assert!(cached.is_none());
    }

    #[test]
    fn test_project_info_cache_invalidations() {
        let cache = ProjectInfoCacheImpl::new(ProjectInfoCacheOptions {
            size: 2,
            max_roles: 2,
            ttl: Duration::from_secs(1),
        });
        let project_id = "project".into();
        let endpoint_id = "endpoint".into();
        let user1: SmolStr = "user1".into();
        let user2: SmolStr = "user2".into();
        let secret1 = AuthSecret::Scram(ServerSecret::mock(user1.as_str(), [1; 32]));
        let secret2 = AuthSecret::Scram(ServerSecret::mock(user2.as_str(), [2; 32]));
        let allowed_ips = Arc::new(vec!["allowed_ip1".into(), "allowed_ip2".into()]);
        cache.insert_role_secret(&project_id, &endpoint_id, &user1, secret1.clone());
        cache.insert_role_secret(&project_id, &endpoint_id, &user2, secret2.clone());
        cache.insert_allowed_ips(&project_id, &endpoint_id, allowed_ips.clone());

        cache.disable_ttl();
        std::thread::sleep(Duration::from_secs(2));
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
}
