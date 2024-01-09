use std::{
    collections::HashSet,
    convert::Infallible,
    sync::{atomic::AtomicBool, Arc},
    time::Instant,
};

use dashmap::{DashMap, RwLock};
use rand::{thread_rng, Rng};
use smol_str::SmolStr;
use tracing::{debug, info};

use crate::{config::ProjectInfoCacheOptions, console::AuthSecret};

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

#[derive(Default)]
struct EndpointInfo {
    secret: std::collections::HashMap<SmolStr, Entry<AuthSecret>>,
    allowed_ips: Option<Entry<Arc<Vec<SmolStr>>>>,
}

impl EndpointInfo {
    pub fn new_with_secret(role_name: SmolStr, secret: AuthSecret) -> Self {
        let mut secret_map = std::collections::HashMap::new();
        secret_map.insert(role_name, secret.into());
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
    fn check_ignore_cache(ignore_cache_since: Option<Instant>, created_at: Instant) -> bool {
        match ignore_cache_since {
            None => false,
            Some(t) => t < created_at,
        }
    }
    pub fn get_role_secret(
        &self,
        role_name: &SmolStr,
        valid_since: Instant,
        ignore_cache_since: Option<Instant>,
    ) -> Option<(AuthSecret, bool)> {
        if let Some(secret) = self.secret.get(role_name) {
            if valid_since < secret.created_at {
                return Some((
                    secret.value.clone(),
                    Self::check_ignore_cache(ignore_cache_since, secret.created_at),
                ));
            }
        }
        None
    }

    pub fn get_allowed_ips(
        &self,
        valid_since: Instant,
        ignore_cache_since: Option<Instant>,
    ) -> Option<(Arc<Vec<SmolStr>>, bool)> {
        if let Some(allowed_ips) = &self.allowed_ips {
            if valid_since < allowed_ips.created_at {
                return Some((
                    allowed_ips.value.clone(),
                    Self::check_ignore_cache(ignore_cache_since, allowed_ips.created_at),
                ));
            }
        }
        None
    }
    pub fn invalidate_allowed_ips(&mut self) {
        self.allowed_ips = None;
    }
    pub fn invalidate_role_secret(&mut self, role_name: &SmolStr) {
        self.secret.remove(role_name);
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

    ttl_disabled_since: RwLock<Instant>,
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
            if let Some(mut endpoint_info) = self.cache.get_mut(&endpoint_id) {
                endpoint_info.invalidate_allowed_ips();
            }
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
            if let Some(mut endpoint_info) = self.cache.get_mut(&endpoint_id) {
                endpoint_info.invalidate_role_secret(role_name);
            }
        }
    }
    fn enable_ttl(&self) {
        self.ttl_enabled
            .store(true, std::sync::atomic::Ordering::Relaxed)
    }

    fn disable_ttl(&self) {
        self.ttl_enabled
            .store(false, std::sync::atomic::Ordering::Relaxed);
        self.ttl_disabled_since
            .write()
            .clone_from(&(Instant::now() + self.config.ttl));
    }
}

impl ProjectInfoCacheImpl {
    pub fn new(config: ProjectInfoCacheOptions) -> Self {
        Self {
            cache: DashMap::new(),
            project2ep: DashMap::new(),
            config,
            ttl_enabled: true.into(),
            ttl_disabled_since: RwLock::new(Instant::now()),
        }
    }

    pub fn get_role_secret(
        &self,
        endpoint_id: &SmolStr,
        role_name: &SmolStr,
    ) -> Option<Cached<&Self, AuthSecret>> {
        let (valid_since, ignore_cache_since) = self.get_cache_times();
        if let Some(endpoint_info) = self.cache.get(endpoint_id) {
            let value = endpoint_info.get_role_secret(role_name, valid_since, ignore_cache_since);
            if let Some((value, ignore_cache)) = value {
                if !ignore_cache {
                    let cached = Cached {
                        token: Some((
                            self,
                            CachedLookupInfo::new_role_secret(
                                endpoint_id.clone(),
                                role_name.clone(),
                            ),
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
        let (valid_since, ignore_cache_since) = self.get_cache_times();
        if let Some(endpoint_info) = self.cache.get(endpoint_id) {
            let value = endpoint_info.get_allowed_ips(valid_since, ignore_cache_since);
            if let Some((value, ignore_cache)) = value {
                if !ignore_cache {
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
        role_name: &SmolStr,
        secret: AuthSecret,
    ) {
        if self.cache.len() >= self.config.size {
            // If there are too many entries, wait until the next gc cycle.
            return;
        }
        self.inser_project2endpoint(project_id, endpoint_id);
        if let Some(mut endpoint_info) = self.cache.get_mut(endpoint_id) {
            if endpoint_info.secret.len() < self.config.max_roles {
                endpoint_info
                    .secret
                    .insert(role_name.clone(), secret.into());
            }
        } else {
            self.cache.insert(
                endpoint_id.clone(),
                EndpointInfo::new_with_secret(role_name.clone(), secret),
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
    fn get_cache_times(&self) -> (Instant, Option<Instant>) {
        let mut valid_since = Instant::now() - self.config.ttl;
        // Only ignore cache if ttl is disabled.
        let ignore_cache_since = if !self.ttl_enabled.load(std::sync::atomic::Ordering::Relaxed) {
            let ignore_cache_since = *self.ttl_disabled_since.read();
            // We are fine if entry is not older than ttl or was added before we are getting notifications.
            valid_since = valid_since.min(ignore_cache_since);
            Some(ignore_cache_since)
        } else {
            None
        };
        (valid_since, ignore_cache_since)
    }

    pub async fn gc_worker(&self) -> anyhow::Result<Infallible> {
        let mut interval =
            tokio::time::interval(self.config.gc_interval / (self.cache.shards().len()) as u32);
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
                if let Some(mut endpoint_info) = self.cache.get_mut(&key.endpoint_id) {
                    endpoint_info.invalidate_role_secret(role_name);
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
    use std::{sync::Arc, time::Duration};

    #[test]
    fn test_project_info_cache_settings() {
        let cache = ProjectInfoCacheImpl::new(ProjectInfoCacheOptions {
            size: 2,
            max_roles: 2,
            ttl: Duration::from_secs(1),
            gc_interval: Duration::from_secs(600),
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
        let cache = Arc::new(ProjectInfoCacheImpl::new(ProjectInfoCacheOptions {
            size: 2,
            max_roles: 2,
            ttl: Duration::from_millis(500),
            gc_interval: Duration::from_secs(600),
        }));
        cache.clone().disable_ttl();
        std::thread::sleep(Duration::from_secs(1));

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

        std::thread::sleep(Duration::from_secs(1));
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

    #[test]
    fn test_disable_ttl_invalidate_added_before() {
        let cache = Arc::new(ProjectInfoCacheImpl::new(ProjectInfoCacheOptions {
            size: 2,
            max_roles: 2,
            ttl: Duration::from_millis(500),
            gc_interval: Duration::from_secs(600),
        }));

        let project_id = "project".into();
        let endpoint_id = "endpoint".into();
        let user1: SmolStr = "user1".into();
        let user2: SmolStr = "user2".into();
        let secret1 = AuthSecret::Scram(ServerSecret::mock(user1.as_str(), [1; 32]));
        let secret2 = AuthSecret::Scram(ServerSecret::mock(user2.as_str(), [2; 32]));
        let allowed_ips = Arc::new(vec!["allowed_ip1".into(), "allowed_ip2".into()]);
        cache.insert_role_secret(&project_id, &endpoint_id, &user1, secret1.clone());
        cache.clone().disable_ttl();
        std::thread::sleep(Duration::from_millis(100));
        cache.insert_role_secret(&project_id, &endpoint_id, &user2, secret2.clone());

        // Added before ttl was disabled + ttl should be still cached.
        let cached = cache.get_role_secret(&endpoint_id, &user1).unwrap();
        assert!(cached.cached());
        let cached = cache.get_role_secret(&endpoint_id, &user2).unwrap();
        assert!(cached.cached());

        std::thread::sleep(Duration::from_secs(1));
        // Added before ttl was disabled + ttl should expire.
        assert!(cache.get_role_secret(&endpoint_id, &user1).is_none());
        assert!(cache.get_role_secret(&endpoint_id, &user2).is_none());

        // Added after ttl was disabled + ttl should not be cached.
        cache.insert_allowed_ips(&project_id, &endpoint_id, allowed_ips.clone());
        let cached = cache.get_allowed_ips(&endpoint_id).unwrap();
        assert!(!cached.cached());

        std::thread::sleep(Duration::from_secs(1));
        // Added before ttl was disabled + ttl still should expire.
        assert!(cache.get_role_secret(&endpoint_id, &user1).is_none());
        assert!(cache.get_role_secret(&endpoint_id, &user2).is_none());
        // Shouldn't be invalidated.

        let cached = cache.get_allowed_ips(&endpoint_id).unwrap();
        assert!(!cached.cached());
        assert_eq!(cached.value, allowed_ips);
    }
}
