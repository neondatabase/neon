use std::collections::{HashMap, HashSet, hash_map};
use std::convert::Infallible;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use async_trait::async_trait;
use clashmap::ClashMap;
use clashmap::mapref::one::Ref;
use rand::{Rng, thread_rng};
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{debug, info};

use crate::config::ProjectInfoCacheOptions;
use crate::control_plane::{EndpointAccessControl, RoleAccessControl};
use crate::intern::{AccountIdInt, EndpointIdInt, ProjectIdInt, RoleNameInt};
use crate::types::{EndpointId, RoleName};

#[async_trait]
pub(crate) trait ProjectInfoCache {
    fn invalidate_endpoint_access(&self, endpoint_id: EndpointIdInt);
    fn invalidate_endpoint_access_for_project(&self, project_id: ProjectIdInt);
    fn invalidate_endpoint_access_for_org(&self, account_id: AccountIdInt);
    fn invalidate_role_secret_for_project(&self, project_id: ProjectIdInt, role_name: RoleNameInt);
    async fn decrement_active_listeners(&self);
    async fn increment_active_listeners(&self);
}

struct Entry<T> {
    created_at: Instant,
    value: T,
}

impl<T> Entry<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            created_at: Instant::now(),
            value,
        }
    }

    pub(crate) fn get(&self, valid_since: Instant) -> Option<&T> {
        (valid_since < self.created_at).then_some(&self.value)
    }
}

impl<T> From<T> for Entry<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

struct EndpointInfo {
    role_controls: HashMap<RoleNameInt, Entry<RoleAccessControl>>,
    controls: Option<Entry<EndpointAccessControl>>,
}

impl EndpointInfo {
    pub(crate) fn get_role_secret(
        &self,
        role_name: RoleNameInt,
        valid_since: Instant,
    ) -> Option<RoleAccessControl> {
        let controls = self.role_controls.get(&role_name)?;
        controls.get(valid_since).cloned()
    }

    pub(crate) fn get_controls(&self, valid_since: Instant) -> Option<EndpointAccessControl> {
        let controls = self.controls.as_ref()?;
        controls.get(valid_since).cloned()
    }

    pub(crate) fn invalidate_endpoint(&mut self) {
        self.controls = None;
    }

    pub(crate) fn invalidate_role_secret(&mut self, role_name: RoleNameInt) {
        self.role_controls.remove(&role_name);
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
    cache: ClashMap<EndpointIdInt, EndpointInfo>,

    project2ep: ClashMap<ProjectIdInt, HashSet<EndpointIdInt>>,
    // FIXME(stefan): we need a way to GC the account2ep map.
    account2ep: ClashMap<AccountIdInt, HashSet<EndpointIdInt>>,
    config: ProjectInfoCacheOptions,

    start_time: Instant,
    ttl_disabled_since_us: AtomicU64,
    active_listeners_lock: Mutex<usize>,
}

#[async_trait]
impl ProjectInfoCache for ProjectInfoCacheImpl {
    fn invalidate_endpoint_access(&self, endpoint_id: EndpointIdInt) {
        info!("invalidating endpoint access for `{endpoint_id}`");
        if let Some(mut endpoint_info) = self.cache.get_mut(&endpoint_id) {
            endpoint_info.invalidate_endpoint();
        }
    }

    fn invalidate_endpoint_access_for_project(&self, project_id: ProjectIdInt) {
        info!("invalidating endpoint access for project `{project_id}`");
        let endpoints = self
            .project2ep
            .get(&project_id)
            .map(|kv| kv.value().clone())
            .unwrap_or_default();
        for endpoint_id in endpoints {
            if let Some(mut endpoint_info) = self.cache.get_mut(&endpoint_id) {
                endpoint_info.invalidate_endpoint();
            }
        }
    }

    fn invalidate_endpoint_access_for_org(&self, account_id: AccountIdInt) {
        info!("invalidating endpoint access for org `{account_id}`");
        let endpoints = self
            .account2ep
            .get(&account_id)
            .map(|kv| kv.value().clone())
            .unwrap_or_default();
        for endpoint_id in endpoints {
            if let Some(mut endpoint_info) = self.cache.get_mut(&endpoint_id) {
                endpoint_info.invalidate_endpoint();
            }
        }
    }

    fn invalidate_role_secret_for_project(&self, project_id: ProjectIdInt, role_name: RoleNameInt) {
        info!(
            "invalidating role secret for project_id `{}` and role_name `{}`",
            project_id, role_name,
        );
        let endpoints = self
            .project2ep
            .get(&project_id)
            .map(|kv| kv.value().clone())
            .unwrap_or_default();
        for endpoint_id in endpoints {
            if let Some(mut endpoint_info) = self.cache.get_mut(&endpoint_id) {
                endpoint_info.invalidate_role_secret(role_name);
            }
        }
    }

    async fn decrement_active_listeners(&self) {
        let mut listeners_guard = self.active_listeners_lock.lock().await;
        if *listeners_guard == 0 {
            tracing::error!("active_listeners count is already 0, something is broken");
            return;
        }
        *listeners_guard -= 1;
        if *listeners_guard == 0 {
            self.ttl_disabled_since_us
                .store(u64::MAX, std::sync::atomic::Ordering::SeqCst);
        }
    }

    async fn increment_active_listeners(&self) {
        let mut listeners_guard = self.active_listeners_lock.lock().await;
        *listeners_guard += 1;
        if *listeners_guard == 1 {
            let new_ttl = (self.start_time.elapsed() + self.config.ttl).as_micros() as u64;
            self.ttl_disabled_since_us
                .store(new_ttl, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

impl ProjectInfoCacheImpl {
    pub(crate) fn new(config: ProjectInfoCacheOptions) -> Self {
        Self {
            cache: ClashMap::new(),
            project2ep: ClashMap::new(),
            account2ep: ClashMap::new(),
            config,
            ttl_disabled_since_us: AtomicU64::new(u64::MAX),
            start_time: Instant::now(),
            active_listeners_lock: Mutex::new(0),
        }
    }

    fn get_endpoint_cache(
        &self,
        endpoint_id: &EndpointId,
    ) -> Option<Ref<'_, EndpointIdInt, EndpointInfo>> {
        let endpoint_id = EndpointIdInt::get(endpoint_id)?;
        self.cache.get(&endpoint_id)
    }

    pub(crate) fn get_role_secret(
        &self,
        endpoint_id: &EndpointId,
        role_name: &RoleName,
    ) -> Option<RoleAccessControl> {
        let valid_since = self.get_cache_times();
        let role_name = RoleNameInt::get(role_name)?;
        let endpoint_info = self.get_endpoint_cache(endpoint_id)?;
        endpoint_info.get_role_secret(role_name, valid_since)
    }

    pub(crate) fn get_endpoint_access(
        &self,
        endpoint_id: &EndpointId,
    ) -> Option<EndpointAccessControl> {
        let valid_since = self.get_cache_times();
        let endpoint_info = self.get_endpoint_cache(endpoint_id)?;
        endpoint_info.get_controls(valid_since)
    }

    pub(crate) fn insert_endpoint_access(
        &self,
        account_id: Option<AccountIdInt>,
        project_id: ProjectIdInt,
        endpoint_id: EndpointIdInt,
        role_name: RoleNameInt,
        controls: EndpointAccessControl,
        role_controls: RoleAccessControl,
    ) {
        if let Some(account_id) = account_id {
            self.insert_account2endpoint(account_id, endpoint_id);
        }
        self.insert_project2endpoint(project_id, endpoint_id);

        if self.cache.len() >= self.config.size {
            // If there are too many entries, wait until the next gc cycle.
            return;
        }

        let controls = Entry::from(controls);
        let role_controls = Entry::from(role_controls);

        match self.cache.entry(endpoint_id) {
            clashmap::Entry::Vacant(e) => {
                e.insert(EndpointInfo {
                    role_controls: HashMap::from_iter([(role_name, role_controls)]),
                    controls: Some(controls),
                });
            }
            clashmap::Entry::Occupied(mut e) => {
                let ep = e.get_mut();
                ep.controls = Some(controls);
                if ep.role_controls.len() < self.config.max_roles {
                    ep.role_controls.insert(role_name, role_controls);
                }
            }
        }
    }

    fn insert_project2endpoint(&self, project_id: ProjectIdInt, endpoint_id: EndpointIdInt) {
        if let Some(mut endpoints) = self.project2ep.get_mut(&project_id) {
            endpoints.insert(endpoint_id);
        } else {
            self.project2ep
                .insert(project_id, HashSet::from([endpoint_id]));
        }
    }

    fn insert_account2endpoint(&self, account_id: AccountIdInt, endpoint_id: EndpointIdInt) {
        if let Some(mut endpoints) = self.account2ep.get_mut(&account_id) {
            endpoints.insert(endpoint_id);
        } else {
            self.account2ep
                .insert(account_id, HashSet::from([endpoint_id]));
        }
    }

    fn ignore_ttl_since(&self) -> Option<Instant> {
        let ttl_disabled_since_us = self
            .ttl_disabled_since_us
            .load(std::sync::atomic::Ordering::Relaxed);

        if ttl_disabled_since_us == u64::MAX {
            return None;
        }

        Some(self.start_time + Duration::from_micros(ttl_disabled_since_us))
    }

    fn get_cache_times(&self) -> Instant {
        let mut valid_since = Instant::now() - self.config.ttl;
        if let Some(ignore_ttl_since) = self.ignore_ttl_since() {
            // We are fine if entry is not older than ttl or was added before we are getting notifications.
            valid_since = valid_since.min(ignore_ttl_since);
        }
        valid_since
    }

    pub fn maybe_invalidate_role_secret(&self, endpoint_id: &EndpointId, role_name: &RoleName) {
        let Some(endpoint_id) = EndpointIdInt::get(endpoint_id) else {
            return;
        };
        let Some(role_name) = RoleNameInt::get(role_name) else {
            return;
        };

        let Some(mut endpoint_info) = self.cache.get_mut(&endpoint_id) else {
            return;
        };

        let entry = endpoint_info.role_controls.entry(role_name);
        let hash_map::Entry::Occupied(role_controls) = entry else {
            return;
        };

        let created_at = role_controls.get().created_at;
        let expire = match self.ignore_ttl_since() {
            // if ignoring TTL, we should still try and roll the password if it's old
            // and we the client gave an incorrect password. There could be some lag on the redis channel.
            Some(_) => created_at + self.config.ttl < Instant::now(),
            // edge case: redis is down, let's be generous and invalidate the cache immediately.
            None => true,
        };

        if expire {
            role_controls.remove();
        }
    }

    pub async fn gc_worker(&self) -> anyhow::Result<Infallible> {
        let mut interval =
            tokio::time::interval(self.config.gc_interval / (self.cache.shards().len()) as u32);
        loop {
            interval.tick().await;
            if self.cache.len() < self.config.size {
                // If there are not too many entries, wait until the next gc cycle.
                continue;
            }
            self.gc();
        }
    }

    fn gc(&self) {
        let shard = thread_rng().gen_range(0..self.project2ep.shards().len());
        debug!(shard, "project_info_cache: performing epoch reclamation");

        // acquire a random shard lock
        let mut removed = 0;
        let shard = self.project2ep.shards()[shard].write();
        for (_, endpoints) in shard.iter() {
            for endpoint in endpoints {
                self.cache.remove(endpoint);
                removed += 1;
            }
        }
        // We can drop this shard only after making sure that all endpoints are removed.
        drop(shard);
        info!("project_info_cache: removed {removed} endpoints");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::control_plane::messages::EndpointRateLimitConfig;
    use crate::control_plane::{AccessBlockerFlags, AuthSecret};
    use crate::scram::ServerSecret;
    use crate::types::ProjectId;

    #[tokio::test]
    async fn test_project_info_cache_settings() {
        tokio::time::pause();
        let cache = ProjectInfoCacheImpl::new(ProjectInfoCacheOptions {
            size: 2,
            max_roles: 2,
            ttl: Duration::from_secs(1),
            gc_interval: Duration::from_secs(600),
        });
        let project_id: ProjectId = "project".into();
        let endpoint_id: EndpointId = "endpoint".into();
        let account_id: Option<AccountIdInt> = None;

        let user1: RoleName = "user1".into();
        let user2: RoleName = "user2".into();
        let secret1 = Some(AuthSecret::Scram(ServerSecret::mock([1; 32])));
        let secret2 = None;
        let allowed_ips = Arc::new(vec![
            "127.0.0.1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
        ]);

        cache.insert_endpoint_access(
            account_id,
            (&project_id).into(),
            (&endpoint_id).into(),
            (&user1).into(),
            EndpointAccessControl {
                allowed_ips: allowed_ips.clone(),
                allowed_vpce: Arc::new(vec![]),
                flags: AccessBlockerFlags::default(),
                rate_limits: EndpointRateLimitConfig::default(),
            },
            RoleAccessControl {
                secret: secret1.clone(),
            },
        );

        cache.insert_endpoint_access(
            account_id,
            (&project_id).into(),
            (&endpoint_id).into(),
            (&user2).into(),
            EndpointAccessControl {
                allowed_ips: allowed_ips.clone(),
                allowed_vpce: Arc::new(vec![]),
                flags: AccessBlockerFlags::default(),
                rate_limits: EndpointRateLimitConfig::default(),
            },
            RoleAccessControl {
                secret: secret2.clone(),
            },
        );

        let cached = cache.get_role_secret(&endpoint_id, &user1).unwrap();
        assert_eq!(cached.secret, secret1);

        let cached = cache.get_role_secret(&endpoint_id, &user2).unwrap();
        assert_eq!(cached.secret, secret2);

        // Shouldn't add more than 2 roles.
        let user3: RoleName = "user3".into();
        let secret3 = Some(AuthSecret::Scram(ServerSecret::mock([3; 32])));

        cache.insert_endpoint_access(
            account_id,
            (&project_id).into(),
            (&endpoint_id).into(),
            (&user3).into(),
            EndpointAccessControl {
                allowed_ips: allowed_ips.clone(),
                allowed_vpce: Arc::new(vec![]),
                flags: AccessBlockerFlags::default(),
                rate_limits: EndpointRateLimitConfig::default(),
            },
            RoleAccessControl {
                secret: secret3.clone(),
            },
        );

        assert!(cache.get_role_secret(&endpoint_id, &user3).is_none());

        let cached = cache.get_endpoint_access(&endpoint_id).unwrap();
        assert_eq!(cached.allowed_ips, allowed_ips);

        tokio::time::advance(Duration::from_secs(2)).await;
        let cached = cache.get_role_secret(&endpoint_id, &user1);
        assert!(cached.is_none());
        let cached = cache.get_role_secret(&endpoint_id, &user2);
        assert!(cached.is_none());
        let cached = cache.get_endpoint_access(&endpoint_id);
        assert!(cached.is_none());
    }
}
