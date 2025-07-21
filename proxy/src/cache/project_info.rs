use std::collections::{HashMap, HashSet, hash_map};
use std::convert::Infallible;
use std::time::Duration;

use async_trait::async_trait;
use clashmap::ClashMap;
use clashmap::mapref::one::Ref;
use rand::{Rng, thread_rng};
use tokio::time::Instant;
use tracing::{debug, info};

use crate::config::ProjectInfoCacheOptions;
use crate::control_plane::messages::{ControlPlaneErrorMessage, Reason};
use crate::control_plane::{EndpointAccessControl, RoleAccessControl};
use crate::intern::{AccountIdInt, EndpointIdInt, ProjectIdInt, RoleNameInt};
use crate::types::{EndpointId, RoleName};

#[async_trait]
pub(crate) trait ProjectInfoCache {
    fn invalidate_endpoint_access(&self, endpoint_id: EndpointIdInt);
    fn invalidate_endpoint_access_for_project(&self, project_id: ProjectIdInt);
    fn invalidate_endpoint_access_for_org(&self, account_id: AccountIdInt);
    fn invalidate_role_secret_for_project(&self, project_id: ProjectIdInt, role_name: RoleNameInt);
}

struct Entry<T> {
    expires_at: Instant,
    value: T,
}

impl<T> Entry<T> {
    pub(crate) fn new(value: T, ttl: Duration) -> Self {
        Self {
            expires_at: Instant::now() + ttl,
            value,
        }
    }

    pub(crate) fn get(&self) -> Option<&T> {
        (!self.is_expired()).then_some(&self.value)
    }

    fn is_expired(&self) -> bool {
        self.expires_at <= Instant::now()
    }
}

struct EndpointInfo {
    role_controls: HashMap<RoleNameInt, Entry<ControlPlaneResult<RoleAccessControl>>>,
    controls: Option<Entry<ControlPlaneResult<EndpointAccessControl>>>,
}

type ControlPlaneResult<T> = Result<T, Box<ControlPlaneErrorMessage>>;

impl EndpointInfo {
    pub(crate) fn get_role_secret_with_ttl(
        &self,
        role_name: RoleNameInt,
    ) -> Option<(ControlPlaneResult<RoleAccessControl>, Duration)> {
        let entry = self.role_controls.get(&role_name)?;
        let ttl = entry.expires_at - Instant::now();
        Some((entry.get()?.clone(), ttl))
    }

    pub(crate) fn get_controls_with_ttl(
        &self,
    ) -> Option<(ControlPlaneResult<EndpointAccessControl>, Duration)> {
        let entry = self.controls.as_ref()?;
        let ttl = entry.expires_at - Instant::now();
        Some((entry.get()?.clone(), ttl))
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
}

impl ProjectInfoCacheImpl {
    pub(crate) fn new(config: ProjectInfoCacheOptions) -> Self {
        Self {
            cache: ClashMap::new(),
            project2ep: ClashMap::new(),
            account2ep: ClashMap::new(),
            config,
        }
    }

    fn get_endpoint_cache(
        &self,
        endpoint_id: &EndpointId,
    ) -> Option<Ref<'_, EndpointIdInt, EndpointInfo>> {
        let endpoint_id = EndpointIdInt::get(endpoint_id)?;
        self.cache.get(&endpoint_id)
    }

    pub(crate) fn get_role_secret_with_ttl(
        &self,
        endpoint_id: &EndpointId,
        role_name: &RoleName,
    ) -> Option<(ControlPlaneResult<RoleAccessControl>, Duration)> {
        let role_name = RoleNameInt::get(role_name)?;
        let endpoint_info = self.get_endpoint_cache(endpoint_id)?;
        endpoint_info.get_role_secret_with_ttl(role_name)
    }

    pub(crate) fn get_endpoint_access_with_ttl(
        &self,
        endpoint_id: &EndpointId,
    ) -> Option<(ControlPlaneResult<EndpointAccessControl>, Duration)> {
        let endpoint_info = self.get_endpoint_cache(endpoint_id)?;
        endpoint_info.get_controls_with_ttl()
    }

    pub(crate) fn insert_endpoint_access(
        &self,
        account_id: Option<AccountIdInt>,
        project_id: Option<ProjectIdInt>,
        endpoint_id: EndpointIdInt,
        role_name: RoleNameInt,
        controls: EndpointAccessControl,
        role_controls: RoleAccessControl,
    ) {
        if let Some(account_id) = account_id {
            self.insert_account2endpoint(account_id, endpoint_id);
        }
        if let Some(project_id) = project_id {
            self.insert_project2endpoint(project_id, endpoint_id);
        }

        if self.cache.len() >= self.config.size {
            // If there are too many entries, wait until the next gc cycle.
            return;
        }

        debug!(
            key = &*endpoint_id,
            "created a cache entry for endpoint access"
        );

        let controls = Some(Entry::new(Ok(controls), self.config.ttl));
        let role_controls = Entry::new(Ok(role_controls), self.config.ttl);

        match self.cache.entry(endpoint_id) {
            clashmap::Entry::Vacant(e) => {
                e.insert(EndpointInfo {
                    role_controls: HashMap::from_iter([(role_name, role_controls)]),
                    controls,
                });
            }
            clashmap::Entry::Occupied(mut e) => {
                let ep = e.get_mut();
                ep.controls = controls;
                if ep.role_controls.len() < self.config.max_roles {
                    ep.role_controls.insert(role_name, role_controls);
                }
            }
        }
    }

    pub(crate) fn insert_endpoint_access_err(
        &self,
        endpoint_id: EndpointIdInt,
        role_name: RoleNameInt,
        msg: Box<ControlPlaneErrorMessage>,
        ttl: Option<Duration>,
    ) {
        if self.cache.len() >= self.config.size {
            // If there are too many entries, wait until the next gc cycle.
            return;
        }

        debug!(
            key = &*endpoint_id,
            "created a cache entry for an endpoint access error"
        );

        let ttl = ttl.unwrap_or(self.config.ttl);

        let controls = if msg.get_reason() == Reason::RoleProtected {
            // RoleProtected is the only role-specific error that control plane can give us.
            // If a given role name does not exist, it still returns a successful response,
            // just with an empty secret.
            None
        } else {
            // We can cache all the other errors in EndpointInfo.controls,
            // because they don't depend on what role name we pass to control plane.
            Some(Entry::new(Err(msg.clone()), ttl))
        };

        let role_controls = Entry::new(Err(msg), ttl);

        match self.cache.entry(endpoint_id) {
            clashmap::Entry::Vacant(e) => {
                e.insert(EndpointInfo {
                    role_controls: HashMap::from_iter([(role_name, role_controls)]),
                    controls,
                });
            }
            clashmap::Entry::Occupied(mut e) => {
                let ep = e.get_mut();
                if let Some(entry) = &ep.controls
                    && !entry.is_expired()
                    && entry.value.is_ok()
                {
                    // If we have cached non-expired, non-error controls, keep them.
                } else {
                    ep.controls = controls;
                }
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

        if role_controls.get().is_expired() {
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
    use super::*;
    use crate::control_plane::messages::{Details, EndpointRateLimitConfig, ErrorInfo, Status};
    use crate::control_plane::{AccessBlockerFlags, AuthSecret};
    use crate::scram::ServerSecret;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_project_info_cache_settings() {
        tokio::time::pause();
        let cache = ProjectInfoCacheImpl::new(ProjectInfoCacheOptions {
            size: 2,
            max_roles: 2,
            ttl: Duration::from_secs(1),
            gc_interval: Duration::from_secs(600),
        });
        let project_id: Option<ProjectIdInt> = Some(ProjectIdInt::from(&"project".into()));
        let endpoint_id: EndpointId = "endpoint".into();
        let account_id = None;

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
            project_id,
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
            project_id,
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

        let (cached, ttl) = cache
            .get_role_secret_with_ttl(&endpoint_id, &user1)
            .unwrap();
        assert_eq!(cached.unwrap().secret, secret1);
        assert_eq!(ttl, cache.config.ttl);

        let (cached, ttl) = cache
            .get_role_secret_with_ttl(&endpoint_id, &user2)
            .unwrap();
        assert_eq!(cached.unwrap().secret, secret2);
        assert_eq!(ttl, cache.config.ttl);

        // Shouldn't add more than 2 roles.
        let user3: RoleName = "user3".into();
        let secret3 = Some(AuthSecret::Scram(ServerSecret::mock([3; 32])));

        cache.insert_endpoint_access(
            account_id,
            project_id,
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

        assert!(
            cache
                .get_role_secret_with_ttl(&endpoint_id, &user3)
                .is_none()
        );

        let cached = cache
            .get_endpoint_access_with_ttl(&endpoint_id)
            .unwrap()
            .0
            .unwrap();
        assert_eq!(cached.allowed_ips, allowed_ips);

        tokio::time::advance(Duration::from_secs(2)).await;
        let cached = cache.get_role_secret_with_ttl(&endpoint_id, &user1);
        assert!(cached.is_none());
        let cached = cache.get_role_secret_with_ttl(&endpoint_id, &user2);
        assert!(cached.is_none());
        let cached = cache.get_endpoint_access_with_ttl(&endpoint_id);
        assert!(cached.is_none());
    }

    #[tokio::test]
    async fn test_caching_project_info_errors() {
        let cache = ProjectInfoCacheImpl::new(ProjectInfoCacheOptions {
            size: 10,
            max_roles: 10,
            ttl: Duration::from_secs(1),
            gc_interval: Duration::from_secs(600),
        });
        let project_id = Some(ProjectIdInt::from(&"project".into()));
        let endpoint_id: EndpointId = "endpoint".into();
        let account_id = None;

        let user1: RoleName = "user1".into();
        let user2: RoleName = "user2".into();
        let secret = Some(AuthSecret::Scram(ServerSecret::mock([1; 32])));

        let role_msg = Box::new(ControlPlaneErrorMessage {
            error: "role is protected and cannot be used for password-based authentication"
                .to_owned()
                .into_boxed_str(),
            http_status_code: http::StatusCode::NOT_FOUND,
            status: Some(Status {
                code: "PERMISSION_DENIED".to_owned().into_boxed_str(),
                message: "role is protected and cannot be used for password-based authentication"
                    .to_owned()
                    .into_boxed_str(),
                details: Details {
                    error_info: Some(ErrorInfo {
                        reason: Reason::RoleProtected,
                    }),
                    retry_info: None,
                    user_facing_message: None,
                },
            }),
        });

        let generic_msg = Box::new(ControlPlaneErrorMessage {
            error: "oh noes".to_owned().into_boxed_str(),
            http_status_code: http::StatusCode::NOT_FOUND,
            status: None,
        });

        let get_role_secret = |endpoint_id, role_name| {
            cache
                .get_role_secret_with_ttl(endpoint_id, role_name)
                .unwrap()
                .0
        };
        let get_endpoint_access =
            |endpoint_id| cache.get_endpoint_access_with_ttl(endpoint_id).unwrap().0;

        // stores role-specific errors only for get_role_secret
        cache.insert_endpoint_access_err(
            (&endpoint_id).into(),
            (&user1).into(),
            role_msg.clone(),
            None,
        );
        assert_eq!(
            get_role_secret(&endpoint_id, &user1).unwrap_err().error,
            role_msg.error
        );
        assert!(cache.get_endpoint_access_with_ttl(&endpoint_id).is_none());

        // stores non-role specific errors for both get_role_secret and get_endpoint_access
        cache.insert_endpoint_access_err(
            (&endpoint_id).into(),
            (&user1).into(),
            generic_msg.clone(),
            None,
        );
        assert_eq!(
            get_role_secret(&endpoint_id, &user1).unwrap_err().error,
            generic_msg.error
        );
        assert_eq!(
            get_endpoint_access(&endpoint_id).unwrap_err().error,
            generic_msg.error
        );

        // error isn't returned for other roles in the same endpoint
        assert!(
            cache
                .get_role_secret_with_ttl(&endpoint_id, &user2)
                .is_none()
        );

        // success for a role does not overwrite errors for other roles
        cache.insert_endpoint_access(
            account_id,
            project_id,
            (&endpoint_id).into(),
            (&user2).into(),
            EndpointAccessControl {
                allowed_ips: Arc::new(vec![]),
                allowed_vpce: Arc::new(vec![]),
                flags: AccessBlockerFlags::default(),
                rate_limits: EndpointRateLimitConfig::default(),
            },
            RoleAccessControl {
                secret: secret.clone(),
            },
        );
        assert!(get_role_secret(&endpoint_id, &user1).is_err());
        assert!(get_role_secret(&endpoint_id, &user2).is_ok());
        // ...but does clear the access control error
        assert!(get_endpoint_access(&endpoint_id).is_ok());

        // storing an error does not overwrite successful access control response
        cache.insert_endpoint_access_err(
            (&endpoint_id).into(),
            (&user2).into(),
            generic_msg.clone(),
            None,
        );
        assert!(get_role_secret(&endpoint_id, &user2).is_err());
        assert!(get_endpoint_access(&endpoint_id).is_ok());
    }
}
