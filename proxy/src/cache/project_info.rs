use std::collections::HashSet;
use std::convert::Infallible;
use std::time::Duration;

use clashmap::ClashMap;
use moka::sync::Cache;
use tracing::{debug, info};

use crate::cache::common::{ControlPlaneResult, CplaneExpiry};
use crate::config::ProjectInfoCacheOptions;
use crate::control_plane::messages::{ControlPlaneErrorMessage, Reason};
use crate::control_plane::{EndpointAccessControl, RoleAccessControl};
use crate::intern::{AccountIdInt, EndpointIdInt, ProjectIdInt, RoleNameInt};
use crate::types::{EndpointId, RoleName};

/// Cache for project info.
/// This is used to cache auth data for endpoints.
/// Invalidation is done by console notifications or by TTL (if console notifications are disabled).
///
/// We also store endpoint-to-project mapping in the cache, to be able to access per-endpoint data.
/// One may ask, why the data is stored per project, when on the user request there is only data about the endpoint available?
/// On the cplane side updates are done per project (or per branch), so it's easier to invalidate the whole project cache.
pub struct ProjectInfoCache {
    role_controls: Cache<(EndpointIdInt, RoleNameInt), ControlPlaneResult<RoleAccessControl>>,
    ep_controls: Cache<EndpointIdInt, ControlPlaneResult<EndpointAccessControl>>,

    project2ep: ClashMap<ProjectIdInt, HashSet<EndpointIdInt>>,
    // FIXME(stefan): we need a way to GC the account2ep map.
    account2ep: ClashMap<AccountIdInt, HashSet<EndpointIdInt>>,

    config: ProjectInfoCacheOptions,
}

impl ProjectInfoCache {
    pub fn invalidate_endpoint_access(&self, endpoint_id: EndpointIdInt) {
        info!("invalidating endpoint access for `{endpoint_id}`");
        self.ep_controls.invalidate(&endpoint_id);
    }

    pub fn invalidate_endpoint_access_for_project(&self, project_id: ProjectIdInt) {
        info!("invalidating endpoint access for project `{project_id}`");
        let endpoints = self
            .project2ep
            .get(&project_id)
            .map(|kv| kv.value().clone())
            .unwrap_or_default();
        for endpoint_id in endpoints {
            self.ep_controls.invalidate(&endpoint_id);
        }
    }

    pub fn invalidate_endpoint_access_for_org(&self, account_id: AccountIdInt) {
        info!("invalidating endpoint access for org `{account_id}`");
        let endpoints = self
            .account2ep
            .get(&account_id)
            .map(|kv| kv.value().clone())
            .unwrap_or_default();
        for endpoint_id in endpoints {
            self.ep_controls.invalidate(&endpoint_id);
        }
    }

    pub fn invalidate_role_secret_for_project(
        &self,
        project_id: ProjectIdInt,
        role_name: RoleNameInt,
    ) {
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
            self.role_controls.invalidate(&(endpoint_id, role_name));
        }
    }
}

impl ProjectInfoCache {
    pub(crate) fn new(config: ProjectInfoCacheOptions) -> Self {
        // we cache errors for 30 seconds, unless retry_at is set.
        let expiry = CplaneExpiry {
            error: Duration::from_secs(30),
        };
        Self {
            role_controls: Cache::builder()
                .name("role_access_controls")
                .max_capacity(config.size * config.max_roles)
                .time_to_live(config.ttl)
                .expire_after(expiry)
                .build(),
            ep_controls: Cache::builder()
                .name("endpoint_access_controls")
                .max_capacity(config.size)
                .time_to_live(config.ttl)
                .expire_after(expiry)
                .build(),
            project2ep: ClashMap::new(),
            account2ep: ClashMap::new(),
            config,
        }
    }

    pub(crate) fn get_role_secret(
        &self,
        endpoint_id: &EndpointId,
        role_name: &RoleName,
    ) -> Option<ControlPlaneResult<RoleAccessControl>> {
        let endpoint_id = EndpointIdInt::get(endpoint_id)?;
        let role_name = RoleNameInt::get(role_name)?;

        self.role_controls.get(&(endpoint_id, role_name))
    }

    pub(crate) fn get_endpoint_access(
        &self,
        endpoint_id: &EndpointId,
    ) -> Option<ControlPlaneResult<EndpointAccessControl>> {
        let endpoint_id = EndpointIdInt::get(endpoint_id)?;

        self.ep_controls.get(&endpoint_id)
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

        debug!(
            key = &*endpoint_id,
            "created a cache entry for endpoint access"
        );

        self.ep_controls.insert(endpoint_id, Ok(controls));
        self.role_controls
            .insert((endpoint_id, role_name), Ok(role_controls));
    }

    pub(crate) fn insert_endpoint_access_err(
        &self,
        endpoint_id: EndpointIdInt,
        role_name: RoleNameInt,
        msg: Box<ControlPlaneErrorMessage>,
    ) {
        debug!(
            key = &*endpoint_id,
            "created a cache entry for an endpoint access error"
        );

        // RoleProtected is the only role-specific error that control plane can give us.
        // If a given role name does not exist, it still returns a successful response,
        // just with an empty secret.
        if msg.get_reason() != Reason::RoleProtected {
            // We can cache all the other errors in ep_controls because they don't
            // depend on what role name we pass to control plane.
            self.ep_controls
                .entry(endpoint_id)
                .and_compute_with(|entry| match entry {
                    // leave the entry alone if it's already Ok
                    Some(entry) if entry.value().is_ok() => moka::ops::compute::Op::Nop,
                    // replace the entry
                    _ => moka::ops::compute::Op::Put(Err(msg.clone())),
                });
        }

        self.role_controls
            .insert((endpoint_id, role_name), Err(msg));
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

    pub fn maybe_invalidate_role_secret(&self, _endpoint_id: &EndpointId, _role_name: &RoleName) {
        // TODO: Expire the value early if the key is idle.
        // Currently not an issue as we would just use the TTL to decide, which is what already happens.
    }

    pub async fn gc_worker(&self) -> anyhow::Result<Infallible> {
        let mut interval = tokio::time::interval(self.config.gc_interval);
        loop {
            interval.tick().await;
            self.ep_controls.run_pending_tasks();
            self.role_controls.run_pending_tasks();
        }
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
        let cache = ProjectInfoCache::new(ProjectInfoCacheOptions {
            size: 1,
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

        let cached = cache.get_role_secret(&endpoint_id, &user1).unwrap();
        assert_eq!(cached.unwrap().secret, secret1);

        let cached = cache.get_role_secret(&endpoint_id, &user2).unwrap();
        assert_eq!(cached.unwrap().secret, secret2);

        // Shouldn't add more than 2 roles.
        let user3: RoleName = "user3".into();
        let secret3 = Some(AuthSecret::Scram(ServerSecret::mock([3; 32])));

        cache.role_controls.run_pending_tasks();
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

        cache.role_controls.run_pending_tasks();
        assert_eq!(cache.role_controls.entry_count(), 2);

        tokio::time::sleep(Duration::from_secs(2)).await;

        cache.role_controls.run_pending_tasks();
        assert_eq!(cache.role_controls.entry_count(), 0);
    }

    #[tokio::test]
    async fn test_caching_project_info_errors() {
        let cache = ProjectInfoCache::new(ProjectInfoCacheOptions {
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

        let get_role_secret =
            |endpoint_id, role_name| cache.get_role_secret(endpoint_id, role_name).unwrap();
        let get_endpoint_access = |endpoint_id| cache.get_endpoint_access(endpoint_id).unwrap();

        // stores role-specific errors only for get_role_secret
        cache.insert_endpoint_access_err((&endpoint_id).into(), (&user1).into(), role_msg.clone());
        assert_eq!(
            get_role_secret(&endpoint_id, &user1).unwrap_err().error,
            role_msg.error
        );
        assert!(cache.get_endpoint_access(&endpoint_id).is_none());

        // stores non-role specific errors for both get_role_secret and get_endpoint_access
        cache.insert_endpoint_access_err(
            (&endpoint_id).into(),
            (&user1).into(),
            generic_msg.clone(),
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
        assert!(cache.get_role_secret(&endpoint_id, &user2).is_none());

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
        );
        assert!(get_role_secret(&endpoint_id, &user2).is_err());
        assert!(get_endpoint_access(&endpoint_id).is_ok());
    }
}
