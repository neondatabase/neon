use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::equivalent::{Comparable, Equivalent};
use moka::sync::Cache;
use tracing::{debug, info};

use crate::cache::common::{
    ControlPlaneResult, CplaneExpiry, count_cache_insert, count_cache_outcome, eviction_listener,
};
use crate::config::ProjectInfoCacheOptions;
use crate::control_plane::messages::{ControlPlaneErrorMessage, Reason};
use crate::control_plane::{EndpointAccessControl, RoleAccessControl};
use crate::ext::LockExt;
use crate::intern::{AccountIdInt, EndpointIdInt, ProjectIdInt, RoleNameInt};
use crate::metrics::{CacheKind, Metrics};
use crate::types::{EndpointId, RoleName};

/// Cache for project info.
/// This is used to cache auth data for endpoints.
/// Invalidation is done by console notifications or by TTL (if console notifications are disabled).
///
/// We also store endpoint-to-project mapping in the cache, to be able to access per-endpoint data.
/// One may ask, why the data is stored per project, when on the user request there is only data about the endpoint available?
/// On the cplane side updates are done per project (or per branch), so it's easier to invalidate the whole project cache.
pub struct ProjectInfoCache {
    role_controls:
        Cache<(EndpointIdInt, RoleNameInt), ControlPlaneResult<Entry<RoleAccessControl>>>,
    ep_controls: Cache<EndpointIdInt, ControlPlaneResult<Entry<EndpointAccessControl>>>,

    project2ep: Arc<RefCountMultiSet<ProjectIdInt, EndpointIdInt>>,
    account2ep: Arc<RefCountMultiSet<AccountIdInt, EndpointIdInt>>,

    config: ProjectInfoCacheOptions,
}

type RefCount = Mutex<usize>;

// This is rather hacky.
// We use an ordered map of (K, V) -> RefCount.
// We use range queries over `(K, _)..(K+1, _)` to do the invalidation.
// We use the RefCount to know when to remove entries.
type RefCountMultiSet<K, V> = SkipMap<KeyValue<K, V>, RefCount>;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
struct KeyValue<K, V>(K, V);
struct Key<'a, K>(&'a K, bool);

impl<'a, K> Key<'a, K> {
    fn prefix(key: &'a K) -> std::ops::Range<Self> {
        Self(key, false)..Self(key, true)
    }
}

impl<'a, K: Ord, V> Equivalent<Key<'a, K>> for KeyValue<K, V> {
    fn equivalent(&self, key: &Key<'a, K>) -> bool {
        self.0 == *key.0 && !key.1
    }
}
impl<'a, K: Ord, V> Comparable<Key<'a, K>> for KeyValue<K, V> {
    fn compare(&self, key: &Key<'a, K>) -> std::cmp::Ordering {
        self.0.cmp(key.0).then(false.cmp(&key.1))
    }
}

#[derive(Clone)]
struct Entry<T> {
    project_id: Option<ProjectIdInt>,
    account_id: Option<AccountIdInt>,
    value: T,
}

impl<T> Entry<T> {
    fn dec_ref_counts(
        self,
        project2ep: &RefCountMultiSet<ProjectIdInt, EndpointIdInt>,
        account2ep: &RefCountMultiSet<AccountIdInt, EndpointIdInt>,
        endpoint_id: EndpointIdInt,
    ) {
        if let Some(project_id) = self.project_id {
            dec_ref_count(project2ep, project_id, endpoint_id);
        }
        if let Some(account_id) = self.account_id {
            dec_ref_count(account2ep, account_id, endpoint_id);
        }
    }
}

fn dec_ref_count<Id: Ord + Send + 'static>(
    id2ep: &RefCountMultiSet<Id, EndpointIdInt>,
    id: Id,
    endpoint_id: EndpointIdInt,
) {
    if let Some(entry) = id2ep.get(&KeyValue(id, endpoint_id)) {
        let mut count = entry.value().lock_propagate_poison();
        *count -= 1;
        if *count == 0 {
            // remove the entry while holding the lock
            entry.remove();
        }
    }
}

impl ProjectInfoCache {
    pub fn invalidate_endpoint_access(&self, endpoint_id: EndpointIdInt) {
        info!("invalidating endpoint access for `{endpoint_id}`");
        self.ep_controls.invalidate(&endpoint_id);
    }

    pub fn invalidate_endpoint_access_for_project(&self, project_id: ProjectIdInt) {
        info!("invalidating endpoint access for project `{project_id}`");

        for entry in self.project2ep.range(Key::prefix(&project_id)) {
            self.ep_controls.invalidate(&entry.key().1);
        }
    }

    pub fn invalidate_endpoint_access_for_org(&self, account_id: AccountIdInt) {
        info!("invalidating endpoint access for org `{account_id}`");

        for entry in self.account2ep.range(Key::prefix(&account_id)) {
            self.ep_controls.invalidate(&entry.key().1);
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

        for entry in self.project2ep.range(Key::prefix(&project_id)) {
            self.role_controls.invalidate(&(entry.key().1, role_name));
        }
    }
}

impl ProjectInfoCache {
    pub(crate) fn new(config: ProjectInfoCacheOptions) -> Self {
        Metrics::get().cache.capacity.set(
            CacheKind::ProjectInfoRoles,
            (config.size * config.max_roles) as i64,
        );
        Metrics::get()
            .cache
            .capacity
            .set(CacheKind::ProjectInfoEndpoints, config.size as i64);

        let project2ep = Arc::new(RefCountMultiSet::<ProjectIdInt, EndpointIdInt>::new());
        let account2ep = Arc::new(RefCountMultiSet::<AccountIdInt, EndpointIdInt>::new());
        let project2ep1 = Arc::clone(&project2ep);
        let project2ep2 = Arc::clone(&project2ep);
        let account2ep1 = Arc::clone(&account2ep);
        let account2ep2 = Arc::clone(&account2ep);

        // we cache errors for 30 seconds, unless retry_at is set.
        let expiry = CplaneExpiry::default();
        Self {
            role_controls: Cache::builder()
                .name("role_access_controls")
                .eviction_listener(
                    move |k, v: ControlPlaneResult<Entry<RoleAccessControl>>, cause| {
                        eviction_listener(CacheKind::ProjectInfoRoles, cause);

                        let (endpoint_id, _): (EndpointIdInt, RoleNameInt) = *k;
                        if let Ok(v) = v {
                            v.dec_ref_counts(&project2ep1, &account2ep1, endpoint_id);
                        }
                    },
                )
                .max_capacity(config.size * config.max_roles)
                .time_to_live(config.ttl)
                .expire_after(expiry)
                .build(),
            ep_controls: Cache::builder()
                .name("endpoint_access_controls")
                .eviction_listener(
                    move |k, v: ControlPlaneResult<Entry<EndpointAccessControl>>, cause| {
                        eviction_listener(CacheKind::ProjectInfoEndpoints, cause);

                        let endpoint_id: EndpointIdInt = *k;
                        if let Ok(v) = v {
                            v.dec_ref_counts(&project2ep2, &account2ep2, endpoint_id);
                        }
                    },
                )
                .max_capacity(config.size)
                .time_to_live(config.ttl)
                .expire_after(expiry)
                .build(),
            project2ep,
            account2ep,
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

        count_cache_outcome(
            CacheKind::ProjectInfoRoles,
            self.role_controls
                .get(&(endpoint_id, role_name))
                .map(|e| e.map(|e| e.value)),
        )
    }

    pub(crate) fn get_endpoint_access(
        &self,
        endpoint_id: &EndpointId,
    ) -> Option<ControlPlaneResult<EndpointAccessControl>> {
        let endpoint_id = EndpointIdInt::get(endpoint_id)?;

        count_cache_outcome(
            CacheKind::ProjectInfoEndpoints,
            self.ep_controls
                .get(&endpoint_id)
                .map(|e| e.map(|e| e.value)),
        )
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
        // 2 corresponds to how many cache inserts we do.
        if let Some(account_id) = account_id {
            self.inc_account2ep_ref(account_id, endpoint_id, 2);
        }
        if let Some(project_id) = project_id {
            self.inc_project2ep_ref(project_id, endpoint_id, 2);
        }

        debug!(
            key = &*endpoint_id,
            "created a cache entry for endpoint access"
        );

        count_cache_insert(CacheKind::ProjectInfoEndpoints);
        count_cache_insert(CacheKind::ProjectInfoRoles);

        self.ep_controls.insert(
            endpoint_id,
            Ok(Entry {
                account_id,
                project_id,
                value: controls,
            }),
        );
        self.role_controls.insert(
            (endpoint_id, role_name),
            Ok(Entry {
                account_id,
                project_id,
                value: role_controls,
            }),
        );
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
                    _ => {
                        count_cache_insert(CacheKind::ProjectInfoEndpoints);
                        moka::ops::compute::Op::Put(Err(msg.clone()))
                    }
                });
        }

        count_cache_insert(CacheKind::ProjectInfoRoles);
        self.role_controls
            .insert((endpoint_id, role_name), Err(msg));
    }

    fn inc_project2ep_ref(&self, project_id: ProjectIdInt, endpoint_id: EndpointIdInt, x: usize) {
        let entry = self
            .project2ep
            .get_or_insert(KeyValue(project_id, endpoint_id), Mutex::new(0));
        *entry.value().lock_propagate_poison() += x;
    }

    fn inc_account2ep_ref(&self, account_id: AccountIdInt, endpoint_id: EndpointIdInt, x: usize) {
        let entry = self
            .account2ep
            .get_or_insert(KeyValue(account_id, endpoint_id), Mutex::new(0));
        *entry.value().lock_propagate_poison() += x;
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
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::control_plane::messages::{Details, EndpointRateLimitConfig, ErrorInfo, Status};
    use crate::control_plane::{AccessBlockerFlags, AuthSecret};
    use crate::scram::ServerSecret;

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

        cache.ep_controls.run_pending_tasks();
        cache.role_controls.run_pending_tasks();

        // check the project mappings are there
        assert_eq!(cache.project2ep.len(), 1);

        // check the ref counts
        let entry = cache.project2ep.front().unwrap();
        assert_eq!(*entry.value().lock_propagate_poison(), 2);

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

        cache.ep_controls.run_pending_tasks();
        cache.role_controls.run_pending_tasks();

        // check the project mappings are still there
        assert_eq!(cache.project2ep.len(), 1);

        // check the ref counts
        let entry = cache.project2ep.front().unwrap();
        assert_eq!(*entry.value().lock_propagate_poison(), 3);

        // check both entries exist
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

        cache.ep_controls.run_pending_tasks();
        cache.role_controls.run_pending_tasks();

        assert_eq!(cache.role_controls.entry_count(), 2);

        // check the project mappings are still there
        assert_eq!(cache.project2ep.len(), 1);

        // check the ref counts are unchanged.
        let entry = cache.project2ep.front().unwrap();
        assert_eq!(*entry.value().lock_propagate_poison(), 3);

        tokio::time::sleep(Duration::from_secs(2)).await;

        cache.ep_controls.run_pending_tasks();
        cache.role_controls.run_pending_tasks();
        assert_eq!(cache.role_controls.entry_count(), 0);

        // check the project/account mappings are no longer there
        assert!(cache.project2ep.is_empty());
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
