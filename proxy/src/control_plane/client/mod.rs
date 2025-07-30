pub mod cplane_proxy_v1;
#[cfg(any(test, feature = "testing"))]
pub mod mock;

use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;
use tracing::{debug, info};

use super::{EndpointAccessControl, RoleAccessControl};
use crate::auth::backend::ComputeUserInfo;
use crate::auth::backend::jwt::{AuthRule, FetchAuthRules, FetchAuthRulesError};
use crate::cache::node_info::{CachedNodeInfo, NodeInfoCache};
use crate::cache::project_info::ProjectInfoCache;
use crate::config::{CacheOptions, ProjectInfoCacheOptions};
use crate::context::RequestContext;
use crate::control_plane::{ControlPlaneApi, errors};
use crate::error::ReportableError;
use crate::metrics::ApiLockMetrics;
use crate::rate_limiter::{DynamicLimiter, Outcome, RateLimiterConfig, Token};
use crate::types::EndpointId;

#[non_exhaustive]
#[derive(Clone)]
pub enum ControlPlaneClient {
    /// Proxy V1 control plane API
    ProxyV1(cplane_proxy_v1::NeonControlPlaneClient),
    /// Local mock control plane.
    #[cfg(any(test, feature = "testing"))]
    PostgresMock(mock::MockControlPlane),
    /// Internal testing
    #[cfg(test)]
    #[allow(private_interfaces)]
    Test(Box<dyn TestControlPlaneClient>),
}

impl ControlPlaneApi for ControlPlaneClient {
    async fn get_role_access_control(
        &self,
        ctx: &RequestContext,
        endpoint: &EndpointId,
        role: &crate::types::RoleName,
    ) -> Result<RoleAccessControl, errors::GetAuthInfoError> {
        match self {
            Self::ProxyV1(api) => api.get_role_access_control(ctx, endpoint, role).await,
            #[cfg(any(test, feature = "testing"))]
            Self::PostgresMock(api) => api.get_role_access_control(ctx, endpoint, role).await,
            #[cfg(test)]
            Self::Test(_api) => {
                unreachable!("this function should never be called in the test backend")
            }
        }
    }

    async fn get_endpoint_access_control(
        &self,
        ctx: &RequestContext,
        endpoint: &EndpointId,
        role: &crate::types::RoleName,
    ) -> Result<EndpointAccessControl, errors::GetAuthInfoError> {
        match self {
            Self::ProxyV1(api) => api.get_endpoint_access_control(ctx, endpoint, role).await,
            #[cfg(any(test, feature = "testing"))]
            Self::PostgresMock(api) => api.get_endpoint_access_control(ctx, endpoint, role).await,
            #[cfg(test)]
            Self::Test(api) => api.get_access_control(),
        }
    }

    async fn get_endpoint_jwks(
        &self,
        ctx: &RequestContext,
        endpoint: &EndpointId,
    ) -> Result<Vec<AuthRule>, errors::GetEndpointJwksError> {
        match self {
            Self::ProxyV1(api) => api.get_endpoint_jwks(ctx, endpoint).await,
            #[cfg(any(test, feature = "testing"))]
            Self::PostgresMock(api) => api.get_endpoint_jwks(ctx, endpoint).await,
            #[cfg(test)]
            Self::Test(_api) => Ok(vec![]),
        }
    }

    async fn wake_compute(
        &self,
        ctx: &RequestContext,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, errors::WakeComputeError> {
        match self {
            Self::ProxyV1(api) => api.wake_compute(ctx, user_info).await,
            #[cfg(any(test, feature = "testing"))]
            Self::PostgresMock(api) => api.wake_compute(ctx, user_info).await,
            #[cfg(test)]
            Self::Test(api) => api.wake_compute(),
        }
    }
}

#[cfg(test)]
pub(crate) trait TestControlPlaneClient: Send + Sync + 'static {
    fn wake_compute(&self) -> Result<CachedNodeInfo, errors::WakeComputeError>;

    fn get_access_control(&self) -> Result<EndpointAccessControl, errors::GetAuthInfoError>;

    fn dyn_clone(&self) -> Box<dyn TestControlPlaneClient>;
}

#[cfg(test)]
impl Clone for Box<dyn TestControlPlaneClient> {
    fn clone(&self) -> Self {
        TestControlPlaneClient::dyn_clone(&**self)
    }
}

/// Various caches for [`control_plane`](super).
pub struct ApiCaches {
    /// Cache for the `wake_compute` API method.
    pub(crate) node_info: NodeInfoCache,
    /// Cache which stores project_id -> endpoint_ids mapping.
    pub project_info: Arc<ProjectInfoCache>,
}

impl ApiCaches {
    pub fn new(
        wake_compute_cache_config: CacheOptions,
        project_info_cache_config: ProjectInfoCacheOptions,
    ) -> Self {
        Self {
            node_info: NodeInfoCache::new(wake_compute_cache_config),
            project_info: Arc::new(ProjectInfoCache::new(project_info_cache_config)),
        }
    }
}

/// Various caches for [`control_plane`](super).
pub struct ApiLocks<K> {
    name: &'static str,
    node_locks: papaya::HashMap<K, Arc<DynamicLimiter>>,
    config: RateLimiterConfig,
    timeout: Duration,
    epoch: std::time::Duration,
    metrics: &'static ApiLockMetrics,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApiLockError {
    #[error("timeout acquiring resource permit")]
    TimeoutError(#[from] tokio::time::error::Elapsed),
}

impl ReportableError for ApiLockError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            ApiLockError::TimeoutError(_) => crate::error::ErrorKind::RateLimit,
        }
    }
}

impl<K: Hash + Eq + Clone> ApiLocks<K> {
    pub fn new(
        name: &'static str,
        config: RateLimiterConfig,
        timeout: Duration,
        epoch: std::time::Duration,
        metrics: &'static ApiLockMetrics,
    ) -> Self {
        Self {
            name,
            node_locks: papaya::HashMap::new(),
            config,
            timeout,
            epoch,
            metrics,
        }
    }

    pub(crate) async fn get_permit(&self, key: &K) -> Result<WakeComputePermit, ApiLockError> {
        if self.config.initial_limit == 0 {
            return Ok(WakeComputePermit {
                permit: Token::disabled(),
            });
        }

        let now = Instant::now();

        let semaphore = self
            .node_locks
            .pin()
            .get_or_insert_with(key.clone(), || {
                self.metrics.semaphores_registered.inc();
                DynamicLimiter::new(self.config)
            })
            .clone();
        let permit = semaphore.acquire_timeout(self.timeout).await;

        self.metrics
            .semaphore_acquire_seconds
            .observe(now.elapsed().as_secs_f64());

        if permit.is_ok() {
            debug!(elapsed = ?now.elapsed(), "acquired permit");
        } else {
            debug!(elapsed = ?now.elapsed(), "timed out acquiring permit");
        }
        Ok(WakeComputePermit { permit: permit? })
    }

    pub async fn garbage_collect_worker(&self) {
        if self.config.initial_limit == 0 {
            return;
        }
        let mut interval = tokio::time::interval(self.epoch);
        loop {
            interval.tick().await;
            info!(name = self.name, "performing epoch reclamation on api lock");

            let timer = self.metrics.reclamation_lag_seconds.start_timer();

            let mut count = 0;
            let guard = self.node_locks.pin();
            for (key, sem) in &guard {
                // check if we might be able to remove
                if Arc::strong_count(sem) == 1 {
                    // try and atomically remove
                    let res = guard.remove_if(key, |_key, sem| Arc::strong_count(sem) == 1);
                    if let Ok(Some(..)) = res {
                        count += 1;
                    }
                }
            }
            drop(guard);
            timer.observe();
            self.metrics.semaphores_unregistered.inc_by(count as u64);
        }
    }
}

pub(crate) struct WakeComputePermit {
    permit: Token,
}

impl WakeComputePermit {
    pub(crate) fn should_check_cache(&self) -> bool {
        !self.permit.is_disabled()
    }
    pub(crate) fn release(self, outcome: Outcome) {
        self.permit.release(outcome);
    }
    pub(crate) fn release_result<T, E>(self, res: Result<T, E>) -> Result<T, E> {
        match res {
            Ok(_) => self.release(Outcome::Success),
            Err(_) => self.release(Outcome::Overload),
        }
        res
    }
}

impl FetchAuthRules for ControlPlaneClient {
    async fn fetch_auth_rules(
        &self,
        ctx: &RequestContext,
        endpoint: EndpointId,
    ) -> Result<Vec<AuthRule>, FetchAuthRulesError> {
        self.get_endpoint_jwks(ctx, &endpoint)
            .await
            .map_err(FetchAuthRulesError::GetEndpointJwks)
    }
}
