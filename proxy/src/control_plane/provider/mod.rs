#[cfg(any(test, feature = "testing"))]
pub mod mock;
pub mod neon;

use crate::{
    auth::backend::{
        jwt::{AuthRule, FetchAuthRules},
        ComputeUserInfo,
    },
    cache::{endpoints::EndpointsCache, project_info::ProjectInfoCacheImpl},
    config::{CacheOptions, EndpointCacheConfig, ProjectInfoCacheOptions},
    context::RequestMonitoring,
    control_plane::{
        api::{
            errors::{ApiLockError, GetAuthInfoError, WakeComputeError},
            ControlPlaneApi,
        },
        CachedAllowedIps, CachedNodeInfo, CachedRoleSecret, NodeInfoCache,
    },
    metrics::ApiLockMetrics,
    rate_limiter::{DynamicLimiter, Outcome, RateLimiterConfig, Token},
    EndpointId,
};
use dashmap::DashMap;
use std::{hash::Hash, sync::Arc, time::Duration};
use tokio::time::Instant;
use tracing::info;

#[non_exhaustive]
#[derive(Clone)]
pub enum ControlPlaneClient {
    /// Current Management API (V2).
    Management(neon::NeonControlPlaneClient),
    /// Local mock control plane.
    #[cfg(any(test, feature = "testing"))]
    PostgresMock(mock::MockControlPlane),
    /// Internal testing
    #[cfg(test)]
    #[allow(private_interfaces)]
    Test(Box<dyn crate::auth::backend::TestBackend>),
}

impl ControlPlaneApi for ControlPlaneClient {
    async fn get_role_secret(
        &self,
        ctx: &RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedRoleSecret, GetAuthInfoError> {
        match self {
            Self::Management(api) => api.get_role_secret(ctx, user_info).await,
            #[cfg(any(test, feature = "testing"))]
            Self::PostgresMock(api) => api.get_role_secret(ctx, user_info).await,
            #[cfg(test)]
            Self::Test(_) => {
                unreachable!("this function should never be called in the test backend")
            }
        }
    }

    async fn get_allowed_ips_and_secret(
        &self,
        ctx: &RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), GetAuthInfoError> {
        match self {
            Self::Management(api) => api.get_allowed_ips_and_secret(ctx, user_info).await,
            #[cfg(any(test, feature = "testing"))]
            Self::PostgresMock(api) => api.get_allowed_ips_and_secret(ctx, user_info).await,
            #[cfg(test)]
            Self::Test(api) => api.get_allowed_ips_and_secret(),
        }
    }

    async fn get_endpoint_jwks(
        &self,
        ctx: &RequestMonitoring,
        endpoint: EndpointId,
    ) -> anyhow::Result<Vec<AuthRule>> {
        match self {
            Self::Management(api) => api.get_endpoint_jwks(ctx, endpoint).await,
            #[cfg(any(test, feature = "testing"))]
            Self::PostgresMock(api) => api.get_endpoint_jwks(ctx, endpoint).await,
            #[cfg(test)]
            Self::Test(_api) => Ok(vec![]),
        }
    }

    async fn wake_compute(
        &self,
        ctx: &RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, WakeComputeError> {
        match self {
            Self::Management(api) => api.wake_compute(ctx, user_info).await,
            #[cfg(any(test, feature = "testing"))]
            Self::PostgresMock(api) => api.wake_compute(ctx, user_info).await,
            #[cfg(test)]
            Self::Test(api) => api.wake_compute(),
        }
    }
}

/// Various caches for [`control_plane`](super).
pub struct ApiCaches {
    /// Cache for the `wake_compute` API method.
    pub(crate) node_info: NodeInfoCache,
    /// Cache which stores project_id -> endpoint_ids mapping.
    pub project_info: Arc<ProjectInfoCacheImpl>,
    /// List of all valid endpoints.
    pub endpoints_cache: Arc<EndpointsCache>,
}

impl ApiCaches {
    pub fn new(
        wake_compute_cache_config: CacheOptions,
        project_info_cache_config: ProjectInfoCacheOptions,
        endpoint_cache_config: EndpointCacheConfig,
    ) -> Self {
        Self {
            node_info: NodeInfoCache::new(
                "node_info_cache",
                wake_compute_cache_config.size,
                wake_compute_cache_config.ttl,
                true,
            ),
            project_info: Arc::new(ProjectInfoCacheImpl::new(project_info_cache_config)),
            endpoints_cache: Arc::new(EndpointsCache::new(endpoint_cache_config)),
        }
    }
}

/// Various caches for [`control_plane`](super).
pub struct ApiLocks<K> {
    name: &'static str,
    node_locks: DashMap<K, Arc<DynamicLimiter>>,
    config: RateLimiterConfig,
    timeout: Duration,
    epoch: std::time::Duration,
    metrics: &'static ApiLockMetrics,
}

impl<K: Hash + Eq + Clone> ApiLocks<K> {
    pub fn new(
        name: &'static str,
        config: RateLimiterConfig,
        shards: usize,
        timeout: Duration,
        epoch: std::time::Duration,
        metrics: &'static ApiLockMetrics,
    ) -> prometheus::Result<Self> {
        Ok(Self {
            name,
            node_locks: DashMap::with_shard_amount(shards),
            config,
            timeout,
            epoch,
            metrics,
        })
    }

    pub(crate) async fn get_permit(&self, key: &K) -> Result<WakeComputePermit, ApiLockError> {
        if self.config.initial_limit == 0 {
            return Ok(WakeComputePermit {
                permit: Token::disabled(),
            });
        }
        let now = Instant::now();
        let semaphore = {
            // get fast path
            if let Some(semaphore) = self.node_locks.get(key) {
                semaphore.clone()
            } else {
                self.node_locks
                    .entry(key.clone())
                    .or_insert_with(|| {
                        self.metrics.semaphores_registered.inc();
                        DynamicLimiter::new(self.config)
                    })
                    .clone()
            }
        };
        let permit = semaphore.acquire_timeout(self.timeout).await;

        self.metrics
            .semaphore_acquire_seconds
            .observe(now.elapsed().as_secs_f64());
        info!("acquired permit {:?}", now.elapsed().as_secs_f64());
        Ok(WakeComputePermit { permit: permit? })
    }

    pub async fn garbage_collect_worker(&self) {
        if self.config.initial_limit == 0 {
            return;
        }
        let mut interval =
            tokio::time::interval(self.epoch / (self.node_locks.shards().len()) as u32);
        loop {
            for (i, shard) in self.node_locks.shards().iter().enumerate() {
                interval.tick().await;
                // temporary lock a single shard and then clear any semaphores that aren't currently checked out
                // race conditions: if strong_count == 1, there's no way that it can increase while the shard is locked
                // therefore releasing it is safe from race conditions
                info!(
                    name = self.name,
                    shard = i,
                    "performing epoch reclamation on api lock"
                );
                let mut lock = shard.write();
                let timer = self.metrics.reclamation_lag_seconds.start_timer();
                let count = lock
                    .extract_if(|_, semaphore| Arc::strong_count(semaphore.get_mut()) == 1)
                    .count();
                drop(lock);
                self.metrics.semaphores_unregistered.inc_by(count as u64);
                timer.observe();
            }
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
        ctx: &RequestMonitoring,
        endpoint: EndpointId,
    ) -> anyhow::Result<Vec<AuthRule>> {
        self.get_endpoint_jwks(ctx, endpoint).await
    }
}
