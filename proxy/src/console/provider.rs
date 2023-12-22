#[cfg(feature = "testing")]
pub mod mock;
pub mod neon;

use super::messages::MetricsAuxInfo;
use crate::{
    auth::backend::ComputeUserInfo,
    cache::{timed_lru, TimedLru},
    compute,
    context::RequestContext,
    scram,
};
use async_trait::async_trait;
use dashmap::DashMap;
use smol_str::SmolStr;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tracing::info;

pub mod errors {
    use crate::{
        error::{io_error, UserFacingError},
        http,
        proxy::retry::ShouldRetry,
    };
    use thiserror::Error;

    /// A go-to error message which doesn't leak any detail.
    const REQUEST_FAILED: &str = "Console request failed";

    /// Common console API error.
    #[derive(Debug, Error)]
    pub enum ApiError {
        /// Error returned by the console itself.
        #[error("{REQUEST_FAILED} with {}: {}", .status, .text)]
        Console {
            status: http::StatusCode,
            text: Box<str>,
        },

        /// Various IO errors like broken pipe or malformed payload.
        #[error("{REQUEST_FAILED}: {0}")]
        Transport(#[from] std::io::Error),
    }

    impl ApiError {
        /// Returns HTTP status code if it's the reason for failure.
        pub fn http_status_code(&self) -> Option<http::StatusCode> {
            use ApiError::*;
            match self {
                Console { status, .. } => Some(*status),
                _ => None,
            }
        }
    }

    impl UserFacingError for ApiError {
        fn to_string_client(&self) -> String {
            use ApiError::*;
            match self {
                // To minimize risks, only select errors are forwarded to users.
                // Ask @neondatabase/control-plane for review before adding more.
                Console { status, .. } => match *status {
                    http::StatusCode::NOT_FOUND => {
                        // Status 404: failed to get a project-related resource.
                        format!("{REQUEST_FAILED}: endpoint cannot be found")
                    }
                    http::StatusCode::NOT_ACCEPTABLE => {
                        // Status 406: endpoint is disabled (we don't allow connections).
                        format!("{REQUEST_FAILED}: endpoint is disabled")
                    }
                    http::StatusCode::LOCKED => {
                        // Status 423: project might be in maintenance mode (or bad state), or quotas exceeded.
                        format!("{REQUEST_FAILED}: endpoint is temporary unavailable. check your quotas and/or contact our support")
                    }
                    _ => REQUEST_FAILED.to_owned(),
                },
                _ => REQUEST_FAILED.to_owned(),
            }
        }
    }

    impl ShouldRetry for ApiError {
        fn could_retry(&self) -> bool {
            match self {
                // retry some transport errors
                Self::Transport(io) => io.could_retry(),
                // retry some temporary failures because the compute was in a bad state
                // (bad request can be returned when the endpoint was in transition)
                Self::Console {
                    status: http::StatusCode::BAD_REQUEST,
                    ..
                } => true,
                // locked can be returned when the endpoint was in transition
                // or when quotas are exceeded. don't retry when quotas are exceeded
                Self::Console {
                    status: http::StatusCode::LOCKED,
                    ref text,
                } => {
                    // written data quota exceeded
                    // data transfer quota exceeded
                    // compute time quota exceeded
                    // logical size quota exceeded
                    !text.contains("quota exceeded")
                        && !text.contains("the limit for current plan reached")
                }
                _ => false,
            }
        }
    }

    impl From<reqwest::Error> for ApiError {
        fn from(e: reqwest::Error) -> Self {
            io_error(e).into()
        }
    }

    impl From<reqwest_middleware::Error> for ApiError {
        fn from(e: reqwest_middleware::Error) -> Self {
            io_error(e).into()
        }
    }

    #[derive(Debug, Error)]
    pub enum GetAuthInfoError {
        // We shouldn't include the actual secret here.
        #[error("Console responded with a malformed auth secret")]
        BadSecret,

        #[error(transparent)]
        ApiError(ApiError),
    }

    // This allows more useful interactions than `#[from]`.
    impl<E: Into<ApiError>> From<E> for GetAuthInfoError {
        fn from(e: E) -> Self {
            Self::ApiError(e.into())
        }
    }

    impl UserFacingError for GetAuthInfoError {
        fn to_string_client(&self) -> String {
            use GetAuthInfoError::*;
            match self {
                // We absolutely should not leak any secrets!
                BadSecret => REQUEST_FAILED.to_owned(),
                // However, API might return a meaningful error.
                ApiError(e) => e.to_string_client(),
            }
        }
    }
    #[derive(Debug, Error)]
    pub enum WakeComputeError {
        #[error("Console responded with a malformed compute address: {0}")]
        BadComputeAddress(Box<str>),

        #[error(transparent)]
        ApiError(ApiError),

        #[error("Timeout waiting to acquire wake compute lock")]
        TimeoutError,
    }

    // This allows more useful interactions than `#[from]`.
    impl<E: Into<ApiError>> From<E> for WakeComputeError {
        fn from(e: E) -> Self {
            Self::ApiError(e.into())
        }
    }

    impl From<tokio::sync::AcquireError> for WakeComputeError {
        fn from(_: tokio::sync::AcquireError) -> Self {
            WakeComputeError::TimeoutError
        }
    }
    impl From<tokio::time::error::Elapsed> for WakeComputeError {
        fn from(_: tokio::time::error::Elapsed) -> Self {
            WakeComputeError::TimeoutError
        }
    }

    impl UserFacingError for WakeComputeError {
        fn to_string_client(&self) -> String {
            use WakeComputeError::*;
            match self {
                // We shouldn't show user the address even if it's broken.
                // Besides, user is unlikely to care about this detail.
                BadComputeAddress(_) => REQUEST_FAILED.to_owned(),
                // However, API might return a meaningful error.
                ApiError(e) => e.to_string_client(),

                TimeoutError => "timeout while acquiring the compute resource lock".to_owned(),
            }
        }
    }
}

/// Extra query params we'd like to pass to the console.
pub struct ConsoleReqExtra {
    pub options: Vec<(String, String)>,
}

impl ConsoleReqExtra {
    // https://swagger.io/docs/specification/serialization/ DeepObject format
    // paramName[prop1]=value1&paramName[prop2]=value2&....
    pub fn options_as_deep_object(&self) -> Vec<(String, String)> {
        self.options
            .iter()
            .map(|(k, v)| (format!("options[{}]", k), v.to_string()))
            .collect()
    }
}

/// Auth secret which is managed by the cloud.
#[derive(Clone)]
pub enum AuthSecret {
    #[cfg(feature = "testing")]
    /// Md5 hash of user's password.
    Md5([u8; 16]),

    /// [SCRAM](crate::scram) authentication info.
    Scram(scram::ServerSecret),
}

#[derive(Default)]
pub struct AuthInfo {
    pub secret: Option<AuthSecret>,
    /// List of IP addresses allowed for the autorization.
    pub allowed_ips: Vec<String>,
}

/// Info for establishing a connection to a compute node.
/// This is what we get after auth succeeded, but not before!
#[derive(Clone)]
pub struct NodeInfo {
    /// Compute node connection params.
    /// It's sad that we have to clone this, but this will improve
    /// once we migrate to a bespoke connection logic.
    pub config: compute::ConnCfg,

    /// Labels for proxy's metrics.
    pub aux: MetricsAuxInfo,

    /// Whether we should accept self-signed certificates (for testing)
    pub allow_self_signed_compute: bool,
}

pub type NodeInfoCache = TimedLru<Arc<str>, NodeInfo>;
pub type CachedNodeInfo = timed_lru::Cached<&'static NodeInfoCache>;
pub type AllowedIpsCache = TimedLru<SmolStr, Arc<Vec<String>>>;
pub type RoleSecretCache = TimedLru<(SmolStr, SmolStr), Option<AuthSecret>>;
pub type CachedRoleSecret = timed_lru::Cached<&'static RoleSecretCache>;

/// This will allocate per each call, but the http requests alone
/// already require a few allocations, so it should be fine.
#[async_trait]
pub trait Api {
    /// Get the client's auth secret for authentication.
    async fn get_role_secret(
        &self,
        ctx: &mut RequestContext,
        creds: &ComputeUserInfo,
    ) -> Result<CachedRoleSecret, errors::GetAuthInfoError>;

    async fn get_allowed_ips(
        &self,
        ctx: &mut RequestContext,
        creds: &ComputeUserInfo,
    ) -> Result<Arc<Vec<String>>, errors::GetAuthInfoError>;

    /// Wake up the compute node and return the corresponding connection info.
    async fn wake_compute(
        &self,
        ctx: &mut RequestContext,
        extra: &ConsoleReqExtra,
        creds: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, errors::WakeComputeError>;
}

/// Various caches for [`console`](super).
pub struct ApiCaches {
    /// Cache for the `wake_compute` API method.
    pub node_info: NodeInfoCache,
    /// Cache for the `get_allowed_ips`. TODO(anna): use notifications listener instead.
    pub allowed_ips: AllowedIpsCache,
    /// Cache for the `get_role_secret`. TODO(anna): use notifications listener instead.
    pub role_secret: RoleSecretCache,
}

/// Various caches for [`console`](super).
pub struct ApiLocks {
    name: &'static str,
    node_locks: DashMap<Arc<str>, Arc<Semaphore>>,
    permits: usize,
    timeout: Duration,
    registered: prometheus::IntCounter,
    unregistered: prometheus::IntCounter,
    reclamation_lag: prometheus::Histogram,
    lock_acquire_lag: prometheus::Histogram,
}

impl ApiLocks {
    pub fn new(
        name: &'static str,
        permits: usize,
        shards: usize,
        timeout: Duration,
    ) -> prometheus::Result<Self> {
        let registered = prometheus::IntCounter::with_opts(
            prometheus::Opts::new(
                "semaphores_registered",
                "Number of semaphores registered in this api lock",
            )
            .namespace(name),
        )?;
        prometheus::register(Box::new(registered.clone()))?;
        let unregistered = prometheus::IntCounter::with_opts(
            prometheus::Opts::new(
                "semaphores_unregistered",
                "Number of semaphores unregistered in this api lock",
            )
            .namespace(name),
        )?;
        prometheus::register(Box::new(unregistered.clone()))?;
        let reclamation_lag = prometheus::Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "reclamation_lag_seconds",
                "Time it takes to reclaim unused semaphores in the api lock",
            )
            .namespace(name)
            // 1us -> 65ms
            // benchmarks on my mac indicate it's usually in the range of 256us and 512us
            .buckets(prometheus::exponential_buckets(1e-6, 2.0, 16)?),
        )?;
        prometheus::register(Box::new(reclamation_lag.clone()))?;
        let lock_acquire_lag = prometheus::Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "semaphore_acquire_seconds",
                "Time it takes to reclaim unused semaphores in the api lock",
            )
            .namespace(name)
            // 0.1ms -> 6s
            .buckets(prometheus::exponential_buckets(1e-4, 2.0, 16)?),
        )?;
        prometheus::register(Box::new(lock_acquire_lag.clone()))?;

        Ok(Self {
            name,
            node_locks: DashMap::with_shard_amount(shards),
            permits,
            timeout,
            lock_acquire_lag,
            registered,
            unregistered,
            reclamation_lag,
        })
    }

    pub async fn get_wake_compute_permit(
        &self,
        key: &Arc<str>,
    ) -> Result<WakeComputePermit, errors::WakeComputeError> {
        if self.permits == 0 {
            return Ok(WakeComputePermit { permit: None });
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
                        self.registered.inc();
                        Arc::new(Semaphore::new(self.permits))
                    })
                    .clone()
            }
        };
        let permit = tokio::time::timeout_at(now + self.timeout, semaphore.acquire_owned()).await;

        self.lock_acquire_lag
            .observe((Instant::now() - now).as_secs_f64());

        Ok(WakeComputePermit {
            permit: Some(permit??),
        })
    }

    pub async fn garbage_collect_worker(&self, epoch: std::time::Duration) {
        if self.permits == 0 {
            return;
        }

        let mut interval = tokio::time::interval(epoch / (self.node_locks.shards().len()) as u32);
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
                let timer = self.reclamation_lag.start_timer();
                let count = lock
                    .extract_if(|_, semaphore| Arc::strong_count(semaphore.get_mut()) == 1)
                    .count();
                drop(lock);
                self.unregistered.inc_by(count as u64);
                timer.observe_duration()
            }
        }
    }
}

pub struct WakeComputePermit {
    // None if the lock is disabled
    permit: Option<OwnedSemaphorePermit>,
}

impl WakeComputePermit {
    pub fn should_check_cache(&self) -> bool {
        self.permit.is_some()
    }
}
