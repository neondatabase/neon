pub mod mock;
pub mod neon;

use super::messages::MetricsAuxInfo;
use crate::{
    auth::ClientCredentials,
    cache::{timed_lru, TimedLru},
    compute, scram,
};
use async_trait::async_trait;
use std::sync::Arc;

pub mod errors {
    use crate::{
        error::{io_error, UserFacingError},
        http,
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
                        // Status 423: project might be in maintenance mode (or bad state).
                        format!("{REQUEST_FAILED}: endpoint is temporary unavailable")
                    }
                    _ => REQUEST_FAILED.to_owned(),
                },
                _ => REQUEST_FAILED.to_owned(),
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
    }

    // This allows more useful interactions than `#[from]`.
    impl<E: Into<ApiError>> From<E> for WakeComputeError {
        fn from(e: E) -> Self {
            Self::ApiError(e.into())
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
            }
        }
    }
}

/// Extra query params we'd like to pass to the console.
pub struct ConsoleReqExtra<'a> {
    /// A unique identifier for a connection.
    pub session_id: uuid::Uuid,
    /// Name of client application, if set.
    pub application_name: Option<&'a str>,
}

/// Auth secret which is managed by the cloud.
pub enum AuthInfo {
    /// Md5 hash of user's password.
    Md5([u8; 16]),

    /// [SCRAM](crate::scram) authentication info.
    Scram(scram::ServerSecret),
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
    pub aux: Arc<MetricsAuxInfo>,

    /// Whether we should accept self-signed certificates (for testing)
    pub allow_self_signed_compute: bool,
}

pub type NodeInfoCache = TimedLru<Arc<str>, NodeInfo>;
pub type CachedNodeInfo = timed_lru::Cached<&'static NodeInfoCache>;

/// This will allocate per each call, but the http requests alone
/// already require a few allocations, so it should be fine.
#[async_trait]
pub trait Api {
    /// Get the client's auth secret for authentication.
    async fn get_auth_info(
        &self,
        extra: &ConsoleReqExtra<'_>,
        creds: &ClientCredentials<'_>,
    ) -> Result<Option<AuthInfo>, errors::GetAuthInfoError>;

    /// Wake up the compute node and return the corresponding connection info.
    async fn wake_compute(
        &self,
        extra: &ConsoleReqExtra<'_>,
        creds: &ClientCredentials<'_>,
    ) -> Result<CachedNodeInfo, errors::WakeComputeError>;
}

/// Various caches for [`console`](super).
pub struct ApiCaches {
    /// Cache for the `wake_compute` API method.
    pub node_info: NodeInfoCache,
}
