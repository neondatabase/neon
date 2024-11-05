use thiserror::Error;

use crate::control_plane::client::ApiLockError;
use crate::control_plane::messages::{self, ControlPlaneErrorMessage, Reason};
use crate::error::{io_error, ErrorKind, ReportableError, UserFacingError};
use crate::proxy::retry::CouldRetry;

/// A go-to error message which doesn't leak any detail.
pub(crate) const REQUEST_FAILED: &str = "Console request failed";

/// Common console API error.
#[derive(Debug, Error)]
pub(crate) enum ControlPlaneError {
    /// Error returned by the console itself.
    #[error("{REQUEST_FAILED} with {0}")]
    Message(Box<ControlPlaneErrorMessage>),

    /// Various IO errors like broken pipe or malformed payload.
    #[error("{REQUEST_FAILED}: {0}")]
    Transport(#[from] std::io::Error),
}

impl ControlPlaneError {
    /// Returns HTTP status code if it's the reason for failure.
    pub(crate) fn get_reason(&self) -> messages::Reason {
        match self {
            ControlPlaneError::Message(e) => e.get_reason(),
            ControlPlaneError::Transport(_) => messages::Reason::Unknown,
        }
    }
}

impl UserFacingError for ControlPlaneError {
    fn to_string_client(&self) -> String {
        match self {
            // To minimize risks, only select errors are forwarded to users.
            ControlPlaneError::Message(c) => c.get_user_facing_message(),
            ControlPlaneError::Transport(_) => REQUEST_FAILED.to_owned(),
        }
    }
}

impl ReportableError for ControlPlaneError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            ControlPlaneError::Message(e) => match e.get_reason() {
                Reason::RoleProtected => ErrorKind::User,
                Reason::ResourceNotFound => ErrorKind::User,
                Reason::ProjectNotFound => ErrorKind::User,
                Reason::EndpointNotFound => ErrorKind::User,
                Reason::BranchNotFound => ErrorKind::User,
                Reason::RateLimitExceeded => ErrorKind::ServiceRateLimit,
                Reason::NonDefaultBranchComputeTimeExceeded => ErrorKind::Quota,
                Reason::ActiveTimeQuotaExceeded => ErrorKind::Quota,
                Reason::ComputeTimeQuotaExceeded => ErrorKind::Quota,
                Reason::WrittenDataQuotaExceeded => ErrorKind::Quota,
                Reason::DataTransferQuotaExceeded => ErrorKind::Quota,
                Reason::LogicalSizeQuotaExceeded => ErrorKind::Quota,
                Reason::ConcurrencyLimitReached => ErrorKind::ControlPlane,
                Reason::LockAlreadyTaken => ErrorKind::ControlPlane,
                Reason::RunningOperations => ErrorKind::ControlPlane,
                Reason::ActiveEndpointsLimitExceeded => ErrorKind::ControlPlane,
                Reason::Unknown => ErrorKind::ControlPlane,
            },
            ControlPlaneError::Transport(_) => crate::error::ErrorKind::ControlPlane,
        }
    }
}

impl CouldRetry for ControlPlaneError {
    fn could_retry(&self) -> bool {
        match self {
            // retry some transport errors
            Self::Transport(io) => io.could_retry(),
            Self::Message(e) => e.could_retry(),
        }
    }
}

impl From<reqwest::Error> for ControlPlaneError {
    fn from(e: reqwest::Error) -> Self {
        io_error(e).into()
    }
}

impl From<reqwest_middleware::Error> for ControlPlaneError {
    fn from(e: reqwest_middleware::Error) -> Self {
        io_error(e).into()
    }
}

#[derive(Debug, Error)]
pub(crate) enum GetAuthInfoError {
    // We shouldn't include the actual secret here.
    #[error("Console responded with a malformed auth secret")]
    BadSecret,

    #[error(transparent)]
    ApiError(ControlPlaneError),
}

// This allows more useful interactions than `#[from]`.
impl<E: Into<ControlPlaneError>> From<E> for GetAuthInfoError {
    fn from(e: E) -> Self {
        Self::ApiError(e.into())
    }
}

impl UserFacingError for GetAuthInfoError {
    fn to_string_client(&self) -> String {
        match self {
            // We absolutely should not leak any secrets!
            Self::BadSecret => REQUEST_FAILED.to_owned(),
            // However, API might return a meaningful error.
            Self::ApiError(e) => e.to_string_client(),
        }
    }
}

impl ReportableError for GetAuthInfoError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            Self::BadSecret => crate::error::ErrorKind::ControlPlane,
            Self::ApiError(_) => crate::error::ErrorKind::ControlPlane,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum WakeComputeError {
    #[error("Console responded with a malformed compute address: {0}")]
    BadComputeAddress(Box<str>),

    #[error(transparent)]
    ControlPlane(ControlPlaneError),

    #[error("Too many connections attempts")]
    TooManyConnections,

    #[error("error acquiring resource permit: {0}")]
    TooManyConnectionAttempts(#[from] ApiLockError),
}

// This allows more useful interactions than `#[from]`.
impl<E: Into<ControlPlaneError>> From<E> for WakeComputeError {
    fn from(e: E) -> Self {
        Self::ControlPlane(e.into())
    }
}

impl UserFacingError for WakeComputeError {
    fn to_string_client(&self) -> String {
        match self {
            // We shouldn't show user the address even if it's broken.
            // Besides, user is unlikely to care about this detail.
            Self::BadComputeAddress(_) => REQUEST_FAILED.to_owned(),
            // However, control plane might return a meaningful error.
            Self::ControlPlane(e) => e.to_string_client(),

            Self::TooManyConnections => self.to_string(),

            Self::TooManyConnectionAttempts(_) => {
                "Failed to acquire permit to connect to the database. Too many database connection attempts are currently ongoing.".to_owned()
            }
        }
    }
}

impl ReportableError for WakeComputeError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            Self::BadComputeAddress(_) => crate::error::ErrorKind::ControlPlane,
            Self::ControlPlane(e) => e.get_error_kind(),
            Self::TooManyConnections => crate::error::ErrorKind::RateLimit,
            Self::TooManyConnectionAttempts(e) => e.get_error_kind(),
        }
    }
}

impl CouldRetry for WakeComputeError {
    fn could_retry(&self) -> bool {
        match self {
            Self::BadComputeAddress(_) => false,
            Self::ControlPlane(e) => e.could_retry(),
            Self::TooManyConnections => false,
            Self::TooManyConnectionAttempts(_) => false,
        }
    }
}

#[derive(Debug, Error)]
pub enum GetEndpointJwksError {
    #[error("endpoint not found")]
    EndpointNotFound,

    #[error("failed to build control plane request: {0}")]
    RequestBuild(#[source] reqwest::Error),

    #[error("failed to send control plane request: {0}")]
    RequestExecute(#[source] reqwest_middleware::Error),

    #[error(transparent)]
    ControlPlane(#[from] ControlPlaneError),

    #[cfg(any(test, feature = "testing"))]
    #[error(transparent)]
    TokioPostgres(#[from] tokio_postgres::Error),

    #[cfg(any(test, feature = "testing"))]
    #[error(transparent)]
    ParseUrl(#[from] url::ParseError),

    #[cfg(any(test, feature = "testing"))]
    #[error(transparent)]
    TaskJoin(#[from] tokio::task::JoinError),
}
