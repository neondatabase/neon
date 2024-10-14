use crate::{
    control_plane::api::messages::{self, ControlPlaneError, Reason},
    error::{io_error, ErrorKind, ReportableError, UserFacingError},
    proxy::retry::CouldRetry,
};
use thiserror::Error;

/// A go-to error message which doesn't leak any detail.
pub(crate) const REQUEST_FAILED: &str = "Console request failed";

/// Common console API error.
#[derive(Debug, Error)]
pub(crate) enum ControlPlaneApiError {
    /// Error returned by the console itself.
    #[error("{REQUEST_FAILED} with {0}")]
    ControlPlane(ControlPlaneError),

    /// Various IO errors like broken pipe or malformed payload.
    #[error("{REQUEST_FAILED}: {0}")]
    Transport(#[from] std::io::Error),
}

impl ControlPlaneApiError {
    /// Returns HTTP status code if it's the reason for failure.
    pub(crate) fn get_reason(&self) -> messages::Reason {
        match self {
            ControlPlaneApiError::ControlPlane(e) => e.get_reason(),
            ControlPlaneApiError::Transport(_) => messages::Reason::Unknown,
        }
    }
}

impl UserFacingError for ControlPlaneApiError {
    fn to_string_client(&self) -> String {
        match self {
            // To minimize risks, only select errors are forwarded to users.
            ControlPlaneApiError::ControlPlane(c) => c.get_user_facing_message(),
            ControlPlaneApiError::Transport(_) => REQUEST_FAILED.to_owned(),
        }
    }
}

impl ReportableError for ControlPlaneApiError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            ControlPlaneApiError::ControlPlane(e) => match e.get_reason() {
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
                Reason::Unknown => match &e {
                    ControlPlaneError {
                        http_status_code:
                            http::StatusCode::NOT_FOUND | http::StatusCode::NOT_ACCEPTABLE,
                        ..
                    } => crate::error::ErrorKind::User,
                    ControlPlaneError {
                        http_status_code: http::StatusCode::UNPROCESSABLE_ENTITY,
                        error,
                        ..
                    } if error
                        .contains("compute time quota of non-primary branches is exceeded") =>
                    {
                        crate::error::ErrorKind::Quota
                    }
                    ControlPlaneError {
                        http_status_code: http::StatusCode::LOCKED,
                        error,
                        ..
                    } if error.contains("quota exceeded")
                        || error.contains("the limit for current plan reached") =>
                    {
                        crate::error::ErrorKind::Quota
                    }
                    ControlPlaneError {
                        http_status_code: http::StatusCode::TOO_MANY_REQUESTS,
                        ..
                    } => crate::error::ErrorKind::ServiceRateLimit,
                    ControlPlaneError { .. } => crate::error::ErrorKind::ControlPlane,
                },
            },
            ControlPlaneApiError::Transport(_) => crate::error::ErrorKind::ControlPlane,
        }
    }
}

impl CouldRetry for ControlPlaneApiError {
    fn could_retry(&self) -> bool {
        match self {
            // retry some transport errors
            Self::Transport(io) => io.could_retry(),
            Self::ControlPlane(e) => e.could_retry(),
        }
    }
}

impl From<reqwest::Error> for ControlPlaneApiError {
    fn from(e: reqwest::Error) -> Self {
        io_error(e).into()
    }
}

impl From<reqwest_middleware::Error> for ControlPlaneApiError {
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
    ApiError(ControlPlaneApiError),
}

// This allows more useful interactions than `#[from]`.
impl<E: Into<ControlPlaneApiError>> From<E> for GetAuthInfoError {
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
    ControlPlaneApi(ControlPlaneApiError),

    #[error("Too many connections attempts")]
    TooManyConnections,

    #[error("error acquiring resource permit: {0}")]
    TooManyConnectionAttempts(#[from] ApiLockError),
}

// This allows more useful interactions than `#[from]`.
impl<E: Into<ControlPlaneApiError>> From<E> for WakeComputeError {
    fn from(e: E) -> Self {
        Self::ControlPlaneApi(e.into())
    }
}

impl UserFacingError for WakeComputeError {
    fn to_string_client(&self) -> String {
        match self {
          // We shouldn't show user the address even if it's broken.
          // Besides, user is unlikely to care about this detail.
          Self::BadComputeAddress(_) => REQUEST_FAILED.to_owned(),
          // However, API might return a meaningful error.
          Self::ControlPlaneApi(e) => e.to_string_client(),

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
            Self::ControlPlaneApi(e) => e.get_error_kind(),
            Self::TooManyConnections => crate::error::ErrorKind::RateLimit,
            Self::TooManyConnectionAttempts(e) => e.get_error_kind(),
        }
    }
}

impl CouldRetry for WakeComputeError {
    fn could_retry(&self) -> bool {
        match self {
            Self::BadComputeAddress(_) => false,
            Self::ControlPlaneApi(e) => e.could_retry(),
            Self::TooManyConnections => false,
            Self::TooManyConnectionAttempts(_) => false,
        }
    }
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
