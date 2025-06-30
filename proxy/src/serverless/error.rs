use http::StatusCode;
use http::header::HeaderName;

use crate::auth::ComputeUserInfoParseError;
use crate::error::{ErrorKind, ReportableError, UserFacingError};
use crate::http::ReadBodyError;

pub trait HttpCodeError {
    fn get_http_status_code(&self) -> StatusCode;
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConnInfoError {
    #[error("invalid header: {0}")]
    InvalidHeader(&'static HeaderName),
    #[error("invalid connection string: {0}")]
    UrlParseError(#[from] url::ParseError),
    #[error("incorrect scheme")]
    IncorrectScheme,
    #[error("missing database name")]
    MissingDbName,
    #[error("invalid database name")]
    InvalidDbName,
    #[error("missing username")]
    MissingUsername,
    #[error("invalid username: {0}")]
    InvalidUsername(#[from] std::string::FromUtf8Error),
    #[error("missing authentication credentials: {0}")]
    MissingCredentials(Credentials),
    #[error("missing hostname")]
    MissingHostname,
    #[error("invalid hostname: {0}")]
    InvalidEndpoint(#[from] ComputeUserInfoParseError),
    #[error("malformed endpoint")]
    MalformedEndpoint,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Credentials {
    #[error("required password")]
    Password,
    #[error("required authorization bearer token in JWT format")]
    BearerJwt,
}

impl ReportableError for ConnInfoError {
    fn get_error_kind(&self) -> ErrorKind {
        ErrorKind::User
    }
}

impl UserFacingError for ConnInfoError {
    fn to_string_client(&self) -> String {
        self.to_string()
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReadPayloadError {
    #[error("could not read the HTTP request body: {0}")]
    Read(#[from] hyper::Error),
    #[error("request is too large (max is {limit} bytes)")]
    BodyTooLarge { limit: usize },
    #[error("could not parse the HTTP request body: {0}")]
    Parse(#[from] serde_json::Error),
}

impl From<ReadBodyError<hyper::Error>> for ReadPayloadError {
    fn from(value: ReadBodyError<hyper::Error>) -> Self {
        match value {
            ReadBodyError::BodyTooLarge { limit } => Self::BodyTooLarge { limit },
            ReadBodyError::Read(e) => Self::Read(e),
        }
    }
}

impl ReportableError for ReadPayloadError {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            ReadPayloadError::Read(_) => ErrorKind::ClientDisconnect,
            ReadPayloadError::BodyTooLarge { .. } => ErrorKind::User,
            ReadPayloadError::Parse(_) => ErrorKind::User,
        }
    }
}

impl HttpCodeError for ReadPayloadError {
    fn get_http_status_code(&self) -> StatusCode {
        match self {
            ReadPayloadError::Read(_) => StatusCode::BAD_REQUEST,
            ReadPayloadError::BodyTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            ReadPayloadError::Parse(_) => StatusCode::BAD_REQUEST,
        }
    }
}
