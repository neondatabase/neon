//! Client authentication mechanisms.

pub mod backend;
pub use backend::Backend;

mod credentials;
pub(crate) use credentials::{
    check_peer_addr_is_in_list, endpoint_sni, ComputeUserInfoMaybeEndpoint,
    ComputeUserInfoParseError, IpPattern,
};

mod password_hack;
pub(crate) use password_hack::parse_endpoint_param;
use password_hack::PasswordHackPayload;

mod flow;
pub(crate) use flow::*;
use tokio::time::error::Elapsed;

use crate::{
    console,
    error::{ReportableError, UserFacingError},
};
use std::{io, net::IpAddr};
use thiserror::Error;

/// Convenience wrapper for the authentication error.
pub(crate) type Result<T> = std::result::Result<T, AuthError>;

/// Common authentication error.
#[derive(Debug, Error)]
pub(crate) enum AuthErrorImpl {
    #[error(transparent)]
    Web(#[from] backend::WebAuthError),

    #[error(transparent)]
    GetAuthInfo(#[from] console::errors::GetAuthInfoError),

    /// SASL protocol errors (includes [SCRAM](crate::scram)).
    #[error(transparent)]
    Sasl(#[from] crate::sasl::Error),

    #[error("Unsupported authentication method: {0}")]
    BadAuthMethod(Box<str>),

    #[error("Malformed password message: {0}")]
    MalformedPassword(&'static str),

    #[error(
        "Endpoint ID is not specified. \
        Either please upgrade the postgres client library (libpq) for SNI support \
        or pass the endpoint ID (first part of the domain name) as a parameter: '?options=endpoint%3D<endpoint-id>'. \
        See more at https://neon.tech/sni"
    )]
    MissingEndpointName,

    #[error("password authentication failed for user '{0}'")]
    AuthFailed(Box<str>),

    /// Errors produced by e.g. [`crate::stream::PqStream`].
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(
        "This IP address {0} is not allowed to connect to this endpoint. \
        Please add it to the allowed list in the Neon console. \
        Make sure to check for IPv4 or IPv6 addresses."
    )]
    IpAddressNotAllowed(IpAddr),

    #[error("Too many connections to this endpoint. Please try again later.")]
    TooManyConnections,

    #[error("Authentication timed out")]
    UserTimeout(Elapsed),

    #[error("Disconnected due to inactivity after {0}.")]
    ConfirmationTimeout(humantime::Duration),
}

#[derive(Debug, Error)]
#[error(transparent)]
pub(crate) struct AuthError(Box<AuthErrorImpl>);

impl AuthError {
    pub(crate) fn bad_auth_method(name: impl Into<Box<str>>) -> Self {
        AuthErrorImpl::BadAuthMethod(name.into()).into()
    }

    pub(crate) fn auth_failed(user: impl Into<Box<str>>) -> Self {
        AuthErrorImpl::AuthFailed(user.into()).into()
    }

    pub(crate) fn ip_address_not_allowed(ip: IpAddr) -> Self {
        AuthErrorImpl::IpAddressNotAllowed(ip).into()
    }

    pub(crate) fn too_many_connections() -> Self {
        AuthErrorImpl::TooManyConnections.into()
    }

    pub(crate) fn is_auth_failed(&self) -> bool {
        matches!(self.0.as_ref(), AuthErrorImpl::AuthFailed(_))
    }

    pub(crate) fn user_timeout(elapsed: Elapsed) -> Self {
        AuthErrorImpl::UserTimeout(elapsed).into()
    }

    pub(crate) fn confirmation_timeout(timeout: humantime::Duration) -> Self {
        AuthErrorImpl::ConfirmationTimeout(timeout).into()
    }
}

impl<E: Into<AuthErrorImpl>> From<E> for AuthError {
    fn from(e: E) -> Self {
        Self(Box::new(e.into()))
    }
}

impl UserFacingError for AuthError {
    fn to_string_client(&self) -> String {
        match self.0.as_ref() {
            AuthErrorImpl::Web(e) => e.to_string_client(),
            AuthErrorImpl::GetAuthInfo(e) => e.to_string_client(),
            AuthErrorImpl::Sasl(e) => e.to_string_client(),
            AuthErrorImpl::AuthFailed(_) => self.to_string(),
            AuthErrorImpl::BadAuthMethod(_) => self.to_string(),
            AuthErrorImpl::MalformedPassword(_) => self.to_string(),
            AuthErrorImpl::MissingEndpointName => self.to_string(),
            AuthErrorImpl::Io(_) => "Internal error".to_string(),
            AuthErrorImpl::IpAddressNotAllowed(_) => self.to_string(),
            AuthErrorImpl::TooManyConnections => self.to_string(),
            AuthErrorImpl::UserTimeout(_) => self.to_string(),
            AuthErrorImpl::ConfirmationTimeout(_) => self.to_string(),
        }
    }
}

impl ReportableError for AuthError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self.0.as_ref() {
            AuthErrorImpl::Web(e) => e.get_error_kind(),
            AuthErrorImpl::GetAuthInfo(e) => e.get_error_kind(),
            AuthErrorImpl::Sasl(e) => e.get_error_kind(),
            AuthErrorImpl::AuthFailed(_) => crate::error::ErrorKind::User,
            AuthErrorImpl::BadAuthMethod(_) => crate::error::ErrorKind::User,
            AuthErrorImpl::MalformedPassword(_) => crate::error::ErrorKind::User,
            AuthErrorImpl::MissingEndpointName => crate::error::ErrorKind::User,
            AuthErrorImpl::Io(_) => crate::error::ErrorKind::ClientDisconnect,
            AuthErrorImpl::IpAddressNotAllowed(_) => crate::error::ErrorKind::User,
            AuthErrorImpl::TooManyConnections => crate::error::ErrorKind::RateLimit,
            AuthErrorImpl::UserTimeout(_) => crate::error::ErrorKind::User,
            AuthErrorImpl::ConfirmationTimeout(_) => crate::error::ErrorKind::User,
        }
    }
}
