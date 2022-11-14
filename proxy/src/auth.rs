//! Client authentication mechanisms.

pub mod backend;
pub use backend::{BackendType, ConsoleReqExtra};

mod credentials;
pub use credentials::ClientCredentials;

mod password_hack;
use password_hack::PasswordHackPayload;

mod flow;
pub use flow::*;

use crate::error::UserFacingError;
use std::io;
use thiserror::Error;

/// Convenience wrapper for the authentication error.
pub type Result<T> = std::result::Result<T, AuthError>;

/// Common authentication error.
#[derive(Debug, Error)]
pub enum AuthErrorImpl {
    #[error(transparent)]
    Link(#[from] backend::LinkAuthError),

    #[error(transparent)]
    GetAuthInfo(#[from] backend::GetAuthInfoError),

    #[error(transparent)]
    WakeCompute(#[from] backend::WakeComputeError),

    /// SASL protocol errors (includes [SCRAM](crate::scram)).
    #[error(transparent)]
    Sasl(#[from] crate::sasl::Error),

    #[error("Unsupported authentication method: {0}")]
    BadAuthMethod(Box<str>),

    #[error("Malformed password message: {0}")]
    MalformedPassword(&'static str),

    #[error(
        "Project ID is not specified. \
        Either please upgrade the postgres client library (libpq) for SNI support \
        or pass the project ID (first part of the domain name) as a parameter: '?options=project%3D<project-id>'. \
        See more at https://neon.tech/sni"
    )]
    MissingProjectName,

    /// Errors produced by e.g. [`crate::stream::PqStream`].
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct AuthError(Box<AuthErrorImpl>);

impl AuthError {
    pub fn bad_auth_method(name: impl Into<Box<str>>) -> Self {
        AuthErrorImpl::BadAuthMethod(name.into()).into()
    }
}

impl<E: Into<AuthErrorImpl>> From<E> for AuthError {
    fn from(e: E) -> Self {
        Self(Box::new(e.into()))
    }
}

impl UserFacingError for AuthError {
    fn to_string_client(&self) -> String {
        use AuthErrorImpl::*;
        match self.0.as_ref() {
            Link(e) => e.to_string_client(),
            GetAuthInfo(e) => e.to_string_client(),
            WakeCompute(e) => e.to_string_client(),
            Sasl(e) => e.to_string_client(),
            BadAuthMethod(_) => self.to_string(),
            MalformedPassword(_) => self.to_string(),
            MissingProjectName => self.to_string(),
            _ => "Internal error".to_string(),
        }
    }
}
