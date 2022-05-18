//! Client authentication mechanisms.

pub mod backend;
pub use backend::DatabaseInfo;

mod credentials;
pub use credentials::ClientCredentials;

mod flow;
pub use flow::*;

use crate::{error::UserFacingError, waiters};
use std::io;
use thiserror::Error;

/// Convenience wrapper for the authentication error.
pub type Result<T> = std::result::Result<T, AuthError>;

/// Common authentication error.
#[derive(Debug, Error)]
pub enum AuthErrorImpl {
    /// Authentication error reported by the console.
    #[error(transparent)]
    Console(#[from] backend::AuthError),

    #[error(transparent)]
    GetAuthInfo(#[from] backend::console::ConsoleAuthError),

    #[error(transparent)]
    Sasl(#[from] crate::sasl::Error),

    /// For passwords that couldn't be processed by [`backend::legacy_console::parse_password`].
    #[error("Malformed password message")]
    MalformedPassword,

    /// Errors produced by [`crate::stream::PqStream`].
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl AuthErrorImpl {
    pub fn auth_failed(msg: impl Into<String>) -> Self {
        Self::Console(backend::AuthError::auth_failed(msg))
    }
}

impl From<waiters::RegisterError> for AuthErrorImpl {
    fn from(e: waiters::RegisterError) -> Self {
        Self::Console(backend::AuthError::from(e))
    }
}

impl From<waiters::WaitError> for AuthErrorImpl {
    fn from(e: waiters::WaitError) -> Self {
        Self::Console(backend::AuthError::from(e))
    }
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct AuthError(Box<AuthErrorImpl>);

impl<T> From<T> for AuthError
where
    AuthErrorImpl: From<T>,
{
    fn from(e: T) -> Self {
        Self(Box::new(e.into()))
    }
}

impl UserFacingError for AuthError {
    fn to_string_client(&self) -> String {
        use AuthErrorImpl::*;
        match self.0.as_ref() {
            Console(e) => e.to_string_client(),
            GetAuthInfo(e) => e.to_string_client(),
            MalformedPassword => self.to_string(),
            _ => "Internal error".to_string(),
        }
    }
}
