mod credentials;
mod flow;

use crate::auth_backend::{console, legacy_console, link, postgres};
use crate::config::{AuthBackendType, ProxyConfig};
use crate::error::UserFacingError;
use crate::stream::PqStream;
use crate::{auth_backend, compute, waiters};
use std::io;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

pub use credentials::ClientCredentials;
pub use flow::*;

/// Common authentication error.
#[derive(Debug, Error)]
pub enum AuthErrorImpl {
    /// Authentication error reported by the console.
    #[error(transparent)]
    Console(#[from] auth_backend::AuthError),

    #[error(transparent)]
    GetAuthInfo(#[from] auth_backend::console::ConsoleAuthError),

    #[error(transparent)]
    Sasl(#[from] crate::sasl::Error),

    /// For passwords that couldn't be processed by [`parse_password`].
    #[error("Malformed password message")]
    MalformedPassword,

    /// Errors produced by [`PqStream`].
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl AuthErrorImpl {
    pub fn auth_failed(msg: impl Into<String>) -> Self {
        AuthErrorImpl::Console(auth_backend::AuthError::auth_failed(msg))
    }
}

impl From<waiters::RegisterError> for AuthErrorImpl {
    fn from(e: waiters::RegisterError) -> Self {
        AuthErrorImpl::Console(auth_backend::AuthError::from(e))
    }
}

impl From<waiters::WaitError> for AuthErrorImpl {
    fn from(e: waiters::WaitError) -> Self {
        AuthErrorImpl::Console(auth_backend::AuthError::from(e))
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
        AuthError(Box::new(e.into()))
    }
}

impl UserFacingError for AuthError {
    fn to_string_client(&self) -> String {
        use AuthErrorImpl::*;
        match self.0.as_ref() {
            Console(e) => e.to_string_client(),
            MalformedPassword => self.to_string(),
            _ => "Internal error".to_string(),
        }
    }
}

async fn handle_user(
    config: &ProxyConfig,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin + Send>,
    creds: ClientCredentials,
) -> Result<compute::NodeInfo, AuthError> {
    match config.auth_backend {
        AuthBackendType::LegacyConsole => {
            legacy_console::handle_user(
                &config.auth_endpoint,
                &config.auth_link_uri,
                client,
                &creds,
            )
            .await
        }
        AuthBackendType::Console => {
            console::handle_user(config.auth_endpoint.as_ref(), client, &creds).await
        }
        AuthBackendType::Postgres => {
            postgres::handle_user(&config.auth_endpoint, client, &creds).await
        }
        AuthBackendType::Link => link::handle_user(config.auth_link_uri.as_ref(), client).await,
    }
}
