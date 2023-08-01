mod classic;
mod hacks;
mod link;

pub use link::LinkAuthError;

use crate::{
    auth::{self, ClientCredentials},
    console::{
        self,
        provider::{CachedNodeInfo, ConsoleReqExtra},
        Api,
    },
    stream, url,
};
use futures::TryFutureExt;
use std::borrow::Cow;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;

/// A product of successful authentication.
pub struct AuthSuccess<T> {
    /// Did we send [`pq_proto::BeMessage::AuthenticationOk`] to client?
    pub reported_auth_ok: bool,
    /// Something to be considered a positive result.
    pub value: T,
}

impl<T> AuthSuccess<T> {
    /// Very similar to [`std::option::Option::map`].
    /// Maps [`AuthSuccess<T>`] to [`AuthSuccess<R>`] by applying
    /// a function to a contained value.
    pub fn map<R>(self, f: impl FnOnce(T) -> R) -> AuthSuccess<R> {
        AuthSuccess {
            reported_auth_ok: self.reported_auth_ok,
            value: f(self.value),
        }
    }
}

/// This type serves two purposes:
///
/// * When `T` is `()`, it's just a regular auth backend selector
///   which we use in [`crate::config::ProxyConfig`].
///
/// * However, when we substitute `T` with [`ClientCredentials`],
///   this helps us provide the credentials only to those auth
///   backends which require them for the authentication process.
pub enum BackendType<'a, T> {
    /// Current Cloud API (V2).
    Console(Cow<'a, console::provider::neon::Api>, T),
    /// Local mock of Cloud API (V2).
    Postgres(Cow<'a, console::provider::mock::Api>, T),
    /// Authentication via a web browser.
    Link(Cow<'a, url::ApiUrl>),
    /// Test backend.
    Test(&'a dyn TestBackend),
}

pub trait TestBackend: Send + Sync + 'static {
    fn wake_compute(&self) -> Result<CachedNodeInfo, console::errors::WakeComputeError>;
}

impl std::fmt::Display for BackendType<'_, ()> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use BackendType::*;
        match self {
            Console(endpoint, _) => fmt.debug_tuple("Console").field(&endpoint.url()).finish(),
            Postgres(endpoint, _) => fmt.debug_tuple("Postgres").field(&endpoint.url()).finish(),
            Link(url) => fmt.debug_tuple("Link").field(&url.as_str()).finish(),
            Test(_) => fmt.debug_tuple("Test").finish(),
        }
    }
}

impl<T> BackendType<'_, T> {
    /// Very similar to [`std::option::Option::as_ref`].
    /// This helps us pass structured config to async tasks.
    pub fn as_ref(&self) -> BackendType<'_, &T> {
        use BackendType::*;
        match self {
            Console(c, x) => Console(Cow::Borrowed(c), x),
            Postgres(c, x) => Postgres(Cow::Borrowed(c), x),
            Link(c) => Link(Cow::Borrowed(c)),
            Test(x) => Test(*x),
        }
    }
}

impl<'a, T> BackendType<'a, T> {
    /// Very similar to [`std::option::Option::map`].
    /// Maps [`BackendType<T>`] to [`BackendType<R>`] by applying
    /// a function to a contained value.
    pub fn map<R>(self, f: impl FnOnce(T) -> R) -> BackendType<'a, R> {
        use BackendType::*;
        match self {
            Console(c, x) => Console(c, f(x)),
            Postgres(c, x) => Postgres(c, f(x)),
            Link(c) => Link(c),
            Test(x) => Test(x),
        }
    }
}

impl<'a, T, E> BackendType<'a, Result<T, E>> {
    /// Very similar to [`std::option::Option::transpose`].
    /// This is most useful for error handling.
    pub fn transpose(self) -> Result<BackendType<'a, T>, E> {
        use BackendType::*;
        match self {
            Console(c, x) => x.map(|x| Console(c, x)),
            Postgres(c, x) => x.map(|x| Postgres(c, x)),
            Link(c) => Ok(Link(c)),
            Test(x) => Ok(Test(x)),
        }
    }
}

/// True to its name, this function encapsulates our current auth trade-offs.
/// Here, we choose the appropriate auth flow based on circumstances.
async fn auth_quirks(
    api: &impl console::Api,
    extra: &ConsoleReqExtra<'_>,
    creds: &mut ClientCredentials<'_>,
    client: &mut stream::PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    allow_cleartext: bool,
) -> auth::Result<AuthSuccess<CachedNodeInfo>> {
    // If there's no project so far, that entails that client doesn't
    // support SNI or other means of passing the endpoint (project) name.
    // We now expect to see a very specific payload in the place of password.
    if creds.project.is_none() {
        // Password will be checked by the compute node later.
        return hacks::password_hack(api, extra, creds, client).await;
    }

    // Password hack should set the project name.
    // TODO: make `creds.project` more type-safe.
    assert!(creds.project.is_some());

    // Perform cleartext auth if we're allowed to do that.
    // Currently, we use it for websocket connections (latency).
    if allow_cleartext {
        // Password will be checked by the compute node later.
        return hacks::cleartext_hack(api, extra, creds, client).await;
    }

    // Finally, proceed with the main auth flow (SCRAM-based).
    classic::authenticate(api, extra, creds, client).await
}

impl BackendType<'_, ClientCredentials<'_>> {
    /// Get compute endpoint name from the credentials.
    pub fn get_endpoint(&self) -> Option<String> {
        use BackendType::*;

        match self {
            Console(_, creds) => creds.project.clone(),
            Postgres(_, creds) => creds.project.clone(),
            Link(_) => Some("link".to_owned()),
            Test(_) => Some("test".to_owned()),
        }
    }
    /// Authenticate the client via the requested backend, possibly using credentials.
    #[tracing::instrument(fields(allow_cleartext = allow_cleartext), skip_all)]
    pub async fn authenticate(
        &mut self,
        extra: &ConsoleReqExtra<'_>,
        client: &mut stream::PqStream<impl AsyncRead + AsyncWrite + Unpin>,
        allow_cleartext: bool,
    ) -> auth::Result<AuthSuccess<CachedNodeInfo>> {
        use BackendType::*;

        let res = match self {
            Console(api, creds) => {
                info!(
                    user = creds.user,
                    project = creds.project(),
                    "performing authentication using the console"
                );

                let api = api.as_ref();
                auth_quirks(api, extra, creds, client, allow_cleartext).await?
            }
            Postgres(api, creds) => {
                info!(
                    user = creds.user,
                    project = creds.project(),
                    "performing authentication using a local postgres instance"
                );

                let api = api.as_ref();
                auth_quirks(api, extra, creds, client, allow_cleartext).await?
            }
            // NOTE: this auth backend doesn't use client credentials.
            Link(url) => {
                info!("performing link authentication");

                link::authenticate(url, client)
                    .await?
                    .map(CachedNodeInfo::new_uncached)
            }
            Test(_) => {
                unreachable!("this function should never be called in the test backend")
            }
        };

        info!("user successfully authenticated");
        Ok(res)
    }

    /// When applicable, wake the compute node, gaining its connection info in the process.
    /// The link auth flow doesn't support this, so we return [`None`] in that case.
    pub async fn wake_compute(
        &self,
        extra: &ConsoleReqExtra<'_>,
    ) -> Result<Option<CachedNodeInfo>, console::errors::WakeComputeError> {
        use BackendType::*;

        match self {
            Console(api, creds) => api.wake_compute(extra, creds).map_ok(Some).await,
            Postgres(api, creds) => api.wake_compute(extra, creds).map_ok(Some).await,
            Link(_) => Ok(None),
            Test(x) => x.wake_compute().map(Some),
        }
    }
}
