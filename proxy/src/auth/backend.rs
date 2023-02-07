mod classic;

mod link;
use futures::TryFutureExt;
pub use link::LinkAuthError;

use crate::{
    auth::{self, AuthFlow, ClientCredentials},
    console::{
        self,
        provider::{CachedNodeInfo, ConsoleReqExtra},
        Api,
    },
    stream, url,
};
use std::borrow::Cow;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, warn};

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
}

impl std::fmt::Display for BackendType<'_, ()> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use BackendType::*;
        match self {
            Console(endpoint, _) => fmt.debug_tuple("Console").field(&endpoint.url()).finish(),
            Postgres(endpoint, _) => fmt.debug_tuple("Postgres").field(&endpoint.url()).finish(),
            Link(url) => fmt.debug_tuple("Link").field(&url.as_str()).finish(),
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
        }
    }
}

// TODO: get rid of explicit lifetimes in this block (there's a bug in rustc).
// Read more: https://github.com/rust-lang/rust/issues/99190
// Alleged fix: https://github.com/rust-lang/rust/pull/89056
impl<'l> BackendType<'l, ClientCredentials<'_>> {
    /// Do something special if user didn't provide the `project` parameter.
    async fn try_password_hack<'a>(
        &'a mut self,
        extra: &'a ConsoleReqExtra<'a>,
        client: &'a mut stream::PqStream<impl AsyncRead + AsyncWrite + Unpin>,
        use_cleartext_password_flow: bool,
    ) -> auth::Result<Option<AuthSuccess<CachedNodeInfo>>> {
        use BackendType::*;

        // If there's no project so far, that entails that client doesn't
        // support SNI or other means of passing the project name.
        // We now expect to see a very specific payload in the place of password.
        let fetch_magic_payload = |client| async {
            warn!("project name not specified, resorting to the password hack auth flow");
            let payload = AuthFlow::new(client)
                .begin(auth::PasswordHack)
                .await?
                .authenticate()
                .await?;

            info!(project = &payload.project, "received missing parameter");
            auth::Result::Ok(payload)
        };

        // If we want to use cleartext password flow, we can read the password
        // from the client and pretend that it's a magic payload (PasswordHack hack).
        let fetch_plaintext_password = |client| async {
            info!("using cleartext password flow");
            let payload = AuthFlow::new(client)
                .begin(auth::CleartextPassword)
                .await?
                .authenticate()
                .await?;

            auth::Result::Ok(auth::password_hack::PasswordHackPayload {
                project: String::new(),
                password: payload,
            })
        };

        // TODO: find a proper way to merge those very similar blocks.
        let (mut node, password) = match self {
            Console(api, creds) if creds.project.is_none() => {
                let payload = fetch_magic_payload(client).await?;
                creds.project = Some(payload.project.into());
                let node = api.wake_compute(extra, creds).await?;

                (node, payload.password)
            }
            // This is a hack to allow cleartext password in secure connections (wss).
            Console(api, creds) if use_cleartext_password_flow => {
                let payload = fetch_plaintext_password(client).await?;
                let node = api.wake_compute(extra, creds).await?;

                (node, payload.password)
            }
            Postgres(api, creds) if creds.project.is_none() => {
                let payload = fetch_magic_payload(client).await?;
                creds.project = Some(payload.project.into());
                let node = api.wake_compute(extra, creds).await?;

                (node, payload.password)
            }
            _ => return Ok(None),
        };

        node.config.password(password);
        Ok(Some(AuthSuccess {
            reported_auth_ok: false,
            value: node,
        }))
    }

    /// Authenticate the client via the requested backend, possibly using credentials.
    ///
    /// If `use_cleartext_password_flow` is true, we use the old cleartext password
    /// flow. It is used for websocket connections, which want to minimize the number
    /// of round trips. (Plaintext password authentication requires only one round-trip,
    /// where SCRAM requires two.)
    pub async fn authenticate<'a>(
        &mut self,
        extra: &'a ConsoleReqExtra<'a>,
        client: &'a mut stream::PqStream<impl AsyncRead + AsyncWrite + Unpin>,
        use_cleartext_password_flow: bool,
    ) -> auth::Result<AuthSuccess<CachedNodeInfo>> {
        use BackendType::*;

        // Handle cases when `project` is missing in `creds`.
        // TODO: type safety: return `creds` with irrefutable `project`.
        if let Some(res) = self
            .try_password_hack(extra, client, use_cleartext_password_flow)
            .await?
        {
            info!("user successfully authenticated (using the password hack)");
            return Ok(res);
        }

        let res = match self {
            Console(api, creds) => {
                info!(
                    user = creds.user,
                    project = creds.project(),
                    "performing authentication using the console"
                );

                assert!(creds.project.is_some());
                classic::handle_user(api.as_ref(), extra, creds, client).await?
            }
            Postgres(api, creds) => {
                info!("performing mock authentication using a local postgres instance");

                assert!(creds.project.is_some());
                classic::handle_user(api.as_ref(), extra, creds, client).await?
            }
            // NOTE: this auth backend doesn't use client credentials.
            Link(url) => {
                info!("performing link authentication");

                link::handle_user(url, client)
                    .await?
                    .map(CachedNodeInfo::new_uncached)
            }
        };

        info!("user successfully authenticated");
        Ok(res)
    }

    /// When applicable, wake the compute node, gaining its connection info in the process.
    /// The link auth flow doesn't support this, so we return [`None`] in that case.
    pub async fn wake_compute<'a>(
        &self,
        extra: &'a ConsoleReqExtra<'a>,
    ) -> Result<Option<CachedNodeInfo>, console::errors::WakeComputeError> {
        use BackendType::*;

        match self {
            Console(api, creds) => api.wake_compute(extra, creds).map_ok(Some).await,
            Postgres(api, creds) => api.wake_compute(extra, creds).map_ok(Some).await,
            Link(_) => Ok(None),
        }
    }
}
