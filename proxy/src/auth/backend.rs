mod postgres;

mod link;
pub use link::LinkAuthError;

mod console;
pub use console::{GetAuthInfoError, WakeComputeError};

use crate::{
    auth::{self, AuthFlow, ClientCredentials},
    cache::{timed_lru, TimedLru},
    compute,
    console::messages::MetricsAuxInfo,
    http, mgmt, stream, url,
    waiters::{self, Waiter, Waiters},
};
use once_cell::sync::Lazy;
use std::{borrow::Cow, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, warn};

static CPLANE_WAITERS: Lazy<Waiters<mgmt::ComputeReady>> = Lazy::new(Default::default);

/// Give caller an opportunity to wait for the cloud's reply.
pub async fn with_waiter<R, T, E>(
    psql_session_id: impl Into<String>,
    action: impl FnOnce(Waiter<'static, mgmt::ComputeReady>) -> R,
) -> Result<T, E>
where
    R: std::future::Future<Output = Result<T, E>>,
    E: From<waiters::RegisterError>,
{
    let waiter = CPLANE_WAITERS.register(psql_session_id.into())?;
    action(waiter).await
}

pub fn notify(psql_session_id: &str, msg: mgmt::ComputeReady) -> Result<(), waiters::NotifyError> {
    CPLANE_WAITERS.notify(psql_session_id, msg)
}

/// Extra query params we'd like to pass to the console.
pub struct ConsoleReqExtra<'a> {
    /// A unique identifier for a connection.
    pub session_id: uuid::Uuid,
    /// Name of client application, if set.
    pub application_name: Option<&'a str>,
}

/// This type serves two purposes:
///
/// * When `T` is `()`, it's just a regular auth backend selector
///   which we use in [`crate::config::ProxyConfig`].
///
/// * However, when we substitute `T` with [`ClientCredentials`],
///   this helps us provide the credentials only to those auth
///   backends which require them for the authentication process.
#[derive(Debug)]
pub enum BackendType<'a, T> {
    /// Current Cloud API (V2).
    Console(Cow<'a, http::Endpoint>, T),
    /// Local mock of Cloud API (V2).
    Postgres(Cow<'a, url::ApiUrl>, T),
    /// Authentication via a web browser.
    Link(Cow<'a, url::ApiUrl>),
}

impl std::fmt::Display for BackendType<'_, ()> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use BackendType::*;
        match self {
            Console(endpoint, _) => fmt
                .debug_tuple("Console")
                .field(&endpoint.url().as_str())
                .finish(),
            Postgres(endpoint, _) => fmt
                .debug_tuple("Postgres")
                .field(&endpoint.as_str())
                .finish(),
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
}

pub type NodeInfoCache = TimedLru<Arc<str>, NodeInfo>;
pub type CachedNodeInfo = timed_lru::Cached<&'static NodeInfoCache>;

/// Various caches for [`console`].
pub struct ApiCaches {
    /// Cache for the `wake_compute` API method.
    pub node_info: NodeInfoCache,
}

// TODO: get rid of explicit lifetimes in this block (there's a bug in rustc).
// Read more: https://github.com/rust-lang/rust/issues/99190
// Alleged fix: https://github.com/rust-lang/rust/pull/89056
impl<'l> BackendType<'l, ClientCredentials<'_>> {
    /// Do something special if user didn't provide the `project` parameter.
    async fn try_password_hack<'a>(
        &'a mut self,
        caches: &'static ApiCaches,
        extra: &'a ConsoleReqExtra<'a>,
        client: &'a mut stream::PqStream<impl AsyncRead + AsyncWrite + Unpin + Send>,
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
        let (mut node, payload) = match self {
            Console(endpoint, creds) if creds.project.is_none() => {
                let payload = fetch_magic_payload(client).await?;

                let mut creds = creds.as_ref();
                creds.project = Some(payload.project.as_str().into());
                let node = console::Api::new(endpoint, extra, &creds, caches)
                    .wake_compute()
                    .await?;

                (node, payload)
            }
            // This is a hack to allow cleartext password in secure connections (wss).
            Console(endpoint, creds) if creds.use_cleartext_password_flow => {
                let payload = fetch_plaintext_password(client).await?;
                let creds = creds.as_ref();
                let node = console::Api::new(endpoint, extra, &creds, caches)
                    .wake_compute()
                    .await?;

                (node, payload)
            }
            Postgres(endpoint, creds) if creds.project.is_none() => {
                let payload = fetch_magic_payload(client).await?;

                let mut creds = creds.as_ref();
                creds.project = Some(payload.project.as_str().into());
                let node = postgres::Api::new(endpoint, &creds).wake_compute().await?;

                (node, payload)
            }
            _ => return Ok(None),
        };

        node.config.password(payload.password);
        Ok(Some(AuthSuccess {
            reported_auth_ok: false,
            value: node,
        }))
    }

    /// Authenticate the client via the requested backend, possibly using credentials.
    pub async fn authenticate<'a>(
        mut self,
        caches: &'static ApiCaches,
        extra: &'a ConsoleReqExtra<'a>,
        client: &'a mut stream::PqStream<impl AsyncRead + AsyncWrite + Unpin + Send>,
    ) -> auth::Result<AuthSuccess<CachedNodeInfo>> {
        use BackendType::*;

        // Handle cases when `project` is missing in `creds`.
        // TODO: type safety: return `creds` with irrefutable `project`.
        if let Some(res) = self.try_password_hack(caches, extra, client).await? {
            info!("user successfully authenticated (using the password hack)");
            return Ok(res);
        }

        let res = match self {
            Console(endpoint, creds) => {
                info!(
                    user = creds.user,
                    project = creds.project(),
                    "performing authentication using the console"
                );

                assert!(creds.project.is_some());
                console::Api::new(&endpoint, extra, &creds, caches)
                    .handle_user(client)
                    .await?
            }
            Postgres(endpoint, creds) => {
                info!("performing mock authentication using a local postgres instance");

                assert!(creds.project.is_some());
                postgres::Api::new(&endpoint, &creds)
                    .handle_user(client)
                    .await?
            }
            // NOTE: this auth backend doesn't use client credentials.
            Link(url) => {
                info!("performing link authentication");

                link::handle_user(&url, client)
                    .await?
                    .map(CachedNodeInfo::new_uncached)
            }
        };

        info!("user successfully authenticated");
        Ok(res)
    }
}
