mod postgres;

mod link;
pub use link::LinkAuthError;

mod console;
pub use console::{GetAuthInfoError, WakeComputeError};

use crate::{
    auth::{self, AuthFlow, ClientCredentials},
    compute, http, mgmt, stream, url,
    waiters::{self, Waiter, Waiters},
};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use tokio::io::{AsyncRead, AsyncWrite};

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

/// Compute node connection params provided by the cloud.
/// Note how it implements serde traits, since we receive it over the wire.
#[derive(Serialize, Deserialize, Default)]
pub struct DatabaseInfo {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: Option<String>,
}

// Manually implement debug to omit personal and sensitive info.
impl std::fmt::Debug for DatabaseInfo {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.debug_struct("DatabaseInfo")
            .field("host", &self.host)
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}

impl From<DatabaseInfo> for tokio_postgres::Config {
    fn from(db_info: DatabaseInfo) -> Self {
        let mut config = tokio_postgres::Config::new();

        config
            .host(&db_info.host)
            .port(db_info.port)
            .dbname(&db_info.dbname)
            .user(&db_info.user);

        if let Some(password) = db_info.password {
            config.password(password);
        }

        config
    }
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

impl BackendType<'_, ClientCredentials<'_>> {
    /// Authenticate the client via the requested backend, possibly using credentials.
    pub async fn authenticate(
        mut self,
        extra: &ConsoleReqExtra<'_>,
        client: &mut stream::PqStream<impl AsyncRead + AsyncWrite + Unpin + Send>,
    ) -> super::Result<compute::NodeInfo> {
        use BackendType::*;

        if let Console(_, creds) | Postgres(_, creds) = &mut self {
            // If there's no project so far, that entails that client doesn't
            // support SNI or other means of passing the project name.
            // We now expect to see a very specific payload in the place of password.
            if creds.project().is_none() {
                let payload = AuthFlow::new(client)
                    .begin(auth::PasswordHack)
                    .await?
                    .authenticate()
                    .await?;

                // Finally we may finish the initialization of `creds`.
                // TODO: add missing type safety to ClientCredentials.
                creds.project = Some(payload.project.into());

                let mut config = match &self {
                    Console(endpoint, creds) => {
                        console::Api::new(endpoint, extra, creds)
                            .wake_compute()
                            .await?
                    }
                    Postgres(endpoint, creds) => {
                        postgres::Api::new(endpoint, creds).wake_compute().await?
                    }
                    _ => unreachable!("see the patterns above"),
                };

                // We should use a password from payload as well.
                config.password(payload.password);

                return Ok(compute::NodeInfo {
                    reported_auth_ok: false,
                    config,
                });
            }
        }

        match self {
            Console(endpoint, creds) => {
                console::Api::new(&endpoint, extra, &creds)
                    .handle_user(client)
                    .await
            }
            Postgres(endpoint, creds) => {
                postgres::Api::new(&endpoint, &creds)
                    .handle_user(client)
                    .await
            }
            // NOTE: this auth backend doesn't use client credentials.
            Link(url) => link::handle_user(&url, client).await,
        }
    }
}
