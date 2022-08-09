mod link;
mod postgres;

pub mod console;

mod legacy_console;
pub use legacy_console::{AuthError, AuthErrorImpl};

use crate::{
    auth::{self, AuthFlow, ClientCredentials},
    compute, config, mgmt,
    stream::PqStream,
    waiters::{self, Waiter, Waiters},
};

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
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
            .finish()
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

/// This type serves two purposes:
///
/// * When `T` is `()`, it's just a regular auth backend selector
///   which we use in [`crate::config::ProxyConfig`].
///
/// * However, when we substitute `T` with [`ClientCredentials`],
///   this helps us provide the credentials only to those auth
///   backends which require them for the authentication process.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BackendType<T> {
    /// Legacy Cloud API (V1) + link auth.
    LegacyConsole(T),
    /// Current Cloud API (V2).
    Console(T),
    /// Local mock of Cloud API (V2).
    Postgres(T),
    /// Authentication via a web browser.
    Link,
}

impl<T> BackendType<T> {
    /// Very similar to [`std::option::Option::map`].
    /// Maps [`BackendType<T>`] to [`BackendType<R>`] by applying
    /// a function to a contained value.
    pub fn map<R>(self, f: impl FnOnce(T) -> R) -> BackendType<R> {
        use BackendType::*;
        match self {
            LegacyConsole(x) => LegacyConsole(f(x)),
            Console(x) => Console(f(x)),
            Postgres(x) => Postgres(f(x)),
            Link => Link,
        }
    }
}

impl<T, E> BackendType<Result<T, E>> {
    /// Very similar to [`std::option::Option::transpose`].
    /// This is most useful for error handling.
    pub fn transpose(self) -> Result<BackendType<T>, E> {
        use BackendType::*;
        match self {
            LegacyConsole(x) => x.map(LegacyConsole),
            Console(x) => x.map(Console),
            Postgres(x) => x.map(Postgres),
            Link => Ok(Link),
        }
    }
}

impl BackendType<ClientCredentials> {
    /// Authenticate the client via the requested backend, possibly using credentials.
    pub async fn authenticate(
        mut self,
        urls: &config::AuthUrls,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin + Send>,
    ) -> super::Result<compute::NodeInfo> {
        use BackendType::*;

        if let Console(creds) | Postgres(creds) = &mut self {
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
                creds.project = Some(payload.project);

                let mut config = match &self {
                    Console(creds) => {
                        console::Api::new(&urls.auth_endpoint, creds)
                            .wake_compute()
                            .await?
                    }
                    Postgres(creds) => {
                        postgres::Api::new(&urls.auth_endpoint, creds)
                            .wake_compute()
                            .await?
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
            LegacyConsole(creds) => {
                legacy_console::handle_user(
                    &urls.auth_endpoint,
                    &urls.auth_link_uri,
                    &creds,
                    client,
                )
                .await
            }
            Console(creds) => {
                console::Api::new(&urls.auth_endpoint, &creds)
                    .handle_user(client)
                    .await
            }
            Postgres(creds) => {
                postgres::Api::new(&urls.auth_endpoint, &creds)
                    .handle_user(client)
                    .await
            }
            // NOTE: this auth backend doesn't use client credentials.
            Link => link::handle_user(&urls.auth_link_uri, client).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_type_map() {
        let values = [
            BackendType::LegacyConsole(0),
            BackendType::Console(0),
            BackendType::Postgres(0),
            BackendType::Link,
        ];

        for value in values {
            assert_eq!(value.map(|x| x), value);
        }
    }

    #[test]
    fn test_backend_type_transpose() {
        let values = [
            BackendType::LegacyConsole(Ok::<_, ()>(0)),
            BackendType::Console(Ok(0)),
            BackendType::Postgres(Ok(0)),
            BackendType::Link,
        ];

        for value in values {
            assert_eq!(value.map(Result::unwrap), value.transpose().unwrap());
        }
    }
}
