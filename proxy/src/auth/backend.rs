mod legacy_console;
mod link;
mod postgres;

pub mod console;

pub use legacy_console::{AuthError, AuthErrorImpl};

use super::ClientCredentials;
use crate::{
    compute,
    config::{AuthBackendType, ProxyConfig},
    mgmt,
    stream::PqStream,
    waiters::{self, Waiter, Waiters},
};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

lazy_static! {
    static ref CPLANE_WAITERS: Waiters<mgmt::ComputeReady> = Default::default();
}

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

pub(super) async fn handle_user(
    config: &ProxyConfig,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin + Send>,
    creds: ClientCredentials,
) -> super::Result<compute::NodeInfo> {
    use AuthBackendType::*;
    match config.auth_backend {
        LegacyConsole => {
            legacy_console::handle_user(
                &config.auth_endpoint,
                &config.auth_link_uri,
                client,
                &creds,
            )
            .await
        }
        Console => {
            console::Api::new(&config.auth_endpoint, &creds)?
                .handle_user(client)
                .await
        }
        Postgres => {
            postgres::Api::new(&config.auth_endpoint, &creds)?
                .handle_user(client)
                .await
        }
        Link => link::handle_user(&config.auth_link_uri, client).await,
    }
}
