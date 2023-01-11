//! Local mock of Cloud API V2.

use super::{
    console::{self, AuthInfo, GetAuthInfoError, WakeComputeError},
    AuthSuccess, CachedNodeInfo, NodeInfo,
};
use crate::{
    auth::{self, ClientCredentials},
    compute,
    error::io_error,
    scram,
    stream::PqStream,
    url::ApiUrl,
};
use futures::TryFutureExt;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, info_span, warn, Instrument};

#[derive(Debug, Error)]
enum MockApiError {
    #[error("Failed to read password: {0}")]
    PasswordNotSet(tokio_postgres::Error),
}

impl From<MockApiError> for console::ApiError {
    fn from(e: MockApiError) -> Self {
        io_error(e).into()
    }
}

impl From<tokio_postgres::Error> for console::ApiError {
    fn from(e: tokio_postgres::Error) -> Self {
        io_error(e).into()
    }
}

pub struct Api<'a> {
    endpoint: &'a ApiUrl,
    creds: &'a ClientCredentials<'a>,
}

impl<'a> AsRef<ClientCredentials<'a>> for Api<'a> {
    fn as_ref(&self) -> &ClientCredentials<'a> {
        self.creds
    }
}

impl<'a> Api<'a> {
    /// Construct an API object containing the auth parameters.
    pub fn new(endpoint: &'a ApiUrl, creds: &'a ClientCredentials) -> Self {
        Self { endpoint, creds }
    }

    /// Authenticate the existing user or throw an error.
    pub async fn handle_user(
        &'a self,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin + Send>,
    ) -> auth::Result<AuthSuccess<CachedNodeInfo>> {
        // We reuse user handling logic from a production module.
        console::handle_user(client, self, Self::get_auth_info, Self::wake_compute).await
    }
}

impl Api<'_> {
    /// This implementation fetches the auth info from a local postgres instance.
    async fn get_auth_info(&self) -> Result<Option<AuthInfo>, GetAuthInfoError> {
        async {
            // Perhaps we could persist this connection, but then we'd have to
            // write more code for reopening it if it got closed, which doesn't
            // seem worth it.
            let (client, connection) =
                tokio_postgres::connect(self.endpoint.as_str(), tokio_postgres::NoTls).await?;

            tokio::spawn(connection);
            let query = "select rolpassword from pg_catalog.pg_authid where rolname = $1";
            let rows = client.query(query, &[&self.creds.user]).await?;

            // We can get at most one row, because `rolname` is unique.
            let row = match rows.get(0) {
                Some(row) => row,
                // This means that the user doesn't exist, so there can be no secret.
                // However, this is still a *valid* outcome which is very similar
                // to getting `404 Not found` from the Neon console.
                None => {
                    warn!("user '{}' does not exist", self.creds.user);
                    return Ok(None);
                }
            };

            let entry = row
                .try_get("rolpassword")
                .map_err(MockApiError::PasswordNotSet)?;

            info!("got a secret: {entry}"); // safe since it's not a prod scenario
            let secret = scram::ServerSecret::parse(entry).map(AuthInfo::Scram);
            Ok(secret.or_else(|| parse_md5(entry).map(AuthInfo::Md5)))
        }
        .map_err(crate::error::log_error)
        .instrument(info_span!("get_auth_info", mock = self.endpoint.as_str()))
        .await
    }

    /// We don't need to wake anything locally, so we just return the connection info.
    pub async fn wake_compute(&self) -> Result<CachedNodeInfo, WakeComputeError> {
        let mut config = compute::ConnCfg::new();
        config
            .host(self.endpoint.host_str().unwrap_or("localhost"))
            .port(self.endpoint.port().unwrap_or(5432))
            .dbname(self.creds.dbname)
            .user(self.creds.user);

        let node = NodeInfo {
            config,
            aux: Default::default(),
        };

        Ok(CachedNodeInfo::uncached(node))
    }
}

fn parse_md5(input: &str) -> Option<[u8; 16]> {
    let text = input.strip_prefix("md5")?;

    let mut bytes = [0u8; 16];
    hex::decode_to_slice(text, &mut bytes).ok()?;

    Some(bytes)
}
