//! Mock console backend which relies on a user-provided postgres instance.

use super::{
    errors::{ApiError, GetAuthInfoError, WakeComputeError},
    AuthInfo, CachedNodeInfo, ConsoleReqExtra, NodeInfo,
};
use crate::{auth::ClientCredentials, compute, error::io_error, scram, url::ApiUrl};
use async_trait::async_trait;
use futures::TryFutureExt;
use thiserror::Error;
use tokio_postgres::config::SslMode;
use tracing::{error, info, info_span, warn, Instrument};

#[derive(Debug, Error)]
enum MockApiError {
    #[error("Failed to read password: {0}")]
    PasswordNotSet(tokio_postgres::Error),
}

impl From<MockApiError> for ApiError {
    fn from(e: MockApiError) -> Self {
        io_error(e).into()
    }
}

impl From<tokio_postgres::Error> for ApiError {
    fn from(e: tokio_postgres::Error) -> Self {
        io_error(e).into()
    }
}

#[derive(Clone)]
pub struct Api {
    endpoint: ApiUrl,
}

impl Api {
    pub fn new(endpoint: ApiUrl) -> Self {
        Self { endpoint }
    }

    pub fn url(&self) -> &str {
        self.endpoint.as_str()
    }

    async fn do_get_auth_info(
        &self,
        creds: &ClientCredentials<'_>,
    ) -> Result<Option<AuthInfo>, GetAuthInfoError> {
        async {
            // Perhaps we could persist this connection, but then we'd have to
            // write more code for reopening it if it got closed, which doesn't
            // seem worth it.
            let (client, connection) =
                tokio_postgres::connect(self.endpoint.as_str(), tokio_postgres::NoTls).await?;

            tokio::spawn(connection);
            let query = "select rolpassword from pg_catalog.pg_authid where rolname = $1";
            let rows = client.query(query, &[&creds.user]).await?;

            // We can get at most one row, because `rolname` is unique.
            let row = match rows.get(0) {
                Some(row) => row,
                // This means that the user doesn't exist, so there can be no secret.
                // However, this is still a *valid* outcome which is very similar
                // to getting `404 Not found` from the Neon console.
                None => {
                    warn!("user '{}' does not exist", creds.user);
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
        .instrument(info_span!("postgres", url = self.endpoint.as_str()))
        .await
    }

    async fn do_wake_compute(&self) -> Result<NodeInfo, WakeComputeError> {
        let mut config = compute::ConnCfg::new();
        config
            .host(self.endpoint.host_str().unwrap_or("localhost"))
            .port(self.endpoint.port().unwrap_or(5432))
            .ssl_mode(SslMode::Disable);

        let node = NodeInfo {
            config,
            aux: Default::default(),
            allow_self_signed_compute: false,
        };

        Ok(node)
    }
}

#[async_trait]
impl super::Api for Api {
    #[tracing::instrument(skip_all)]
    async fn get_auth_info(
        &self,
        _extra: &ConsoleReqExtra<'_>,
        creds: &ClientCredentials,
    ) -> Result<Option<AuthInfo>, GetAuthInfoError> {
        self.do_get_auth_info(creds).await
    }

    #[tracing::instrument(skip_all)]
    async fn wake_compute(
        &self,
        _extra: &ConsoleReqExtra<'_>,
        _creds: &ClientCredentials,
    ) -> Result<CachedNodeInfo, WakeComputeError> {
        self.do_wake_compute()
            .map_ok(CachedNodeInfo::new_uncached)
            .await
    }
}

fn parse_md5(input: &str) -> Option<[u8; 16]> {
    let text = input.strip_prefix("md5")?;

    let mut bytes = [0u8; 16];
    hex::decode_to_slice(text, &mut bytes).ok()?;

    Some(bytes)
}
