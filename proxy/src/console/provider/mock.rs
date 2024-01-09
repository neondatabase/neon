//! Mock console backend which relies on a user-provided postgres instance.

use super::{
    errors::{ApiError, GetAuthInfoError, WakeComputeError},
    AuthInfo, AuthSecret, CachedNodeInfo, ConsoleReqExtra, NodeInfo,
};
use crate::cache::Cached;
use crate::console::provider::{CachedAllowedIps, CachedRoleSecret};
use crate::{auth::backend::ComputeUserInfo, compute, error::io_error, scram, url::ApiUrl};
use crate::{console::provider::CachedRoleSecret, context::RequestMonitoring};
use async_trait::async_trait;
use futures::TryFutureExt;
use smol_str::SmolStr;
use std::sync::Arc;
use thiserror::Error;
use tokio_postgres::{config::SslMode, Client};
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
        creds: &ComputeUserInfo,
    ) -> Result<AuthInfo, GetAuthInfoError> {
        let (secret, allowed_ips) = async {
            // Perhaps we could persist this connection, but then we'd have to
            // write more code for reopening it if it got closed, which doesn't
            // seem worth it.
            let (client, connection) =
                tokio_postgres::connect(self.endpoint.as_str(), tokio_postgres::NoTls).await?;

            tokio::spawn(connection);
            let secret = match get_execute_postgres_query(
                &client,
                "select rolpassword from pg_catalog.pg_authid where rolname = $1",
                &[&&*creds.inner.user],
                "rolpassword",
            )
            .await?
            {
                Some(entry) => {
                    info!("got a secret: {entry}"); // safe since it's not a prod scenario
                    let secret = scram::ServerSecret::parse(&entry).map(AuthSecret::Scram);
                    secret.or_else(|| parse_md5(&entry).map(AuthSecret::Md5))
                }
                None => {
                    warn!("user '{}' does not exist", creds.inner.user);
                    None
                }
            };
            let allowed_ips = match get_execute_postgres_query(
                &client,
                "select allowed_ips from neon_control_plane.endpoints where endpoint_id = $1",
                &[&creds.endpoint.as_str()],
                "allowed_ips",
            )
            .await?
            {
                Some(s) => {
                    info!("got allowed_ips: {s}");
                    s.split(',').map(String::from).collect()
                }
                None => vec![],
            };

            Ok((secret, allowed_ips))
        }
        .map_err(crate::error::log_error::<GetAuthInfoError>)
        .instrument(info_span!("postgres", url = self.endpoint.as_str()))
        .await?;
        Ok(AuthInfo {
            secret,
            allowed_ips: allowed_ips.iter().map(SmolStr::from).collect(),
            project_id: None,
        })
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

async fn get_execute_postgres_query(
    client: &Client,
    query: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    idx: &str,
) -> Result<Option<String>, GetAuthInfoError> {
    let rows = client.query(query, params).await?;

    // We can get at most one row, because `rolname` is unique.
    let row = match rows.first() {
        Some(row) => row,
        // This means that the user doesn't exist, so there can be no secret.
        // However, this is still a *valid* outcome which is very similar
        // to getting `404 Not found` from the Neon console.
        None => return Ok(None),
    };

    let entry = row.try_get(idx).map_err(MockApiError::PasswordNotSet)?;
    Ok(Some(entry))
}

#[async_trait]
impl super::Api for Api {
    #[tracing::instrument(skip_all)]
    async fn get_role_secret(
        &self,
        _ctx: &mut RequestMonitoring,
        creds: &ComputeUserInfo,
    ) -> Result<Option<CachedRoleSecret>, GetAuthInfoError> {
        Ok(self
            .do_get_auth_info(creds)
            .await?
            .secret
            .map(CachedRoleSecret::new_uncached))
    }

    async fn get_allowed_ips(
        &self,
        _ctx: &mut RequestMonitoring,
        creds: &ComputeUserInfo,
    ) -> Result<CachedAllowedIps, GetAuthInfoError> {
        Ok(Cached::new_uncached(Arc::new(
            self.do_get_auth_info(creds).await?.allowed_ips,
        )))
    }

    #[tracing::instrument(skip_all)]
    async fn wake_compute(
        &self,
        _ctx: &mut RequestMonitoring,
        _extra: &ConsoleReqExtra,
        _creds: &ComputeUserInfo,
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
