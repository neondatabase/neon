//! Mock console backend which relies on a user-provided postgres instance.

use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;
use std::sync::Arc;

use futures::TryFutureExt;
use postgres_client::config::SslMode;
use thiserror::Error;
use tokio_postgres::Client;
use tracing::{Instrument, error, info, info_span, warn};

use crate::auth::IpPattern;
use crate::auth::backend::ComputeUserInfo;
use crate::auth::backend::jwt::AuthRule;
use crate::cache::Cached;
use crate::compute::ConnectInfo;
use crate::context::RequestContext;
use crate::control_plane::errors::{
    ControlPlaneError, GetAuthInfoError, GetEndpointJwksError, WakeComputeError,
};
use crate::control_plane::messages::{EndpointRateLimitConfig, MetricsAuxInfo};
use crate::control_plane::{
    AccessBlockerFlags, AuthInfo, AuthSecret, CachedNodeInfo, EndpointAccessControl, NodeInfo,
    RoleAccessControl,
};
use crate::intern::RoleNameInt;
use crate::scram;
use crate::types::{BranchId, EndpointId, ProjectId, RoleName};
use crate::url::ApiUrl;

#[derive(Debug, Error)]
enum MockApiError {
    #[error("Failed to read password: {0}")]
    PasswordNotSet(tokio_postgres::Error),
}

impl From<MockApiError> for ControlPlaneError {
    fn from(e: MockApiError) -> Self {
        io::Error::other(e).into()
    }
}

impl From<tokio_postgres::Error> for ControlPlaneError {
    fn from(e: tokio_postgres::Error) -> Self {
        io::Error::other(e).into()
    }
}

#[derive(Clone)]
pub struct MockControlPlane {
    endpoint: ApiUrl,
    ip_allowlist_check_enabled: bool,
}

impl MockControlPlane {
    pub fn new(endpoint: ApiUrl, ip_allowlist_check_enabled: bool) -> Self {
        Self {
            endpoint,
            ip_allowlist_check_enabled,
        }
    }

    pub(crate) fn url(&self) -> &str {
        self.endpoint.as_str()
    }

    async fn do_get_auth_info(
        &self,
        endpoint: &EndpointId,
        role: &RoleName,
    ) -> Result<AuthInfo, GetAuthInfoError> {
        let (secret, allowed_ips) = async {
            // Perhaps we could persist this connection, but then we'd have to
            // write more code for reopening it if it got closed, which doesn't
            // seem worth it.
            let (client, connection) =
                tokio_postgres::connect(self.endpoint.as_str(), tokio_postgres::NoTls).await?;

            tokio::spawn(connection);

            let secret = if let Some(entry) = get_execute_postgres_query(
                &client,
                "select rolpassword from pg_catalog.pg_authid where rolname = $1",
                &[&role.as_str()],
                "rolpassword",
            )
            .await?
            {
                info!("got a secret: {entry}"); // safe since it's not a prod scenario
                scram::ServerSecret::parse(&entry).map(AuthSecret::Scram)
            } else {
                warn!("user '{role}' does not exist");
                None
            };

            let allowed_ips = if self.ip_allowlist_check_enabled {
                match get_execute_postgres_query(
                    &client,
                    "select allowed_ips from neon_control_plane.endpoints where endpoint_id = $1",
                    &[&endpoint.as_str()],
                    "allowed_ips",
                )
                .await?
                {
                    Some(s) => {
                        info!("got allowed_ips: {s}");
                        s.split(',')
                            .map(|s| {
                                IpPattern::from_str(s).expect("mocked ip pattern should be correct")
                            })
                            .collect()
                    }
                    None => vec![],
                }
            } else {
                vec![]
            };

            Ok((secret, allowed_ips))
        }
        .inspect_err(|e: &GetAuthInfoError| tracing::error!("{e}"))
        .instrument(info_span!("postgres", url = self.endpoint.as_str()))
        .await?;
        Ok(AuthInfo {
            secret,
            allowed_ips,
            allowed_vpc_endpoint_ids: vec![],
            project_id: None,
            account_id: None,
            access_blocker_flags: AccessBlockerFlags::default(),
            rate_limits: EndpointRateLimitConfig::default(),
        })
    }

    async fn do_get_endpoint_jwks(
        &self,
        endpoint: &EndpointId,
    ) -> Result<Vec<AuthRule>, GetEndpointJwksError> {
        let (client, connection) =
            tokio_postgres::connect(self.endpoint.as_str(), tokio_postgres::NoTls).await?;

        let connection = tokio::spawn(connection);

        let res = client.query(
                "select id, jwks_url, audience, role_names from neon_control_plane.endpoint_jwks where endpoint_id = $1",
                &[&endpoint.as_str()],
            )
            .await?;

        let mut rows = vec![];
        for row in res {
            rows.push(AuthRule {
                id: row.get("id"),
                jwks_url: url::Url::parse(row.get("jwks_url"))?,
                audience: row.get("audience"),
                role_names: row
                    .get::<_, Vec<String>>("role_names")
                    .into_iter()
                    .map(RoleName::from)
                    .map(|s| RoleNameInt::from(&s))
                    .collect(),
            });
        }

        drop(client);
        connection.await??;

        Ok(rows)
    }

    async fn do_wake_compute(&self) -> Result<NodeInfo, WakeComputeError> {
        let port = self.endpoint.port().unwrap_or(5432);
        let conn_info = match self.endpoint.host_str() {
            None => ConnectInfo {
                host_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
                host: "localhost".into(),
                port,
                ssl_mode: SslMode::Disable,
            },
            Some(host) => ConnectInfo {
                host_addr: IpAddr::from_str(host).ok(),
                host: host.into(),
                port,
                ssl_mode: SslMode::Disable,
            },
        };

        let node = NodeInfo {
            conn_info,
            aux: MetricsAuxInfo {
                endpoint_id: (&EndpointId::from("endpoint")).into(),
                project_id: (&ProjectId::from("project")).into(),
                branch_id: (&BranchId::from("branch")).into(),
                compute_id: "compute".into(),
                cold_start_info: crate::control_plane::messages::ColdStartInfo::Warm,
            },
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
    let Some(row) = rows.first() else {
        // This means that the user doesn't exist, so there can be no secret.
        // However, this is still a *valid* outcome which is very similar
        // to getting `404 Not found` from the Neon console.
        return Ok(None);
    };

    let entry = row.try_get(idx).map_err(MockApiError::PasswordNotSet)?;
    Ok(Some(entry))
}

impl super::ControlPlaneApi for MockControlPlane {
    async fn get_endpoint_access_control(
        &self,
        _ctx: &RequestContext,
        endpoint: &EndpointId,
        role: &RoleName,
    ) -> Result<EndpointAccessControl, GetAuthInfoError> {
        let info = self.do_get_auth_info(endpoint, role).await?;
        Ok(EndpointAccessControl {
            allowed_ips: Arc::new(info.allowed_ips),
            allowed_vpce: Arc::new(info.allowed_vpc_endpoint_ids),
            flags: info.access_blocker_flags,
            rate_limits: info.rate_limits,
        })
    }

    async fn get_role_access_control(
        &self,
        _ctx: &RequestContext,
        endpoint: &EndpointId,
        role: &RoleName,
    ) -> Result<RoleAccessControl, GetAuthInfoError> {
        let info = self.do_get_auth_info(endpoint, role).await?;
        Ok(RoleAccessControl {
            secret: info.secret,
        })
    }

    async fn get_endpoint_jwks(
        &self,
        _ctx: &RequestContext,
        endpoint: &EndpointId,
    ) -> Result<Vec<AuthRule>, GetEndpointJwksError> {
        self.do_get_endpoint_jwks(endpoint).await
    }

    #[tracing::instrument(skip_all)]
    async fn wake_compute(
        &self,
        _ctx: &RequestContext,
        _user_info: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, WakeComputeError> {
        self.do_wake_compute().map_ok(Cached::new_uncached).await
    }
}
