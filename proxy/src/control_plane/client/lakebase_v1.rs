//! Production console backend.

use std::sync::Arc;

use ::http::HeaderName;
use postgres_client::config::SslMode;

use crate::auth::backend::ComputeUserInfo;
use crate::auth::backend::jwt::AuthRule;
use crate::compute::ConnectInfo;
use crate::context::RequestContext;
use crate::control_plane::errors::{
    ControlPlaneError, GetAuthInfoError, GetEndpointJwksError, WakeComputeError,
};
use crate::control_plane::messages::{ColdStartInfo, EndpointRateLimitConfig, MetricsAuxInfo};
use crate::control_plane::{
    AccessBlockerFlags, CachedNodeInfo, EndpointAccessControl, NodeInfo, RoleAccessControl,
};
use crate::intern::{BranchIdInt, ProjectIdInt};
use crate::types::{BranchId, EndpointId, ProjectId, RoleName};

pub(crate) const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

#[derive(Clone)]
pub struct LakebaseClient {
    pub namespace: String,
    pub port: u16,
}

impl LakebaseClient {
    /// Construct an API object containing the auth parameters.
    pub fn new(namespace: String, port: u16) -> Self {
        Self { namespace, port }
    }
}

impl super::ControlPlaneApi for LakebaseClient {
    #[tracing::instrument(skip_all)]
    async fn get_role_access_control(
        &self,
        _ctx: &RequestContext,
        _endpoint: &EndpointId,
        _role: &RoleName,
    ) -> Result<RoleAccessControl, crate::control_plane::errors::GetAuthInfoError> {
        Ok(RoleAccessControl { secret: None })
    }

    #[tracing::instrument(skip_all)]
    async fn get_endpoint_access_control(
        &self,
        _ctx: &RequestContext,
        _endpoint: &EndpointId,
        _role: &RoleName,
    ) -> Result<EndpointAccessControl, GetAuthInfoError> {
        Ok(EndpointAccessControl {
            allowed_ips: Arc::new(vec![]),
            allowed_vpce: Arc::new(vec![]),
            flags: AccessBlockerFlags::default(),
            rate_limits: EndpointRateLimitConfig::default(),
        })
    }

    #[tracing::instrument(skip_all)]
    async fn get_endpoint_jwks(
        &self,
        _ctx: &RequestContext,
        _endpoint: &EndpointId,
    ) -> Result<Vec<AuthRule>, GetEndpointJwksError> {
        Err(GetEndpointJwksError::ControlPlane(
            ControlPlaneError::Transport(std::io::Error::other("unsupported")),
        ))
    }

    #[tracing::instrument(skip_all)]
    async fn wake_compute(
        &self,
        _ctx: &RequestContext,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, WakeComputeError> {
        let instance_id = user_info.endpoint.as_str();
        let namespace = self.namespace.as_str();

        let host = format!("{instance_id}.{namespace}.svc.cluster.local").into();
        let server_name = format!("{instance_id}.online-tables.dev.databricks.com");
        let port = self.port;

        Ok(CachedNodeInfo::new_uncached(NodeInfo {
            conn_info: ConnectInfo {
                host_addr: None,
                host,
                server_name,
                port,
                ssl_mode: SslMode::Require,
            },
            aux: MetricsAuxInfo {
                endpoint_id: user_info.endpoint.normalize_intern(),
                project_id: ProjectIdInt::from(&ProjectId::from("unknown")),
                branch_id: BranchIdInt::from(&BranchId::from("unknown")),
                compute_id: user_info.endpoint.as_str().into(),
                cold_start_info: ColdStartInfo::WarmCached,
            },
        }))
    }
}
