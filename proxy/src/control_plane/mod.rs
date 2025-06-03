//! Various stuff for dealing with the Neon Console.
//! Later we might move some API wrappers here.

/// Payloads used in the console's APIs.
pub mod messages;

/// Wrappers for console APIs and their mocks.
pub mod client;

pub(crate) mod errors;

use std::sync::Arc;

use crate::auth::backend::jwt::AuthRule;
use crate::auth::backend::{ComputeCredentialKeys, ComputeUserInfo};
use crate::auth::{AuthError, IpPattern, check_peer_addr_is_in_list};
use crate::cache::{Cached, TimedLru};
use crate::config::ComputeConfig;
use crate::context::RequestContext;
use crate::control_plane::messages::{ControlPlaneErrorMessage, MetricsAuxInfo};
use crate::intern::{AccountIdInt, ProjectIdInt};
use crate::protocol2::ConnectionInfoExtra;
use crate::types::{EndpointCacheKey, EndpointId, RoleName};
use crate::{compute, scram};

/// Various cache-related types.
pub mod caches {
    pub use super::client::ApiCaches;
}

/// Various cache-related types.
pub mod locks {
    pub use super::client::ApiLocks;
}

/// Console's management API.
pub mod mgmt;

/// Auth secret which is managed by the cloud.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum AuthSecret {
    /// [SCRAM](crate::scram) authentication info.
    Scram(scram::ServerSecret),
}

#[derive(Default)]
pub(crate) struct AuthInfo {
    pub(crate) secret: Option<AuthSecret>,
    /// List of IP addresses allowed for the autorization.
    pub(crate) allowed_ips: Vec<IpPattern>,
    /// List of VPC endpoints allowed for the autorization.
    pub(crate) allowed_vpc_endpoint_ids: Vec<String>,
    /// Project ID. This is used for cache invalidation.
    pub(crate) project_id: Option<ProjectIdInt>,
    /// Account ID. This is used for cache invalidation.
    pub(crate) account_id: Option<AccountIdInt>,
    /// Are public connections or VPC connections blocked?
    pub(crate) access_blocker_flags: AccessBlockerFlags,
}

/// Info for establishing a connection to a compute node.
/// This is what we get after auth succeeded, but not before!
#[derive(Clone)]
pub(crate) struct NodeInfo {
    /// Compute node connection params.
    /// It's sad that we have to clone this, but this will improve
    /// once we migrate to a bespoke connection logic.
    pub(crate) config: compute::ConnCfg,

    /// Labels for proxy's metrics.
    pub(crate) aux: MetricsAuxInfo,
}

impl NodeInfo {
    pub(crate) async fn connect(
        &self,
        ctx: &RequestContext,
        config: &ComputeConfig,
        user_info: ComputeUserInfo,
    ) -> Result<compute::PostgresConnection, compute::ConnectionError> {
        self.config
            .connect(ctx, self.aux.clone(), config, user_info)
            .await
    }

    pub(crate) fn reuse_settings(&mut self, other: Self) {
        self.config.reuse_password(other.config);
    }

    pub(crate) fn set_keys(&mut self, keys: &ComputeCredentialKeys) {
        match keys {
            ComputeCredentialKeys::AuthKeys(auth_keys) => self.config.auth_keys(*auth_keys),
            ComputeCredentialKeys::JwtPayload(_) | ComputeCredentialKeys::None => &mut self.config,
        };
    }
}

#[derive(Copy, Clone, Default)]
pub(crate) struct AccessBlockerFlags {
    pub public_access_blocked: bool,
    pub vpc_access_blocked: bool,
}

pub(crate) type NodeInfoCache =
    TimedLru<EndpointCacheKey, Result<NodeInfo, Box<ControlPlaneErrorMessage>>>;
pub(crate) type CachedNodeInfo = Cached<&'static NodeInfoCache, NodeInfo>;

#[derive(Clone)]
pub struct RoleAccessControl {
    pub secret: Option<AuthSecret>,
}

#[derive(Clone)]
pub struct EndpointAccessControl {
    pub allowed_ips: Arc<Vec<IpPattern>>,
    pub allowed_vpce: Arc<Vec<String>>,
    pub flags: AccessBlockerFlags,
}

impl EndpointAccessControl {
    pub fn check(
        &self,
        ctx: &RequestContext,
        check_ip_allowed: bool,
        check_vpc_allowed: bool,
    ) -> Result<(), AuthError> {
        if check_ip_allowed && !check_peer_addr_is_in_list(&ctx.peer_addr(), &self.allowed_ips) {
            return Err(AuthError::IpAddressNotAllowed(ctx.peer_addr()));
        }

        // check if a VPC endpoint ID is coming in and if yes, if it's allowed
        if check_vpc_allowed {
            if self.flags.vpc_access_blocked {
                return Err(AuthError::NetworkNotAllowed);
            }

            let incoming_vpc_endpoint_id = match ctx.extra() {
                None => return Err(AuthError::MissingVPCEndpointId),
                Some(ConnectionInfoExtra::Aws { vpce_id }) => vpce_id.to_string(),
                Some(ConnectionInfoExtra::Azure { link_id }) => link_id.to_string(),
            };

            let vpce = &self.allowed_vpce;
            // TODO: For now an empty VPC endpoint ID list means all are allowed. We should replace that.
            if !vpce.is_empty() && !vpce.contains(&incoming_vpc_endpoint_id) {
                return Err(AuthError::vpc_endpoint_id_not_allowed(
                    incoming_vpc_endpoint_id,
                ));
            }
        } else if self.flags.public_access_blocked {
            return Err(AuthError::NetworkNotAllowed);
        }

        Ok(())
    }
}

/// This will allocate per each call, but the http requests alone
/// already require a few allocations, so it should be fine.
pub(crate) trait ControlPlaneApi {
    async fn get_role_access_control(
        &self,
        ctx: &RequestContext,
        endpoint: &EndpointId,
        role: &RoleName,
    ) -> Result<RoleAccessControl, errors::GetAuthInfoError>;

    async fn get_endpoint_access_control(
        &self,
        ctx: &RequestContext,
        endpoint: &EndpointId,
        role: &RoleName,
    ) -> Result<EndpointAccessControl, errors::GetAuthInfoError>;

    async fn get_endpoint_jwks(
        &self,
        ctx: &RequestContext,
        endpoint: &EndpointId,
    ) -> Result<Vec<AuthRule>, errors::GetEndpointJwksError>;

    /// Wake up the compute node and return the corresponding connection info.
    async fn wake_compute(
        &self,
        ctx: &RequestContext,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, errors::WakeComputeError>;
}
