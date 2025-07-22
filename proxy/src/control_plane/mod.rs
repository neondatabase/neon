//! Various stuff for dealing with the Neon Console.
//! Later we might move some API wrappers here.

/// Payloads used in the console's APIs.
pub mod messages;

/// Wrappers for console APIs and their mocks.
pub mod client;

pub(crate) mod errors;

use std::sync::Arc;

use messages::EndpointRateLimitConfig;

use crate::auth::backend::ComputeUserInfo;
use crate::auth::backend::jwt::AuthRule;
use crate::auth::{AuthError, IpPattern, check_peer_addr_is_in_list};
use crate::cache::{Cached, TimedLru};
use crate::config::ComputeConfig;
use crate::context::RequestContext;
use crate::control_plane::messages::{ControlPlaneErrorMessage, MetricsAuxInfo};
use crate::intern::{AccountIdInt, EndpointIdInt, ProjectIdInt};
use crate::protocol2::ConnectionInfoExtra;
use crate::rate_limiter::{EndpointRateLimiter, LeakyBucketConfig};
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
    /// The rate limits for this endpoint.
    pub(crate) rate_limits: EndpointRateLimitConfig,
}

/// Info for establishing a connection to a compute node.
#[derive(Clone)]
pub(crate) struct NodeInfo {
    pub(crate) conn_info: compute::ConnectInfo,

    /// Labels for proxy's metrics.
    pub(crate) aux: MetricsAuxInfo,
}

impl NodeInfo {
    pub(crate) async fn connect(
        &self,
        ctx: &RequestContext,
        config: &ComputeConfig,
    ) -> Result<compute::ComputeConnection, compute::ConnectionError> {
        self.conn_info.connect(ctx, &self.aux, config).await
    }
}

#[derive(Copy, Clone, Default, Debug)]
pub(crate) struct AccessBlockerFlags {
    pub public_access_blocked: bool,
    pub vpc_access_blocked: bool,
}

pub(crate) type NodeInfoCache =
    TimedLru<EndpointCacheKey, Result<NodeInfo, Box<ControlPlaneErrorMessage>>>;
pub(crate) type CachedNodeInfo = Cached<&'static NodeInfoCache, NodeInfo>;

#[derive(Clone, Debug)]
pub struct RoleAccessControl {
    pub secret: Option<AuthSecret>,
}

#[derive(Clone, Debug)]
pub struct EndpointAccessControl {
    pub allowed_ips: Arc<Vec<IpPattern>>,
    pub allowed_vpce: Arc<Vec<String>>,
    pub flags: AccessBlockerFlags,

    pub rate_limits: EndpointRateLimitConfig,
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

    pub fn connection_attempt_rate_limit(
        &self,
        ctx: &RequestContext,
        endpoint: &EndpointId,
        rate_limiter: &EndpointRateLimiter,
    ) -> Result<(), AuthError> {
        let endpoint = EndpointIdInt::from(endpoint);

        let limits = &self.rate_limits.connection_attempts;
        let config = match ctx.protocol() {
            crate::metrics::Protocol::Http => limits.http,
            crate::metrics::Protocol::Ws => limits.ws,
            crate::metrics::Protocol::Tcp => limits.tcp,
            crate::metrics::Protocol::SniRouter => return Ok(()),
        };
        let config = config.and_then(|config| {
            if config.rps <= 0.0 || config.burst <= 0.0 {
                return None;
            }

            Some(LeakyBucketConfig::new(config.rps, config.burst))
        });

        if !rate_limiter.check(endpoint, config, 1) {
            return Err(AuthError::too_many_connections());
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
