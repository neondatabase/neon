//! Various stuff for dealing with the Neon Console.
//! Later we might move some API wrappers here.

pub mod api;

/// Implementations of the control plane API and their mocks.
pub mod provider;

/// Console's management API.
pub mod mgmt;

use std::sync::Arc;

use crate::{
    auth::{backend::ComputeCredentialKeys, IpPattern},
    cache::{project_info::ProjectInfoCacheImpl, Cached, TimedLru},
    compute,
    control_plane::api::messages::{ControlPlaneError, MetricsAuxInfo},
    intern::ProjectIdInt,
    scram, EndpointCacheKey,
};

pub(crate) type NodeInfoCache =
    TimedLru<EndpointCacheKey, Result<NodeInfo, Box<ControlPlaneError>>>;
pub(crate) type CachedNodeInfo = Cached<&'static NodeInfoCache, NodeInfo>;
pub(crate) type CachedRoleSecret = Cached<&'static ProjectInfoCacheImpl, Option<AuthSecret>>;
pub(crate) type CachedAllowedIps = Cached<&'static ProjectInfoCacheImpl, Arc<Vec<IpPattern>>>;

/// Auth secret which is managed by the cloud.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum AuthSecret {
    #[cfg(any(test, feature = "testing"))]
    /// Md5 hash of user's password.
    Md5([u8; 16]),

    /// [SCRAM](crate::scram) authentication info.
    Scram(scram::ServerSecret),
}

#[derive(Default)]
pub(crate) struct AuthInfo {
    pub(crate) secret: Option<AuthSecret>,
    /// List of IP addresses allowed for the autorization.
    pub(crate) allowed_ips: Vec<IpPattern>,
    /// Project ID. This is used for cache invalidation.
    pub(crate) project_id: Option<ProjectIdInt>,
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

    /// Whether we should accept self-signed certificates (for testing)
    pub(crate) allow_self_signed_compute: bool,
}

impl NodeInfo {
    pub(crate) fn reuse_settings(&mut self, other: Self) {
        self.allow_self_signed_compute = other.allow_self_signed_compute;
        self.config.reuse_password(other.config);
    }

    pub(crate) fn set_keys(&mut self, keys: &ComputeCredentialKeys) {
        match keys {
            #[cfg(any(test, feature = "testing"))]
            ComputeCredentialKeys::Password(password) => self.config.password(password),
            ComputeCredentialKeys::AuthKeys(auth_keys) => self.config.auth_keys(*auth_keys),
            ComputeCredentialKeys::JwtPayload(_) | ComputeCredentialKeys::None => &mut self.config,
        };
    }
}
