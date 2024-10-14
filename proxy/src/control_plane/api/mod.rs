pub(crate) mod errors;

/// Payloads used in the console's APIs.
pub mod messages;

use crate::{
    auth::backend::{jwt::AuthRule, ComputeUserInfo},
    context::RequestMonitoring,
    control_plane::{
        api::errors::{GetAuthInfoError, WakeComputeError},
        CachedAllowedIps, CachedNodeInfo, CachedRoleSecret,
    },
    EndpointId,
};

/// This will allocate per each call, but the http requests alone
/// already require a few allocations, so it should be fine.
pub(crate) trait ControlPlaneApi {
    /// Get the client's auth secret for authentication.
    /// Returns option because user not found situation is special.
    /// We still have to mock the scram to avoid leaking information that user doesn't exist.
    async fn get_role_secret(
        &self,
        ctx: &RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedRoleSecret, GetAuthInfoError>;

    async fn get_allowed_ips_and_secret(
        &self,
        ctx: &RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), GetAuthInfoError>;

    async fn get_endpoint_jwks(
        &self,
        ctx: &RequestMonitoring,
        endpoint: EndpointId,
    ) -> anyhow::Result<Vec<AuthRule>>;

    /// Wake up the compute node and return the corresponding connection info.
    async fn wake_compute(
        &self,
        ctx: &RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, WakeComputeError>;
}
