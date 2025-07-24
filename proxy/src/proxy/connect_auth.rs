use thiserror::Error;

use crate::auth::Backend;
use crate::auth::backend::ComputeUserInfo;
use crate::compute::{AuthInfo, ComputeConnection, ConnectionError, PostgresError};
use crate::config::ProxyConfig;
use crate::context::RequestContext;
use crate::control_plane::client::ControlPlaneClient;
use crate::error::{ReportableError, UserFacingError};
use crate::proxy::connect_compute::{TlsNegotiation, connect_to_compute};
use crate::proxy::retry::ShouldRetryWakeCompute;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error(transparent)]
    Auth(#[from] PostgresError),
    #[error(transparent)]
    Connect(#[from] ConnectionError),
}

impl UserFacingError for AuthError {
    fn to_string_client(&self) -> String {
        match self {
            AuthError::Auth(postgres_error) => postgres_error.to_string_client(),
            AuthError::Connect(connection_error) => connection_error.to_string_client(),
        }
    }
}

impl ReportableError for AuthError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            AuthError::Auth(postgres_error) => postgres_error.get_error_kind(),
            AuthError::Connect(connection_error) => connection_error.get_error_kind(),
        }
    }
}

/// Try to connect to the compute node, retrying if necessary.
#[tracing::instrument(skip_all)]
pub(crate) async fn connect_to_compute_and_auth(
    ctx: &RequestContext,
    config: &ProxyConfig,
    user_info: &Backend<'_, ComputeUserInfo>,
    auth_info: AuthInfo,
    tls: TlsNegotiation,
) -> Result<ComputeConnection, AuthError> {
    let mut attempt = 0;

    // NOTE: This is messy, but should hopefully be detangled with PGLB.
    // We wanted to separate the concerns of **connect** to compute (a PGLB operation),
    // from **authenticate** to compute (a NeonKeeper operation).
    //
    // This unfortunately removed retry handling for one error case where
    // the compute was cached, and we connected, but the compute cache was actually stale
    // and is associated with the wrong endpoint. We detect this when the **authentication** fails.
    // As such, we retry once here if the `authenticate` function fails and the error is valid to retry.
    loop {
        attempt += 1;
        let mut node = connect_to_compute(ctx, config, user_info, tls).await?;

        let res = auth_info.authenticate(ctx, &mut node).await;
        match res {
            Ok(()) => return Ok(node),
            Err(e) => {
                if attempt < 2
                    && let Backend::ControlPlane(cplane, user_info) = user_info
                    && let ControlPlaneClient::ProxyV1(cplane_proxy_v1) = &**cplane
                    && e.should_retry_wake_compute()
                {
                    tracing::warn!(error = ?e, "retrying wake compute");
                    let key = user_info.endpoint_cache_key();
                    cplane_proxy_v1.caches.node_info.invalidate(&key);
                    continue;
                }

                return Err(e)?;
            }
        }
    }
}
