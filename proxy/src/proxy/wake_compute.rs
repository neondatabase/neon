use crate::auth::backend::{ComputeCredentialKeys, ComputeCredentials, ComputeUserInfo};
use crate::console::errors::WakeComputeError;
use crate::console::{self, provider::CachedNodeInfo};
use crate::context::RequestMonitoring;
use crate::proxy::connect_compute::handle_try_wake;
use crate::proxy::retry::retry_after;
use std::ops::ControlFlow;
use tracing::{error, warn};

/// wake a compute (or retrieve an existing compute session from cache)
pub async fn wake_compute(
    ctx: &mut RequestMonitoring,
    api: &impl console::Api,
    compute_credentials: ComputeCredentials<ComputeCredentialKeys>,
) -> Result<(CachedNodeInfo, ComputeUserInfo), WakeComputeError> {
    let mut num_retries = 0;
    let mut node = loop {
        let wake_res = api.wake_compute(ctx, &compute_credentials.info).await;
        match handle_try_wake(wake_res, num_retries) {
            Err(e) => {
                error!(error = ?e, num_retries, retriable = false, "couldn't wake compute node");
                return Err(e);
            }
            Ok(ControlFlow::Continue(e)) => {
                warn!(error = ?e, num_retries, retriable = true, "couldn't wake compute node");
            }
            Ok(ControlFlow::Break(n)) => break n,
        }

        let wait_duration = retry_after(num_retries);
        num_retries += 1;
        tokio::time::sleep(wait_duration).await;
    };

    ctx.set_project(node.aux.clone());

    match compute_credentials.keys {
        #[cfg(feature = "testing")]
        ComputeCredentialKeys::Password(password) => node.config.password(password),
        ComputeCredentialKeys::AuthKeys(auth_keys) => node.config.auth_keys(auth_keys),
    };

    Ok((node, compute_credentials.info))
}
