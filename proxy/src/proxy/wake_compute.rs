use crate::auth::backend::{ComputeCredentialKeys, ComputeCredentials, ComputeUserInfo};
use crate::console::errors::WakeComputeError;
use crate::console::{self, provider::CachedNodeInfo};
use crate::context::RequestMonitoring;
use crate::proxy::retry::retry_after;
use std::ops::ControlFlow;
use tracing::{error, warn};

use super::retry::ShouldRetry;

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

/// Attempts to wake up the compute node.
/// * Returns Ok(Continue(e)) if there was an error waking but retries are acceptable
/// * Returns Ok(Break(node)) if the wakeup succeeded
/// * Returns Err(e) if there was an error
pub fn handle_try_wake(
    result: Result<console::CachedNodeInfo, WakeComputeError>,
    num_retries: u32,
) -> Result<ControlFlow<console::CachedNodeInfo, WakeComputeError>, WakeComputeError> {
    match result {
        Err(err) => match &err {
            WakeComputeError::ApiError(api) if api.should_retry(num_retries) => {
                Ok(ControlFlow::Continue(err))
            }
            _ => Err(err),
        },
        // Ready to try again.
        Ok(new) => Ok(ControlFlow::Break(new)),
    }
}
