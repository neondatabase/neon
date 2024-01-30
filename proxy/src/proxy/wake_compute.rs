use crate::auth::backend::ComputeUserInfo;
use crate::console::{
    errors::WakeComputeError,
    provider::{CachedNodeInfo, ConsoleBackend},
    Api,
};
use crate::context::RequestMonitoring;
use crate::metrics::{bool_to_str, NUM_WAKEUP_FAILURES};
use crate::proxy::retry::retry_after;
use hyper::StatusCode;
use std::ops::ControlFlow;
use tracing::{error, warn};

use super::retry::ShouldRetry;

/// wake a compute (or retrieve an existing compute session from cache)
pub async fn wake_compute(
    num_retries: &mut u32,
    ctx: &mut RequestMonitoring,
    api: &ConsoleBackend,
    info: &ComputeUserInfo,
) -> Result<CachedNodeInfo, WakeComputeError> {
    loop {
        let wake_res = api.wake_compute(ctx, info).await;
        match handle_try_wake(wake_res, *num_retries) {
            Err(e) => {
                error!(error = ?e, num_retries, retriable = false, "couldn't wake compute node");
                report_error(&e, false);
                return Err(e);
            }
            Ok(ControlFlow::Continue(e)) => {
                warn!(error = ?e, num_retries, retriable = true, "couldn't wake compute node");
                report_error(&e, true);
            }
            Ok(ControlFlow::Break(n)) => return Ok(n),
        }

        let wait_duration = retry_after(*num_retries);
        *num_retries += 1;
        tokio::time::sleep(wait_duration).await;
    }
}

/// Attempts to wake up the compute node.
/// * Returns Ok(Continue(e)) if there was an error waking but retries are acceptable
/// * Returns Ok(Break(node)) if the wakeup succeeded
/// * Returns Err(e) if there was an error
pub fn handle_try_wake(
    result: Result<CachedNodeInfo, WakeComputeError>,
    num_retries: u32,
) -> Result<ControlFlow<CachedNodeInfo, WakeComputeError>, WakeComputeError> {
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

fn report_error(e: &WakeComputeError, retry: bool) {
    use crate::console::errors::ApiError;
    let retry = bool_to_str(retry);
    let kind = match e {
        WakeComputeError::BadComputeAddress(_) => "bad_compute_address",
        WakeComputeError::ApiError(ApiError::Transport(_)) => "api_transport_error",
        WakeComputeError::ApiError(ApiError::Console {
            status: StatusCode::LOCKED,
            ref text,
        }) if text.contains("written data quota exceeded")
            || text.contains("the limit for current plan reached") =>
        {
            "quota_exceeded"
        }
        WakeComputeError::ApiError(ApiError::Console {
            status: StatusCode::LOCKED,
            ..
        }) => "api_console_locked",
        WakeComputeError::ApiError(ApiError::Console {
            status: StatusCode::BAD_REQUEST,
            ..
        }) => "api_console_bad_request",
        WakeComputeError::ApiError(ApiError::Console { status, .. })
            if status.is_server_error() =>
        {
            "api_console_other_server_error"
        }
        WakeComputeError::ApiError(ApiError::Console { .. }) => "api_console_other_error",
        WakeComputeError::TimeoutError => "timeout_error",
    };
    NUM_WAKEUP_FAILURES.with_label_values(&[retry, kind]).inc();
}
