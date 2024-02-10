use crate::console::{errors::WakeComputeError, provider::CachedNodeInfo};
use crate::context::RequestMonitoring;
use crate::metrics::{ConnectionFailuresBreakdownGroup, Metrics, WakeupFailureKind};
use crate::proxy::retry::retry_after;
use hyper::StatusCode;
use std::ops::ControlFlow;
use tracing::{error, warn};

use super::connect_compute::ComputeConnectBackend;
use super::retry::ShouldRetry;

pub async fn wake_compute<B: ComputeConnectBackend>(
    num_retries: &mut u32,
    ctx: &mut RequestMonitoring,
    api: &B,
) -> Result<CachedNodeInfo, WakeComputeError> {
    loop {
        let wake_res = api.wake_compute(ctx).await;
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
    let kind = match e {
        WakeComputeError::BadComputeAddress(_) => WakeupFailureKind::BadComputeAddress,
        WakeComputeError::ApiError(ApiError::Transport(_)) => WakeupFailureKind::ApiTransportError,
        WakeComputeError::ApiError(ApiError::Console {
            status: StatusCode::LOCKED,
            ref text,
        }) if text.contains("written data quota exceeded")
            || text.contains("the limit for current plan reached") =>
        {
            WakeupFailureKind::QuotaExceeded
        }
        WakeComputeError::ApiError(ApiError::Console {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            ref text,
        }) if text.contains("compute time quota of non-primary branches is exceeded") => {
            WakeupFailureKind::QuotaExceeded
        }
        WakeComputeError::ApiError(ApiError::Console {
            status: StatusCode::LOCKED,
            ..
        }) => WakeupFailureKind::ApiConsoleLocked,
        WakeComputeError::ApiError(ApiError::Console {
            status: StatusCode::BAD_REQUEST,
            ..
        }) => WakeupFailureKind::ApiConsoleBadRequest,
        WakeComputeError::ApiError(ApiError::Console { status, .. })
            if status.is_server_error() =>
        {
            WakeupFailureKind::ApiConsoleOtherServerError
        }
        WakeComputeError::ApiError(ApiError::Console { .. }) => {
            WakeupFailureKind::ApiConsoleOtherError
        }
        WakeComputeError::TimeoutError => WakeupFailureKind::TimeoutError,
    };
    Metrics::get()
        .proxy
        .connection_failures_breakdown
        .inc(ConnectionFailuresBreakdownGroup {
            kind,
            retry: retry.into(),
        });
}
