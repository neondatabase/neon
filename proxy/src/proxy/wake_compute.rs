use crate::config::RetryConfig;
use crate::console::messages::ConsoleError;
use crate::console::{errors::WakeComputeError, provider::CachedNodeInfo};
use crate::context::RequestMonitoring;
use crate::metrics::{
    ConnectOutcome, ConnectionFailuresBreakdownGroup, Metrics, RetriesMetricGroup, RetryType,
    WakeupFailureKind,
};
use crate::proxy::retry::retry_after;
use hyper1::StatusCode;
use std::ops::ControlFlow;
use tracing::{error, info, warn};

use super::connect_compute::ComputeConnectBackend;
use super::retry::ShouldRetry;

pub async fn wake_compute<B: ComputeConnectBackend>(
    num_retries: &mut u32,
    ctx: &mut RequestMonitoring,
    api: &B,
    config: RetryConfig,
) -> Result<CachedNodeInfo, WakeComputeError> {
    let retry_type = RetryType::WakeCompute;
    loop {
        let wake_res = api.wake_compute(ctx).await;
        match handle_try_wake(wake_res, *num_retries, config) {
            Err(e) => {
                error!(error = ?e, num_retries, retriable = false, "couldn't wake compute node");
                report_error(&e, false);
                Metrics::get().proxy.retries_metric.observe(
                    RetriesMetricGroup {
                        outcome: ConnectOutcome::Failed,
                        retry_type,
                    },
                    (*num_retries).into(),
                );
                return Err(e);
            }
            Ok(ControlFlow::Continue(e)) => {
                warn!(error = ?e, num_retries, retriable = true, "couldn't wake compute node");
                report_error(&e, true);
            }
            Ok(ControlFlow::Break(n)) => {
                Metrics::get().proxy.retries_metric.observe(
                    RetriesMetricGroup {
                        outcome: ConnectOutcome::Success,
                        retry_type,
                    },
                    (*num_retries).into(),
                );
                info!(?num_retries, "compute node woken up after");
                return Ok(n);
            }
        }

        let wait_duration = retry_after(*num_retries, config);
        *num_retries += 1;
        let pause = ctx
            .latency_timer
            .pause(crate::metrics::Waiting::RetryTimeout);
        tokio::time::sleep(wait_duration).await;
        drop(pause);
    }
}

/// Attempts to wake up the compute node.
/// * Returns Ok(Continue(e)) if there was an error waking but retries are acceptable
/// * Returns Ok(Break(node)) if the wakeup succeeded
/// * Returns Err(e) if there was an error
pub fn handle_try_wake(
    result: Result<CachedNodeInfo, WakeComputeError>,
    num_retries: u32,
    config: RetryConfig,
) -> Result<ControlFlow<CachedNodeInfo, WakeComputeError>, WakeComputeError> {
    match result {
        Err(err) => match &err {
            WakeComputeError::ApiError(api) if api.should_retry(num_retries, config) => {
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
        WakeComputeError::ApiError(ApiError::Console(e)) => match e.get_reason() {
            crate::console::messages::Reason::RoleProtected => {
                WakeupFailureKind::ApiConsoleBadRequest
            }
            crate::console::messages::Reason::ResourceNotFound => {
                WakeupFailureKind::ApiConsoleBadRequest
            }
            crate::console::messages::Reason::ProjectNotFound => {
                WakeupFailureKind::ApiConsoleBadRequest
            }
            crate::console::messages::Reason::EndpointNotFound => {
                WakeupFailureKind::ApiConsoleBadRequest
            }
            crate::console::messages::Reason::BranchNotFound => {
                WakeupFailureKind::ApiConsoleBadRequest
            }
            crate::console::messages::Reason::RateLimitExceeded => {
                WakeupFailureKind::ApiConsoleLocked
            }
            crate::console::messages::Reason::NonPrimaryBranchComputeTimeExceeded => {
                WakeupFailureKind::QuotaExceeded
            }
            crate::console::messages::Reason::ActiveTimeQuotaExceeded => {
                WakeupFailureKind::QuotaExceeded
            }
            crate::console::messages::Reason::ComputeTimeQuotaExceeded => {
                WakeupFailureKind::QuotaExceeded
            }
            crate::console::messages::Reason::WrittenDataQuotaExceeded => {
                WakeupFailureKind::QuotaExceeded
            }
            crate::console::messages::Reason::DataTransferQuotaExceeded => {
                WakeupFailureKind::QuotaExceeded
            }
            crate::console::messages::Reason::LogicalSizeQuotaExceeded => {
                WakeupFailureKind::QuotaExceeded
            }
            crate::console::messages::Reason::Unknown => match e {
                ConsoleError {
                    http_status_code: StatusCode::LOCKED,
                    ref error,
                    ..
                } if error.contains("written data quota exceeded")
                    || error.contains("the limit for current plan reached") =>
                {
                    WakeupFailureKind::QuotaExceeded
                }
                ConsoleError {
                    http_status_code: StatusCode::UNPROCESSABLE_ENTITY,
                    ref error,
                    ..
                } if error.contains("compute time quota of non-primary branches is exceeded") => {
                    WakeupFailureKind::QuotaExceeded
                }
                ConsoleError {
                    http_status_code: StatusCode::LOCKED,
                    ..
                } => WakeupFailureKind::ApiConsoleLocked,
                ConsoleError {
                    http_status_code: StatusCode::BAD_REQUEST,
                    ..
                } => WakeupFailureKind::ApiConsoleBadRequest,
                ConsoleError {
                    http_status_code, ..
                } if http_status_code.is_server_error() => {
                    WakeupFailureKind::ApiConsoleOtherServerError
                }
                ConsoleError { .. } => WakeupFailureKind::ApiConsoleOtherError,
            },
        },
        WakeComputeError::TooManyConnections => WakeupFailureKind::ApiConsoleLocked,
        WakeComputeError::TooManyConnectionAttempts(_) => WakeupFailureKind::TimeoutError,
    };
    Metrics::get()
        .proxy
        .connection_failures_breakdown
        .inc(ConnectionFailuresBreakdownGroup {
            kind,
            retry: retry.into(),
        });
}
