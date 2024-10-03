use crate::config::RetryConfig;
use crate::context::RequestMonitoring;
use crate::control_plane::messages::{ControlPlaneError, Reason};
use crate::control_plane::{errors::WakeComputeError, provider::CachedNodeInfo};
use crate::metrics::{
    ConnectOutcome, ConnectionFailuresBreakdownGroup, Metrics, RetriesMetricGroup, RetryType,
    WakeupFailureKind,
};
use crate::proxy::retry::{retry_after, should_retry};
use hyper1::StatusCode;
use tracing::{error, info, warn};

use super::connect_compute::ComputeConnectBackend;

pub(crate) async fn wake_compute<B: ComputeConnectBackend>(
    num_retries: &mut u32,
    ctx: &RequestMonitoring,
    api: &B,
    config: RetryConfig,
) -> Result<CachedNodeInfo, WakeComputeError> {
    let retry_type = RetryType::WakeCompute;
    loop {
        match api.wake_compute(ctx).await {
            Err(e) if !should_retry(&e, *num_retries, config) => {
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
            Err(e) => {
                warn!(error = ?e, num_retries, retriable = true, "couldn't wake compute node");
                report_error(&e, true);
            }
            Ok(n) => {
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
        let pause = ctx.latency_timer_pause(crate::metrics::Waiting::RetryTimeout);
        tokio::time::sleep(wait_duration).await;
        drop(pause);
    }
}

fn report_error(e: &WakeComputeError, retry: bool) {
    use crate::control_plane::errors::ApiError;
    let kind = match e {
        WakeComputeError::BadComputeAddress(_) => WakeupFailureKind::BadComputeAddress,
        WakeComputeError::ApiError(ApiError::Transport(_)) => WakeupFailureKind::ApiTransportError,
        WakeComputeError::ApiError(ApiError::ControlPlane(e)) => match e.get_reason() {
            Reason::RoleProtected => WakeupFailureKind::ApiConsoleBadRequest,
            Reason::ResourceNotFound => WakeupFailureKind::ApiConsoleBadRequest,
            Reason::ProjectNotFound => WakeupFailureKind::ApiConsoleBadRequest,
            Reason::EndpointNotFound => WakeupFailureKind::ApiConsoleBadRequest,
            Reason::BranchNotFound => WakeupFailureKind::ApiConsoleBadRequest,
            Reason::RateLimitExceeded => WakeupFailureKind::ApiConsoleLocked,
            Reason::NonDefaultBranchComputeTimeExceeded => WakeupFailureKind::QuotaExceeded,
            Reason::ActiveTimeQuotaExceeded => WakeupFailureKind::QuotaExceeded,
            Reason::ComputeTimeQuotaExceeded => WakeupFailureKind::QuotaExceeded,
            Reason::WrittenDataQuotaExceeded => WakeupFailureKind::QuotaExceeded,
            Reason::DataTransferQuotaExceeded => WakeupFailureKind::QuotaExceeded,
            Reason::LogicalSizeQuotaExceeded => WakeupFailureKind::QuotaExceeded,
            Reason::ConcurrencyLimitReached => WakeupFailureKind::ApiConsoleLocked,
            Reason::LockAlreadyTaken => WakeupFailureKind::ApiConsoleLocked,
            Reason::RunningOperations => WakeupFailureKind::ApiConsoleLocked,
            Reason::Unknown => match e {
                ControlPlaneError {
                    http_status_code: StatusCode::LOCKED,
                    ref error,
                    ..
                } if error.contains("written data quota exceeded")
                    || error.contains("the limit for current plan reached") =>
                {
                    WakeupFailureKind::QuotaExceeded
                }
                ControlPlaneError {
                    http_status_code: StatusCode::UNPROCESSABLE_ENTITY,
                    ref error,
                    ..
                } if error.contains("compute time quota of non-primary branches is exceeded") => {
                    WakeupFailureKind::QuotaExceeded
                }
                ControlPlaneError {
                    http_status_code: StatusCode::LOCKED,
                    ..
                } => WakeupFailureKind::ApiConsoleLocked,
                ControlPlaneError {
                    http_status_code: StatusCode::BAD_REQUEST,
                    ..
                } => WakeupFailureKind::ApiConsoleBadRequest,
                ControlPlaneError {
                    http_status_code, ..
                } if http_status_code.is_server_error() => {
                    WakeupFailureKind::ApiConsoleOtherServerError
                }
                ControlPlaneError { .. } => WakeupFailureKind::ApiConsoleOtherError,
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
