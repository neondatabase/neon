use tracing::{error, info, warn};

use super::connect_compute::ComputeConnectBackend;
use crate::config::RetryConfig;
use crate::context::RequestContext;
use crate::control_plane::errors::WakeComputeError;
use crate::control_plane::CachedNodeInfo;
use crate::error::ReportableError;
use crate::metrics::{
    ConnectOutcome, ConnectionFailuresBreakdownGroup, Metrics, RetriesMetricGroup, RetryType,
};
use crate::proxy::retry::{retry_after, should_retry};

pub(crate) async fn wake_compute<B: ComputeConnectBackend>(
    num_retries: &mut u32,
    ctx: &RequestContext,
    api: &B,
    config: RetryConfig,
) -> Result<CachedNodeInfo, WakeComputeError> {
    loop {
        match api.wake_compute(ctx).await {
            Err(e) if !should_retry(&e, *num_retries, config) => {
                error!(error = ?e, num_retries, retriable = false, "couldn't wake compute node");
                report_error(&e, false);
                Metrics::get().proxy.retries_metric.observe(
                    RetriesMetricGroup {
                        outcome: ConnectOutcome::Failed,
                        retry_type: RetryType::WakeCompute,
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
                        retry_type: RetryType::WakeCompute,
                    },
                    (*num_retries).into(),
                );
                // TODO: is this necessary? We have a metric.
                // TODO: this log line is misleading as "wake_compute" might return cached (and stale) info.
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
    let kind = e.get_error_kind();

    Metrics::get()
        .proxy
        .connection_failures_breakdown
        .inc(ConnectionFailuresBreakdownGroup {
            kind,
            retry: retry.into(),
        });
}
