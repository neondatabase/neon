use smol_str::{SmolStr, ToSmolStr};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::task::task_tracker::TaskTrackerToken;
use tracing::debug;
use utils::measured_stream::MeasuredStream;

use super::copy_bidirectional::ErrorSource;
use crate::cancellation;
use crate::compute::PostgresConnection;
use crate::config::ComputeConfig;
use crate::context::RequestContext;
use crate::control_plane::messages::MetricsAuxInfo;
use crate::metrics::{Direction, Metrics, NumClientConnectionsGuard, NumConnectionRequestsGuard};
use crate::protocol2::ConnectionInfoExtra;
use crate::stream::Stream;
use crate::usage_metrics::{Ids, MetricCounterRecorder, USAGE_METRICS};

/// Forward bytes in both directions (client <-> compute).
pub(crate) async fn proxy_pass(
    client: impl AsyncRead + AsyncWrite + Unpin,
    compute: impl AsyncRead + AsyncWrite + Unpin,
    aux: MetricsAuxInfo,
    private_link_id: Option<SmolStr>,
) -> Result<(), ErrorSource> {
    // we will report ingress at a later date
    let usage_tx = USAGE_METRICS.register(Ids {
        endpoint_id: aux.endpoint_id,
        branch_id: aux.branch_id,
        private_link_id,
    });

    let metrics = &Metrics::get().proxy.io_bytes;
    let m_sent = metrics.with_labels(Direction::Tx);
    let mut client = MeasuredStream::new(
        client,
        |_| {},
        |cnt| {
            // Number of bytes we sent to the client (outbound).
            metrics.get_metric(m_sent).inc_by(cnt as u64);
            usage_tx.record_egress(cnt as u64);
        },
    );

    let m_recv = metrics.with_labels(Direction::Rx);
    let mut compute = MeasuredStream::new(
        compute,
        |_| {},
        |cnt| {
            // Number of bytes the client sent to the compute node (inbound).
            metrics.get_metric(m_recv).inc_by(cnt as u64);
            usage_tx.record_ingress(cnt as u64);
        },
    );

    // Starting from here we only proxy the client's traffic.
    debug!("performing the proxy pass...");
    let _ = crate::proxy::copy_bidirectional::copy_bidirectional_client_compute(
        &mut client,
        &mut compute,
    )
    .await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn passthrough<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    ctx: RequestContext,
    compute_config: &'static ComputeConfig,

    client: Stream<S>,
    compute: PostgresConnection,
    cancel: cancellation::Session,

    _req: NumConnectionRequestsGuard<'static>,
    _conn: NumClientConnectionsGuard<'static>,
    _tracker: TaskTrackerToken,
) {
    let session_id = ctx.session_id();
    let private_link_id = match ctx.extra() {
        Some(ConnectionInfoExtra::Aws { vpce_id }) => Some(vpce_id.clone()),
        Some(ConnectionInfoExtra::Azure { link_id }) => Some(link_id.to_smolstr()),
        None => None,
    };

    let _disconnect = ctx.log_connect();
    let res = proxy_pass(client, compute.stream, compute.aux, private_link_id).await;

    match res {
        Ok(()) => {}
        Err(ErrorSource::Client(e)) => {
            tracing::warn!(
                session_id = ?session_id,
                "per-client task finished with an IO error from the client: {e:#}"
            );
        }
        Err(ErrorSource::Compute(e)) => {
            tracing::error!(
                session_id = ?session_id,
                "per-client task finished with an IO error from the compute: {e:#}"
            );
        }
    }

    if let Err(err) = compute
        .cancel_closure
        .try_cancel_query(compute_config)
        .await
    {
        tracing::warn!(session_id = ?session_id, ?err, "could not cancel the query in the database");
    }

    // we don't need a result. If the queue is full, we just log the error
    drop(cancel.remove_cancel_key());
}
