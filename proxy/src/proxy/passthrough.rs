use smol_str::SmolStr;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::debug;

use super::copy_bidirectional::ErrorSource;
use crate::cancellation;
use crate::compute::PostgresConnection;
use crate::config::ComputeConfig;
use crate::control_plane::messages::MetricsAuxInfo;
use crate::metrics::{Direction, Metrics, NumClientConnectionsGuard, NumConnectionRequestsGuard};
use crate::proxy::copy_bidirectional_client_compute;
use crate::stream::Stream;
use crate::usage_metrics::{Ids, MetricCounterRecorder, USAGE_METRICS};

/// Forward bytes in both directions (client <-> compute).
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) async fn proxy_pass(
    mut client: Stream<impl AsyncRead + AsyncWrite + Unpin>,
    mut compute: impl AsyncRead + AsyncWrite + Unpin,
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
    let m_sent = metrics.with_labels(Direction::ComputeToClient);
    let m_recv = metrics.with_labels(Direction::ClientToCompute);

    let inspect = |direction, bytes: &[u8]| match direction {
        Direction::ComputeToClient => {
            metrics.get_metric(m_sent).inc_by(bytes.len() as u64);
            usage_tx.record_egress(bytes.len() as u64);
        }
        Direction::ClientToCompute => {
            metrics.get_metric(m_recv).inc_by(bytes.len() as u64);
            usage_tx.record_ingress(bytes.len() as u64);
        }
    };

    // Starting from here we only proxy the client's traffic.
    debug!("performing the proxy pass...");

    // reduce branching internal to the hot path.
    match &mut client {
        Stream::Raw { raw } => copy_bidirectional_client_compute(raw, &mut compute, inspect).await,
        Stream::Tls { tls, .. } => {
            copy_bidirectional_client_compute(&mut *tls, &mut compute, inspect).await
        }
    }
}

pub(crate) struct ProxyPassthrough<S> {
    pub(crate) client: Stream<S>,
    pub(crate) compute: PostgresConnection,
    pub(crate) aux: MetricsAuxInfo,
    pub(crate) session_id: uuid::Uuid,
    pub(crate) private_link_id: Option<SmolStr>,
    pub(crate) cancel: cancellation::Session,

    pub(crate) _req: NumConnectionRequestsGuard<'static>,
    pub(crate) _conn: NumClientConnectionsGuard<'static>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> ProxyPassthrough<S> {
    pub(crate) async fn proxy_pass(
        self,
        compute_config: &ComputeConfig,
    ) -> Result<(), ErrorSource> {
        let res = proxy_pass(
            self.client,
            self.compute.stream,
            self.aux,
            self.private_link_id,
        )
        .await;
        if let Err(err) = self
            .compute
            .cancel_closure
            .try_cancel_query(compute_config)
            .await
        {
            tracing::warn!(session_id = ?self.session_id, ?err, "could not cancel the query in the database");
        }

        drop(self.cancel.remove_cancel_key()); // we don't need a result. If the queue is full, we just log the error

        res
    }
}
