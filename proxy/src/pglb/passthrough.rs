use std::convert::Infallible;

use smol_str::SmolStr;
use postgres_client::connect_raw::StartupStream;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::debug;
use utils::measured_stream::MeasuredStream;

use super::copy_bidirectional::ErrorSource;
use crate::compute::{ComputeConnection, MaybeRustlsStream};
use crate::config::TcpPoolConfig;
use crate::control_plane::messages::MetricsAuxInfo;
use crate::metrics::{
    Direction, Metrics, NumClientConnectionsGuard, NumConnectionRequestsGuard,
};
use crate::stream::Stream;
use crate::tcp_pool::TcpPoolCheckout;
use crate::usage_metrics::{Ids, MetricCounterRecorder, USAGE_METRICS};

/// Forward bytes in both directions (client <-> compute).
#[tracing::instrument(skip_all)]
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
    let _ = crate::pglb::copy_bidirectional::copy_bidirectional_client_compute(
        &mut client,
        &mut compute,
    )
    .await?;

    Ok(())
}

/// Same as `proxy_pass` but preserves compute stream on client EOF for pooled reuse.
#[tracing::instrument(skip_all)]
pub(crate) async fn proxy_pass_pooled(
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
            metrics.get_metric(m_sent).inc_by(cnt as u64);
            usage_tx.record_egress(cnt as u64);
        },
    );

    let m_recv = metrics.with_labels(Direction::Rx);
    let mut compute = MeasuredStream::new(
        compute,
        |_| {},
        |cnt| {
            metrics.get_metric(m_recv).inc_by(cnt as u64);
            usage_tx.record_ingress(cnt as u64);
        },
    );

    debug!("performing the pooled proxy pass...");
    let _ = crate::pglb::copy_bidirectional::copy_bidirectional_client_compute_pooled(
        &mut client,
        &mut compute,
    )
    .await?;

    Ok(())
}

pub(crate) struct ProxyPassthrough<S> {
    pub(crate) client: Stream<S>,
    pub(crate) compute: ComputeConnection,
    pub(crate) private_link_id: Option<SmolStr>,
    pub(crate) tcp_pool_checkout: Option<TcpPoolCheckout>,
    pub(crate) tcp_pool_config: TcpPoolConfig,

    pub(crate) _cancel_on_shutdown: tokio::sync::oneshot::Sender<Infallible>,

    pub(crate) _req: NumConnectionRequestsGuard<'static>,
    pub(crate) _conn: NumClientConnectionsGuard<'static>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> ProxyPassthrough<S> {
    pub(crate) async fn proxy_pass(self) -> Result<(), ErrorSource> {
        let mut compute = self.compute;
        let _keep_db_guard_live = &compute.guage;
        let mut stream: MaybeRustlsStream = compute.stream.into_framed().into_inner();
        let aux = compute.aux.clone();

        let result = if self.tcp_pool_checkout.is_some() {
            proxy_pass_pooled(self.client, &mut stream, aux, self.private_link_id).await
        } else {
            proxy_pass(self.client, &mut stream, aux, self.private_link_id).await
        };

        if let Some(checkout) = self.tcp_pool_checkout {
            compute.stream = StartupStream::new(stream);
            checkout.release(&self.tcp_pool_config, compute, result.is_ok());
        }

        result
    }
}
