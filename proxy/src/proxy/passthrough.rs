use crate::{
    cancellation::{self, CancelClosure},
    compute::PostgresConnection,
    console::messages::MetricsAuxInfo,
    metrics::NUM_BYTES_PROXIED_COUNTER,
    stream::Stream,
    usage_metrics::{Ids, USAGE_METRICS},
};
use metrics::IntCounterPairGuard;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;
use utils::measured_stream::MeasuredStream;

/// Forward bytes in both directions (client <-> compute).
#[tracing::instrument(skip_all)]
pub async fn proxy_pass(
    client: impl AsyncRead + AsyncWrite + Unpin,
    compute: impl AsyncRead + AsyncWrite + Unpin,
    cancel_closure: Option<CancelClosure>,
    aux: MetricsAuxInfo,
) -> anyhow::Result<()> {
    let usage = USAGE_METRICS.register(Ids {
        endpoint_id: aux.endpoint_id.clone(),
        branch_id: aux.branch_id.clone(),
    });

    let m_sent = NUM_BYTES_PROXIED_COUNTER.with_label_values(&["tx"]);
    let mut client = MeasuredStream::new(
        client,
        |_| {},
        |cnt| {
            // Number of bytes we sent to the client (outbound).
            m_sent.inc_by(cnt as u64);
            usage.record_egress(cnt as u64);
        },
    );

    let m_recv = NUM_BYTES_PROXIED_COUNTER.with_label_values(&["rx"]);
    let mut compute = MeasuredStream::new(
        compute,
        |_| {},
        |cnt| {
            // Number of bytes the client sent to the compute node (inbound).
            m_recv.inc_by(cnt as u64);
        },
    );

    // Starting from here we only proxy the client's traffic.
    info!("performing the proxy pass...");
    let _ = crate::proxy::copy_bidirectional::copy_bidirectional_client_compute(
        &mut client,
        &mut compute,
        cancel_closure,
    )
    .await?;

    Ok(())
}

pub struct ProxyPassthrough<S> {
    pub client: Stream<S>,
    pub compute: PostgresConnection,
    pub aux: MetricsAuxInfo,

    pub req: IntCounterPairGuard,
    pub conn: IntCounterPairGuard,
    pub cancel: cancellation::Session,
}

impl<S: AsyncRead + AsyncWrite + Unpin> ProxyPassthrough<S> {
    pub async fn proxy_pass(self) -> anyhow::Result<()> {
        proxy_pass(
            self.client,
            self.compute.stream,
            Some(self.compute.cancel_closure),
            self.aux,
        )
        .await
    }
}
