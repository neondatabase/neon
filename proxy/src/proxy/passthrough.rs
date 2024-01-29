use crate::{
    console::messages::MetricsAuxInfo,
    context::RequestMonitoring,
    metrics::{NUM_BYTES_PROXIED_COUNTER, NUM_BYTES_PROXIED_PER_CLIENT_COUNTER},
    usage_metrics::{Ids, USAGE_METRICS},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;
use utils::measured_stream::MeasuredStream;

/// Forward bytes in both directions (client <-> compute).
#[tracing::instrument(skip_all)]
pub async fn proxy_pass(
    ctx: &mut RequestMonitoring,
    client: impl AsyncRead + AsyncWrite + Unpin,
    compute: impl AsyncRead + AsyncWrite + Unpin,
    aux: MetricsAuxInfo,
) -> anyhow::Result<()> {
    ctx.set_success();
    ctx.log();

    let usage = USAGE_METRICS.register(Ids {
        endpoint_id: aux.endpoint_id.clone(),
        branch_id: aux.branch_id.clone(),
    });

    let m_sent = NUM_BYTES_PROXIED_COUNTER.with_label_values(&["tx"]);
    let m_sent2 = NUM_BYTES_PROXIED_PER_CLIENT_COUNTER.with_label_values(&aux.traffic_labels("tx"));
    let mut client = MeasuredStream::new(
        client,
        |_| {},
        |cnt| {
            // Number of bytes we sent to the client (outbound).
            m_sent.inc_by(cnt as u64);
            m_sent2.inc_by(cnt as u64);
            usage.record_egress(cnt as u64);
        },
    );

    let m_recv = NUM_BYTES_PROXIED_COUNTER.with_label_values(&["rx"]);
    let m_recv2 = NUM_BYTES_PROXIED_PER_CLIENT_COUNTER.with_label_values(&aux.traffic_labels("rx"));
    let mut compute = MeasuredStream::new(
        compute,
        |_| {},
        |cnt| {
            // Number of bytes the client sent to the compute node (inbound).
            m_recv.inc_by(cnt as u64);
            m_recv2.inc_by(cnt as u64);
        },
    );

    // Starting from here we only proxy the client's traffic.
    info!("performing the proxy pass...");
    let _ = tokio::io::copy_bidirectional(&mut client, &mut compute).await?;

    Ok(())
}
