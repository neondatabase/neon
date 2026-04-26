use std::convert::Infallible;

use smol_str::SmolStr;
use postgres_client::connect_raw::StartupStream;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, warn};
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
        // Three paths share this entry point:
        //   - no pool checkout: legacy direct passthrough.
        //   - pool checkout, mode = Session: pooled session-level passthrough.
        //   - pool checkout, mode = Transaction: multiplex loop that releases
        //     and re-acquires the compute conn at every transaction boundary.
        let use_pool = self.tcp_pool_checkout.is_some();
        let transaction_mode = use_pool
            && self.tcp_pool_config.mode == crate::config::TcpPoolMode::Transaction;

        if transaction_mode {
            return proxy_pass_transaction_mode(self).await;
        }

        let mut compute = self.compute;
        let _keep_db_guard_live = &compute.guage;
        let mut stream: MaybeRustlsStream = compute.stream.into_framed().into_inner();
        let aux = compute.aux.clone();

        let result = if use_pool {
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

/// Transaction-mode multiplex loop. Pumps one transaction at a time using
/// `copy_bidirectional_until_boundary`; at each ReadyForQuery `'I'` the
/// compute connection is released to the pool and a new one is acquired
/// for the next transaction. Mid-transaction (status `'T'` / `'E'`) the
/// same compute is held until the transaction completes or aborts.
///
/// Caveats (standard PgBouncer transaction-mode caveats apply):
///   - `SET` (without `LOCAL`) does not survive across transactions
///   - `LISTEN` notifications may be lost
///   - prepared statements created via simple `PREPARE` are not visible
///     to the next transaction
///   - `pg_cancel_backend(pid)` does not work as expected because the pid
///     the client received is synthesized, not a real backend pid
async fn proxy_pass_transaction_mode<S: AsyncRead + AsyncWrite + Unpin>(
    pt: ProxyPassthrough<S>,
) -> Result<(), ErrorSource> {
    use crate::pglb::copy_bidirectional::{
        BoundaryReason, copy_bidirectional_until_boundary,
    };

    let ProxyPassthrough {
        mut client,
        mut compute,
        private_link_id,
        tcp_pool_checkout,
        tcp_pool_config,
        ..
    } = pt;

    // SAFETY: transaction mode is only entered when checkout was Some.
    let initial_checkout = tcp_pool_checkout.expect("transaction mode requires a pool checkout");
    let pool_key = initial_checkout.key().clone();
    let mut current_checkout = Some(initial_checkout);

    let aux = compute.aux.clone();
    let _keep_db_guard_live = &compute.guage;
    let mut stream: MaybeRustlsStream = compute.stream.into_framed().into_inner();

    // Mirror proxy_pass_pooled's metrics wiring so the per-transaction
    // pump still records bytes-in/bytes-out for the session.
    let usage_tx = USAGE_METRICS.register(Ids {
        endpoint_id: aux.endpoint_id,
        branch_id: aux.branch_id,
        private_link_id,
    });
    let metrics = &Metrics::get().proxy.io_bytes;
    let m_sent = metrics.with_labels(Direction::Tx);
    let m_recv = metrics.with_labels(Direction::Rx);
    let mut measured_client = MeasuredStream::new(
        &mut client,
        |_| {},
        |cnt| {
            metrics.get_metric(m_sent).inc_by(cnt as u64);
            usage_tx.record_egress(cnt as u64);
        },
    );

    debug!("performing the transaction-mode pump...");

    // After auth handshake and forward_compute_params_to_client, compute has
    // sent its first 'Z' I to the proxy (consumed by forward_compute_params).
    // Subsequently it stays idle until the client sends a query. So at the
    // start of the multiplex loop, the compute is in idle state.
    let mut last_known_status: u8 = b'I';

    let final_result: Result<(), ErrorSource> = loop {
        let mut measured_compute = MeasuredStream::new(
            &mut stream,
            |_| {},
            |cnt| {
                metrics.get_metric(m_recv).inc_by(cnt as u64);
                usage_tx.record_ingress(cnt as u64);
            },
        );

        let boundary =
            copy_bidirectional_until_boundary(&mut measured_client, &mut measured_compute).await;
        drop(measured_compute);

        match boundary {
            Ok(BoundaryReason::ReadyForQuery(status)) => {
                last_known_status = status;
                if status == b'I' {
                    // Transaction complete: release and try to re-acquire.
                    compute.stream = StartupStream::new(stream);
                    if let Some(checkout) = current_checkout.take() {
                        checkout.release(&tcp_pool_config, compute, true);
                    }

                    match crate::tcp_pool::manager()
                        .try_acquire_idle(&tcp_pool_config, &pool_key)
                    {
                        Some((next_compute, next_checkout)) => {
                            compute = next_compute;
                            stream = compute.stream.into_framed().into_inner();
                            current_checkout = Some(next_checkout);
                            // Freshly acquired conn was released at idle, so
                            // it's still in idle state.
                            last_known_status = b'I';
                        }
                        None => {
                            warn!(pool_key = %pool_key, "tcp pool: empty at re-acquire; closing client");
                            break Ok(());
                        }
                    }
                }
                // 'T'/'E': stay in inner loop, keep compute held.
            }
            Ok(BoundaryReason::ClientTerminated) => {
                // Pool the compute only if it's known to be idle. If we were
                // mid-transaction (last_status was 'T' or 'E'), the conn has
                // an open BEGIN block and is unsafe to share.
                let reusable = last_known_status == b'I';
                compute.stream = StartupStream::new(stream);
                if let Some(checkout) = current_checkout.take() {
                    checkout.release(&tcp_pool_config, compute, reusable);
                }
                break Ok(());
            }
            Ok(BoundaryReason::ComputeClosed) => {
                if let Some(checkout) = current_checkout.take() {
                    compute.stream = StartupStream::new(stream);
                    checkout.release(&tcp_pool_config, compute, false);
                }
                break Err(ErrorSource::Compute(std::io::Error::other(
                    "compute closed connection",
                )));
            }
            Err(e) => {
                if let Some(checkout) = current_checkout.take() {
                    compute.stream = StartupStream::new(stream);
                    checkout.release(&tcp_pool_config, compute, false);
                }
                break Err(e);
            }
        }
    };

    final_result
}
