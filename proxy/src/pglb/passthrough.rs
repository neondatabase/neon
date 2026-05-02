use std::convert::Infallible;

use postgres_client::connect_raw::StartupStream;
use smol_str::SmolStr;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::debug;
use utils::measured_stream::MeasuredStream;

use super::copy_bidirectional::ErrorSource;
use crate::compute::{ComputeConnection, MaybeRustlsStream};
use crate::config::TcpPoolConfig;
use crate::control_plane::messages::MetricsAuxInfo;
use crate::metrics::{Direction, Metrics, NumClientConnectionsGuard, NumConnectionRequestsGuard};
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
        let transaction_mode =
            use_pool && self.tcp_pool_config.mode == crate::config::TcpPoolMode::Transaction;

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
            checkout.release(compute, result.is_ok());
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
    use crate::pglb::copy_bidirectional::{BoundaryReason, copy_bidirectional_until_boundary};

    let ProxyPassthrough {
        mut client,
        compute,
        private_link_id,
        tcp_pool_checkout,
        tcp_pool_config: _,
        ..
    } = pt;

    // SAFETY: transaction mode is only entered when checkout was Some.
    let initial_checkout = tcp_pool_checkout.expect("transaction mode requires a pool checkout");
    let pool_key = initial_checkout.key().clone();
    let reset_query = initial_checkout.reset_query();
    let mut current_checkout = Some(initial_checkout);

    struct ComputeParts {
        aux: MetricsAuxInfo,
        hostname: crate::types::Host,
        ssl_mode: postgres_client::config::SslMode,
        socket_addr: std::net::SocketAddr,
        guage: crate::metrics::NumDbConnectionsGuard<'static>,
    }

    fn split_compute(conn: ComputeConnection) -> (ComputeParts, MaybeRustlsStream) {
        let stream = conn.stream.into_framed().into_inner();
        let parts = ComputeParts {
            aux: conn.aux,
            hostname: conn.hostname,
            ssl_mode: conn.ssl_mode,
            socket_addr: conn.socket_addr,
            guage: conn.guage,
        };
        (parts, stream)
    }

    fn join_compute(parts: ComputeParts, stream: MaybeRustlsStream) -> ComputeConnection {
        ComputeConnection {
            stream: StartupStream::new(stream),
            aux: parts.aux,
            hostname: parts.hostname,
            ssl_mode: parts.ssl_mode,
            socket_addr: parts.socket_addr,
            guage: parts.guage,
        }
    }

    let (initial_parts, initial_stream) = split_compute(compute);
    let aux = initial_parts.aux.clone();
    let mut stream: Option<MaybeRustlsStream> = Some(initial_stream);
    let mut compute_parts = Some(initial_parts);
    let mut client_msg_buf = Vec::new();

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

    debug!("performing the transaction-mode pump...");

    // Release immediately: in transaction mode we only need compute when
    // actual frontend traffic arrives.
    if let Some(checkout) = current_checkout.take() {
        let stream_inner = stream
            .take()
            .expect("compute stream must exist before initial release");
        let c = join_compute(
            compute_parts
                .take()
                .expect("compute must exist before release"),
            stream_inner,
        );
        checkout.release(c, true);
    }

    // After startup handshake the backend is idle from transaction perspective.
    let mut last_known_status: u8 = b'I';

    let final_result: Result<(), ErrorSource> = loop {
        if current_checkout.is_none() {
            let (tag, body) =
                crate::pqproto::read_message(&mut client, &mut client_msg_buf, i32::MAX as u32)
                    .await
                    .map_err(ErrorSource::Client)?;

            if tag == b'X' {
                break Ok(());
            }

            let (next_compute, next_checkout) = crate::tcp_pool::manager()
                .reacquire(pool_key.clone(), reset_query.clone())
                .await
                .map_err(|e| ErrorSource::Compute(std::io::Error::other(e.to_string())))?;

            let (next_parts, mut next_stream) = split_compute(next_compute);
            next_stream
                .write_u8(tag)
                .await
                .map_err(ErrorSource::Compute)?;
            next_stream
                .write_u32((body.len() + 4) as u32)
                .await
                .map_err(ErrorSource::Compute)?;
            next_stream
                .write_all(body)
                .await
                .map_err(ErrorSource::Compute)?;
            next_stream.flush().await.map_err(ErrorSource::Compute)?;

            stream = Some(next_stream);
            compute_parts = Some(next_parts);
            current_checkout = Some(next_checkout);
        }

        let mut measured_client = MeasuredStream::new(
            &mut client,
            |_| {},
            |cnt| {
                metrics.get_metric(m_sent).inc_by(cnt as u64);
                usage_tx.record_egress(cnt as u64);
            },
        );

        let stream_ref = stream
            .as_mut()
            .expect("compute stream must be checked out in transaction loop");
        let mut measured_compute = MeasuredStream::new(
            stream_ref,
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
                if !matches!(status, b'I' | b'T' | b'E') {
                    if let Some(checkout) = current_checkout.take() {
                        let stream_inner = stream
                            .take()
                            .expect("compute stream must exist before release");
                        let c = join_compute(
                            compute_parts
                                .take()
                                .expect("compute must exist before release"),
                            stream_inner,
                        );
                        checkout.release(c, false);
                    }
                    break Err(ErrorSource::Compute(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid ReadyForQuery status: {status}"),
                    )));
                }
                last_known_status = status;
                if status == b'I' {
                    // Transaction complete: release and wait for next frontend message.
                    let stream_inner = stream
                        .take()
                        .expect("compute stream must exist before release");
                    let c = join_compute(
                        compute_parts
                            .take()
                            .expect("compute must exist before release"),
                        stream_inner,
                    );
                    if let Some(checkout) = current_checkout.take() {
                        checkout.release(c, true);
                    }
                    last_known_status = b'I';
                }
                // 'T'/'E': stay in inner loop, keep compute held.
            }
            Ok(BoundaryReason::ClientTerminated) => {
                // Pool the compute only if it's known to be idle. If we were
                // mid-transaction (last_status was 'T' or 'E'), the conn has
                // an open BEGIN block and is unsafe to share.
                if let Some(checkout) = current_checkout.take() {
                    let stream_inner = stream
                        .take()
                        .expect("compute stream must exist before release");
                    let reusable = last_known_status == b'I';
                    let c = join_compute(
                        compute_parts
                            .take()
                            .expect("compute must exist before release"),
                        stream_inner,
                    );
                    checkout.release(c, reusable);
                }
                break Ok(());
            }
            Ok(BoundaryReason::ComputeClosed) => {
                if let Some(checkout) = current_checkout.take() {
                    let stream_inner = stream
                        .take()
                        .expect("compute stream must exist before release");
                    let c = join_compute(
                        compute_parts
                            .take()
                            .expect("compute must exist before release"),
                        stream_inner,
                    );
                    checkout.release(c, false);
                }
                break Err(ErrorSource::Compute(std::io::Error::other(
                    "compute closed connection",
                )));
            }
            Err(e) => {
                if let Some(checkout) = current_checkout.take() {
                    let stream_inner = stream
                        .take()
                        .expect("compute stream must exist before release");
                    let c = join_compute(
                        compute_parts
                            .take()
                            .expect("compute must exist before release"),
                        stream_inner,
                    );
                    checkout.release(c, false);
                }
                break Err(e);
            }
        }
    };

    final_result
}
