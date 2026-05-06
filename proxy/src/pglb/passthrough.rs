use std::convert::Infallible;

use postgres_client::connect_raw::StartupStream;
use smol_str::SmolStr;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::debug;
use utils::measured_stream::MeasuredStream;

use super::copy_bidirectional::ErrorSource;
use crate::compute::{ComputeConnection, MaybeRustlsStream};
use crate::config::{TcpPoolConfig, TcpPoolMode};
use crate::control_plane::messages::MetricsAuxInfo;
use crate::metrics::{Direction, Metrics, NumClientConnectionsGuard, NumConnectionRequestsGuard};
use crate::stream::Stream;
use crate::tcp_pool::{TcpPoolCheckout, TcpPoolReacquire};
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

fn release_compute_checkout(
    current_checkout: &mut Option<TcpPoolCheckout>,
    stream: &mut Option<MaybeRustlsStream>,
    compute_parts: &mut Option<ComputeParts>,
    reusable: bool,
) {
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
        checkout.release(c, reusable);
    }
}

async fn write_frontend_message<S>(stream: &mut S, tag: u8, body: &[u8]) -> Result<(), ErrorSource>
where
    S: AsyncWrite + Unpin,
{
    stream.write_u8(tag).await.map_err(ErrorSource::Compute)?;
    stream
        .write_u32((body.len() + 4) as u32)
        .await
        .map_err(ErrorSource::Compute)?;
    stream.write_all(body).await.map_err(ErrorSource::Compute)?;
    stream.flush().await.map_err(ErrorSource::Compute)
}

pub(crate) struct ProxyPassthrough<S> {
    pub(crate) client: Stream<S>,
    pub(crate) compute: Option<ComputeConnection>,
    pub(crate) private_link_id: Option<SmolStr>,
    pub(crate) tcp_pool_checkout: Option<TcpPoolCheckout>,
    pub(crate) tcp_pool_reacquire: Option<TcpPoolReacquire>,
    pub(crate) tcp_pool_config: TcpPoolConfig,

    pub(crate) _cancel_on_shutdown: Option<tokio::sync::oneshot::Sender<Infallible>>,

    pub(crate) _req: NumConnectionRequestsGuard<'static>,
    pub(crate) _conn: NumClientConnectionsGuard<'static>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> ProxyPassthrough<S> {
    pub(crate) async fn proxy_pass(self) -> Result<(), ErrorSource> {
        let mut pt = self;

        // Three paths share this entry point:
        //   - no pool checkout: legacy direct passthrough.
        //   - session pool metadata: acquire compute only after first frontend
        //     message, then hold it for the rest of the client session.
        //   - transaction pool metadata: multiplex loop that acquires compute
        //     only while a transaction is active.
        let transaction_mode =
            pt.tcp_pool_reacquire.is_some() && pt.tcp_pool_config.mode == TcpPoolMode::Transaction;
        let session_mode = pt.tcp_pool_config.mode == TcpPoolMode::Session
            && (pt.tcp_pool_checkout.is_some() || pt.tcp_pool_reacquire.is_some());

        if session_mode || transaction_mode {
            if let Some(compute) = pt.compute.take() {
                let checkout = pt
                    .tcp_pool_checkout
                    .take()
                    .expect("initial pooled compute requires a pool checkout");
                if pt.tcp_pool_reacquire.is_none() {
                    pt.tcp_pool_reacquire = Some(checkout.reacquire_info());
                }
                checkout.release(compute, true);
            }
        }

        if transaction_mode {
            return proxy_pass_transaction_mode(pt).await;
        }

        if session_mode {
            return proxy_pass_session_mode(pt).await;
        }

        let compute = pt
            .compute
            .expect("non-transaction passthrough requires an initial compute connection");
        let _keep_db_guard_live = &compute.guage;
        let mut stream: MaybeRustlsStream = compute.stream.into_framed().into_inner();
        let aux = compute.aux.clone();

        proxy_pass(pt.client, &mut stream, aux, pt.private_link_id).await
    }
}

/// Session-mode pool path. Startup may be cold (an initial compute was needed
/// to obtain startup params) or warm (startup params were cached). In either
/// case, do not hold compute while the client is idle after startup.
async fn proxy_pass_session_mode<S: AsyncRead + AsyncWrite + Unpin>(
    pt: ProxyPassthrough<S>,
) -> Result<(), ErrorSource> {
    let ProxyPassthrough {
        mut client,
        private_link_id,
        tcp_pool_reacquire,
        ..
    } = pt;

    debug!("performing the session-mode pump...");

    let reacquire = tcp_pool_reacquire.expect("session mode requires pool reacquire metadata");
    let pool_key = reacquire.key().clone();
    let reset_query = reacquire.reset_query();

    let mut client_msg_buf = Vec::new();

    let (tag, body) =
        crate::pqproto::read_message(&mut client, &mut client_msg_buf, i32::MAX as u32)
            .await
            .map_err(ErrorSource::Client)?;

    if tag == b'X' {
        return Ok(());
    }

    let (compute, checkout) = crate::tcp_pool::manager()
        .reacquire(pool_key, reset_query)
        .await
        .map_err(|e| ErrorSource::Compute(std::io::Error::other(e.to_string())))?;

    let (parts, mut stream) = split_compute(compute);
    write_frontend_message(&mut stream, tag, &body).await?;

    let aux = parts.aux.clone();
    let result = proxy_pass_pooled(client, &mut stream, aux, private_link_id).await;
    let compute = join_compute(parts, stream);
    checkout.release(compute, result.is_ok());

    result
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
        private_link_id,
        tcp_pool_reacquire,
        tcp_pool_config: _,
        ..
    } = pt;

    // SAFETY: transaction mode is only entered with reacquire metadata.
    let reacquire = tcp_pool_reacquire.expect("transaction mode requires pool reacquire metadata");
    let pool_key = reacquire.key().clone();
    let reset_query = reacquire.reset_query();
    let mut current_checkout: Option<TcpPoolCheckout> = None;
    let mut stream: Option<MaybeRustlsStream> = None;
    let mut compute_parts: Option<ComputeParts> = None;
    let mut client_msg_buf = Vec::new();

    // Mirror proxy_pass_pooled's metrics wiring so the per-transaction
    // pump still records bytes-in/bytes-out for the session.
    let mut usage_tx = None;
    let metrics = &Metrics::get().proxy.io_bytes;
    let m_sent = metrics.with_labels(Direction::Tx);
    let m_recv = metrics.with_labels(Direction::Rx);

    debug!("performing the transaction-mode pump...");

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
            write_frontend_message(&mut next_stream, tag, &body).await?;

            stream = Some(next_stream);
            compute_parts = Some(next_parts);
            current_checkout = Some(next_checkout);
        }

        let usage_tx = usage_tx.get_or_insert_with(|| {
            let aux = &compute_parts
                .as_ref()
                .expect("compute metadata must be checked out in transaction loop")
                .aux;
            USAGE_METRICS.register(Ids {
                endpoint_id: aux.endpoint_id,
                branch_id: aux.branch_id,
                private_link_id: private_link_id.clone(),
            })
        });

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
                    release_compute_checkout(
                        &mut current_checkout,
                        &mut stream,
                        &mut compute_parts,
                        false,
                    );
                    break Err(ErrorSource::Compute(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid ReadyForQuery status: {status}"),
                    )));
                }
                last_known_status = status;
                if status == b'I' {
                    // Transaction complete: release and wait for next frontend message.
                    release_compute_checkout(
                        &mut current_checkout,
                        &mut stream,
                        &mut compute_parts,
                        true,
                    );
                    last_known_status = b'I';
                }
                // 'T'/'E': stay in inner loop, keep compute held.
            }
            Ok(BoundaryReason::ClientTerminated) => {
                // Pool the compute only if it's known to be idle. If we were
                // mid-transaction (last_status was 'T' or 'E'), the conn has
                // an open BEGIN block and is unsafe to share.
                release_compute_checkout(
                    &mut current_checkout,
                    &mut stream,
                    &mut compute_parts,
                    last_known_status == b'I',
                );
                break Ok(());
            }
            Ok(BoundaryReason::ComputeClosed) => {
                release_compute_checkout(
                    &mut current_checkout,
                    &mut stream,
                    &mut compute_parts,
                    false,
                );
                break Err(ErrorSource::Compute(std::io::Error::other(
                    "compute closed connection",
                )));
            }
            Err(e) => {
                release_compute_checkout(
                    &mut current_checkout,
                    &mut stream,
                    &mut compute_parts,
                    false,
                );
                break Err(e);
            }
        }
    };

    final_result
}
