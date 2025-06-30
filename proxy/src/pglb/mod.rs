pub mod copy_bidirectional;
pub mod handshake;
pub mod inprocess;
pub mod passthrough;

use std::sync::Arc;

use futures::FutureExt;
use smol_str::ToSmolStr;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, warn};

use crate::auth;
use crate::cancellation::{self, CancellationHandler};
use crate::config::{ProxyConfig, ProxyProtocolV2, TlsConfig};
use crate::context::RequestContext;
use crate::error::{ReportableError, UserFacingError};
use crate::metrics::{Metrics, NumClientConnectionsGuard};
pub use crate::pglb::copy_bidirectional::ErrorSource;
use crate::pglb::handshake::{HandshakeData, HandshakeError, handshake};
use crate::pglb::passthrough::ProxyPassthrough;
use crate::protocol2::{ConnectHeader, ConnectionInfo, ConnectionInfoExtra, read_proxy_protocol};
use crate::proxy::handle_client;
use crate::rate_limiter::EndpointRateLimiter;
use crate::stream::Stream;
use crate::util::run_until_cancelled;

pub const ERR_INSECURE_CONNECTION: &str = "connection is insecure (try using `sslmode=require`)";

#[derive(Error, Debug)]
#[error("{ERR_INSECURE_CONNECTION}")]
pub struct TlsRequired;

impl ReportableError for TlsRequired {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        crate::error::ErrorKind::User
    }
}

impl UserFacingError for TlsRequired {}

pub async fn task_main(
    config: &'static ProxyConfig,
    auth_backend: &'static auth::Backend<'static, ()>,
    listener: tokio::net::TcpListener,
    cancellation_token: CancellationToken,
    cancellation_handler: Arc<CancellationHandler>,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("proxy has shut down");
    }

    // When set for the server socket, the keepalive setting
    // will be inherited by all accepted client sockets.
    socket2::SockRef::from(&listener).set_keepalive(true)?;

    let connections = tokio_util::task::task_tracker::TaskTracker::new();
    let cancellations = tokio_util::task::task_tracker::TaskTracker::new();

    while let Some(accept_result) =
        run_until_cancelled(listener.accept(), &cancellation_token).await
    {
        let (socket, peer_addr) = accept_result?;

        let conn_gauge = Metrics::get()
            .proxy
            .client_connections
            .guard(crate::metrics::Protocol::Tcp);

        let session_id = uuid::Uuid::new_v4();
        let cancellation_handler = Arc::clone(&cancellation_handler);
        let cancellations = cancellations.clone();

        debug!(protocol = "tcp", %session_id, "accepted new TCP connection");
        let endpoint_rate_limiter2 = endpoint_rate_limiter.clone();

        connections.spawn(async move {
            let (socket, conn_info) = match config.proxy_protocol_v2 {
                ProxyProtocolV2::Required => {
                    match read_proxy_protocol(socket).await {
                        Err(e) => {
                            warn!("per-client task finished with an error: {e:#}");
                            return;
                        }
                        // our load balancers will not send any more data. let's just exit immediately
                        Ok((_socket, ConnectHeader::Local)) => {
                            debug!("healthcheck received");
                            return;
                        }
                        Ok((socket, ConnectHeader::Proxy(info))) => (socket, info),
                    }
                }
                // ignore the header - it cannot be confused for a postgres or http connection so will
                // error later.
                ProxyProtocolV2::Rejected => (
                    socket,
                    ConnectionInfo {
                        addr: peer_addr,
                        extra: None,
                    },
                ),
            };

            match socket.set_nodelay(true) {
                Ok(()) => {}
                Err(e) => {
                    error!(
                        "per-client task finished with an error: failed to set socket option: {e:#}"
                    );
                    return;
                }
            }

            let ctx = RequestContext::new(session_id, conn_info, crate::metrics::Protocol::Tcp);

            let res = handle_connection(
                config,
                auth_backend,
                &ctx,
                cancellation_handler,
                socket,
                ClientMode::Tcp,
                endpoint_rate_limiter2,
                conn_gauge,
                cancellations,
            )
            .instrument(ctx.span())
            .boxed()
            .await;

            match res {
                Err(e) => {
                    ctx.set_error_kind(e.get_error_kind());
                    warn!(parent: &ctx.span(), "per-client task finished with an error: {e:#}");
                }
                Ok(None) => {
                    ctx.set_success();
                }
                Ok(Some(p)) => {
                    ctx.set_success();
                    let _disconnect = ctx.log_connect();
                    match p.proxy_pass().await {
                        Ok(()) => {}
                        Err(ErrorSource::Client(e)) => {
                            warn!(
                                ?session_id,
                                "per-client task finished with an IO error from the client: {e:#}"
                            );
                        }
                        Err(ErrorSource::Compute(e)) => {
                            error!(
                                ?session_id,
                                "per-client task finished with an IO error from the compute: {e:#}"
                            );
                        }
                    }
                }
            }
        });
    }

    connections.close();
    cancellations.close();
    drop(listener);

    // Drain connections
    connections.wait().await;
    cancellations.wait().await;

    Ok(())
}

pub(crate) enum ClientMode {
    Tcp,
    Websockets { hostname: Option<String> },
}

/// Abstracts the logic of handling TCP vs WS clients
impl ClientMode {
    pub fn allow_cleartext(&self) -> bool {
        match self {
            ClientMode::Tcp => false,
            ClientMode::Websockets { .. } => true,
        }
    }

    pub fn hostname<'a, S>(&'a self, s: &'a Stream<S>) -> Option<&'a str> {
        match self {
            ClientMode::Tcp => s.sni_hostname(),
            ClientMode::Websockets { hostname } => hostname.as_deref(),
        }
    }

    pub fn handshake_tls<'a>(&self, tls: Option<&'a TlsConfig>) -> Option<&'a TlsConfig> {
        match self {
            ClientMode::Tcp => tls,
            // TLS is None here if using websockets, because the connection is already encrypted.
            ClientMode::Websockets { .. } => None,
        }
    }
}

#[derive(Debug, Error)]
// almost all errors should be reported to the user, but there's a few cases where we cannot
// 1. Cancellation: we are not allowed to tell the client any cancellation statuses for security reasons
// 2. Handshake: handshake reports errors if it can, otherwise if the handshake fails due to protocol violation,
//    we cannot be sure the client even understands our error message
// 3. PrepareClient: The client disconnected, so we can't tell them anyway...
pub(crate) enum ClientRequestError {
    #[error("{0}")]
    Cancellation(#[from] cancellation::CancelError),
    #[error("{0}")]
    Handshake(#[from] HandshakeError),
    #[error("{0}")]
    HandshakeTimeout(#[from] tokio::time::error::Elapsed),
    #[error("{0}")]
    PrepareClient(#[from] std::io::Error),
    #[error("{0}")]
    ReportedError(#[from] crate::stream::ReportedError),
}

impl ReportableError for ClientRequestError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            ClientRequestError::Cancellation(e) => e.get_error_kind(),
            ClientRequestError::Handshake(e) => e.get_error_kind(),
            ClientRequestError::HandshakeTimeout(_) => crate::error::ErrorKind::RateLimit,
            ClientRequestError::ReportedError(e) => e.get_error_kind(),
            ClientRequestError::PrepareClient(_) => crate::error::ErrorKind::ClientDisconnect,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_connection<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &'static ProxyConfig,
    auth_backend: &'static auth::Backend<'static, ()>,
    ctx: &RequestContext,
    cancellation_handler: Arc<CancellationHandler>,
    client: S,
    mode: ClientMode,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    conn_gauge: NumClientConnectionsGuard<'static>,
    cancellations: tokio_util::task::task_tracker::TaskTracker,
) -> Result<Option<ProxyPassthrough<S>>, ClientRequestError> {
    debug!(
        protocol = %ctx.protocol(),
        "handling interactive connection from client"
    );

    let metrics = &Metrics::get().proxy;
    let proto = ctx.protocol();
    let request_gauge = metrics.connection_requests.guard(proto);

    let tls = config.tls_config.load();
    let tls = tls.as_deref();

    let record_handshake_error = !ctx.has_private_peer_addr();
    let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Client);
    let do_handshake = handshake(ctx, client, mode.handshake_tls(tls), record_handshake_error);

    let (mut client, params) = match tokio::time::timeout(config.handshake_timeout, do_handshake)
        .await??
    {
        HandshakeData::Startup(client, params) => (client, params),
        HandshakeData::Cancel(cancel_key_data) => {
            // spawn a task to cancel the session, but don't wait for it
            cancellations.spawn({
                let cancellation_handler_clone = Arc::clone(&cancellation_handler);
                let ctx = ctx.clone();
                let cancel_span = tracing::span!(parent: None, tracing::Level::INFO, "cancel_session", session_id = ?ctx.session_id());
                cancel_span.follows_from(tracing::Span::current());
                async move {
                    cancellation_handler_clone
                        .cancel_session(
                            cancel_key_data,
                            ctx,
                            config.authentication_config.ip_allowlist_check_enabled,
                            config.authentication_config.is_vpc_acccess_proxy,
                            auth_backend.get_api(),
                        )
                        .await
                        .inspect_err(|e | debug!(error = ?e, "cancel_session failed")).ok();
                }.instrument(cancel_span)
            });

            return Ok(None);
        }
    };
    drop(pause);

    ctx.set_db_options(params.clone());

    let common_names = tls.map(|tls| &tls.common_names);

    let (node, cancel_on_shutdown) = handle_client(
        config,
        auth_backend,
        ctx,
        cancellation_handler,
        &mut client,
        &mode,
        endpoint_rate_limiter,
        common_names,
        &params,
    )
    .await?;

    let client = client.flush_and_into_inner().await?;

    let private_link_id = match ctx.extra() {
        Some(ConnectionInfoExtra::Aws { vpce_id }) => Some(vpce_id.clone()),
        Some(ConnectionInfoExtra::Azure { link_id }) => Some(link_id.to_smolstr()),
        None => None,
    };

    Ok(Some(ProxyPassthrough {
        client,
        compute: node.stream,

        aux: node.aux,
        private_link_id,

        _cancel_on_shutdown: cancel_on_shutdown,

        _req: request_gauge,
        _conn: conn_gauge,
        _db_conn: node.guage,
    }))
}
