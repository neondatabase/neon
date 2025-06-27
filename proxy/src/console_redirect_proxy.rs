use std::sync::Arc;

use futures::{FutureExt, TryFutureExt};
use postgres_client::RawCancelToken;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info};

use crate::auth::backend::ConsoleRedirectBackend;
use crate::cancellation::{CancelClosure, CancellationHandler};
use crate::config::{ProxyConfig, ProxyProtocolV2};
use crate::context::RequestContext;
use crate::error::ReportableError;
use crate::metrics::{Metrics, NumClientConnectionsGuard};
use crate::pglb::ClientRequestError;
use crate::pglb::handshake::{HandshakeData, handshake};
use crate::pglb::passthrough::ProxyPassthrough;
use crate::pqproto::CancelKeyData;
use crate::protocol2::{ConnectHeader, ConnectionInfo, read_proxy_protocol};
use crate::proxy::{
    ErrorSource, connect_compute, forward_compute_params_to_client, send_client_greeting,
};
use crate::util::run_until_cancelled;

pub async fn task_main(
    config: &'static ProxyConfig,
    backend: &'static ConsoleRedirectBackend,
    listener: tokio::net::TcpListener,
    cancellation_token: CancellationToken,
    cancellation_handler: Arc<CancellationHandler>,
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

        connections.spawn(async move {
            let (socket, conn_info) = match config.proxy_protocol_v2 {
                ProxyProtocolV2::Required => {
                    match read_proxy_protocol(socket).await {
                        Err(e) => {
                            error!("per-client task finished with an error: {e:#}");
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

            let res = handle_client(
                config,
                backend,
                &ctx,
                cancellation_handler,
                socket,
                conn_gauge,
                cancellations,
            )
            .instrument(ctx.span())
            .boxed()
            .await;

            match res {
                Err(e) => {
                    ctx.set_error_kind(e.get_error_kind());
                    error!(parent: &ctx.span(), "per-client task finished with an error: {e:#}");
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
                            error!(
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_client<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &'static ProxyConfig,
    backend: &'static ConsoleRedirectBackend,
    ctx: &RequestContext,
    cancellation_handler: Arc<CancellationHandler>,
    stream: S,
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
    let do_handshake = handshake(ctx, stream, tls, record_handshake_error);

    let (mut stream, params) = match tokio::time::timeout(config.handshake_timeout, do_handshake)
        .await??
    {
        HandshakeData::Startup(stream, params) => (stream, params),
        HandshakeData::Cancel(cancel_key_data) => {
            // spawn a task to cancel the session, but don't wait for it
            cancellations.spawn({
                let cancellation_handler_clone  = Arc::clone(&cancellation_handler);
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
                            backend.get_api(),
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

    let (node_info, mut auth_info, _user_info) = match backend
        .authenticate(ctx, &config.authentication_config, &mut stream)
        .await
    {
        Ok(auth_result) => auth_result,
        Err(e) => Err(stream.throw_error(e, Some(ctx)).await)?,
    };
    auth_info.set_startup_params(&params, true);

    let mut node = connect_compute::connect_to_compute(
        ctx,
        config,
        &node_info,
        connect_compute::TlsNegotiation::Postgres,
    )
    .or_else(|e| async { Err(stream.throw_error(e, Some(ctx)).await) })
    .await?;

    auth_info
        .authenticate(ctx, &mut node)
        .or_else(|e| async { Err(stream.throw_error(e, Some(ctx)).await) })
        .await?;
    send_client_greeting(ctx, &config.greetings, &mut stream);

    // let session = cancellation_handler.get_key();

    let (_process_id, _secret_key) =
        forward_compute_params_to_client(ctx, None, &mut stream, &mut node.stream).await?;
    let stream = stream.flush_and_into_inner().await?;
    // let hostname = node.hostname.to_string();

    // let session_id = ctx.session_id();
    let (cancel_on_shutdown, _cancel) = tokio::sync::oneshot::channel();
    // tokio::spawn(async move {
    //     session
    //         .maintain_cancel_key(
    //             session_id,
    //             cancel,
    //             &CancelClosure {
    //                 socket_addr: node.socket_addr,
    //                 cancel_token: RawCancelToken {
    //                     ssl_mode: node.ssl_mode,
    //                     process_id,
    //                     secret_key,
    //                 },
    //                 hostname,
    //                 user_info,
    //             },
    //             &config.connect_to_compute,
    //         )
    //         .await;
    // });

    Ok(Some(ProxyPassthrough {
        client: stream,
        compute: node.stream.into_framed().into_inner(),

        aux: node.aux,
        private_link_id: None,

        _cancel_on_shutdown: cancel_on_shutdown,

        _req: request_gauge,
        _conn: conn_gauge,
        _db_conn: node.guage,
    }))
}
