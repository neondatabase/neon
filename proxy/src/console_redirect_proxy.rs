use crate::auth::backend::ConsoleRedirectBackend;
use crate::config::{ProxyConfig, ProxyProtocolV2};
use crate::proxy::{
    prepare_client_connection, run_until_cancelled, ClientRequestError, ErrorSource,
};
use crate::{
    cancellation::CancellationHandlerMain,
    context::RequestMonitoring,
    error::ReportableError,
    metrics::{Metrics, NumClientConnectionsGuard},
    protocol2::read_proxy_protocol,
    proxy::handshake::{handshake, HandshakeData},
};
use futures::TryFutureExt;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, Instrument};

use crate::proxy::{
    connect_compute::{connect_to_compute, TcpMechanism},
    passthrough::ProxyPassthrough,
};

pub async fn task_main(
    config: &'static ProxyConfig,
    backend: &'static ConsoleRedirectBackend,
    listener: tokio::net::TcpListener,
    cancellation_token: CancellationToken,
    cancellation_handler: Arc<CancellationHandlerMain>,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("proxy has shut down");
    }

    // When set for the server socket, the keepalive setting
    // will be inherited by all accepted client sockets.
    socket2::SockRef::from(&listener).set_keepalive(true)?;

    let connections = tokio_util::task::task_tracker::TaskTracker::new();

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

        tracing::info!(protocol = "tcp", %session_id, "accepted new TCP connection");

        connections.spawn(async move {
            let (socket, peer_addr) = match read_proxy_protocol(socket).await {
                Err(e) => {
                    error!("per-client task finished with an error: {e:#}");
                    return;
                }
                Ok((_socket, None)) if config.proxy_protocol_v2 == ProxyProtocolV2::Required => {
                    error!("missing required proxy protocol header");
                    return;
                }
                Ok((_socket, Some(_))) if config.proxy_protocol_v2 == ProxyProtocolV2::Rejected => {
                    error!("proxy protocol header not supported");
                    return;
                }
                Ok((socket, Some(addr))) => (socket, addr.ip()),
                Ok((socket, None)) => (socket, peer_addr.ip()),
            };

            match socket.inner.set_nodelay(true) {
                Ok(()) => {}
                Err(e) => {
                    error!("per-client task finished with an error: failed to set socket option: {e:#}");
                    return;
                }
            };

            let ctx = RequestMonitoring::new(
                session_id,
                peer_addr,
                crate::metrics::Protocol::Tcp,
                &config.region,
            );
            let span = ctx.span();

            let startup = Box::pin(
                handle_client(
                    config,
                    backend,
                    &ctx,
                    cancellation_handler,
                    socket,
                    conn_gauge,
                )
                .instrument(span.clone()),
            );
            let res = startup.await;

            match res {
                Err(e) => {
                    // todo: log and push to ctx the error kind
                    ctx.set_error_kind(e.get_error_kind());
                    error!(parent: &span, "per-client task finished with an error: {e:#}");
                }
                Ok(None) => {
                    ctx.set_success();
                }
                Ok(Some(p)) => {
                    ctx.set_success();
                    ctx.log_connect();
                    match p.proxy_pass().instrument(span.clone()).await {
                        Ok(()) => {}
                        Err(ErrorSource::Client(e)) => {
                            error!(parent: &span, "per-client task finished with an IO error from the client: {e:#}");
                        }
                        Err(ErrorSource::Compute(e)) => {
                            error!(parent: &span, "per-client task finished with an IO error from the compute: {e:#}");
                        }
                    }
                }
            }
        });
    }

    connections.close();
    drop(listener);

    // Drain connections
    connections.wait().await;

    Ok(())
}

pub(crate) async fn handle_client<S: AsyncRead + AsyncWrite + Unpin>(
    config: &'static ProxyConfig,
    backend: &'static ConsoleRedirectBackend,
    ctx: &RequestMonitoring,
    cancellation_handler: Arc<CancellationHandlerMain>,
    stream: S,
    conn_gauge: NumClientConnectionsGuard<'static>,
) -> Result<Option<ProxyPassthrough<S>>, ClientRequestError> {
    info!(
        protocol = %ctx.protocol(),
        "handling interactive connection from client"
    );

    let metrics = &Metrics::get().proxy;
    let proto = ctx.protocol();
    let request_gauge = metrics.connection_requests.guard(proto);

    let tls = config.tls_config.as_ref();

    let record_handshake_error = !ctx.has_private_peer_addr();
    let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Client);
    let do_handshake = handshake(ctx, stream, tls, record_handshake_error);
    let (mut stream, params) =
        match tokio::time::timeout(config.handshake_timeout, do_handshake).await?? {
            HandshakeData::Startup(stream, params) => (stream, params),
            HandshakeData::Cancel(cancel_key_data) => {
                return Ok(cancellation_handler
                    .cancel_session(cancel_key_data, ctx.session_id())
                    .await
                    .map(|()| None)?)
            }
        };
    drop(pause);

    ctx.set_db_options(params.clone());

    let user_info = match backend
        .authenticate(ctx, &config.authentication_config, &mut stream)
        .await
    {
        Ok(auth_result) => auth_result,
        Err(e) => {
            return stream.throw_error(e).await?;
        }
    };

    let mut node = connect_to_compute(
        ctx,
        &TcpMechanism {
            params: &params,
            locks: &config.connect_compute_locks,
        },
        &user_info,
        config.allow_self_signed_compute,
        config.wake_compute_retry_config,
        config.connect_to_compute_retry_config,
    )
    .or_else(|e| stream.throw_error(e))
    .await?;

    let session = cancellation_handler.get_session();
    prepare_client_connection(&node, &session, &mut stream).await?;

    // Before proxy passing, forward to compute whatever data is left in the
    // PqStream input buffer. Normally there is none, but our serverless npm
    // driver in pipeline mode sends startup, password and first query
    // immediately after opening the connection.
    let (stream, read_buf) = stream.into_inner();
    node.stream.write_all(&read_buf).await?;

    Ok(Some(ProxyPassthrough {
        client: stream,
        aux: node.aux.clone(),
        compute: node,
        _req: request_gauge,
        _conn: conn_gauge,
        _cancel: session,
    }))
}
