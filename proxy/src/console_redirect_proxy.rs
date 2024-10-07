use crate::auth::backend::ConsoleRedirectBackend;
use crate::config::ProxyConfig;
use crate::metrics::Protocol;
use crate::proxy::{prepare_client_connection, transition_connection, ClientRequestError};
use crate::{
    cancellation::CancellationHandlerMain,
    context::RequestMonitoring,
    metrics::{Metrics, NumClientConnectionsGuard},
    proxy::handshake::{handshake, HandshakeData},
};
use futures::TryFutureExt;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tracing::{info, Instrument};

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

    super::connection_loop(
        config,
        listener,
        cancellation_token,
        Protocol::Tcp,
        C {
            config,
            backend,
            cancellation_handler,
        },
    )
    .await
}

#[derive(Clone)]
struct C {
    config: &'static ProxyConfig,
    backend: &'static ConsoleRedirectBackend,
    cancellation_handler: Arc<CancellationHandlerMain>,
}

impl super::ConnHandler for C {
    async fn handle(
        self,
        session_id: uuid::Uuid,
        peer_addr: IpAddr,
        socket: crate::protocol2::ChainRW<tokio::net::TcpStream>,
        conn_gauge: crate::metrics::NumClientConnectionsGuard<'static>,
    ) {
        let ctx = RequestMonitoring::new(session_id, peer_addr, Protocol::Tcp, &self.config.region);
        let span = ctx.span();

        let startup = Box::pin(
            handle_client(
                self.config,
                self.backend,
                &ctx,
                self.cancellation_handler,
                socket,
                conn_gauge,
            )
            .instrument(span.clone()),
        );

        let res = startup.await;
        transition_connection(ctx, res).await;
    }
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
