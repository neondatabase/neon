//! Routers for our serverless APIs
//!
//! Handles both SQL over HTTP and SQL over Websockets.

mod backend;
pub mod cancel_set;
mod conn_pool;
mod conn_pool_lib;
mod error;
mod http_conn_pool;
mod http_util;
mod json;
mod local_conn_pool;
mod sql_over_http;
mod websocket;

use std::net::{IpAddr, SocketAddr};
use std::pin::{Pin, pin};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use bytes::Bytes;
pub use conn_pool_lib::GlobalConnPoolOptions;
use futures::future::{Either, select};
use futures::{FutureExt, TryFutureExt};
use http::{Method, Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty};
use http_util::{NEON_REQUEST_ID, uuid_to_header_value};
use http_utils::error::ApiError;
use hyper::body::Incoming;
use hyper_util::rt::TokioExecutor;
use hyper_util::server::conn::auto::Builder;
use rand::SeedableRng;
use rand::rngs::StdRng;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{Instrument, info, warn};

use crate::cancellation::CancellationHandler;
use crate::config::{ProxyConfig, ProxyProtocolV2};
use crate::context::RequestContext;
use crate::ext::TaskExt;
use crate::id::{ClientConnId, RequestId};
use crate::metrics::{Metrics, Protocol};
use crate::protocol2::{ConnectHeader, ConnectionInfo, read_proxy_protocol};
use crate::rate_limiter::EndpointRateLimiter;
use crate::serverless::backend::PoolingBackend;
use crate::serverless::http_util::{api_error_into_response, json_response};
use crate::util::run_until_cancelled;

pub(crate) const SERVERLESS_DRIVER_SNI: &str = "api";
pub(crate) const AUTH_BROKER_SNI: &str = "apiauth";

pub async fn task_main(
    config: &'static ProxyConfig,
    auth_backend: &'static crate::auth::Backend<'static, ()>,
    ws_listener: TcpListener,
    cancellation_token: CancellationToken,
    cancellation_handler: Arc<CancellationHandler>,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("websocket server has shut down");
    }

    let local_pool = local_conn_pool::LocalConnPool::new(&config.http_config);
    let conn_pool = conn_pool_lib::GlobalConnPool::new(&config.http_config);
    {
        let conn_pool = Arc::clone(&conn_pool);
        tokio::spawn(async move {
            conn_pool.gc_worker(StdRng::from_entropy()).await;
        });
    }

    // shutdown the connection pool
    tokio::spawn({
        let cancellation_token = cancellation_token.clone();
        let conn_pool = conn_pool.clone();
        async move {
            cancellation_token.cancelled().await;
            tokio::task::spawn_blocking(move || conn_pool.shutdown())
                .await
                .propagate_task_panic();
        }
    });

    let http_conn_pool = conn_pool_lib::GlobalConnPool::new(&config.http_config);
    {
        let http_conn_pool = Arc::clone(&http_conn_pool);
        tokio::spawn(async move {
            http_conn_pool.gc_worker(StdRng::from_entropy()).await;
        });
    }

    // shutdown the connection pool
    tokio::spawn({
        let cancellation_token = cancellation_token.clone();
        let http_conn_pool = http_conn_pool.clone();
        async move {
            cancellation_token.cancelled().await;
            tokio::task::spawn_blocking(move || http_conn_pool.shutdown())
                .await
                .propagate_task_panic();
        }
    });

    let backend = Arc::new(PoolingBackend {
        http_conn_pool: Arc::clone(&http_conn_pool),
        local_pool,
        pool: Arc::clone(&conn_pool),
        config,
        auth_backend,
        endpoint_rate_limiter: Arc::clone(&endpoint_rate_limiter),
    });
    let tls_acceptor: Arc<dyn MaybeTlsAcceptor> = Arc::new(&config.tls_config);

    let connections = tokio_util::task::task_tracker::TaskTracker::new();
    connections.close(); // allows `connections.wait to complete`

    let cancellations = tokio_util::task::task_tracker::TaskTracker::new();
    while let Some(res) = run_until_cancelled(ws_listener.accept(), &cancellation_token).await {
        let (conn, peer_addr) = res.context("could not accept TCP stream")?;
        if let Err(e) = conn.set_nodelay(true) {
            tracing::error!("could not set nodelay: {e}");
            continue;
        }
        let conn_id = ClientConnId::new();

        let n_connections = Metrics::get()
            .proxy
            .client_connections
            .sample(Protocol::Http);
        tracing::trace!(?n_connections, threshold = ?config.http_config.client_conn_threshold, "check");
        if n_connections > config.http_config.client_conn_threshold {
            tracing::trace!("attempting to cancel a random connection");
            if let Some(token) = config.http_config.cancel_set.take() {
                tracing::debug!("cancelling a random connection");
                token.cancel();
            }
        }

        let conn_token = cancellation_token.child_token();
        let tls_acceptor = tls_acceptor.clone();
        let backend = backend.clone();
        let connections2 = connections.clone();
        let cancellation_handler = cancellation_handler.clone();
        let endpoint_rate_limiter = endpoint_rate_limiter.clone();
        let cancellations = cancellations.clone();
        connections.spawn(async move {
            let conn_token2 = conn_token.clone();
            let _cancel_guard = config.http_config.cancel_set.insert(conn_id, conn_token2);

            let _gauge = Metrics::get()
                .proxy
                .client_connections
                .guard(Protocol::Http);

            let startup_result = Box::pin(connection_startup(
                config,
                tls_acceptor,
                conn_id,
                conn,
                peer_addr,
            ))
            .await;
            let Some((conn, conn_info)) = startup_result else {
                return;
            };

            Box::pin(connection_handler(
                config,
                backend,
                connections2,
                cancellations,
                cancellation_handler,
                endpoint_rate_limiter,
                conn_token,
                conn,
                conn_info,
                conn_id,
            ))
            .await;
        });
    }

    connections.wait().await;

    Ok(())
}

pub(crate) trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + 'static {}
impl<T: AsyncRead + AsyncWrite + Send + 'static> AsyncReadWrite for T {}
pub(crate) type AsyncRW = Pin<Box<dyn AsyncReadWrite>>;

#[async_trait]
trait MaybeTlsAcceptor: Send + Sync + 'static {
    async fn accept(&self, conn: TcpStream) -> std::io::Result<AsyncRW>;
}

#[async_trait]
impl MaybeTlsAcceptor for &'static ArcSwapOption<crate::config::TlsConfig> {
    async fn accept(&self, conn: TcpStream) -> std::io::Result<AsyncRW> {
        match &*self.load() {
            Some(config) => Ok(Box::pin(
                TlsAcceptor::from(config.http_config.clone())
                    .accept(conn)
                    .await?,
            )),
            None => Ok(Box::pin(conn)),
        }
    }
}

/// Handles the TCP startup lifecycle.
/// 1. Parses PROXY protocol V2
/// 2. Handles TLS handshake
async fn connection_startup(
    config: &ProxyConfig,
    tls_acceptor: Arc<dyn MaybeTlsAcceptor>,
    conn_id: ClientConnId,
    conn: TcpStream,
    peer_addr: SocketAddr,
) -> Option<(AsyncRW, ConnectionInfo)> {
    // handle PROXY protocol
    let (conn, conn_info) = match config.proxy_protocol_v2 {
        ProxyProtocolV2::Required => {
            match read_proxy_protocol(conn).await {
                Err(e) => {
                    warn!("per-client task finished with an error: {e:#}");
                    return None;
                }
                // our load balancers will not send any more data. let's just exit immediately
                Ok((_conn, ConnectHeader::Local)) => {
                    tracing::debug!("healthcheck received");
                    return None;
                }
                Ok((conn, ConnectHeader::Proxy(info))) => (conn, info),
            }
        }
        // ignore the header - it cannot be confused for a postgres or http connection so will
        // error later.
        ProxyProtocolV2::Rejected => (
            conn,
            ConnectionInfo {
                addr: peer_addr,
                extra: None,
            },
        ),
    };

    let has_private_peer_addr = match conn_info.addr.ip() {
        IpAddr::V4(ip) => ip.is_private(),
        IpAddr::V6(_) => false,
    };
    info!(%conn_id, %conn_info, "accepted new TCP connection");

    // try upgrade to TLS, but with a timeout.
    let conn = match timeout(config.handshake_timeout, tls_acceptor.accept(conn)).await {
        Ok(Ok(conn)) => {
            info!(%conn_id, %conn_info, "accepted new TLS connection");
            conn
        }
        // The handshake failed
        Ok(Err(e)) => {
            if !has_private_peer_addr {
                Metrics::get().proxy.tls_handshake_failures.inc();
            }
            warn!(%conn_id, %conn_info, "failed to accept TLS connection: {e:?}");
            return None;
        }
        // The handshake timed out
        Err(e) => {
            if !has_private_peer_addr {
                Metrics::get().proxy.tls_handshake_failures.inc();
            }
            warn!(%conn_id, %conn_info, "failed to accept TLS connection: {e:?}");
            return None;
        }
    };

    Some((conn, conn_info))
}

/// Handles HTTP connection
/// 1. With graceful shutdowns
/// 2. With graceful request cancellation with connection failure
/// 3. With websocket upgrade support.
#[allow(clippy::too_many_arguments)]
async fn connection_handler(
    config: &'static ProxyConfig,
    backend: Arc<PoolingBackend>,
    connections: TaskTracker,
    cancellations: TaskTracker,
    cancellation_handler: Arc<CancellationHandler>,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    cancellation_token: CancellationToken,
    conn: AsyncRW,
    conn_info: ConnectionInfo,
    conn_id: ClientConnId,
) {
    // Cancel all current inflight HTTP requests if the HTTP connection is closed.
    let http_cancellation_token = CancellationToken::new();
    let _cancel_connection = http_cancellation_token.clone().drop_guard();

    let conn_info2 = conn_info.clone();
    let server = Builder::new(TokioExecutor::new());
    let conn = server.serve_connection_with_upgrades(
        hyper_util::rt::TokioIo::new(conn),
        hyper::service::service_fn(move |req: hyper::Request<Incoming>| {
            // Cancel the current inflight HTTP request if the requets stream is closed.
            // This is slightly different to `_cancel_connection` in that
            // h2 can cancel individual requests with a `RST_STREAM`.
            let http_request_token = http_cancellation_token.child_token();
            let cancel_request = http_request_token.clone().drop_guard();

            // `request_handler` is not cancel safe. It expects to be cancelled only at specific times.
            // By spawning the future, we ensure it never gets cancelled until it decides to.
            let cancellations = cancellations.clone();
            let handler = connections.spawn(
                request_handler(
                    req,
                    config,
                    backend.clone(),
                    connections.clone(),
                    cancellation_handler.clone(),
                    conn_id,
                    conn_info2.clone(),
                    http_request_token,
                    endpoint_rate_limiter.clone(),
                    cancellations,
                )
                .in_current_span()
                .map_ok_or_else(api_error_into_response, |r| r),
            );
            async move {
                let res = handler.await;
                cancel_request.disarm();
                res
            }
        }),
    );

    // On cancellation, trigger the HTTP connection handler to shut down.
    let res = match select(pin!(cancellation_token.cancelled()), pin!(conn)).await {
        Either::Left((_cancelled, mut conn)) => {
            tracing::debug!(%conn_info, "cancelling connection");
            conn.as_mut().graceful_shutdown();
            conn.await
        }
        Either::Right((res, _)) => res,
    };

    match res {
        Ok(()) => tracing::info!(%conn_info, "HTTP connection closed"),
        Err(e) => tracing::warn!(%conn_info, "HTTP connection error {e}"),
    }
}

fn get_request_id(backend: &PoolingBackend, req: &hyper::Request<Incoming>) -> RequestId {
    if matches!(backend.auth_backend, crate::auth::Backend::Local(_)) {
        // take session_id from request, if given.

        if let Some(id) = req
            .headers()
            .get(&NEON_REQUEST_ID)
            .and_then(|id| uuid::Uuid::try_parse_ascii(id.as_bytes()).ok())
        {
            return RequestId::from_uuid(id);
        }

        if let Some(id) = req
            .headers()
            .get(&NEON_REQUEST_ID)
            .and_then(|id| id.to_str().ok())
            .and_then(|id| RequestId::from_str(id).ok())
        {
            return id;
        }
    }

    RequestId::new()
}

fn set_request_id<T, E>(
    mut res: Result<hyper::Response<T>, E>,
    session_id: RequestId,
) -> Result<hyper::Response<T>, E> {
    // add the session ID to the response
    if let Ok(resp) = &mut res {
        resp.headers_mut()
            .append(&NEON_REQUEST_ID, uuid_to_header_value(session_id));
    }

    res
}

#[allow(clippy::too_many_arguments)]
async fn request_handler(
    mut request: hyper::Request<Incoming>,
    config: &'static ProxyConfig,
    backend: Arc<PoolingBackend>,
    ws_connections: TaskTracker,
    cancellation_handler: Arc<CancellationHandler>,
    conn_id: ClientConnId,
    conn_info: ConnectionInfo,
    // used to cancel in-flight HTTP requests. not used to cancel websockets
    http_cancellation_token: CancellationToken,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    cancellations: TaskTracker,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, ApiError> {
    let host = request
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.split(':').next())
        .map(|s| s.to_string());

    // Check if the request is a websocket upgrade request.
    if config.http_config.accept_websockets
        && framed_websockets::upgrade::is_upgrade_request(&request)
    {
        let session_id = RequestId::from_uuid(conn_id.uuid());
        let ctx = RequestContext::new(conn_id, session_id, conn_info, Protocol::Ws);

        ctx.set_user_agent(
            request
                .headers()
                .get(hyper::header::USER_AGENT)
                .and_then(|h| h.to_str().ok())
                .map(Into::into),
        );

        let span = ctx.span();
        info!(parent: &span, "performing websocket upgrade");

        let (response, websocket) = framed_websockets::upgrade::upgrade(&mut request)
            .map_err(|e| ApiError::BadRequest(e.into()))?;

        let cancellations = cancellations.clone();
        ws_connections.spawn(
            async move {
                if let Err(e) = websocket::serve_websocket(
                    config,
                    backend.auth_backend,
                    ctx,
                    websocket,
                    cancellation_handler,
                    endpoint_rate_limiter,
                    host,
                    cancellations,
                )
                .await
                {
                    warn!("error in websocket connection: {e:#}");
                }
            }
            .instrument(span),
        );

        // Return the response so the spawned future can continue.
        Ok(response.map(|b| b.map_err(|x| match x {}).boxed()))
    } else if request.uri().path() == "/sql" && *request.method() == Method::POST {
        let session_id = get_request_id(&backend, &request);
        let ctx = RequestContext::new(conn_id, session_id, conn_info, Protocol::Http);
        let span = ctx.span();

        let testodrome_id = request
            .headers()
            .get("X-Neon-Query-ID")
            .and_then(|value| value.to_str().ok())
            .map(|s| s.to_string());

        if let Some(query_id) = testodrome_id {
            info!(parent: &ctx.span(), "testodrome query ID: {query_id}");
            ctx.set_testodrome_id(query_id.into());
        }

        sql_over_http::handle(config, ctx, request, backend, http_cancellation_token)
            .instrument(span)
            .map(|res| set_request_id(res, session_id))
            .await
    } else if request.uri().path() == "/sql" && *request.method() == Method::OPTIONS {
        Response::builder()
            .header("Allow", "OPTIONS, POST")
            .header("Access-Control-Allow-Origin", "*")
            .header(
                "Access-Control-Allow-Headers",
                "Authorization, Neon-Connection-String, Neon-Raw-Text-Output, Neon-Array-Mode, Neon-Pool-Opt-In, Neon-Batch-Read-Only, Neon-Batch-Isolation-Level",
            )
            .header("Access-Control-Max-Age", "86400" /* 24 hours */)
            .status(StatusCode::OK) // 204 is also valid, but see: https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS#status_code
            .body(Empty::new().map_err(|x| match x {}).boxed())
            .map_err(|e| ApiError::InternalServerError(e.into()))
    } else {
        json_response(StatusCode::BAD_REQUEST, "query is not supported")
    }
}
