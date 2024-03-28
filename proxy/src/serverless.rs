//! Routers for our serverless APIs
//!
//! Handles both SQL over HTTP and SQL over Websockets.

mod backend;
mod conn_pool;
mod json;
mod sql_over_http;
pub mod tls_listener;
mod websocket;

pub use conn_pool::GlobalConnPoolOptions;

use anyhow::bail;
use hyper::StatusCode;
use metrics::IntCounterPairGuard;
use rand::rngs::StdRng;
use rand::SeedableRng;
pub use reqwest_middleware::{ClientWithMiddleware, Error};
pub use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use tokio_util::task::TaskTracker;
use tracing::instrument::Instrumented;

use crate::cancellation::CancellationHandlerMain;
use crate::config::ProxyConfig;
use crate::context::RequestMonitoring;
use crate::protocol2::{ProxyProtocolAccept, WithClientIp, WithConnectionGuard};
use crate::rate_limiter::EndpointRateLimiter;
use crate::serverless::backend::PoolingBackend;
use hyper::{
    server::conn::{AddrIncoming, AddrStream},
    Body, Method, Request, Response,
};

use std::net::IpAddr;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tls_listener::TlsListener;
use tokio::net::TcpListener;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{error, info, warn, Instrument};
use utils::http::{error::ApiError, json::json_response};

pub const SERVERLESS_DRIVER_SNI: &str = "api";

pub async fn task_main(
    config: &'static ProxyConfig,
    ws_listener: TcpListener,
    cancellation_token: CancellationToken,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    cancellation_handler: Arc<CancellationHandlerMain>,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("websocket server has shut down");
    }

    let conn_pool = conn_pool::GlobalConnPool::new(&config.http_config);
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
                .unwrap();
        }
    });

    let backend = Arc::new(PoolingBackend {
        pool: Arc::clone(&conn_pool),
        config,
    });

    let tls_config = match config.tls_config.as_ref() {
        Some(config) => config,
        None => {
            warn!("TLS config is missing, WebSocket Secure server will not be started");
            return Ok(());
        }
    };
    let mut tls_server_config = rustls::ServerConfig::clone(&tls_config.to_server_config());
    // prefer http2, but support http/1.1
    tls_server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let tls_acceptor: tokio_rustls::TlsAcceptor = Arc::new(tls_server_config).into();

    let mut addr_incoming = AddrIncoming::from_listener(ws_listener)?;
    let _ = addr_incoming.set_nodelay(true);
    let addr_incoming = ProxyProtocolAccept {
        incoming: addr_incoming,
        protocol: "http",
    };

    let ws_connections = tokio_util::task::task_tracker::TaskTracker::new();
    ws_connections.close(); // allows `ws_connections.wait to complete`

    let tls_listener = TlsListener::new(tls_acceptor, addr_incoming, config.handshake_timeout);

    let make_svc = hyper::service::make_service_fn(
        |stream: &tokio_rustls::server::TlsStream<
            WithConnectionGuard<WithClientIp<AddrStream>>,
        >| {
            let (conn, _) = stream.get_ref();

            // this is jank. should dissapear with hyper 1.0 migration.
            let gauge = conn
                .gauge
                .lock()
                .expect("lock should not be poisoned")
                .take()
                .expect("gauge should be set on connection start");

            // Cancel all current inflight HTTP requests if the HTTP connection is closed.
            let http_cancellation_token = CancellationToken::new();
            let cancel_connection = http_cancellation_token.clone().drop_guard();

            let span = conn.span.clone();
            let client_addr = conn.inner.client_addr();
            let remote_addr = conn.inner.inner.remote_addr();
            let backend = backend.clone();
            let ws_connections = ws_connections.clone();
            let endpoint_rate_limiter = endpoint_rate_limiter.clone();
            let cancellation_handler = cancellation_handler.clone();
            async move {
                let peer_addr = match client_addr {
                    Some(addr) => addr,
                    None if config.require_client_ip => bail!("missing required client ip"),
                    None => remote_addr,
                };
                Ok(MetricService::new(
                    hyper::service::service_fn(move |req: Request<Body>| {
                        let backend = backend.clone();
                        let ws_connections2 = ws_connections.clone();
                        let endpoint_rate_limiter = endpoint_rate_limiter.clone();
                        let cancellation_handler = cancellation_handler.clone();
                        let http_cancellation_token = http_cancellation_token.child_token();

                        // `request_handler` is not cancel safe. It expects to be cancelled only at specific times.
                        // By spawning the future, we ensure it never gets cancelled until it decides to.
                        ws_connections.spawn(
                            async move {
                                // Cancel the current inflight HTTP request if the requets stream is closed.
                                // This is slightly different to `_cancel_connection` in that
                                // h2 can cancel individual requests with a `RST_STREAM`.
                                let _cancel_session = http_cancellation_token.clone().drop_guard();

                                let res = request_handler(
                                    req,
                                    config,
                                    backend,
                                    ws_connections2,
                                    cancellation_handler,
                                    peer_addr.ip(),
                                    endpoint_rate_limiter,
                                    http_cancellation_token,
                                )
                                .await
                                .map_or_else(|e| e.into_response(), |r| r);

                                _cancel_session.disarm();

                                res
                            }
                            .in_current_span(),
                        )
                    }),
                    gauge,
                    cancel_connection,
                    span,
                ))
            }
        },
    );

    hyper::Server::builder(tls_listener)
        .serve(make_svc)
        .with_graceful_shutdown(async {
            cancellation_token.cancelled().await;
            tokio::time::sleep(Duration::from_secs(20)).await;
        })
        .await?;

    // await websocket connections
    ws_connections.wait().await;

    Ok(())
}

struct MetricService<S> {
    inner: S,
    _gauge: IntCounterPairGuard,
    _cancel: DropGuard,
    span: tracing::Span,
}

impl<S> MetricService<S> {
    fn new(
        inner: S,
        _gauge: IntCounterPairGuard,
        _cancel: DropGuard,
        span: tracing::Span,
    ) -> MetricService<S> {
        MetricService {
            inner,
            _gauge,
            _cancel,
            span,
        }
    }
}

impl<S, ReqBody> hyper::service::Service<Request<ReqBody>> for MetricService<S>
where
    S: hyper::service::Service<Request<ReqBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Instrumented<S::Future>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        self.span
            .in_scope(|| self.inner.call(req))
            .instrument(self.span.clone())
    }
}

#[allow(clippy::too_many_arguments)]
async fn request_handler(
    mut request: Request<Body>,
    config: &'static ProxyConfig,
    backend: Arc<PoolingBackend>,
    ws_connections: TaskTracker,
    cancellation_handler: Arc<CancellationHandlerMain>,
    peer_addr: IpAddr,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    // used to cancel in-flight HTTP requests. not used to cancel websockets
    http_cancellation_token: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let session_id = uuid::Uuid::new_v4();

    let host = request
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.split(':').next())
        .map(|s| s.to_string());

    // Check if the request is a websocket upgrade request.
    if hyper_tungstenite::is_upgrade_request(&request) {
        let ctx = RequestMonitoring::new(session_id, peer_addr, "ws", &config.region);
        let span = ctx.span.clone();
        info!(parent: &span, "performing websocket upgrade");

        let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)
            .map_err(|e| ApiError::BadRequest(e.into()))?;

        ws_connections.spawn(
            async move {
                if let Err(e) = websocket::serve_websocket(
                    config,
                    ctx,
                    websocket,
                    cancellation_handler,
                    host,
                    endpoint_rate_limiter,
                )
                .await
                {
                    error!("error in websocket connection: {e:#}");
                }
            }
            .instrument(span),
        );

        // Return the response so the spawned future can continue.
        Ok(response)
    } else if request.uri().path() == "/sql" && request.method() == Method::POST {
        let ctx = RequestMonitoring::new(session_id, peer_addr, "http", &config.region);
        let span = ctx.span.clone();

        sql_over_http::handle(config, ctx, request, backend, http_cancellation_token)
            .instrument(span)
            .await
    } else if request.uri().path() == "/sql" && request.method() == Method::OPTIONS {
        Response::builder()
            .header("Allow", "OPTIONS, POST")
            .header("Access-Control-Allow-Origin", "*")
            .header(
                "Access-Control-Allow-Headers",
                "Neon-Connection-String, Neon-Raw-Text-Output, Neon-Array-Mode, Neon-Pool-Opt-In, Neon-Batch-Read-Only, Neon-Batch-Isolation-Level",
            )
            .header("Access-Control-Max-Age", "86400" /* 24 hours */)
            .status(StatusCode::OK) // 204 is also valid, but see: https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS#status_code
            .body(Body::empty())
            .map_err(|e| ApiError::InternalServerError(e.into()))
    } else {
        json_response(StatusCode::BAD_REQUEST, "query is not supported")
    }
}
