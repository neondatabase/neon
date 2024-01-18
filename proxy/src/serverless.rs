//! Routers for our serverless APIs
//!
//! Handles both SQL over HTTP and SQL over Websockets.

mod conn_pool;
mod sql_over_http;
mod websocket;

use bytes::Bytes;
pub use conn_pool::GlobalConnPoolOptions;

use http_body_util::Full;
use hyper::body::Incoming;
use hyper::StatusCode;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn;
use metrics::IntCounterPairGuard;
use rand::rngs::StdRng;
use rand::SeedableRng;
pub use reqwest_middleware::{ClientWithMiddleware, Error};
pub use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::select;
use tokio_rustls::TlsAcceptor;
use tokio_util::task::TaskTracker;

use crate::config::TlsConfig;
use crate::context::RequestMonitoring;
use crate::metrics::NUM_CLIENT_CONNECTION_GAUGE;
use crate::protocol2::ProxyProtocolAccept;
use crate::rate_limiter::EndpointRateLimiter;
use crate::{cancellation::CancelMap, config::ProxyConfig};
use hyper::{Method, Request, Response};

use std::net::IpAddr;
use std::pin::pin;
use std::sync::Arc;
use tls_listener::{AsyncTls, TlsListener};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, warn, Instrument};
use utils::http::{error::ApiError, json::json_response};

#[derive(Clone)]
struct Tls(TlsAcceptor);

impl<C: AsyncRead + AsyncWrite + Unpin> AsyncTls<C> for Tls {
    type Stream = tokio_rustls::server::TlsStream<C>;
    type Error = std::io::Error;
    type AcceptFuture = tokio_rustls::Accept<C>;

    fn accept(&self, conn: C) -> Self::AcceptFuture {
        tokio_rustls::TlsAcceptor::accept(&self.0, conn)
    }
}

pub async fn task_main(
    config: &'static ProxyConfig,
    ws_listener: TcpListener,
    cancellation_token: CancellationToken,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("websocket server has shut down");
    }

    let conn_pool = conn_pool::GlobalConnPool::new(config);

    let conn_pool2 = Arc::clone(&conn_pool);
    tokio::spawn(async move {
        conn_pool2.gc_worker(StdRng::from_entropy()).await;
    });

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

    let tls_config = match config.tls_config.as_ref() {
        Some(config) => config,
        None => {
            warn!("TLS config is missing, WebSocket Secure server will not be started");
            return Ok(());
        }
    };
    let tls_acceptor: tokio_rustls::TlsAcceptor = tls_config.to_server_config().into();

    // let mut addr_incoming = AddrIncoming::from_listener(ws_listener)?;
    // let _ = addr_incoming.set_nodelay(true);
    let addr_incoming = ProxyProtocolAccept {
        incoming: ws_listener,
    };

    let ws_connections = tokio_util::task::task_tracker::TaskTracker::new();
    let ws_connections2 = ws_connections.clone();
    ws_connections.close(); // allows `ws_connections.wait to complete`

    let mut tls_listener = TlsListener::new(Tls(tls_acceptor), addr_incoming);

    tokio::spawn(async move {
        loop {
            let (stream, remote_addr) = select! {
                res = tls_listener.accept() => {
                    match res {
                        Err(err) =>
                        {error!("failed to accept TLS connection for websockets: {err:?}"); continue},
                        Ok(s) => s,
                    }
                }
                _ = cancellation_token.cancelled() => break,
            };
            let (io, tls) = stream.get_ref();
            let client_addr = io.client_addr();
            let sni_name = tls.server_name().map(|s| s.to_string());
            let conn_pool = conn_pool.clone();
            let ws_connections = ws_connections2.clone();
            let endpoint_rate_limiter = endpoint_rate_limiter.clone();

            let peer_addr = match client_addr {
                Some(addr) => addr,
                None if config.require_client_ip => {
                    tracing::error!("Error serving connection: missing required client ip");
                    continue;
                }
                None => remote_addr,
            };

            let io = TokioIo::new(stream);

            let cancellation_token = cancellation_token.clone();
            tokio::task::spawn(async move {
                let service = MetricService::new(hyper::service::service_fn(
                    move |req: Request<Incoming>| {
                        let sni_name = sni_name.clone();
                        let conn_pool = conn_pool.clone();
                        let ws_connections = ws_connections.clone();
                        let endpoint_rate_limiter = endpoint_rate_limiter.clone();

                        async move {
                            let cancel_map = Arc::new(CancelMap::default());
                            let session_id = uuid::Uuid::new_v4();

                            request_handler(
                                req,
                                config,
                                tls_config,
                                conn_pool,
                                ws_connections,
                                cancel_map,
                                session_id,
                                sni_name,
                                peer_addr.ip(),
                                endpoint_rate_limiter,
                            )
                            .instrument(info_span!(
                                "serverless",
                                session = %session_id,
                                %peer_addr,
                            ))
                            .await
                        }
                    },
                ));
                let builder = conn::auto::Builder::new(TokioExecutor::new());
                let mut conn = pin!(builder.serve_connection(io, service));
                let res = select! {
                    _ = cancellation_token.cancelled() => {
                        conn.as_mut().graceful_shutdown();
                        conn.await
                    }
                    res = conn.as_mut() => res,
                };
                if let Err(err) = res {
                    tracing::error!("Error serving connection: {:?}", err);
                }
            });
        }
    });

    // await websocket connections
    ws_connections.wait().await;

    Ok(())
}

struct MetricService<S> {
    inner: S,
    _gauge: IntCounterPairGuard,
}

impl<S> MetricService<S> {
    fn new(inner: S) -> MetricService<S> {
        MetricService {
            inner,
            _gauge: NUM_CLIENT_CONNECTION_GAUGE
                .with_label_values(&["http"])
                .guard(),
        }
    }
}

impl<S, ReqBody> hyper::service::Service<Request<ReqBody>> for MetricService<S>
where
    S: hyper::service::Service<Request<ReqBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn call(&self, req: Request<ReqBody>) -> Self::Future {
        self.inner.call(req)
    }
}

#[allow(clippy::too_many_arguments)]
async fn request_handler(
    mut request: Request<Incoming>,
    config: &'static ProxyConfig,
    tls: &'static TlsConfig,
    conn_pool: Arc<conn_pool::GlobalConnPool>,
    ws_connections: TaskTracker,
    cancel_map: Arc<CancelMap>,
    session_id: uuid::Uuid,
    sni_hostname: Option<String>,
    peer_addr: IpAddr,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> Result<Response<Full<Bytes>>, ApiError> {
    let host = request
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.split(':').next())
        .map(|s| s.to_string());

    // Check if the request is a websocket upgrade request.
    if hyper_tungstenite::is_upgrade_request(&request) {
        info!(session_id = ?session_id, "performing websocket upgrade");

        let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)
            .map_err(|e| ApiError::BadRequest(e.into()))?;

        ws_connections.spawn(
            async move {
                let mut ctx = RequestMonitoring::new(session_id, peer_addr, "ws", &config.region);

                if let Err(e) = websocket::serve_websocket(
                    config,
                    &mut ctx,
                    websocket,
                    &cancel_map,
                    host,
                    endpoint_rate_limiter,
                )
                .await
                {
                    error!(session_id = ?session_id, "error in websocket connection: {e:#}");
                }
            }
            .in_current_span(),
        );

        // Return the response so the spawned future can continue.
        Ok(response)
    } else if request.uri().path() == "/sql" && request.method() == Method::POST {
        let mut ctx = RequestMonitoring::new(session_id, peer_addr, "http", &config.region);

        sql_over_http::handle(
            tls,
            &config.http_config,
            &mut ctx,
            request,
            sni_hostname,
            conn_pool,
        )
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
            .body(Full::new(Bytes::new()))
            .map_err(|e| ApiError::InternalServerError(e.into()))
    } else {
        json_response(StatusCode::BAD_REQUEST, "query is not supported")
    }
}
