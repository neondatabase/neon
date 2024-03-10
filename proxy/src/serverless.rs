//! Routers for our serverless APIs
//!
//! Handles both SQL over HTTP and SQL over Websockets.

mod backend;
mod conn_pool;
mod http_auto;
mod json;
mod sql_over_http;
mod websocket;

use bytes::Bytes;
pub use conn_pool::GlobalConnPoolOptions;

use anyhow::Context;
use futures::future::{select, Either};
use http1::{Method, Response, StatusCode};
use http_body_util::Full;
use hyper1::body::Incoming;
use rand::rngs::StdRng;
use rand::SeedableRng;
pub use reqwest_middleware::{ClientWithMiddleware, Error};
pub use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde::Serialize;
use tokio::time::timeout;
use tokio_util::task::TaskTracker;

use crate::context::RequestMonitoring;
use crate::metrics::{NUM_CLIENT_CONNECTION_GAUGE, TLS_HANDSHAKE_FAILURES};
use crate::protocol2::WithClientIp;
use crate::proxy::run_until_cancelled;
use crate::rate_limiter::EndpointRateLimiter;
use crate::serverless::backend::PoolingBackend;
use crate::serverless::http_auto::Rewind;
use crate::{cancellation::CancellationHandler, config::ProxyConfig};

use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn, Instrument};
use utils::http::error::ApiError;

pub const SERVERLESS_DRIVER_SNI: &str = "api";

pub async fn task_main(
    config: &'static ProxyConfig,
    ws_listener: TcpListener,
    cancellation_token: CancellationToken,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    cancellation_handler: Arc<CancellationHandler>,
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

    let ws_connections = tokio_util::task::task_tracker::TaskTracker::new();
    ws_connections.close(); // allows `ws_connections.wait to complete`

    let http_connections = tokio_util::task::task_tracker::TaskTracker::new();
    http_connections.close();

    let server = http_auto::Builder::new();

    loop {
        let Some(res) = run_until_cancelled(ws_listener.accept(), &cancellation_token).await else {
            break;
        };

        let (conn, mut peer_addr) = res.context("could not accept TCP stream")?;
        if let Err(e) = conn.set_nodelay(true) {
            tracing::error!("could not set nodolay: {e}");
            continue;
        }
        let cancellation_token = cancellation_token.child_token();

        let tls = tls_acceptor.clone();

        let backend = backend.clone();
        let ws_connections = ws_connections.clone();
        let endpoint_rate_limiter = endpoint_rate_limiter.clone();
        let cancellation_handler = cancellation_handler.clone();
        let server = server.clone();

        http_connections.spawn(async move {
            let _gauge = NUM_CLIENT_CONNECTION_GAUGE
                .with_label_values(&["http"])
                .guard();

            // handle PROXY protocol
            let mut conn = WithClientIp::new(conn);
            let peer = match conn.wait_for_addr().await {
                Ok(peer) => peer,
                Err(e) => {
                    tracing::error!(
                        "failed to accept TCP connection: invalid PROXY protocol V2 header: {e:#}"
                    );
                    return;
                }
            };

            if let Some(peer) = peer {
                peer_addr = peer;
            }
            info!(%peer_addr, protocol = "http", "accepted new TCP connection");

            let accept = tls.accept(conn);
            let conn = match timeout(Duration::from_secs(10), accept).await {
                Ok(Ok(conn)) => {
                    info!(%peer_addr, protocol = "http", "accepted new TLS connection");
                    conn
                }
                // The handshake failed, try getting another connection from the queue
                Ok(Err(e)) => {
                    TLS_HANDSHAKE_FAILURES.inc();
                    warn!(%peer_addr, protocol = "http", "failed to accept TLS connection: {e:?}");
                    return;
                }
                // The handshake timed out, try getting another connection from the queue
                Err(_) => {
                    TLS_HANDSHAKE_FAILURES.inc();
                    warn!(%peer_addr, protocol = "http", "failed to accept TLS connection: timeout");
                    return;
                }
            };

            let (version, conn) = match conn.get_ref().1.alpn_protocol() {
                Some(b"http/1.1") => (http_auto::Version::H1, Rewind::new(conn)),
                Some(b"h2") => (http_auto::Version::H2, Rewind::new(conn)),
                _ => {
                    tracing::debug!("HTTP: no ALPN negotiated");
                    let conn = timeout(Duration::from_secs(10), http_auto::read_version(conn)).await;
                    match conn {
                        Ok(Ok(v)) => v,
                        Ok(Err(e)) => {
                            tracing::warn!("HTTP connection error: {e}");
                            return;
                        },
                        Err(_) => {
                            tracing::warn!("HTTP connection error: timeout determining http version");
                            return;
                        }
                    }
                }
            };

            let conn = server.serve_connection_with_upgrades(
                conn,
                version,
                hyper1::service::service_fn(move |req: hyper1::Request<Incoming>| {
                    let backend = backend.clone();
                    let ws_connections = ws_connections.clone();
                    let endpoint_rate_limiter = endpoint_rate_limiter.clone();
                    let cancellation_handler = cancellation_handler.clone();

                    async move {
                        Ok::<_, Infallible>(
                            request_handler(
                                req,
                                config,
                                backend,
                                ws_connections,
                                cancellation_handler,
                                peer_addr,
                                endpoint_rate_limiter,
                            )
                            .await
                            .map_or_else(api_error_into_response, |r| r),
                        )
                    }
                })
            );


            let cancel = pin!(cancellation_token.cancelled());
            let conn = pin!(conn);
            let res = match select(cancel, conn).await {
                Either::Left((_cancelled, mut conn)) => {
                    conn.as_mut().graceful_shutdown();
                    conn.await
                }
                Either::Right((res, _)) => res,
            };

            match res {
                Ok(()) => {}
                Err(e) => {
                    tracing::warn!("HTTP connection error {e}")
                }
            }
        });
    }

    // await websocket connections
    http_connections.wait().await;
    ws_connections.wait().await;

    Ok(())
}

fn api_error_into_response(this: ApiError) -> Response<Full<Bytes>> {
    match this {
        ApiError::BadRequest(err) => HttpErrorBody::response_from_msg_and_status(
            format!("{err:#?}"), // use debug printing so that we give the cause
            StatusCode::BAD_REQUEST,
        ),
        ApiError::Forbidden(_) => {
            HttpErrorBody::response_from_msg_and_status(this.to_string(), StatusCode::FORBIDDEN)
        }
        ApiError::Unauthorized(_) => {
            HttpErrorBody::response_from_msg_and_status(this.to_string(), StatusCode::UNAUTHORIZED)
        }
        ApiError::NotFound(_) => {
            HttpErrorBody::response_from_msg_and_status(this.to_string(), StatusCode::NOT_FOUND)
        }
        ApiError::Conflict(_) => {
            HttpErrorBody::response_from_msg_and_status(this.to_string(), StatusCode::CONFLICT)
        }
        ApiError::PreconditionFailed(_) => HttpErrorBody::response_from_msg_and_status(
            this.to_string(),
            StatusCode::PRECONDITION_FAILED,
        ),
        ApiError::ShuttingDown => HttpErrorBody::response_from_msg_and_status(
            "Shutting down".to_string(),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        ApiError::ResourceUnavailable(err) => HttpErrorBody::response_from_msg_and_status(
            err.to_string(),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        ApiError::Timeout(err) => HttpErrorBody::response_from_msg_and_status(
            err.to_string(),
            StatusCode::REQUEST_TIMEOUT,
        ),
        ApiError::InternalServerError(err) => HttpErrorBody::response_from_msg_and_status(
            err.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

#[derive(Serialize)]
struct HttpErrorBody {
    pub msg: String,
}

impl HttpErrorBody {
    pub fn response_from_msg_and_status(msg: String, status: StatusCode) -> Response<Full<Bytes>> {
        HttpErrorBody { msg }.to_response(status)
    }

    pub fn to_response(&self, status: StatusCode) -> Response<Full<Bytes>> {
        Response::builder()
            .status(status)
            .header(http1::header::CONTENT_TYPE, "application/json")
            // we do not have nested maps with non string keys so serialization shouldn't fail
            .body(Full::new(Bytes::from(serde_json::to_string(self).unwrap())))
            .unwrap()
    }
}

#[allow(clippy::too_many_arguments)]
async fn request_handler(
    mut request: hyper1::Request<Incoming>,
    config: &'static ProxyConfig,
    backend: Arc<PoolingBackend>,
    ws_connections: TaskTracker,
    cancellation_handler: Arc<CancellationHandler>,
    peer_addr: SocketAddr,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> Result<Response<Full<Bytes>>, ApiError> {
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
    } else if request.uri().path() == "/sql" && *request.method() == Method::POST {
        let ctx = RequestMonitoring::new(session_id, peer_addr, "http", &config.region);
        let span = ctx.span.clone();

        sql_over_http::handle(config, ctx, request, backend)
            .instrument(span)
            .await
    } else if request.uri().path() == "/sql" && *request.method() == Method::OPTIONS {
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

fn json_response<T: Serialize>(
    status: StatusCode,
    data: T,
) -> Result<Response<Full<Bytes>>, ApiError> {
    let json = serde_json::to_string(&data)
        .context("Failed to serialize JSON response")
        .map_err(ApiError::InternalServerError)?;
    let response = Response::builder()
        .status(status)
        .header(http1::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(json)))
        .map_err(|e| ApiError::InternalServerError(e.into()))?;
    Ok(response)
}
