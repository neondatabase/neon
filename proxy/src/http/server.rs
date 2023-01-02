use anyhow::anyhow;
use bytes::{Buf, Bytes};
use futures::{Sink, Stream, StreamExt};
use hyper::server::accept::{self};
use hyper::server::conn::AddrIncoming;
use hyper::upgrade::Upgraded;
use hyper::{Body, Request, Response, StatusCode};
use hyper_tungstenite::{tungstenite, WebSocketStream};
use hyper_tungstenite::{tungstenite::Message, HyperWebsocket};
use pin_project_lite::pin_project;

use std::future::ready;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{convert::Infallible, net::TcpListener};
use tls_listener::TlsListener;

use tokio::io::{self, AsyncBufRead, AsyncRead, AsyncWrite, ReadBuf};

use tracing::{error, info, warn};
use utils::http::{endpoint, error::ApiError, json::json_response, RouterBuilder, RouterService};

use crate::cancellation::CancelMap;
use crate::config::ProxyConfig;
use crate::proxy::handle_client;

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "")
}

fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    let router = endpoint::make_router();
    router.get("/v1/status", status_handler)
}

pub async fn task_main(http_listener: TcpListener) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("http has shut down");
    }

    let service = || RouterService::new(make_router().build()?);

    hyper::Server::from_tcp(http_listener)?
        .serve(service().map_err(|e| anyhow!(e))?)
        .await?;

    Ok(())
}

pin_project! {
    pub struct WebSocketRW {
        #[pin]
        stream: WebSocketStream<Upgraded>,
        chunk: Option<bytes::Bytes>,
    }
}

unsafe impl Sync for WebSocketRW {}

impl WebSocketRW {
    pub fn new(stream: WebSocketStream<Upgraded>) -> Self {
        Self {
            stream,
            chunk: None,
        }
    }

    fn has_chunk(&self) -> bool {
        if let Some(ref chunk) = self.chunk {
            chunk.remaining() > 0
        } else {
            false
        }
    }
}

fn ws_err_into(e: tungstenite::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

impl AsyncWrite for WebSocketRW {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let mut this = self.project();
        match this.stream.as_mut().poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                if let Err(e) = this
                    .stream
                    .as_mut()
                    .start_send(Message::Binary(buf.to_vec()))
                {
                    Poll::Ready(Err(ws_err_into(e)))
                } else {
                    Poll::Ready(Ok(buf.len()))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(ws_err_into(e))),
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().stream.poll_flush(cx).map_err(ws_err_into)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().stream.poll_close(cx).map_err(ws_err_into)
    }
}

impl AsyncRead for WebSocketRW {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let inner_buf = match self.as_mut().poll_fill_buf(cx) {
            Poll::Ready(Ok(buf)) => buf,
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        };
        let len = std::cmp::min(inner_buf.len(), buf.remaining());
        buf.put_slice(&inner_buf[..len]);

        self.consume(len);
        Poll::Ready(Ok(()))
    }
}

impl AsyncBufRead for WebSocketRW {
    fn poll_fill_buf(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        loop {
            if self.as_mut().has_chunk() {
                // This unwrap is very sad, but it can't be avoided.
                let buf = self.project().chunk.as_ref().unwrap().chunk();
                return Poll::Ready(Ok(buf));
            } else {
                match self.as_mut().project().stream.poll_next(cx) {
                    Poll::Ready(Some(Ok(message))) => match message {
                        Message::Text(msg) => {
                            info!("Received text message: {}", msg);
                        }
                        Message::Binary(chunk) => {
                            info!("Received binary message: {:02X?}", chunk);
                            *self.as_mut().project().chunk = Some(Bytes::from(chunk));
                            info!("saved that binary msg");
                        }
                        Message::Ping(msg) => {
                            // No need to send a reply: tungstenite takes care of this for you.
                            info!("Received ping message: {:02X?}", msg);
                        }
                        Message::Pong(msg) => {
                            info!("Received pong message: {:02X?}", msg);
                        }
                        Message::Close(msg) => {
                            // No need to send a reply: tungstenite takes care of this for you.
                            if let Some(msg) = &msg {
                                info!(
                                    "Received close message with code {} and message: {}",
                                    msg.code, msg.reason
                                );
                            } else {
                                info!("Received close message");
                            }
                            return Poll::Ready(Ok(&[]));
                        }
                        Message::Frame(_msg) => {
                            unreachable!();
                        } // // Go around the loop in case the chunk is empty.
                          // *self.as_mut().project().chunk = Some(chunk);
                    },
                    Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(ws_err_into(err))),
                    Poll::Ready(None) => return Poll::Ready(Ok(&[])),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }
    fn consume(self: Pin<&mut Self>, amt: usize) {
        if amt > 0 {
            self.project()
                .chunk
                .as_mut()
                .expect("No chunk present")
                .advance(amt);
        }
    }
}

async fn serve_websocket(
    websocket: HyperWebsocket,
    config: &ProxyConfig,
    cancel_map: &CancelMap,
    session_id: uuid::Uuid,
) -> anyhow::Result<()> {
    let websocket = websocket.await?;

    info!("serving ws");

    handle_client(&config, cancel_map, session_id, WebSocketRW::new(websocket)).await?;
    Ok(())
}

async fn ws_handler(
    mut request: Request<Body>,
    config: &'static ProxyConfig,
    cancel_map: Arc<CancelMap>,
    session_id: uuid::Uuid,
) -> Result<Response<Body>, ApiError> {
    let host = request
        .headers()
        .get("Host")
        .and_then(|h| h.to_str().ok());

    info!("headers: {:#?}", request.headers());

    // Create a new proxy config with the hostname from the request.
    let config = ProxyConfig {
        tls_config: config.tls_config.clone(),
        auth_backend: config.auth_backend.clone(),
        secure_override_hostname: host
            .and_then(|h| {
                // Remove the port from the hostname, if present.
                h.split(':').next()
            })
            .map(|h| h.to_string()),
    };

    // Check if the request is a websocket upgrade request.
    if hyper_tungstenite::is_upgrade_request(&request) {
        let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)
            .map_err(|e| ApiError::BadRequest(e.into()))?;

        tokio::spawn(async move {
            if let Err(e) = serve_websocket(websocket, &config, &cancel_map, session_id).await {
                error!("Error in websocket connection: {:?}", e);
            }
        });

        // Return the response so the spawned future can continue.
        return Ok(response);
    } else {
        json_response(StatusCode::OK, "we")
    }
}

pub async fn wss_thread_main(
    ws_listener: TcpListener,
    config: &'static ProxyConfig,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("websocket server has shut down");
    }

    let tls_config = config.tls_config.as_ref().map(|cfg| cfg.to_server_config());
    let tls_acceptor: tokio_rustls::TlsAcceptor = match tls_config {
        Some(config) => config.into(),
        None => {
            warn!("TLS config is missing, WebSocket Secure server will not be started");
            return Ok(());
        }
    };

    let tokio_listener = tokio::net::TcpListener::from_std(ws_listener)?;

    let addr_incoming = AddrIncoming::from_listener(tokio_listener)?;
    let tls_listener = TlsListener::new(tls_acceptor, addr_incoming).filter(|conn| {
        info!("got a connection, checking if it's TLS");
        if let Err(err) = conn {
            error!("Failed to accept TLS connection for websockets: {:?}", err);
            ready(false)
        } else {
            ready(true)
        }
    });

    let make_svc = hyper::service::make_service_fn(|_stream| {
        // TODO:
        // let remote_addr = stream.get_ref().0.remote_addr();
        info!("got a connection, trying to call a handler");
        async move {
            Ok::<_, Infallible>(hyper::service::service_fn(
                move |req: Request<Body>| async move {
                    let cancel_map = Arc::new(CancelMap::default());
                    let session_id = uuid::Uuid::new_v4();
                    ws_handler(req, config, cancel_map, session_id).await
                },
            ))
        }
    });

    hyper::Server::builder(accept::from_stream(tls_listener))
        .serve(make_svc)
        .await?;

    Ok(())
}
