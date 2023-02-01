use bytes::{Buf, Bytes};
use futures::{Sink, Stream, StreamExt};
use hyper::server::accept;
use hyper::server::conn::AddrIncoming;
use hyper::upgrade::Upgraded;
use hyper::{Body, Request, Response, StatusCode};
use hyper_tungstenite::{tungstenite, WebSocketStream};
use hyper_tungstenite::{tungstenite::Message, HyperWebsocket};
use pin_project_lite::pin_project;
use tokio::net::TcpListener;

use std::convert::Infallible;
use std::future::ready;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use tls_listener::TlsListener;

use tokio::io::{self, AsyncBufRead, AsyncRead, AsyncWrite, ReadBuf};

use tracing::{error, info, info_span, warn, Instrument};
use utils::http::{error::ApiError, json::json_response};

use crate::cancellation::CancelMap;
use crate::config::ProxyConfig;
use crate::proxy::handle_ws_client;

pin_project! {
    /// This is a wrapper around a WebSocketStream that implements AsyncRead and AsyncWrite.
    pub struct WebSocketRW {
        #[pin]
        stream: WebSocketStream<Upgraded>,
        chunk: Option<bytes::Bytes>,
    }
}

// FIXME: explain why this is safe or try to remove `unsafe impl`.
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

        let inner_buf = match ready!(self.as_mut().poll_fill_buf(cx)) {
            Ok(buf) => buf,
            Err(err) => return Poll::Ready(Err(err)),
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
                let buf = self.project().chunk.as_ref().unwrap().chunk();
                return Poll::Ready(Ok(buf));
            } else {
                match ready!(self.as_mut().project().stream.poll_next(cx)) {
                    Some(Ok(message)) => match message {
                        Message::Text(_) => {}
                        Message::Binary(chunk) => {
                            *self.as_mut().project().chunk = Some(Bytes::from(chunk));
                        }
                        Message::Ping(_) => {
                            // No need to send a reply: tungstenite takes care of this for you.
                        }
                        Message::Pong(_) => {}
                        Message::Close(_) => {
                            // No need to send a reply: tungstenite takes care of this for you.
                            return Poll::Ready(Ok(&[]));
                        }
                        Message::Frame(_) => {
                            unreachable!();
                        }
                    },
                    Some(Err(err)) => return Poll::Ready(Err(ws_err_into(err))),
                    None => return Poll::Ready(Ok(&[])),
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
    config: &'static ProxyConfig,
    cancel_map: &CancelMap,
    session_id: uuid::Uuid,
    hostname: Option<String>,
) -> anyhow::Result<()> {
    let websocket = websocket.await?;
    handle_ws_client(
        config,
        cancel_map,
        session_id,
        WebSocketRW::new(websocket),
        hostname,
    )
    .await?;
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
        .get("host")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.split(':').next())
        .map(|s| s.to_string());

    // Check if the request is a websocket upgrade request.
    if hyper_tungstenite::is_upgrade_request(&request) {
        let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)
            .map_err(|e| ApiError::BadRequest(e.into()))?;

        tokio::spawn(async move {
            if let Err(e) = serve_websocket(websocket, config, &cancel_map, session_id, host).await
            {
                error!("error in websocket connection: {:?}", e);
            }
        });

        // Return the response so the spawned future can continue.
        Ok(response)
    } else {
        json_response(StatusCode::OK, "Connect with a websocket client")
    }
}

pub async fn task_main(
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

    let addr_incoming = AddrIncoming::from_listener(ws_listener)?;

    let tls_listener = TlsListener::new(tls_acceptor, addr_incoming).filter(|conn| {
        if let Err(err) = conn {
            error!("failed to accept TLS connection for websockets: {:?}", err);
            ready(false)
        } else {
            ready(true)
        }
    });

    let make_svc = hyper::service::make_service_fn(|_stream| async move {
        Ok::<_, Infallible>(hyper::service::service_fn(
            move |req: Request<Body>| async move {
                let cancel_map = Arc::new(CancelMap::default());
                let session_id = uuid::Uuid::new_v4();
                ws_handler(req, config, cancel_map, session_id)
                    .instrument(info_span!(
                        "ws-client",
                        session = format_args!("{session_id}")
                    ))
                    .await
            },
        ))
    });

    hyper::Server::builder(accept::from_stream(tls_listener))
        .serve(make_svc)
        .await?;

    Ok(())
}
