use crate::{
    cancellation::CancelMap,
    config::ProxyConfig,
    error::io_error,
    proxy::{handle_client, ClientMode},
};
use bytes::{Buf, Bytes};
use futures::{Sink, Stream, StreamExt};
use hyper::{
    server::{
        accept,
        conn::{AddrIncoming, AddrStream},
    },
    upgrade::Upgraded,
    Body, Method, Request, Response, StatusCode,
};
use hyper_tungstenite::{tungstenite::Message, HyperWebsocket, WebSocketStream};
use pin_project_lite::pin_project;
use serde_json::{json, Value};

use std::{
    convert::Infallible,
    future::ready,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};
use tls_listener::TlsListener;
use tokio::{
    io::{self, AsyncBufRead, AsyncRead, AsyncWrite, ReadBuf},
    net::TcpListener,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, warn, Instrument};
use utils::http::{error::ApiError, json::json_response};

// TODO: use `std::sync::Exclusive` once it's stabilized.
// Tracking issue: https://github.com/rust-lang/rust/issues/98407.
use sync_wrapper::SyncWrapper;

use super::{conn_pool::GlobalConnPool, sql_over_http};

pin_project! {
    /// This is a wrapper around a [`WebSocketStream`] that
    /// implements [`AsyncRead`] and [`AsyncWrite`].
    pub struct WebSocketRw {
        #[pin]
        stream: SyncWrapper<WebSocketStream<Upgraded>>,
        bytes: Bytes,
    }
}

impl WebSocketRw {
    pub fn new(stream: WebSocketStream<Upgraded>) -> Self {
        Self {
            stream: stream.into(),
            bytes: Bytes::new(),
        }
    }
}

impl AsyncWrite for WebSocketRw {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut stream = self.project().stream.get_pin_mut();

        ready!(stream.as_mut().poll_ready(cx).map_err(io_error))?;
        match stream.as_mut().start_send(Message::Binary(buf.into())) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(e) => Poll::Ready(Err(io_error(e))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let stream = self.project().stream.get_pin_mut();
        stream.poll_flush(cx).map_err(io_error)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let stream = self.project().stream.get_pin_mut();
        stream.poll_close(cx).map_err(io_error)
    }
}

impl AsyncRead for WebSocketRw {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() > 0 {
            let bytes = ready!(self.as_mut().poll_fill_buf(cx))?;
            let len = std::cmp::min(bytes.len(), buf.remaining());
            buf.put_slice(&bytes[..len]);
            self.consume(len);
        }

        Poll::Ready(Ok(()))
    }
}

impl AsyncBufRead for WebSocketRw {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        // Please refer to poll_fill_buf's documentation.
        const EOF: Poll<io::Result<&[u8]>> = Poll::Ready(Ok(&[]));

        let mut this = self.project();
        loop {
            if !this.bytes.chunk().is_empty() {
                let chunk = (*this.bytes).chunk();
                return Poll::Ready(Ok(chunk));
            }

            let res = ready!(this.stream.as_mut().get_pin_mut().poll_next(cx));
            match res.transpose().map_err(io_error)? {
                Some(message) => match message {
                    Message::Ping(_) => {}
                    Message::Pong(_) => {}
                    Message::Text(text) => {
                        // We expect to see only binary messages.
                        let error = "unexpected text message in the websocket";
                        warn!(length = text.len(), error);
                        return Poll::Ready(Err(io_error(error)));
                    }
                    Message::Frame(_) => {
                        // This case is impossible according to Frame's doc.
                        panic!("unexpected raw frame in the websocket");
                    }
                    Message::Binary(chunk) => {
                        assert!(this.bytes.is_empty());
                        *this.bytes = Bytes::from(chunk);
                    }
                    Message::Close(_) => return EOF,
                },
                None => return EOF,
            }
        }
    }

    fn consume(self: Pin<&mut Self>, amount: usize) {
        self.project().bytes.advance(amount);
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
    handle_client(
        config,
        cancel_map,
        session_id,
        WebSocketRw::new(websocket),
        ClientMode::Websockets { hostname },
    )
    .await?;
    Ok(())
}

async fn ws_handler(
    mut request: Request<Body>,
    config: &'static ProxyConfig,
    conn_pool: Arc<GlobalConnPool>,
    cancel_map: Arc<CancelMap>,
    session_id: uuid::Uuid,
    sni_hostname: Option<String>,
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
                error!("error in websocket connection: {e:?}");
            }
        });

        // Return the response so the spawned future can continue.
        Ok(response)
    // TODO: that deserves a refactor as now this function also handles http json client besides websockets.
    // Right now I don't want to blow up sql-over-http patch with file renames and do that as a follow up instead.
    } else if request.uri().path() == "/sql" && request.method() == Method::POST {
        let result = sql_over_http::handle(request, sni_hostname, conn_pool)
            .instrument(info_span!("sql-over-http"))
            .await;
        let status_code = match result {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::BAD_REQUEST,
        };
        let json = match result {
            Ok(r) => r,
            Err(e) => {
                let message = format!("{:?}", e);
                let code = match e.downcast_ref::<tokio_postgres::Error>() {
                    Some(e) => match e.code() {
                        Some(e) => serde_json::to_value(e.code()).unwrap(),
                        None => Value::Null,
                    },
                    None => Value::Null,
                };
                json!({ "message": message, "code": code })
            }
        };
        json_response(status_code, json).map(|mut r| {
            r.headers_mut().insert(
                "Access-Control-Allow-Origin",
                hyper::http::HeaderValue::from_static("*"),
            );
            r
        })
    } else if request.uri().path() == "/sql" && request.method() == Method::OPTIONS {
        Response::builder()
            .header("Allow", "OPTIONS, POST")
            .header("Access-Control-Allow-Origin", "*")
            .header(
                "Access-Control-Allow-Headers",
                "Neon-Connection-String, Neon-Raw-Text-Output, Neon-Array-Mode, Neon-Pool-Opt-In",
            )
            .header("Access-Control-Max-Age", "86400" /* 24 hours */)
            .status(StatusCode::OK) // 204 is also valid, but see: https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS#status_code
            .body(Body::empty())
            .map_err(|e| ApiError::BadRequest(e.into()))
    } else {
        json_response(StatusCode::BAD_REQUEST, "query is not supported")
    }
}

pub async fn task_main(
    config: &'static ProxyConfig,
    ws_listener: TcpListener,
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("websocket server has shut down");
    }

    let conn_pool: Arc<GlobalConnPool> = GlobalConnPool::new(config);

    let tls_config = config.tls_config.as_ref().map(|cfg| cfg.to_server_config());
    let tls_acceptor: tokio_rustls::TlsAcceptor = match tls_config {
        Some(config) => config.into(),
        None => {
            warn!("TLS config is missing, WebSocket Secure server will not be started");
            return Ok(());
        }
    };

    let mut addr_incoming = AddrIncoming::from_listener(ws_listener)?;
    let _ = addr_incoming.set_nodelay(true);

    let tls_listener = TlsListener::new(tls_acceptor, addr_incoming).filter(|conn| {
        if let Err(err) = conn {
            error!("failed to accept TLS connection for websockets: {err:?}");
            ready(false)
        } else {
            ready(true)
        }
    });

    let make_svc =
        hyper::service::make_service_fn(|stream: &tokio_rustls::server::TlsStream<AddrStream>| {
            let sni_name = stream.get_ref().1.sni_hostname().map(|s| s.to_string());
            let conn_pool = conn_pool.clone();

            async move {
                Ok::<_, Infallible>(hyper::service::service_fn(move |req: Request<Body>| {
                    let sni_name = sni_name.clone();
                    let conn_pool = conn_pool.clone();

                    async move {
                        let cancel_map = Arc::new(CancelMap::default());
                        let session_id = uuid::Uuid::new_v4();

                        ws_handler(req, config, conn_pool, cancel_map, session_id, sni_name)
                            .instrument(info_span!(
                                "ws-client",
                                session = format_args!("{session_id}")
                            ))
                            .await
                    }
                }))
            }
        });

    hyper::Server::builder(accept::from_stream(tls_listener))
        .serve(make_svc)
        .with_graceful_shutdown(cancellation_token.cancelled())
        .await?;

    Ok(())
}
