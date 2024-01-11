use crate::{
    cancellation::CancelMap,
    config::ProxyConfig,
    context::RequestMonitoring,
    error::io_error,
    proxy::{handle_client, ClientMode},
    rate_limiter::EndpointRateLimiter,
};
use bytes::{Buf, Bytes};
use futures::{Sink, Stream};
use hyper::{ext::Protocol, upgrade::Upgraded, Body, Method, Request, Response};
use pin_project_lite::pin_project;
use tokio_tungstenite::WebSocketStream;
use tungstenite::{
    error::{Error as WSError, ProtocolError},
    handshake::derive_accept_key,
    protocol::{Role, WebSocketConfig},
    Message,
};

use std::{
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};
use tokio::io::{self, AsyncBufRead, AsyncRead, AsyncWrite, ReadBuf};
use tracing::warn;

// TODO: use `std::sync::Exclusive` once it's stabilized.
// Tracking issue: https://github.com/rust-lang/rust/issues/98407.
use sync_wrapper::SyncWrapper;

pin_project! {
    /// This is a wrapper around a [`WebSocketStream`] that
    /// implements [`AsyncRead`] and [`AsyncWrite`].
    pub struct WebSocketRw<S = Upgraded> {
        #[pin]
        stream: SyncWrapper<WebSocketStream<S>>,
        bytes: Bytes,
    }
}

impl<S> WebSocketRw<S> {
    pub fn new(stream: WebSocketStream<S>) -> Self {
        Self {
            stream: stream.into(),
            bytes: Bytes::new(),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for WebSocketRw<S> {
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

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for WebSocketRw<S> {
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

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncBufRead for WebSocketRw<S> {
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

pub async fn serve_websocket(
    config: &'static ProxyConfig,
    ctx: &mut RequestMonitoring,
    websocket: HyperWebsocket,
    cancel_map: &CancelMap,
    hostname: Option<String>,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> anyhow::Result<()> {
    let websocket = websocket.await?;
    handle_client(
        config,
        ctx,
        cancel_map,
        WebSocketRw::new(websocket),
        ClientMode::Websockets { hostname },
        endpoint_rate_limiter,
    )
    .await?;
    Ok(())
}

/// Try to upgrade a received `hyper::Request` to a websocket connection.
///
/// The function returns a HTTP response and a future that resolves to the websocket stream.
/// The response body *MUST* be sent to the client before the future can be resolved.
///
/// This functions checks `Sec-WebSocket-Key` and `Sec-WebSocket-Version` headers.
/// It does not inspect the `Origin`, `Sec-WebSocket-Protocol` or `Sec-WebSocket-Extensions` headers.
/// You can inspect the headers manually before calling this function,
/// and modify the response headers appropriately.
///
/// This function also does not look at the `Connection` or `Upgrade` headers.
/// To check if a request is a websocket upgrade request, you can use [`is_upgrade_request`].
/// Alternatively you can inspect the `Connection` and `Upgrade` headers manually.
///
pub fn upgrade<B>(
    mut request: impl std::borrow::BorrowMut<Request<B>>,
    config: Option<WebSocketConfig>,
) -> Result<(Response<Body>, HyperWebsocket), ProtocolError> {
    let request = request.borrow_mut();

    let key = request
        .headers()
        .get("Sec-WebSocket-Key")
        .ok_or(ProtocolError::MissingSecWebSocketKey)?;
    if request
        .headers()
        .get("Sec-WebSocket-Version")
        .map(|v| v.as_bytes())
        != Some(b"13")
    {
        return Err(ProtocolError::MissingSecWebSocketVersionHeader);
    }

    let response = Response::builder()
        .status(hyper::StatusCode::SWITCHING_PROTOCOLS)
        .header(hyper::header::CONNECTION, "upgrade")
        .header(hyper::header::UPGRADE, "websocket")
        .header("Sec-WebSocket-Accept", &derive_accept_key(key.as_bytes()))
        .body(Body::from("switching to websocket protocol"))
        .expect("bug: failed to build response");

    let stream = HyperWebsocket {
        inner: hyper::upgrade::on(request),
        config,
    };

    Ok((response, stream))
}

/// Check if a request is a websocket upgrade request.
///
/// If the `Upgrade` header lists multiple protocols,
/// this function returns true if of them are `"websocket"`,
/// If the server supports multiple upgrade protocols,
/// it would be more appropriate to try each listed protocol in order.
pub fn is_upgrade_request<B>(request: &hyper::Request<B>) -> bool {
    header_contains_value(request.headers(), hyper::header::CONNECTION, "Upgrade")
        && header_contains_value(request.headers(), hyper::header::UPGRADE, "websocket")
}

/// Check if there is a header of the given name containing the wanted value.
fn header_contains_value(
    headers: &hyper::HeaderMap,
    header: impl hyper::header::AsHeaderName,
    value: impl AsRef<[u8]>,
) -> bool {
    let value = value.as_ref();
    for header in headers.get_all(header) {
        if header
            .as_bytes()
            .split(|&c| c == b',')
            .any(|x| trim(x).eq_ignore_ascii_case(value))
        {
            return true;
        }
    }
    false
}

fn trim(data: &[u8]) -> &[u8] {
    trim_end(trim_start(data))
}

fn trim_start(data: &[u8]) -> &[u8] {
    if let Some(start) = data.iter().position(|x| !x.is_ascii_whitespace()) {
        &data[start..]
    } else {
        b""
    }
}

fn trim_end(data: &[u8]) -> &[u8] {
    if let Some(last) = data.iter().rposition(|x| !x.is_ascii_whitespace()) {
        &data[..last + 1]
    } else {
        b""
    }
}

/// Try to upgrade a received `hyper::Request` to a websocket connection.
///
/// The function returns a HTTP response and a future that resolves to the websocket stream.
/// The response body *MUST* be sent to the client before the future can be resolved.
///
/// This functions checks `Sec-WebSocket-Version` header.
/// It does not inspect the `Origin`, `Sec-WebSocket-Protocol` or `Sec-WebSocket-Extensions` headers.
/// You can inspect the headers manually before calling this function,
/// and modify the response headers appropriately.
///
/// This function also does not look at the `Connection` or `Upgrade` headers.
/// To check if a request is a websocket upgrade request, you can use [`is_upgrade2_request`].
/// Alternatively you can inspect the `Connection` and `Upgrade` headers manually.
///
pub fn connect<B>(
    mut request: impl std::borrow::BorrowMut<Request<B>>,
    config: Option<WebSocketConfig>,
) -> Result<(Response<Body>, HyperWebsocket), ProtocolError> {
    let request = request.borrow_mut();

    if request
        .headers()
        .get("Sec-WebSocket-Version")
        .map(|v| v.as_bytes())
        != Some(b"13")
    {
        return Err(ProtocolError::MissingSecWebSocketVersionHeader);
    }

    let response = Response::builder()
        .status(hyper::StatusCode::OK)
        .body(Body::from("switching to websocket protocol"))
        .expect("bug: failed to build response");

    let stream = HyperWebsocket {
        inner: hyper::upgrade::on(request),
        config,
    };

    Ok((response, stream))
}

/// Check if a request is a websocket connect request.
pub fn is_connect_request<B>(request: &hyper::Request<B>) -> bool {
    request.method() == Method::CONNECT
        && request
            .extensions()
            .get::<Protocol>()
            .is_some_and(|protocol| protocol.as_str() == "websocket")
}

pin_project_lite::pin_project! {
    /// A future that resolves to a websocket stream when the associated connection completes.
    #[derive(Debug)]
    pub struct HyperWebsocket {
        #[pin]
        inner: hyper::upgrade::OnUpgrade,
        config: Option<WebSocketConfig>
    }
}

impl std::future::Future for HyperWebsocket {
    type Output = Result<WebSocketStream<hyper::upgrade::Upgraded>, WSError>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
        let this = self.project();
        let upgraded = match this.inner.poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(x) => x,
        };

        let upgraded =
            upgraded.map_err(|_| WSError::Protocol(ProtocolError::HandshakeIncomplete))?;

        let stream = WebSocketStream::from_raw_socket(upgraded, Role::Server, None);
        tokio::pin!(stream);

        // The future returned by `from_raw_socket` is always ready.
        // Not sure why it is a future in the first place.
        match stream.as_mut().poll(cx) {
            Poll::Pending => unreachable!("from_raw_socket should always be created ready"),
            Poll::Ready(x) => Poll::Ready(Ok(x)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::{SinkExt, StreamExt};
    use tokio::{
        io::{duplex, AsyncReadExt, AsyncWriteExt},
        task::JoinSet,
    };
    use tokio_tungstenite::WebSocketStream;
    use tungstenite::{protocol::Role, Message};

    use super::WebSocketRw;

    #[tokio::test]
    async fn websocket_stream_wrapper_happy_path() {
        let (stream1, stream2) = duplex(1024);

        let mut js = JoinSet::new();

        js.spawn(async move {
            let mut client = WebSocketStream::from_raw_socket(stream1, Role::Client, None).await;

            client
                .send(Message::Binary(b"hello world".to_vec()))
                .await
                .unwrap();

            let message = client.next().await.unwrap().unwrap();
            assert_eq!(message, Message::Binary(b"websockets are cool".to_vec()));

            client.close(None).await.unwrap();
        });

        js.spawn(async move {
            let mut rw = pin!(WebSocketRw::new(
                WebSocketStream::from_raw_socket(stream2, Role::Server, None).await
            ));

            let mut buf = vec![0; 1024];
            let n = rw.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"hello world");

            rw.write_all(b"websockets are cool").await.unwrap();
            rw.flush().await.unwrap();

            let n = rw.read_to_end(&mut buf).await.unwrap();
            assert_eq!(n, 0);
        });

        js.join_next().await.unwrap().unwrap();
        js.join_next().await.unwrap().unwrap();
    }
}
