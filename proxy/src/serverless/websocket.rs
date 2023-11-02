use crate::{
    cancellation::CancelMap,
    config::ProxyConfig,
    error::io_error,
    proxy::{handle_client, ClientMode},
};
use bytes::{Buf, Bytes};
use futures::{Sink, Stream};
use hyper::upgrade::Upgraded;
use hyper_tungstenite::{tungstenite::Message, HyperWebsocket, WebSocketStream};
use pin_project_lite::pin_project;

use std::{
    pin::Pin,
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

pub async fn serve_websocket(
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
