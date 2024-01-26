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
use hyper::upgrade::Upgraded;
use hyper_tungstenite::{tungstenite::Message, HyperWebsocket, WebSocketStream};
use pin_project_lite::pin_project;

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
    mut ctx: RequestMonitoring,
    websocket: HyperWebsocket,
    cancel_map: Arc<CancelMap>,
    hostname: Option<String>,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> anyhow::Result<()> {
    let websocket = websocket.await?;
    let res = handle_client(
        config,
        &mut ctx,
        cancel_map,
        WebSocketRw::new(websocket),
        ClientMode::Websockets { hostname },
        endpoint_rate_limiter,
    )
    .await;

    match res {
        Err(e) => {
            // todo: log and push to ctx the error kind
            // ctx.set_error_kind(e.get_error_type())
            ctx.log();
            Err(e)
        }
        Ok(None) => {
            ctx.set_success();
            ctx.log();
            Ok(())
        }
        Ok(Some(p)) => {
            ctx.set_success();
            ctx.log();
            p.proxy_pass().await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::{SinkExt, StreamExt};
    use hyper_tungstenite::{
        tungstenite::{protocol::Role, Message},
        WebSocketStream,
    };
    use tokio::{
        io::{duplex, AsyncReadExt, AsyncWriteExt},
        task::JoinSet,
    };

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
