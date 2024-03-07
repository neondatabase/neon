use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{Future, Stream, StreamExt};
use pin_project_lite::pin_project;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    task::JoinSet,
    time::timeout,
};

/// Default number of concurrent handshakes
pub const DEFAULT_MAX_HANDSHAKES: usize = 64;
/// Default timeout for the TLS handshake.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Trait for TLS implementation.
///
/// Implementations are provided by the rustls and native-tls features.
pub trait AsyncTls<C: AsyncRead + AsyncWrite>: Clone {
    /// The type of the TLS stream created from the underlying stream.
    type Stream: Send + 'static;
    /// Error type for completing the TLS handshake
    type Error: std::error::Error + Send + 'static;
    /// Type of the Future for the TLS stream that is accepted.
    type AcceptFuture: Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static;

    /// Accept a TLS connection on an underlying stream
    fn accept(&self, stream: C) -> Self::AcceptFuture;
}

/// Asynchronously accept connections.
pub trait AsyncAccept {
    /// The type of the connection that is accepted.
    type Connection: AsyncRead + AsyncWrite;
    /// The type of error that may be returned.
    type Error;

    /// Poll to accept the next connection.
    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Connection, Self::Error>>>;

    /// Return a new `AsyncAccept` that stops accepting connections after
    /// `ender` completes.
    ///
    /// Useful for graceful shutdown.
    ///
    /// See [examples/echo.rs](https://github.com/tmccombs/tls-listener/blob/main/examples/echo.rs)
    /// for example of how to use.
    fn until<F: Future>(self, ender: F) -> Until<Self, F>
    where
        Self: Sized,
    {
        Until {
            acceptor: self,
            ender,
        }
    }
}

pin_project! {
    ///
    /// Wraps a `Stream` of connections (such as a TCP listener) so that each connection is itself
    /// encrypted using TLS.
    ///
    /// It is similar to:
    ///
    /// ```ignore
    /// tcpListener.and_then(|s| tlsAcceptor.accept(s))
    /// ```
    ///
    /// except that it has the ability to accept multiple transport-level connections
    /// simultaneously while the TLS handshake is pending for other connections.
    ///
    /// By default, if a client fails the TLS handshake, that is treated as an error, and the
    /// `TlsListener` will return an `Err`. If the `TlsListener` is passed directly to a hyper
    /// [`Server`][1], then an invalid handshake can cause the server to stop accepting connections.
    /// See [`http-stream.rs`][2] or [`http-low-level`][3] examples, for examples of how to avoid this.
    ///
    /// Note that if the maximum number of pending connections is greater than 1, the resulting
    /// [`T::Stream`][4] connections may come in a different order than the connections produced by the
    /// underlying listener.
    ///
    /// [1]: https://docs.rs/hyper/latest/hyper/server/struct.Server.html
    /// [2]: https://github.com/tmccombs/tls-listener/blob/main/examples/http-stream.rs
    /// [3]: https://github.com/tmccombs/tls-listener/blob/main/examples/http-low-level.rs
    /// [4]: AsyncTls::Stream
    ///
    #[allow(clippy::type_complexity)]
    pub struct TlsListener<A: AsyncAccept, T: AsyncTls<A::Connection>> {
        #[pin]
        listener: A,
        tls: T,
        waiting: JoinSet<Result<Result<T::Stream, T::Error>, tokio::time::error::Elapsed>>,
        max_handshakes: usize,
        timeout: Duration,
    }
}

/// Builder for `TlsListener`.
#[derive(Clone)]
pub struct Builder<T> {
    tls: T,
    max_handshakes: usize,
    handshake_timeout: Duration,
}

/// Wraps errors from either the listener or the TLS Acceptor
#[derive(Debug, Error)]
pub enum Error<LE: std::error::Error, TE: std::error::Error> {
    /// An error that arose from the listener ([AsyncAccept::Error])
    #[error("{0}")]
    ListenerError(#[source] LE),
    /// An error that occurred during the TLS accept handshake
    #[error("{0}")]
    TlsAcceptError(#[source] TE),
}

impl<A: AsyncAccept, T> TlsListener<A, T>
where
    T: AsyncTls<A::Connection>,
{
    /// Create a `TlsListener` with default options.
    pub fn new(tls: T, listener: A) -> Self {
        builder(tls).listen(listener)
    }
}

impl<A, T> TlsListener<A, T>
where
    A: AsyncAccept,
    A::Error: std::error::Error,
    T: AsyncTls<A::Connection>,
{
    /// Accept the next connection
    ///
    /// This is essentially an alias to `self.next()` with a more domain-appropriate name.
    pub async fn accept(&mut self) -> Option<<Self as Stream>::Item>
    where
        Self: Unpin,
    {
        self.next().await
    }

    /// Replaces the Tls Acceptor configuration, which will be used for new connections.
    ///
    /// This can be used to change the certificate used at runtime.
    pub fn replace_acceptor(&mut self, acceptor: T) {
        self.tls = acceptor;
    }

    /// Replaces the Tls Acceptor configuration from a pinned reference to `Self`.
    ///
    /// This is useful if your listener is `!Unpin`.
    ///
    /// This can be used to change the certificate used at runtime.
    pub fn replace_acceptor_pin(self: Pin<&mut Self>, acceptor: T) {
        *self.project().tls = acceptor;
    }
}

impl<A, T> Stream for TlsListener<A, T>
where
    A: AsyncAccept,
    A::Error: std::error::Error,
    T: AsyncTls<A::Connection>,
{
    type Item = Result<T::Stream, Error<A::Error, T::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        while this.waiting.len() < *this.max_handshakes {
            match this.listener.as_mut().poll_accept(cx) {
                Poll::Pending => break,
                Poll::Ready(Some(Ok(conn))) => {
                    this.waiting
                        .spawn(timeout(*this.timeout, this.tls.accept(conn)));
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(Error::ListenerError(e))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
            }
        }

        loop {
            return match this.waiting.poll_join_next(cx) {
                Poll::Ready(Some(Ok(Ok(conn)))) => {
                    Poll::Ready(Some(conn.map_err(Error::TlsAcceptError)))
                }
                // The handshake timed out, try getting another connection from the queue
                Poll::Ready(Some(Ok(Err(_)))) => continue,
                // The handshake panicked
                Poll::Ready(Some(Err(e))) if e.is_panic() => {
                    std::panic::resume_unwind(e.into_panic())
                }
                // The handshake was externally aborted
                Poll::Ready(Some(Err(_))) => unreachable!("handshake tasks are never aborted"),
                _ => Poll::Pending,
            };
        }
    }
}

impl<C: AsyncRead + AsyncWrite + Unpin + Send + 'static> AsyncTls<C> for tokio_rustls::TlsAcceptor {
    type Stream = tokio_rustls::server::TlsStream<C>;
    type Error = std::io::Error;
    type AcceptFuture = tokio_rustls::Accept<C>;

    fn accept(&self, conn: C) -> Self::AcceptFuture {
        tokio_rustls::TlsAcceptor::accept(self, conn)
    }
}

impl<T> Builder<T> {
    /// Set the maximum number of concurrent handshakes.
    ///
    /// At most `max` handshakes will be concurrently processed. If that limit is
    /// reached, the `TlsListener` will stop polling the underlying listener until a
    /// handshake completes and the encrypted stream has been returned.
    ///
    /// Defaults to `DEFAULT_MAX_HANDSHAKES`.
    pub fn max_handshakes(&mut self, max: usize) -> &mut Self {
        self.max_handshakes = max;
        self
    }

    /// Set the timeout for handshakes.
    ///
    /// If a timeout takes longer than `timeout`, then the handshake will be
    /// aborted and the underlying connection will be dropped.
    ///
    /// Defaults to `DEFAULT_HANDSHAKE_TIMEOUT`.
    pub fn handshake_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Create a `TlsListener` from the builder
    ///
    /// Actually build the `TlsListener`. The `listener` argument should be
    /// an implementation of the `AsyncAccept` trait that accepts new connections
    /// that the `TlsListener` will  encrypt using TLS.
    pub fn listen<A: AsyncAccept>(&self, listener: A) -> TlsListener<A, T>
    where
        T: AsyncTls<A::Connection>,
    {
        TlsListener {
            listener,
            tls: self.tls.clone(),
            waiting: JoinSet::new(),
            max_handshakes: self.max_handshakes,
            timeout: self.handshake_timeout,
        }
    }
}

/// Create a new Builder for a TlsListener
///
/// `server_config` will be used to configure the TLS sessions.
pub fn builder<T>(tls: T) -> Builder<T> {
    Builder {
        tls,
        max_handshakes: DEFAULT_MAX_HANDSHAKES,
        handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
    }
}

pin_project! {
    /// See [`AsyncAccept::until`]
    pub struct Until<A, E> {
        #[pin]
        acceptor: A,
        #[pin]
        ender: E,
    }
}

impl<A: AsyncAccept, E: Future> AsyncAccept for Until<A, E> {
    type Connection = A::Connection;
    type Error = A::Error;

    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Connection, Self::Error>>> {
        let this = self.project();

        match this.ender.poll(cx) {
            Poll::Pending => this.acceptor.poll_accept(cx),
            Poll::Ready(_) => Poll::Ready(None),
        }
    }
}
