use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use hyper::server::accept::Accept;
use pin_project_lite::pin_project;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    task::JoinSet,
    time::timeout,
};
use tokio_rustls::{server::TlsStream, TlsAcceptor};
use tracing::{info, warn};

use crate::metrics::TLS_HANDSHAKE_FAILURES;

/// Default timeout for the TLS handshake.
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

type TimeoutResult<T> = Result<T, tokio::time::error::Elapsed>;

pin_project! {
    /// Wraps a `Stream` of connections (such as a TCP listener) so that each connection is itself
    /// encrypted using TLS.
    pub(crate) struct TlsListener<A: Accept> {
        #[pin]
        listener: A,
        tls: TlsAcceptor,
        waiting: JoinSet<TimeoutResult<std::io::Result<TlsStream<A::Conn>>>>,
        timeout: Duration,
        protocol: &'static str,
    }
}

impl<A: Accept> TlsListener<A> {
    /// Create a `TlsListener` with default options.
    pub(crate) fn new(tls: TlsAcceptor, listener: A, protocol: &'static str) -> Self {
        TlsListener {
            listener,
            tls,
            waiting: JoinSet::new(),
            timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            protocol,
        }
    }
}

impl<A: Accept> Accept for TlsListener<A>
where
    A::Error: std::error::Error,
    A::Conn: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Conn = TlsStream<A::Conn>;

    type Error = Infallible;

    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Conn, Self::Error>>> {
        let mut this = self.project();

        loop {
            match this.listener.as_mut().poll_accept(cx) {
                Poll::Pending => break,
                Poll::Ready(Some(Ok(conn))) => {
                    this.waiting
                        .spawn(timeout(*this.timeout, this.tls.accept(conn)));
                }
                Poll::Ready(Some(Err(e))) => {
                    tracing::error!("error accepting TCP connection: {e}");
                    continue;
                }
                Poll::Ready(None) => return Poll::Ready(None),
            }
        }

        loop {
            return match this.waiting.poll_join_next(cx) {
                Poll::Ready(Some(Ok(Ok(Ok(conn))))) => {
                    info!(protocol = this.protocol, "accepted new TLS connection");
                    Poll::Ready(Some(Ok(conn)))
                }
                Poll::Ready(Some(Ok(Ok(Err(e))))) => {
                    TLS_HANDSHAKE_FAILURES.inc();
                    warn!(
                        protocol = this.protocol,
                        "failed to accept TLS connection: {e:?}"
                    );
                    continue;
                }
                // The handshake timed out, try getting another connection from the queue
                Poll::Ready(Some(Ok(Err(_)))) => continue,
                // The handshake panicked or was cancelled. ignore and get another connection
                Poll::Ready(Some(Err(e))) => {
                    tracing::warn!("handshake aborted: {e}");
                    continue;
                }
                _ => Poll::Pending,
            };
        }
    }
}
