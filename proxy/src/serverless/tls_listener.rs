use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use hyper::server::{accept::Accept, conn::AddrStream};
use pin_project_lite::pin_project;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    task::JoinSet,
    time::timeout,
};
use tokio_rustls::{server::TlsStream, TlsAcceptor};
use tracing::{info, warn};

use crate::{
    metrics::TLS_HANDSHAKE_FAILURES,
    protocol2::{WithClientIp, WithConnectionGuard},
};

/// Default timeout for the TLS handshake.
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

pin_project! {
    /// Wraps a `Stream` of connections (such as a TCP listener) so that each connection is itself
    /// encrypted using TLS.
    pub(crate) struct TlsListener<A: Accept> {
        #[pin]
        listener: A,
        tls: TlsAcceptor,
        waiting: JoinSet<Option<TlsStream<A::Conn>>>,
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

impl<A> Accept for TlsListener<A>
where
    A: Accept<Conn = WithConnectionGuard<WithClientIp<AddrStream>>>,
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
                    let t = *this.timeout;
                    let accept = this.tls.accept(conn).into_fallible();
                    let protocol = *this.protocol;
                    this.waiting.spawn(async move {
                        match timeout(t, accept).await {
                            Ok(Ok(conn)) => Some(conn),
                            Ok(Err((e, conn))) => {
                                let peer_addr = conn
                                    .inner
                                    .client_addr()
                                    .unwrap_or_else(|| conn.inner.inner.remote_addr());
                                TLS_HANDSHAKE_FAILURES.inc();
                                warn!(%peer_addr, protocol, "failed to accept TLS connection: {e:?}");
                                None
                            }
                            // The handshake timed out, try getting another connection from the queue
                            Err(_) => None,
                        }
                    });
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
                Poll::Ready(Some(Ok(Some(conn)))) => {
                    info!(protocol = this.protocol, "accepted new TLS connection");
                    Poll::Ready(Some(Ok(conn)))
                }
                // The handshake failed to complete, try getting another connection from the queue
                Poll::Ready(Some(Ok(None))) => continue,
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
