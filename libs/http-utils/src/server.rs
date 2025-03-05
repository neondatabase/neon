use std::sync::Arc;
#[cfg(feature = "testing")]
use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use hyper0::Body;
use hyper0::server::conn::Http;
use routerify::{RequestService, RequestServiceBuilder};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::error::ApiError;

/// A simple HTTP server over hyper library.
/// You may want to use it instead of [`hyper0::server::Server`] because:
/// 1. hyper0's Server was removed from hyper v1.
///    It's recommended to replace hyepr0's Server with a manual loop, which is done here.
/// 2. hyper0's Server doesn't support TLS out of the box, and there is no way
///    to support it efficiently with the Accept trait that hyper0's Server uses.
///    That's one of the reasons why it was removed from v1.
///    https://github.com/hyperium/hyper/blob/115339d3df50f20c8717680aa35f48858e9a6205/docs/ROADMAP.md#higher-level-client-and-server-problems
pub struct Server {
    request_service: Arc<RequestServiceBuilder<Body, ApiError>>,
    listener: tokio::net::TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
}

impl Server {
    pub fn new(
        request_service: Arc<RequestServiceBuilder<Body, ApiError>>,
        listener: std::net::TcpListener,
        tls_acceptor: Option<TlsAcceptor>,
    ) -> anyhow::Result<Self> {
        // Note: caller of from_std is responsible for setting nonblocking mode.
        listener.set_nonblocking(true)?;
        let listener = tokio::net::TcpListener::from_std(listener)?;

        Ok(Self {
            request_service,
            listener,
            tls_acceptor,
        })
    }

    pub async fn serve(self, cancel: CancellationToken) -> anyhow::Result<()> {
        let mut connections = FuturesUnordered::new();
        loop {
            tokio::select! {
                stream = self.listener.accept() => {
                    let (tcp_stream, remote_addr) = match stream {
                        Ok(stream) => stream,
                        Err(err) => {
                            error!("Failed to accept TCP connection: {err:#}");
                            continue;
                        }
                    };

                    let service = self.request_service.build(remote_addr);
                    let tls_acceptor = self.tls_acceptor.clone();
                    let cancel = cancel.clone();

                    connections.push(tokio::spawn(
                        async move {
                            match tls_acceptor {
                                Some(tls_acceptor) => {
                                    // Handle HTTPS connection.
                                    let tls_stream = match tls_acceptor.accept(tcp_stream).await {
                                        Ok(tls_stream) => tls_stream,
                                        Err(err) => {
                                            error!("Failed to accept TLS connection: {err:#}");
                                            return;
                                        }
                                    };
                                    if let Err(err) = Self::serve_connection(tls_stream, service, cancel).await {
                                        error!("Failed to serve HTTPS connection: {err:#}");
                                    }
                                }
                                None => {
                                    // Handle HTTP connection.
                                    if let Err(err) = Self::serve_connection(tcp_stream, service, cancel).await {
                                        error!("Failed to serve HTTP connection: {err:#}");
                                    }
                                }
                            };
                        }));
                 }
                Some(conn) = connections.next() => {
                    // Propagate connection panic to the caller.
                    conn.expect("http connection should not panic");
                }
                _ = cancel.cancelled() => {
                    // Wait for graceful shutdown of all connections.
                    while let Some(conn) = connections.next().await {
                        conn.expect("http connection should not panic");
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    /// Serves HTTP connection with graceful shutdown.
    async fn serve_connection<I>(
        io: I,
        service: RequestService<Body, ApiError>,
        cancel: CancellationToken,
    ) -> Result<(), hyper0::Error>
    where
        I: AsyncRead + AsyncWrite + Unpin + std::marker::Send + 'static,
    {
        #[allow(unused_mut)]
        let mut http = Http::new();

        // Note: if a client shutdowns without gracefuly closing a keep-alive connection
        // to the server, the server will wait for the connection to be timed out.
        // It may slow down tests' teardown, as the server will try gracefully close all
        // hanging keep-alive connections during its shutdown.
        // There is no way to specify "keep alive connection timeout" in hyper,
        // but "header_read_timeout" does the trick and closes a keep-alive connection
        // if the server doesn't hear from the client for specified duration.
        // For example, this workaround speeds up test_graceful_cluster_restart by 5-10s.
        #[cfg(feature = "testing")]
        http.http1_header_read_timeout(Duration::from_secs(1));

        let mut conn = http.serve_connection(io, service).with_upgrades();

        tokio::select! {
            res = &mut conn => res,
            _ = cancel.cancelled() => {
                Pin::new(&mut conn).graceful_shutdown();
                // Note: connection should still be awaited for graceful shutdown to complete.
                conn.await
            }
        }
    }
}
