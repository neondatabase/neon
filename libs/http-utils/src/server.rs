use std::{error::Error, sync::Arc};

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use hyper0::Body;
use hyper0::server::conn::Http;
use routerify::{RequestService, RequestServiceBuilder};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::error::ApiError;

/// A simple HTTP server over hyper library.
/// You may want to use it instead of [`hyper0::server::Server`] because:
/// 1. hyper0's Server was removed from hyper v1.
///    It's recommended to replace hyepr0's Server with a manual loop, which is done here.
/// 2. hyper0's Server doesn't support TLS out of the box, and there is no way
///    to support it efficiently with the Accept trait that hyper0's Server uses.
///    That's one of the reasons why it was removed from v1.
///    <https://github.com/hyperium/hyper/blob/115339d3df50f20c8717680aa35f48858e9a6205/docs/ROADMAP.md#higher-level-client-and-server-problems>
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

    // pub fn with_tls(
    //     mut self,
    //     key_filename: &Utf8Path,
    //     cert_filename: &Utf8Path,
    // ) -> anyhow::Result<Self> {
    //     Ok(self)
    // }

    // pub fn new_http(
    //     request_service: Arc<RequestServiceBuilder<Body, ApiError>>,
    //     listener: std::net::TcpListener,
    // ) -> anyhow::Result<Self> {
    //     Self::new(request_service, listener, None)
    // }

    // pub fn new_https(
    //     request_service: Arc<RequestServiceBuilder<Body, ApiError>>,
    //     listener: std::net::TcpListener,
    //     key_filename: &Utf8Path,
    //     cert_filename: &Utf8Path,
    // )

    pub async fn serve(self, cancel: CancellationToken) -> anyhow::Result<()> {
        fn suppress_io_error(err: &std::io::Error) -> bool {
            use std::io::ErrorKind::*;
            matches!(err.kind(), ConnectionReset | ConnectionAborted | BrokenPipe)
        }
        fn suppress_hyper_error(err: &hyper0::Error) -> bool {
            if err.is_incomplete_message() || err.is_closed() || err.is_timeout() {
                return true;
            }
            if let Some(inner) = err.source() {
                if let Some(io) = inner.downcast_ref::<std::io::Error>() {
                    return suppress_io_error(io);
                }
            }
            false
        }

        let mut connections = FuturesUnordered::new();
        loop {
            tokio::select! {
                stream = self.listener.accept() => {
                    let (tcp_stream, remote_addr) = match stream {
                        Ok(stream) => stream,
                        Err(err) => {
                            if !suppress_io_error(&err) {
                                info!("Failed to accept TCP connection: {err:#}");
                            }
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
                                    let tls_stream = tokio::select! {
                                        tls_stream = tls_acceptor.accept(tcp_stream) => tls_stream,
                                        _ = cancel.cancelled() => return,
                                    };
                                    let tls_stream = match tls_stream {
                                        Ok(tls_stream) => tls_stream,
                                        Err(err) => {
                                            if !suppress_io_error(&err) {
                                                info!("Failed to accept TLS connection: {err:#}");
                                            }
                                            return;
                                        }
                                    };
                                    if let Err(err) = Self::serve_connection(tls_stream, service, cancel).await {
                                        if !suppress_hyper_error(&err) {
                                            info!("Failed to serve HTTPS connection: {err:#}");
                                        }
                                    }
                                }
                                None => {
                                    // Handle HTTP connection.
                                    if let Err(err) = Self::serve_connection(tcp_stream, service, cancel).await {
                                        if !suppress_hyper_error(&err) {
                                            info!("Failed to serve HTTP connection: {err:#}");
                                        }
                                    }
                                }
                            };
                        }));
                 }
                Some(conn) = connections.next() => {
                    if let Err(err) = conn {
                        error!("Connection panicked: {err:#}");
                    }
                }
                _ = cancel.cancelled() => {
                    // Wait for graceful shutdown of all connections.
                    while let Some(conn) = connections.next().await {
                        if let Err(err) = conn {
                            error!("Connection panicked: {err:#}");
                        }
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
        I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut conn = Http::new().serve_connection(io, service).with_upgrades();

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
