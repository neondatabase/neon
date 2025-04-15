use std::{error::Error, sync::Arc};

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use hyper0::Body;
use hyper0::server::conn::Http;
use metrics::{IntCounterVec, register_int_counter_vec};
use once_cell::sync::Lazy;
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

static CONNECTION_STARTED_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "http_server_connection_started_total",
        "Number of established http/https connections",
        &["scheme"]
    )
    .expect("failed to define a metric")
});

static CONNECTION_ERROR_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "http_server_connection_errors_total",
        "Number of occured connection errors by type",
        &["type"]
    )
    .expect("failed to define a metric")
});

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

        let tcp_error_cnt = CONNECTION_ERROR_COUNT.with_label_values(&["tcp"]);
        let tls_error_cnt = CONNECTION_ERROR_COUNT.with_label_values(&["tls"]);
        let http_error_cnt = CONNECTION_ERROR_COUNT.with_label_values(&["http"]);
        let https_error_cnt = CONNECTION_ERROR_COUNT.with_label_values(&["https"]);
        let panic_error_cnt = CONNECTION_ERROR_COUNT.with_label_values(&["panic"]);

        let http_connection_cnt = CONNECTION_STARTED_COUNT.with_label_values(&["http"]);
        let https_connection_cnt = CONNECTION_STARTED_COUNT.with_label_values(&["https"]);

        let mut connections = FuturesUnordered::new();
        loop {
            tokio::select! {
                stream = self.listener.accept() => {
                    let (tcp_stream, remote_addr) = match stream {
                        Ok(stream) => stream,
                        Err(err) => {
                            tcp_error_cnt.inc();
                            if !suppress_io_error(&err) {
                                info!("Failed to accept TCP connection: {err:#}");
                            }
                            continue;
                        }
                    };

                    let service = self.request_service.build(remote_addr);
                    let tls_acceptor = self.tls_acceptor.clone();
                    let cancel = cancel.clone();

                    let tls_error_cnt = tls_error_cnt.clone();
                    let http_error_cnt = http_error_cnt.clone();
                    let https_error_cnt = https_error_cnt.clone();
                    let http_connection_cnt = http_connection_cnt.clone();
                    let https_connection_cnt = https_connection_cnt.clone();

                    connections.push(tokio::spawn(
                        async move {
                            match tls_acceptor {
                                Some(tls_acceptor) => {
                                    // Handle HTTPS connection.
                                    https_connection_cnt.inc();
                                    let tls_stream = tokio::select! {
                                        tls_stream = tls_acceptor.accept(tcp_stream) => tls_stream,
                                        _ = cancel.cancelled() => return,
                                    };
                                    let tls_stream = match tls_stream {
                                        Ok(tls_stream) => tls_stream,
                                        Err(err) => {
                                            tls_error_cnt.inc();
                                            if !suppress_io_error(&err) {
                                                info!(%remote_addr, "Failed to accept TLS connection: {err:#}");
                                            }
                                            return;
                                        }
                                    };
                                    if let Err(err) = Self::serve_connection(tls_stream, service, cancel).await {
                                        https_error_cnt.inc();
                                        if !suppress_hyper_error(&err) {
                                            info!(%remote_addr, "Failed to serve HTTPS connection: {err:#}");
                                        }
                                    }
                                }
                                None => {
                                    // Handle HTTP connection.
                                    http_connection_cnt.inc();
                                    if let Err(err) = Self::serve_connection(tcp_stream, service, cancel).await {
                                        http_error_cnt.inc();
                                        if !suppress_hyper_error(&err) {
                                            info!(%remote_addr, "Failed to serve HTTP connection: {err:#}");
                                        }
                                    }
                                }
                            };
                        }));
                 }
                Some(conn) = connections.next() => {
                    if let Err(err) = conn {
                        panic_error_cnt.inc();
                        error!("Connection panicked: {err:#}");
                    }
                }
                _ = cancel.cancelled() => {
                    // Wait for graceful shutdown of all connections.
                    while let Some(conn) = connections.next().await {
                        if let Err(err) = conn {
                            panic_error_cnt.inc();
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
