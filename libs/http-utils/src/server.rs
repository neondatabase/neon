use std::sync::Arc;

use hyper0::Body;
use hyper0::server::conn::Http;
use routerify::RequestServiceBuilder;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::error::ApiError;

pub struct Server {
    request_service: Arc<RequestServiceBuilder<Body, ApiError>>,
    listener: tokio::net::TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
    cancel: CancellationToken,
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
            cancel: CancellationToken::new(),
        })
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub async fn serve(self) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                stream = self.listener.accept() => {
                    let (tcp_stream, remote_addr) = match stream {
                        Ok(stream) => stream,
                        Err(err) => {
                            debug!("Failed to establish TCP connection {err}");
                            continue;
                        }
                    };

                    let service = self.request_service.build(remote_addr);
                    let tls_acceptor = self.tls_acceptor.clone();

                    tokio::spawn(utils::tasks::exit_on_panic_or_error(
                        "http requst handler",
                        async move {
                            match tls_acceptor {
                                Some(tls_acceptor) => {
                                    // Handle HTTPS connection.
                                    let tls_stream = match tls_acceptor.accept(tcp_stream).await {
                                        Ok(tls_stream) => tls_stream,
                                        Err(err) => {
                                            debug!("Failed to accept TLS connection: {err}");
                                            return Ok(());
                                        }
                                    };
                                    if let Err(err) = Http::new().serve_connection(tls_stream, service).await {
                                        debug!("Failed to serve HTTPS connection: {err}");
                                    }
                                }
                                None => {
                                    // Handle HTTP connection.
                                    if let Err(err) = Http::new().serve_connection(tcp_stream, service).await {
                                        debug!("Failed to serve HTTP connection: {err}");
                                    }
                                }
                            };
                            anyhow::Ok(())
                        }));
                 }
                _ = self.cancel.cancelled() => {
                    break;
                }
            }
        }
        Ok(())
    }
}
