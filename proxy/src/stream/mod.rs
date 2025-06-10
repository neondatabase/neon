mod pq_frontend;

use std::pin::Pin;
use std::sync::Arc;
use std::{io, task};

pub use pq_frontend::PqFeStream;
use rustls::ServerConfig;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::server::TlsStream;

use crate::error::{ErrorKind, ReportableError, UserFacingError};
use crate::metrics::Metrics;
use crate::tls::TlsServerEndPoint;

#[derive(Debug)]
pub struct ReportedError {
    source: anyhow::Error,
    error_kind: ErrorKind,
}

impl ReportedError {
    pub fn new(e: (impl UserFacingError + Into<anyhow::Error>)) -> Self {
        let error_kind = e.get_error_kind();
        Self {
            source: e.into(),
            error_kind,
        }
    }
}

impl std::fmt::Display for ReportedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}

impl std::error::Error for ReportedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.source()
    }
}

impl ReportableError for ReportedError {
    fn get_error_kind(&self) -> ErrorKind {
        self.error_kind
    }
}

/// Wrapper for upgrading raw streams into secure streams.
pub enum Stream<S> {
    /// We always begin with a raw stream,
    /// which may then be upgraded into a secure stream.
    Raw { raw: S },
    Tls {
        /// We box [`TlsStream`] since it can be quite large.
        tls: Box<TlsStream<S>>,
        /// Channel binding parameter
        tls_server_end_point: TlsServerEndPoint,
    },
}

impl<S: Unpin> Unpin for Stream<S> {}

impl<S> Stream<S> {
    /// Construct a new instance from a raw stream.
    pub fn from_raw(raw: S) -> Self {
        Self::Raw { raw }
    }

    /// Return SNI hostname when it's available.
    pub fn sni_hostname(&self) -> Option<&str> {
        match self {
            Stream::Raw { .. } => None,
            Stream::Tls { tls, .. } => tls.get_ref().1.server_name(),
        }
    }

    pub(crate) fn tls_server_end_point(&self) -> TlsServerEndPoint {
        match self {
            Stream::Raw { .. } => TlsServerEndPoint::Undefined,
            Stream::Tls {
                tls_server_end_point,
                ..
            } => *tls_server_end_point,
        }
    }
}

#[derive(Debug, Error)]
#[error("Can't upgrade TLS stream")]
pub enum StreamUpgradeError {
    #[error("Bad state reached: can't upgrade TLS stream")]
    AlreadyTls,

    #[error("Can't upgrade stream: IO error: {0}")]
    Io(#[from] io::Error),
}

impl<S: AsyncRead + AsyncWrite + Unpin> Stream<S> {
    /// If possible, upgrade raw stream into a secure TLS-based stream.
    pub async fn upgrade(
        self,
        cfg: Arc<ServerConfig>,
        record_handshake_error: bool,
    ) -> Result<TlsStream<S>, StreamUpgradeError> {
        match self {
            Stream::Raw { raw } => Ok(tokio_rustls::TlsAcceptor::from(cfg)
                .accept(raw)
                .await
                .inspect_err(|_| {
                    if record_handshake_error {
                        Metrics::get().proxy.tls_handshake_failures.inc();
                    }
                })?),
            Stream::Tls { .. } => Err(StreamUpgradeError::AlreadyTls),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for Stream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Raw { raw } => Pin::new(raw).poll_read(context, buf),
            Self::Tls { tls, .. } => Pin::new(tls).poll_read(context, buf),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for Stream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
        buf: &[u8],
    ) -> task::Poll<io::Result<usize>> {
        match &mut *self {
            Self::Raw { raw } => Pin::new(raw).poll_write(context, buf),
            Self::Tls { tls, .. } => Pin::new(tls).poll_write(context, buf),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
    ) -> task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Raw { raw } => Pin::new(raw).poll_flush(context),
            Self::Tls { tls, .. } => Pin::new(tls).poll_flush(context),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
    ) -> task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Raw { raw } => Pin::new(raw).poll_shutdown(context),
            Self::Tls { tls, .. } => Pin::new(tls).poll_shutdown(context),
        }
    }
}
