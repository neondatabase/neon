use std::pin::Pin;
use std::sync::Arc;
use std::{io, task};

use rustls::ServerConfig;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio_rustls::server::TlsStream;

use crate::error::{ErrorKind, ReportableError, UserFacingError};
use crate::metrics::Metrics;
use crate::pqproto::{
    BeMessage, FE_PASSWORD_MESSAGE, FeStartupPacket, SQLSTATE_INTERNAL_ERROR, WriteBuf,
    read_message, read_startup,
};
use crate::tls::TlsServerEndPoint;

/// Stream wrapper which implements libpq's protocol.
///
/// NOTE: This object deliberately doesn't implement [`AsyncRead`]
/// or [`AsyncWrite`] to prevent subtle errors (e.g. trying
/// to pass random malformed bytes through the connection).
pub struct PqStream<S> {
    stream: S,
    read: Vec<u8>,
    write: WriteBuf,
}

impl<S> PqStream<S> {
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// Construct a new libpq protocol wrapper over a stream without the first startup message.
    #[cfg(test)]
    pub fn new_skip_handshake(stream: S) -> Self {
        Self {
            stream,
            read: Vec::new(),
            write: WriteBuf::new(),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> PqStream<S> {
    /// Construct a new libpq protocol wrapper and read the first startup message.
    ///
    /// This is not cancel safe.
    pub async fn parse_startup(mut stream: S) -> io::Result<(Self, FeStartupPacket)> {
        let startup = read_startup(&mut stream).await?;
        Ok((
            Self {
                stream,
                read: Vec::new(),
                write: WriteBuf::new(),
            },
            startup,
        ))
    }

    /// Tell the client that encryption is not supported.
    ///
    /// This is not cancel safe
    pub async fn reject_encryption(&mut self) -> io::Result<FeStartupPacket> {
        // N for No.
        self.write.encryption(b'N');
        self.flush().await?;
        read_startup(&mut self.stream).await
    }
}

impl<S: AsyncRead + Unpin> PqStream<S> {
    /// Read a raw postgres packet, which will respect the max length requested.
    /// This is not cancel safe.
    async fn read_raw_expect(&mut self, tag: u8, max: u32) -> io::Result<&mut [u8]> {
        let (actual_tag, msg) = read_message(&mut self.stream, &mut self.read, max).await?;
        if actual_tag != tag {
            return Err(io::Error::other(format!(
                "incorrect message tag, expected {:?}, got {:?}",
                tag as char, actual_tag as char,
            )));
        }
        Ok(msg)
    }

    /// Read a postgres password message, which will respect the max length requested.
    /// This is not cancel safe.
    pub async fn read_password_message(&mut self) -> io::Result<&mut [u8]> {
        // passwords are usually pretty short
        // and SASL SCRAM messages are no longer than 256 bytes in my testing
        // (a few hashes and random bytes, encoded into base64).
        const MAX_PASSWORD_LENGTH: u32 = 512;
        self.read_raw_expect(FE_PASSWORD_MESSAGE, MAX_PASSWORD_LENGTH)
            .await
    }
}

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

impl<S: AsyncWrite + Unpin> PqStream<S> {
    /// Tell the client that we are willing to accept SSL.
    /// This is not cancel safe
    pub async fn accept_tls(mut self) -> io::Result<S> {
        // S for SSL.
        self.write.encryption(b'S');
        self.flush().await?;
        Ok(self.stream)
    }

    /// Assert that we are using direct TLS.
    pub fn accept_direct_tls(self) -> S {
        self.stream
    }

    /// Write a raw message to the internal buffer.
    pub fn write_raw(&mut self, size_hint: usize, tag: u8, f: impl FnOnce(&mut Vec<u8>)) {
        self.write.write_raw(size_hint, tag, f);
    }

    /// Write the message into an internal buffer
    pub fn write_message(&mut self, message: BeMessage<'_>) {
        message.write_message(&mut self.write);
    }

    /// Flush the output buffer into the underlying stream.
    ///
    /// This is cancel safe.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.stream.write_all_buf(&mut self.write).await?;
        self.write.reset();

        self.stream.flush().await?;

        Ok(())
    }

    /// Flush the output buffer into the underlying stream.
    ///
    /// This is cancel safe.
    pub async fn flush_and_into_inner(mut self) -> io::Result<S> {
        self.flush().await?;
        Ok(self.stream)
    }

    /// Write the error message to the client, then re-throw it.
    ///
    /// Trait [`UserFacingError`] acts as an allowlist for error types.
    /// If `ctx` is provided and has testodrome_id set, error messages will be prefixed according to error kind.
    pub(crate) async fn throw_error<E>(
        &mut self,
        error: E,
        ctx: Option<&crate::context::RequestContext>,
    ) -> ReportedError
    where
        E: UserFacingError + Into<anyhow::Error>,
    {
        let error_kind = error.get_error_kind();
        let msg = error.to_string_client();

        if error_kind != ErrorKind::RateLimit && error_kind != ErrorKind::User {
            tracing::info!(
                kind = error_kind.to_metric_label(),
                msg,
                "forwarding error to user"
            );
        }

        let probe_msg;
        let mut msg = &*msg;
        if let Some(ctx) = ctx {
            if ctx.get_testodrome_id().is_some() {
                let tag = match error_kind {
                    ErrorKind::User => "client",
                    ErrorKind::ClientDisconnect => "client",
                    ErrorKind::RateLimit => "proxy",
                    ErrorKind::ServiceRateLimit => "proxy",
                    ErrorKind::Quota => "proxy",
                    ErrorKind::Service => "proxy",
                    ErrorKind::ControlPlane => "controlplane",
                    ErrorKind::Postgres => "other",
                    ErrorKind::Compute => "compute",
                };
                probe_msg = typed_json::json!({
                    "tag": tag,
                    "msg": msg,
                    "cold_start_info": ctx.cold_start_info(),
                })
                .to_string();
                msg = &probe_msg;
            }
        }

        // TODO: either preserve the error code from postgres, or assign error codes to proxy errors.
        self.write.write_error(msg, SQLSTATE_INTERNAL_ERROR);

        self.flush()
            .await
            .unwrap_or_else(|e| tracing::debug!("write_message failed: {e}"));

        ReportedError::new(error)
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
