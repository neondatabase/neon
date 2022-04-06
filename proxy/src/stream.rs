use crate::error::UserFacingError;
use anyhow::bail;
use bytes::BytesMut;
use pin_project_lite::pin_project;
use rustls::ServerConfig;
use std::pin::Pin;
use std::sync::Arc;
use std::{io, task};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio_rustls::server::TlsStream;
use zenith_utils::pq_proto::{BeMessage, FeMessage, FeStartupPacket};

pin_project! {
    /// Stream wrapper which implements libpq's protocol.
    /// NOTE: This object deliberately doesn't implement [`AsyncRead`]
    /// or [`AsyncWrite`] to prevent subtle errors (e.g. trying
    /// to pass random malformed bytes through the connection).
    pub struct PqStream<S> {
        #[pin]
        stream: S,
        buffer: BytesMut,
    }
}

impl<S> PqStream<S> {
    /// Construct a new libpq protocol wrapper.
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            buffer: Default::default(),
        }
    }

    /// Extract the underlying stream.
    pub fn into_inner(self) -> S {
        self.stream
    }

    /// Get a shared reference to the underlying stream.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }
}

fn err_connection() -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionAborted, "connection is lost")
}

// TODO: change error type of `FeMessage::read_fut`
fn from_anyhow(e: anyhow::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

impl<S: AsyncRead + Unpin> PqStream<S> {
    /// Receive [`FeStartupPacket`], which is a first packet sent by a client.
    pub async fn read_startup_packet(&mut self) -> io::Result<FeStartupPacket> {
        // TODO: `FeStartupPacket::read_fut` should return `FeStartupPacket`
        let msg = FeStartupPacket::read_fut(&mut self.stream)
            .await
            .map_err(from_anyhow)?
            .ok_or_else(err_connection)?;

        match msg {
            FeMessage::StartupPacket(packet) => Ok(packet),
            _ => panic!("unreachable state"),
        }
    }

    pub async fn read_password_message(&mut self) -> io::Result<bytes::Bytes> {
        match self.read_message().await? {
            FeMessage::PasswordMessage(msg) => Ok(msg),
            bad => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected message type: {:?}", bad),
            )),
        }
    }

    async fn read_message(&mut self) -> io::Result<FeMessage> {
        FeMessage::read_fut(&mut self.stream)
            .await
            .map_err(from_anyhow)?
            .ok_or_else(err_connection)
    }
}

impl<S: AsyncWrite + Unpin> PqStream<S> {
    /// Write the message into an internal buffer, but don't flush the underlying stream.
    pub fn write_message_noflush(&mut self, message: &BeMessage<'_>) -> io::Result<&mut Self> {
        BeMessage::write(&mut self.buffer, message)?;
        Ok(self)
    }

    /// Write the message into an internal buffer and flush it.
    pub async fn write_message(&mut self, message: &BeMessage<'_>) -> io::Result<&mut Self> {
        self.write_message_noflush(message)?;
        self.flush().await?;
        Ok(self)
    }

    /// Flush the output buffer into the underlying stream.
    pub async fn flush(&mut self) -> io::Result<&mut Self> {
        self.stream.write_all(&self.buffer).await?;
        self.buffer.clear();
        self.stream.flush().await?;
        Ok(self)
    }

    /// Write the error message using [`Self::write_message`], then re-throw it.
    /// Allowing string literals is safe under the assumption they might not contain any runtime info.
    pub async fn throw_error_str<T>(&mut self, error: &'static str) -> anyhow::Result<T> {
        // This method exists due to `&str` not implementing `Into<anyhow::Error>`
        self.write_message(&BeMessage::ErrorResponse(error)).await?;
        bail!(error)
    }

    /// Write the error message using [`Self::write_message`], then re-throw it.
    /// Trait [`UserFacingError`] acts as an allowlist for error types.
    pub async fn throw_error<T, E>(&mut self, error: E) -> anyhow::Result<T>
    where
        E: UserFacingError + Into<anyhow::Error>,
    {
        let msg = error.to_string_client();
        self.write_message(&BeMessage::ErrorResponse(&msg)).await?;
        bail!(error)
    }
}

pin_project! {
    /// Wrapper for upgrading raw streams into secure streams.
    /// NOTE: it should be possible to decompose this object as necessary.
    #[project = StreamProj]
    pub enum Stream<S> {
        /// We always begin with a raw stream,
        /// which may then be upgraded into a secure stream.
        Raw { #[pin] raw: S },
        /// We box [`TlsStream`] since it can be quite large.
        Tls { #[pin] tls: Box<TlsStream<S>> },
    }
}

impl<S> Stream<S> {
    /// Construct a new instance from a raw stream.
    pub fn from_raw(raw: S) -> Self {
        Self::Raw { raw }
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
    pub async fn upgrade(self, cfg: Arc<ServerConfig>) -> Result<Self, StreamUpgradeError> {
        match self {
            Stream::Raw { raw } => {
                let tls = Box::new(tokio_rustls::TlsAcceptor::from(cfg).accept(raw).await?);
                Ok(Stream::Tls { tls })
            }
            Stream::Tls { .. } => Err(StreamUpgradeError::AlreadyTls),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for Stream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> task::Poll<io::Result<()>> {
        use StreamProj::*;
        match self.project() {
            Raw { raw } => raw.poll_read(context, buf),
            Tls { tls } => tls.poll_read(context, buf),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for Stream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
        buf: &[u8],
    ) -> task::Poll<io::Result<usize>> {
        use StreamProj::*;
        match self.project() {
            Raw { raw } => raw.poll_write(context, buf),
            Tls { tls } => tls.poll_write(context, buf),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
    ) -> task::Poll<io::Result<()>> {
        use StreamProj::*;
        match self.project() {
            Raw { raw } => raw.poll_flush(context),
            Tls { tls } => tls.poll_flush(context),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
    ) -> task::Poll<io::Result<()>> {
        use StreamProj::*;
        match self.project() {
            Raw { raw } => raw.poll_shutdown(context),
            Tls { tls } => tls.poll_shutdown(context),
        }
    }
}

pin_project! {
    /// This stream tracks all writes and calls user provided
    /// callback when the underlying stream is flushed.
    pub struct MetricsStream<S, W> {
        #[pin]
        stream: S,
        write_count: usize,
        inc_write_count: W,
    }
}

impl<S, W> MetricsStream<S, W> {
    pub fn new(stream: S, inc_write_count: W) -> Self {
        Self {
            stream,
            write_count: 0,
            inc_write_count,
        }
    }
}

impl<S: AsyncRead + Unpin, W> AsyncRead for MetricsStream<S, W> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> task::Poll<io::Result<()>> {
        self.project().stream.poll_read(context, buf)
    }
}

impl<S: AsyncWrite + Unpin, W: FnMut(usize)> AsyncWrite for MetricsStream<S, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
        buf: &[u8],
    ) -> task::Poll<io::Result<usize>> {
        let this = self.project();
        this.stream.poll_write(context, buf).map_ok(|cnt| {
            // Increment the write count.
            *this.write_count += cnt;
            cnt
        })
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
    ) -> task::Poll<io::Result<()>> {
        let this = self.project();
        this.stream.poll_flush(context).map_ok(|()| {
            // Call the user provided callback and reset the write count.
            (this.inc_write_count)(*this.write_count);
            *this.write_count = 0;
        })
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
    ) -> task::Poll<io::Result<()>> {
        self.project().stream.poll_shutdown(context)
    }
}
