use anyhow::Context;
use bytes::BytesMut;
use pin_project_lite::pin_project;
use rustls::ServerConfig;
use std::pin::Pin;
use std::sync::Arc;
use std::{io, task};
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

    /// Get a reference to the underlying stream.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }
}

impl<S: AsyncRead + Unpin> PqStream<S> {
    /// Receive [`FeStartupPacket`], which is a first packet sent by a client.
    pub async fn read_startup_packet(&mut self) -> anyhow::Result<FeStartupPacket> {
        match FeStartupPacket::read_fut(&mut self.stream).await? {
            Some(FeMessage::StartupPacket(packet)) => Ok(packet),
            None => anyhow::bail!("connection is lost"),
            other => anyhow::bail!("bad message type: {:?}", other),
        }
    }

    pub async fn read_message(&mut self) -> anyhow::Result<FeMessage> {
        FeMessage::read_fut(&mut self.stream)
            .await?
            .context("connection is lost")
    }
}

impl<S: AsyncWrite + Unpin> PqStream<S> {
    /// Write the message into an internal buffer, but don't flush the underlying stream.
    pub fn write_message_noflush<'a>(&mut self, message: &BeMessage<'a>) -> io::Result<&mut Self> {
        BeMessage::write(&mut self.buffer, message)?;
        Ok(self)
    }

    /// Write the message into an internal buffer and flush it.
    pub async fn write_message<'a>(&mut self, message: &BeMessage<'a>) -> io::Result<&mut Self> {
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

impl<S: AsyncRead + AsyncWrite + Unpin> Stream<S> {
    /// If possible, upgrade raw stream into a secure TLS-based stream.
    pub async fn upgrade(self, cfg: Arc<ServerConfig>) -> anyhow::Result<Self> {
        match self {
            Stream::Raw { raw } => {
                let tls = Box::new(tokio_rustls::TlsAcceptor::from(cfg).accept(raw).await?);
                Ok(Stream::Tls { tls })
            }
            Stream::Tls { .. } => anyhow::bail!("can't upgrade TLS stream"),
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
