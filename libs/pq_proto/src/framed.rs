//! Provides `Framed` -- writing/flushing and reading Postgres messages to/from
//! the async stream based on (and buffered with) BytesMut. All functions are
//! cancellation safe.
//!
//! It is similar to what tokio_util::codec::Framed with appropriate codec
//! provides, but `FramedReader` and `FramedWriter` read/write parts can be used
//! separately without using split from futures::stream::StreamExt (which
//! allocates a [Box] in polling internally). tokio::io::split is used for splitting
//! instead. Plus we customize error messages more than a single type for all io
//! calls.
//!
//! [Box]: https://docs.rs/futures-util/0.3.26/src/futures_util/lock/bilock.rs.html#107
use bytes::{Buf, BytesMut};
use std::{
    future::Future,
    io::{self, ErrorKind},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};

use crate::{BeMessage, FeMessage, FeStartupPacket, ProtocolError};

const INITIAL_CAPACITY: usize = 8 * 1024;

/// Error on postgres connection: either IO (physical transport error) or
/// protocol violation.
#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
}

impl ConnectionError {
    /// Proxy stream.rs uses only io::Error; provide it.
    pub fn into_io_error(self) -> io::Error {
        match self {
            ConnectionError::Io(io) => io,
            ConnectionError::Protocol(pe) => io::Error::new(io::ErrorKind::Other, pe.to_string()),
        }
    }
}

/// Wraps async io `stream`, providing messages to write/flush + read Postgres
/// messages.
pub struct Framed<S> {
    pub stream: S,
    pub read_buf: BytesMut,
    pub write_buf: BytesMut,
}

impl<S> Framed<S> {
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            read_buf: BytesMut::with_capacity(INITIAL_CAPACITY),
            write_buf: BytesMut::with_capacity(INITIAL_CAPACITY),
        }
    }

    /// Get a shared reference to the underlying stream.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// Deconstruct into the underlying stream and read buffer.
    pub fn into_inner(self) -> (S, BytesMut) {
        (self.stream, self.read_buf)
    }

    /// Return new Framed with stream type transformed by async f, for TLS
    /// upgrade.
    pub async fn map_stream<S2, E, F, Fut>(self, f: F) -> Result<Framed<S2>, E>
    where
        F: FnOnce(S) -> Fut,
        Fut: Future<Output = Result<S2, E>>,
    {
        let stream = f(self.stream).await?;
        Ok(Framed {
            stream,
            read_buf: self.read_buf,
            write_buf: self.write_buf,
        })
    }
}

impl<S: AsyncRead + Unpin> Framed<S> {
    pub async fn read_startup_message(
        &mut self,
    ) -> Result<Option<FeStartupPacket>, ConnectionError> {
        read_message(&mut self.stream, &mut self.read_buf, FeStartupPacket::parse).await
    }

    pub async fn read_message(&mut self) -> Result<Option<FeMessage>, ConnectionError> {
        read_message(&mut self.stream, &mut self.read_buf, FeMessage::parse).await
    }
}

impl<S: AsyncWrite + Unpin> Framed<S> {
    /// Write next message to the output buffer; doesn't flush.
    pub fn write_message(&mut self, msg: &BeMessage<'_>) -> Result<(), ProtocolError> {
        BeMessage::write(&mut self.write_buf, msg)
    }

    /// Flush out the buffer. This function is cancellation safe: it can be
    /// interrupted and flushing will be continued in the next call.
    pub async fn flush(&mut self) -> Result<(), io::Error> {
        flush(&mut self.stream, &mut self.write_buf).await
    }

    /// Flush out the buffer and shutdown the stream.
    pub async fn shutdown(&mut self) -> Result<(), io::Error> {
        shutdown(&mut self.stream, &mut self.write_buf).await
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Framed<S> {
    /// Split into owned read and write parts. Beware of potential issues with
    /// using halves in different tasks on TLS stream:
    /// <https://github.com/tokio-rs/tls/issues/40>
    pub fn split(self) -> (FramedReader<S>, FramedWriter<S>) {
        let (read_half, write_half) = tokio::io::split(self.stream);
        let reader = FramedReader {
            stream: read_half,
            read_buf: self.read_buf,
        };
        let writer = FramedWriter {
            stream: write_half,
            write_buf: self.write_buf,
        };
        (reader, writer)
    }

    /// Join read and write parts back.
    pub fn unsplit(reader: FramedReader<S>, writer: FramedWriter<S>) -> Self {
        Self {
            stream: reader.stream.unsplit(writer.stream),
            read_buf: reader.read_buf,
            write_buf: writer.write_buf,
        }
    }
}

/// Read-only version of `Framed`.
pub struct FramedReader<S> {
    stream: ReadHalf<S>,
    read_buf: BytesMut,
}

impl<S: AsyncRead + Unpin> FramedReader<S> {
    pub async fn read_message(&mut self) -> Result<Option<FeMessage>, ConnectionError> {
        read_message(&mut self.stream, &mut self.read_buf, FeMessage::parse).await
    }
}

/// Write-only version of `Framed`.
pub struct FramedWriter<S> {
    stream: WriteHalf<S>,
    write_buf: BytesMut,
}

impl<S: AsyncWrite + Unpin> FramedWriter<S> {
    /// Write next message to the output buffer; doesn't flush.
    pub fn write_message_noflush(&mut self, msg: &BeMessage<'_>) -> Result<(), ProtocolError> {
        BeMessage::write(&mut self.write_buf, msg)
    }

    /// Flush out the buffer. This function is cancellation safe: it can be
    /// interrupted and flushing will be continued in the next call.
    pub async fn flush(&mut self) -> Result<(), io::Error> {
        flush(&mut self.stream, &mut self.write_buf).await
    }

    /// Flush out the buffer and shutdown the stream.
    pub async fn shutdown(&mut self) -> Result<(), io::Error> {
        shutdown(&mut self.stream, &mut self.write_buf).await
    }
}

/// Read next message from the stream. Returns Ok(None), if EOF happened and we
/// don't have remaining data in the buffer. This function is cancellation safe:
/// you can drop future which is not yet complete and finalize reading message
/// with the next call.
///
/// Parametrized to allow reading startup or usual message, having different
/// format.
async fn read_message<S: AsyncRead + Unpin, M, P>(
    stream: &mut S,
    read_buf: &mut BytesMut,
    parse: P,
) -> Result<Option<M>, ConnectionError>
where
    P: Fn(&mut BytesMut) -> Result<Option<M>, ProtocolError>,
{
    loop {
        if let Some(msg) = parse(read_buf)? {
            return Ok(Some(msg));
        }
        // If we can't build a frame yet, try to read more data and try again.
        // Make sure we've got room for at least one byte to read to ensure
        // that we don't get a spurious 0 that looks like EOF.
        read_buf.reserve(1);
        if stream.read_buf(read_buf).await? == 0 {
            if read_buf.has_remaining() {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "EOF with unprocessed data in the buffer",
                )
                .into());
            } else {
                return Ok(None); // clean EOF
            }
        }
    }
}

/// Cancellation safe as long as the AsyncWrite is cancellation safe.
async fn flush<S: AsyncWrite + Unpin>(
    stream: &mut S,
    write_buf: &mut BytesMut,
) -> Result<(), io::Error> {
    while write_buf.has_remaining() {
        let bytes_written = stream.write_buf(write_buf).await?;
        if bytes_written == 0 {
            return Err(io::Error::new(
                ErrorKind::WriteZero,
                "failed to write message",
            ));
        }
    }
    stream.flush().await
}

/// Cancellation safe as long as the AsyncWrite is cancellation safe.
async fn shutdown<S: AsyncWrite + Unpin>(
    stream: &mut S,
    write_buf: &mut BytesMut,
) -> Result<(), io::Error> {
    flush(stream, write_buf).await?;
    stream.shutdown().await
}
