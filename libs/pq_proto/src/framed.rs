//! Provides `Framed` -- writing/flushing and reading Postgres messages to/from
//! the async stream.
use bytes::{Buf, BytesMut};
use std::{
    future::Future,
    io::{self, ErrorKind},
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};

use crate::{BeMessage, ConnectionError, FeMessage, FeStartupPacket};

const INITIAL_CAPACITY: usize = 8 * 1024;

/// Wraps async io `stream`, providing messages to write/flush + read Postgres
/// messages.
pub struct Framed<S> {
    stream: BufReader<S>,
    write_buf: BytesMut,
}

impl<S: AsyncRead + Unpin> Framed<S> {
    pub fn new(stream: S) -> Self {
        Self {
            stream: BufReader::new(stream),
            write_buf: BytesMut::with_capacity(INITIAL_CAPACITY),
        }
    }

    /// Get a shared reference to the underlying stream.
    pub fn get_ref(&self) -> &S {
        self.stream.get_ref()
    }

    /// Extract the underlying stream.
    pub fn into_inner(self) -> S {
        self.stream.into_inner()
    }

    /// Return new Framed with stream type transformed by async f, for TLS
    /// upgrade.
    pub async fn map_stream<S2: AsyncRead, E, F, Fut>(self, f: F) -> Result<Framed<S2>, E>
    where
        F: FnOnce(S) -> Fut,
        Fut: Future<Output = Result<S2, E>>,
    {
        let stream = f(self.stream.into_inner()).await?;
        Ok(Framed {
            stream: BufReader::new(stream),
            write_buf: self.write_buf,
        })
    }
}

impl<S: AsyncRead + Unpin> Framed<S> {
    pub async fn read_startup_message(
        &mut self,
    ) -> Result<Option<FeStartupPacket>, ConnectionError> {
        let msg = FeStartupPacket::read(&mut self.stream).await?;

        match msg {
            Some(FeMessage::StartupPacket(packet)) => Ok(Some(packet)),
            None => Ok(None),
            _ => panic!("unreachable state"),
        }
    }

    pub async fn read_message(&mut self) -> Result<Option<FeMessage>, ConnectionError> {
        FeMessage::read(&mut self.stream).await
    }
}

impl<S: AsyncWrite + AsyncRead + Unpin> Framed<S> {
    /// Write next message to the output buffer; doesn't flush.
    pub fn write_message(&mut self, msg: &BeMessage<'_>) -> Result<(), ConnectionError> {
        BeMessage::write(&mut self.write_buf, msg).map_err(|e| e.into())
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
    /// https://github.com/tokio-rs/tls/issues/40
    pub fn split(self) -> (FramedReader<S>, FramedWriter<S>) {
        let (read_half, write_half) = tokio::io::split(self.stream);
        let reader = FramedReader { stream: read_half };
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
            write_buf: writer.write_buf,
        }
    }
}

/// Read-only version of `Framed`.
pub struct FramedReader<S> {
    stream: ReadHalf<BufReader<S>>,
}

impl<S: AsyncRead + Unpin> FramedReader<S> {
    pub async fn read_message(&mut self) -> Result<Option<FeMessage>, ConnectionError> {
        FeMessage::read(&mut self.stream).await
    }
}

/// Write-only version of `Framed`.
pub struct FramedWriter<S> {
    stream: WriteHalf<BufReader<S>>,
    write_buf: BytesMut,
}

impl<S: AsyncWrite + AsyncRead + Unpin> FramedWriter<S> {
    /// Write next message to the output buffer; doesn't flush.
    pub fn write_message_noflush(&mut self, msg: &BeMessage<'_>) -> Result<(), ConnectionError> {
        BeMessage::write(&mut self.write_buf, msg).map_err(|e| e.into())
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

async fn flush<S: AsyncWrite + Unpin>(
    stream: &mut S,
    write_buf: &mut BytesMut,
) -> Result<(), io::Error> {
    while write_buf.has_remaining() {
        let bytes_written = stream.write(write_buf.chunk()).await?;
        if bytes_written == 0 {
            return Err(io::Error::new(
                ErrorKind::WriteZero,
                "failed to write message",
            ));
        }
        // The advanced part will be garbage collected, likely during shifting
        // data left on next attempt to write to buffer when free space is not
        // enough.
        write_buf.advance(bytes_written);
    }
    write_buf.clear();
    stream.flush().await
}

async fn shutdown<S: AsyncWrite + Unpin>(
    stream: &mut S,
    write_buf: &mut BytesMut,
) -> Result<(), io::Error> {
    flush(stream, write_buf).await?;
    stream.shutdown().await
}
