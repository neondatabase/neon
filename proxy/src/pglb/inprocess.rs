#![allow(dead_code, reason = "TODO: work in progress")]

use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::{fmt, io};

use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::mpsc;

const STREAM_CHANNEL_SIZE: usize = 16;
const MAX_STREAM_BUFFER_SIZE: usize = 4096;

#[derive(Debug)]
pub struct Connection {
    stream_sender: mpsc::Sender<Stream>,
    stream_receiver: mpsc::Receiver<Stream>,
    stream_id_counter: Arc<AtomicUsize>,
}

impl Connection {
    pub fn new() -> (Connection, Connection) {
        let (sender_a, receiver_a) = mpsc::channel(STREAM_CHANNEL_SIZE);
        let (sender_b, receiver_b) = mpsc::channel(STREAM_CHANNEL_SIZE);

        let stream_id_counter = Arc::new(AtomicUsize::new(1));

        let conn_a = Connection {
            stream_sender: sender_a,
            stream_receiver: receiver_b,
            stream_id_counter: Arc::clone(&stream_id_counter),
        };
        let conn_b = Connection {
            stream_sender: sender_b,
            stream_receiver: receiver_a,
            stream_id_counter,
        };

        (conn_a, conn_b)
    }

    #[inline]
    fn next_stream_id(&self) -> StreamId {
        StreamId(self.stream_id_counter.fetch_add(1, Ordering::Relaxed))
    }

    #[tracing::instrument(skip_all, fields(stream_id = tracing::field::Empty, err))]
    pub async fn open_stream(&self) -> io::Result<Stream> {
        let (local, remote) = tokio::io::duplex(MAX_STREAM_BUFFER_SIZE);
        let stream_id = self.next_stream_id();
        tracing::Span::current().record("stream_id", stream_id.0);

        let local = Stream {
            inner: local,
            id: stream_id,
        };
        let remote = Stream {
            inner: remote,
            id: stream_id,
        };

        self.stream_sender
            .send(remote)
            .await
            .map_err(io::Error::other)?;

        Ok(local)
    }

    #[tracing::instrument(skip_all, fields(stream_id = tracing::field::Empty, err))]
    pub async fn accept_stream(&mut self) -> io::Result<Option<Stream>> {
        Ok(self.stream_receiver.recv().await.inspect(|stream| {
            tracing::Span::current().record("stream_id", stream.id.0);
        }))
    }
}

#[derive(Copy, Clone, Debug)]
pub struct StreamId(usize);

impl fmt::Display for StreamId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// TODO: Proper closing. Currently Streams can outlive their Connections.
// Carry WeakSender and check strong_count?
#[derive(Debug)]
pub struct Stream {
    inner: DuplexStream,
    id: StreamId,
}

impl Stream {
    #[inline]
    pub fn id(&self) -> StreamId {
        self.id
    }
}

impl AsyncRead for Stream {
    #[tracing::instrument(level = "debug", skip_all, fields(stream_id = %self.id))]
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        pin!(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Stream {
    #[tracing::instrument(level = "debug", skip_all, fields(stream_id = %self.id))]
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        pin!(&mut self.inner).poll_write(cx, buf)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(stream_id = %self.id))]
    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        pin!(&mut self.inner).poll_flush(cx)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(stream_id = %self.id))]
    #[inline]
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        pin!(&mut self.inner).poll_shutdown(cx)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(stream_id = %self.id))]
    #[inline]
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        pin!(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn test_simple_roundtrip() {
        let (client, mut server) = Connection::new();

        let server_task = tokio::spawn(async move {
            while let Some(mut stream) = server.accept_stream().await.unwrap() {
                tokio::spawn(async move {
                    let mut buf = [0; 64];
                    loop {
                        match stream.read(&mut buf).await.unwrap() {
                            0 => break,
                            n => stream.write(&buf[..n]).await.unwrap(),
                        };
                    }
                });
            }
        });

        let mut stream = client.open_stream().await.unwrap();
        stream.write_all(b"hello!").await.unwrap();
        let mut buf = [0; 64];
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(n, 6);
        assert_eq!(&buf[..n], b"hello!");

        drop(stream);
        drop(client);
        server_task.await.unwrap();
    }
}
