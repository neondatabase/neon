use std::future::poll_fn;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tracing::info;

use crate::metrics::Direction;

enum TransferState {
    Running(CopyBuffer),
    ShuttingDown(Direction),
    Done,
}

#[derive(Debug)]
pub enum ErrorSource {
    Client(io::Error),
    Compute(io::Error),
}

impl ErrorSource {
    fn read(dir: Direction, err: io::Error) -> Self {
        match dir {
            Direction::ComputeToClient => ErrorSource::Compute(err),
            Direction::ClientToCompute => ErrorSource::Client(err),
        }
    }

    fn write(dir: Direction, err: io::Error) -> Self {
        match dir {
            Direction::ComputeToClient => ErrorSource::Client(err),
            Direction::ClientToCompute => ErrorSource::Compute(err),
        }
    }
}

fn transfer_one_direction<A, B>(
    cx: &mut Context<'_>,
    state: &mut TransferState,
    f: &mut impl for<'a> FnMut(Direction, &'a [u8]),
    r: &mut A,
    w: &mut B,
) -> Poll<Result<(), ErrorSource>>
where
    A: AsyncRead + AsyncWrite + Unpin + ?Sized,
    B: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut r = Pin::new(r);
    let mut w = Pin::new(w);
    loop {
        match state {
            TransferState::Running(buf) => {
                ready!(buf.poll_copy(cx, f, r.as_mut(), w.as_mut()))?;
                *state = TransferState::ShuttingDown(buf.dir);
            }
            TransferState::ShuttingDown(dir) => {
                ready!(w.as_mut().poll_shutdown(cx)).map_err(|e| ErrorSource::write(*dir, e))?;
                *state = TransferState::Done;
            }
            TransferState::Done => return Poll::Ready(Ok(())),
        }
    }
}

pub async fn copy_bidirectional_client_compute<Client, Compute>(
    client: &mut Client,
    compute: &mut Compute,
    mut f: impl for<'a> FnMut(Direction, &'a [u8]),
) -> Result<(), ErrorSource>
where
    Client: AsyncRead + AsyncWrite + Unpin + ?Sized,
    Compute: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut client_to_compute = TransferState::Running(CopyBuffer::new(Direction::ClientToCompute));
    let mut compute_to_client = TransferState::Running(CopyBuffer::new(Direction::ComputeToClient));

    poll_fn(|cx| {
        let mut client_to_compute_result =
            transfer_one_direction(cx, &mut client_to_compute, &mut f, client, compute)?;
        let mut compute_to_client_result =
            transfer_one_direction(cx, &mut compute_to_client, &mut f, compute, client)?;

        // TODO: 1 info log, with a enum label for close direction.

        // Early termination checks from compute to client.
        if let TransferState::Done = compute_to_client {
            if let TransferState::Running(buf) = &client_to_compute {
                info!("Compute is done, terminate client");
                // Initiate shutdown
                client_to_compute = TransferState::ShuttingDown(buf.dir);
                client_to_compute_result =
                    transfer_one_direction(cx, &mut client_to_compute, &mut f, client, compute)?;
            }
        }

        // Early termination checks from client to compute.
        if let TransferState::Done = client_to_compute {
            if let TransferState::Running(buf) = &compute_to_client {
                info!("Client is done, terminate compute");
                // Initiate shutdown
                compute_to_client = TransferState::ShuttingDown(buf.dir);
                compute_to_client_result =
                    transfer_one_direction(cx, &mut compute_to_client, &mut f, compute, client)?;
            }
        }

        ready!(client_to_compute_result);
        ready!(compute_to_client_result);

        Poll::Ready(Ok(()))
    })
    .await
}

pub(super) struct CopyBuffer {
    dir: Direction,
    read_done: bool,
    need_flush: bool,
    pos: usize,
    cap: usize,
    buf: Box<[u8]>,
}
const DEFAULT_BUF_SIZE: usize = 1024;

impl CopyBuffer {
    pub(super) fn new(dir: Direction) -> Self {
        Self {
            dir,
            read_done: false,
            need_flush: false,
            pos: 0,
            cap: 0,
            buf: vec![0; DEFAULT_BUF_SIZE].into_boxed_slice(),
        }
    }

    fn poll_fill_buf<R>(
        &mut self,
        cx: &mut Context<'_>,
        f: &mut impl for<'a> FnMut(Direction, &'a [u8]),
        reader: Pin<&mut R>,
    ) -> Poll<Result<(), ErrorSource>>
    where
        R: AsyncRead + ?Sized,
    {
        let me = &mut *self;
        let mut buf = ReadBuf::new(&mut me.buf);
        buf.set_filled(me.cap);

        let res = reader.poll_read(cx, &mut buf);
        f(me.dir, &buf.filled()[me.cap..]);

        if let Poll::Ready(Ok(())) = res {
            let filled_len = buf.filled().len();
            me.read_done = me.cap == filled_len;
            me.cap = filled_len;
        }
        res.map_err(|e| ErrorSource::read(me.dir, e))
    }

    fn poll_write_buf<R, W>(
        &mut self,
        cx: &mut Context<'_>,
        f: &mut impl for<'a> FnMut(Direction, &'a [u8]),
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
    ) -> Poll<Result<(), ErrorSource>>
    where
        R: AsyncRead + ?Sized,
        W: AsyncWrite + ?Sized,
    {
        let me = &mut *self;
        match writer.as_mut().poll_write(cx, &me.buf[me.pos..me.cap]) {
            Poll::Pending => {
                // Top up the buffer towards full if we can read a bit more
                // data - this should improve the chances of a large write
                if !me.read_done && me.cap < me.buf.len() {
                    ready!(me.poll_fill_buf(cx, f, reader.as_mut()))?;
                }
                Poll::Pending
            }
            Poll::Ready(Ok(0)) => {
                let err = io::Error::new(io::ErrorKind::WriteZero, "write zero byte into writer");
                Poll::Ready(Err(ErrorSource::write(self.dir, err)))
            }
            Poll::Ready(Ok(i)) => {
                self.pos += i;
                self.need_flush = true;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(ErrorSource::write(me.dir, e))),
        }
    }

    pub(super) fn poll_copy<R, W>(
        &mut self,
        cx: &mut Context<'_>,
        f: &mut impl for<'a> FnMut(Direction, &'a [u8]),
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
    ) -> Poll<Result<(), ErrorSource>>
    where
        R: AsyncRead + ?Sized,
        W: AsyncWrite + ?Sized,
    {
        loop {
            // If there is some space left in our buffer, then we try to read some
            // data to continue, thus maximizing the chances of a large write.
            if self.cap < self.buf.len() && !self.read_done {
                match self.poll_fill_buf(cx, f, reader.as_mut()) {
                    Poll::Ready(Ok(())) => (),
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => {
                        // Ignore pending reads when our buffer is not empty, because
                        // we can try to write data immediately.
                        if self.pos == self.cap {
                            // Try flushing when the reader has no progress to avoid deadlock
                            // when the reader depends on buffered writer.
                            if self.need_flush {
                                ready!(writer.as_mut().poll_flush(cx))
                                    .map_err(|e| ErrorSource::write(self.dir, e))?;
                                self.need_flush = false;
                            }

                            return Poll::Pending;
                        }
                    }
                }
            }

            // If our buffer has some data, let's write it out!
            while self.pos < self.cap {
                ready!(self.poll_write_buf(cx, f, reader.as_mut(), writer.as_mut()))?;
            }

            // If pos larger than cap, this loop will never stop.
            // In particular, user's wrong poll_write implementation returning
            // incorrect written length may lead to thread blocking.
            debug_assert!(
                self.pos <= self.cap,
                "writer returned length larger than input slice"
            );

            // All data has been written, the buffer can be considered empty again
            self.pos = 0;
            self.cap = 0;

            // If we've written all the data and we've seen EOF, flush out the
            // data and finish the transfer.
            if self.read_done {
                return writer
                    .as_mut()
                    .poll_flush(cx)
                    .map_err(|e| ErrorSource::write(self.dir, e));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn test_client_to_compute() {
        let (mut client_client, mut client_proxy) = tokio::io::duplex(8); // Create a mock duplex stream
        let (mut compute_proxy, mut compute_client) = tokio::io::duplex(32); // Create a mock duplex stream

        // Simulate 'a' finishing while there's still data for 'b'
        client_client.write_all(b"hello").await.unwrap();
        client_client.shutdown().await.unwrap();
        compute_client.write_all(b"Neon").await.unwrap();
        compute_client.shutdown().await.unwrap();

        copy_bidirectional_client_compute(&mut client_proxy, &mut compute_proxy, |_, _| {})
            .await
            .unwrap();

        drop(client_proxy);
        drop(compute_proxy);

        // Assert correct transferred amounts
        let mut client_recv = vec![];
        client_client.read_buf(&mut client_recv).await.unwrap();

        let mut compute_recv = vec![];
        compute_client.read_buf(&mut compute_recv).await.unwrap();

        assert_eq!(compute_recv, b"hello");
        assert_eq!(client_recv, b"Neon");
    }

    #[tokio::test]
    async fn test_compute_to_client() {
        let (mut client_client, mut client_proxy) = tokio::io::duplex(32); // Create a mock duplex stream
        let (mut compute_proxy, mut compute_client) = tokio::io::duplex(8); // Create a mock duplex stream

        // Simulate 'a' finishing while there's still data for 'b'
        compute_client.write_all(b"hello").await.unwrap();
        compute_client.shutdown().await.unwrap();
        client_client
            .write_all(b"Neon Serverless Postgres")
            .await
            .unwrap();

        copy_bidirectional_client_compute(&mut client_proxy, &mut compute_proxy, |_, _| {})
            .await
            .unwrap();

        drop(client_proxy);
        drop(compute_proxy);

        // Assert correct transferred amounts
        let mut client_recv = vec![];
        client_client.read_buf(&mut client_recv).await.unwrap();

        let mut compute_recv = vec![];
        compute_client.read_buf(&mut compute_recv).await.unwrap();

        assert_eq!(client_recv, b"hello");
        assert_eq!(compute_recv, b"Neon Ser");
    }
}
