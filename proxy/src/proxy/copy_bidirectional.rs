use std::future::poll_fn;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tracing::info;

use crate::metrics::Direction;

const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Mark a value as being unlikely.
#[cold]
#[inline(always)]
fn cold<I>(i: I) -> I {
    i
}

#[derive(Debug)]
pub enum ErrorSource {
    Client(io::Error),
    Compute(io::Error),
    Timeout(tokio::time::error::Elapsed),
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

pub async fn copy_bidirectional_client_compute<Client, Compute>(
    client: &mut Client,
    compute: &mut Compute,
    mut f: impl for<'a> FnMut(Direction, &'a [u8]),
) -> Result<(), ErrorSource>
where
    Client: AsyncRead + AsyncWrite + Unpin + ?Sized,
    Compute: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let f = &mut f;
    let mut client_to_compute = CopyBuffer::new(Direction::ClientToCompute);
    let mut compute_to_client = CopyBuffer::new(Direction::ComputeToClient);

    let mut client = Pin::new(client);
    let mut compute = Pin::new(compute);

    // Initial copy hot path
    let close_dir = poll_fn(|cx| -> Poll<Result<_, ErrorSource>> {
        let copy1 = client_to_compute.poll_copy(cx, f, client.as_mut(), compute.as_mut())?;
        let copy2 = compute_to_client.poll_copy(cx, f, compute.as_mut(), client.as_mut())?;

        match (copy1, copy2) {
            (Poll::Pending, Poll::Pending) => Poll::Pending,
            (Poll::Ready(()), _) => Poll::Ready(Ok(client_to_compute.dir)),
            (_, Poll::Ready(())) => Poll::Ready(Ok(compute_to_client.dir)),
        }
    })
    .await?;

    // initiate shutdown.
    match close_dir {
        Direction::ClientToCompute => {
            info!("Client is done, terminate compute");

            // we will never write anymore data to the client.
            compute_to_client.filled = 0..0;

            // make sure to shutdown the client conn.
            compute_to_client.need_flush = true;
        }
        Direction::ComputeToClient => {
            info!("Compute is done, terminate client");

            // we will never write anymore data to the compute.
            client_to_compute.filled = 0..0;

            // make sure to shutdown the compute conn.
            client_to_compute.need_flush = true;
        }
    }

    // Finish sending the rest of the data to client/compute before shutting it down.
    //
    // Edge case:
    // * peer has filled the TCP buffers and is blocking on a `write()`,
    // * proxy has filled the TCP buffers and is waiting on a `write()`.
    // Since no side is reading from the buffers, no progress will be made.
    let shutdown = poll_fn(|cx| {
        let res1 = client_to_compute.poll_empty(cx, compute.as_mut())?;
        let res2 = compute_to_client.poll_empty(cx, client.as_mut())?;

        if res1.is_ready() && res2.is_ready() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    });

    // We assume most peers will have enough buffer space so this issue doesn't arise, but we apply
    // a timeout just in case.
    //
    // We could also update `poll_empty` to try and read the data, but I think this is not an edge case
    // worth overcomplicating.
    let res = tokio::time::timeout(DISCONNECT_TIMEOUT, shutdown).await;

    match res {
        Ok(res) => res,
        Err(timeout) => Err(ErrorSource::Timeout(timeout)),
    }
}

const DEFAULT_BUF_SIZE: usize = 1024;
pub(super) struct CopyBuffer {
    dir: Direction,
    need_flush: bool,
    filled: Range<usize>,
    buf: [u8; DEFAULT_BUF_SIZE],
}

impl CopyBuffer {
    pub(super) const fn new(dir: Direction) -> Self {
        Self {
            dir,
            need_flush: false,
            filled: 0..0,
            buf: [0; DEFAULT_BUF_SIZE],
        }
    }

    /// Returns Ready(Ok(())) when no more writes could progress, and the buffer has space to read.
    #[inline(always)]
    fn poll_write_loop<W>(
        &mut self,
        cx: &mut Context<'_>,
        mut writer: Pin<&mut W>,
    ) -> Poll<Result<(), ErrorSource>>
    where
        W: AsyncWrite + ?Sized,
    {
        debug_assert!(!self.filled.is_empty());

        loop {
            let filled_buf = &self.buf[self.filled.clone()];
            match writer.as_mut().poll_write(cx, filled_buf) {
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Err(ErrorSource::write(self.dir, cold(err))));
                }
                Poll::Ready(Ok(0)) => {
                    let err =
                        io::Error::new(io::ErrorKind::WriteZero, "write zero byte into writer");
                    return Poll::Ready(Err(ErrorSource::write(self.dir, cold(err))));
                }
                Poll::Ready(Ok(i)) => {
                    // update the write head.
                    self.filled.start += i;
                    self.need_flush = true;

                    // we wrote some data, but the filled buffer might not be fully empty yet.
                    if !self.filled.is_empty() {
                        continue;
                    }

                    // the buffer is definitely empty. reset positions.
                    self.filled = 0..0;
                    break;
                }
                // While we couldn't write, we might be able to read.
                Poll::Pending if self.filled.end < self.buf.len() => break,
                // We couldn't write, and have no space to read. Just exit.
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Ready(Ok(()))
    }

    /// Returns Ready(Ok((true))) when read returns EOF.
    /// Returns Ready(Ok((false))) when read returns data.
    #[inline(always)]
    fn poll_read_once<R>(
        &mut self,
        cx: &mut Context<'_>,
        f: &mut impl for<'a> FnMut(Direction, &'a [u8]),
        reader: Pin<&mut R>,
    ) -> Poll<Result<bool, ErrorSource>>
    where
        R: AsyncRead + ?Sized,
    {
        debug_assert!(self.filled.end < self.buf.len());
        let mut buf = ReadBuf::new(&mut self.buf[self.filled.end..]);

        match reader.poll_read(cx, &mut buf) {
            Poll::Ready(Ok(())) => {
                let filled = buf.filled();
                // no more data to read, switch to shutdown mode.
                if filled.is_empty() {
                    self.need_flush = true;
                    return Poll::Ready(Ok(true));
                }

                // run our inspection callback.
                f(self.dir, filled);

                // update the read head.
                self.filled.end += filled.len();

                // read more data
                Poll::Ready(Ok(false))
            }
            // cannot continue on error.
            Poll::Ready(Err(e)) => Poll::Ready(Err(ErrorSource::read(self.dir, cold(e)))),
            // No more data to read, and no more data to write.
            Poll::Pending => Poll::Pending,
        }
    }

    /// Returns Ready(Ok(())) when read returns EOF.
    fn poll_copy<R, W>(
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
        // this register eliminates a branch in the hot loop.
        let mut empty = self.filled.is_empty();

        // write then read hot loop
        loop {
            if !empty {
                ready!(self.poll_write_loop(cx, writer.as_mut())?);
            }

            // If empty is true, there is guaranteed space to read.
            // If empty is false, and the write loop returned ready, then we know there's space for more reads.
            match self.poll_read_once(cx, f, reader.as_mut())? {
                // EOF
                Poll::Ready(true) => return Poll::Ready(Ok(())),
                // Needs write.
                Poll::Ready(false) => empty = false,
                // Cannot read. The peer might not send us anything until
                // they receive data from us, so let's switch to flushing.
                Poll::Pending => break,
            }
        }

        if self.need_flush {
            let flush = writer.as_mut().poll_flush(cx);
            ready!(flush.map_err(|e| ErrorSource::write(self.dir, e))?);
            self.need_flush = false;
        }

        // there might be more data still to read.
        Poll::Pending
    }

    /// Returns Ready(Ok(())) when the conn is fully shutdown.
    pub(super) fn poll_empty<W>(
        &mut self,
        cx: &mut Context<'_>,
        mut writer: Pin<&mut W>,
    ) -> Poll<Result<(), ErrorSource>>
    where
        W: AsyncWrite + ?Sized,
    {
        if !self.filled.is_empty() {
            ready!(self.poll_write_loop(cx, writer.as_mut())?);

            if !self.filled.is_empty() {
                // still some data to write
                return Poll::Pending;
            }
        }

        if self.need_flush {
            let res = writer.poll_shutdown(cx);
            ready!(res.map_err(|e| ErrorSource::write(self.dir, e))?);
            self.need_flush = false;
        }

        // no data to read, no data to write.
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn test_client_to_compute() {
        let (mut client, mut client_proxy) = tokio::io::duplex(32); // Create a mock duplex stream
        let (mut proxy_compute, mut compute) = tokio::io::duplex(16); // Create a mock duplex stream

        client.write_all(b"Neon Serverless Postgres").await.unwrap();
        compute.write_all(b"is amazing").await.unwrap();

        client.shutdown().await.unwrap();

        let copy = tokio::spawn(async move {
            copy_bidirectional_client_compute(&mut client_proxy, &mut proxy_compute, |_, _| {})
                .await
                .unwrap();
        });

        // Assert correct transferred amounts
        let mut client_recv = String::new();
        let mut compute_recv = String::new();

        client.read_to_string(&mut client_recv).await.unwrap();
        compute.read_to_string(&mut compute_recv).await.unwrap();

        assert_eq!(compute_recv, "Neon Serverless Postgres");
        assert_eq!(client_recv, "is amazing");

        copy.await.unwrap();
    }

    #[tokio::test]
    async fn test_compute_to_client() {
        let (mut client, mut client_proxy) = tokio::io::duplex(32); // Create a mock duplex stream
        let (mut proxy_compute, mut compute) = tokio::io::duplex(16); // Create a mock duplex stream

        client.write_all(b"Neon Serverless Postgres").await.unwrap();
        compute.write_all(b"is amazing").await.unwrap();

        compute.shutdown().await.unwrap();

        let copy = tokio::spawn(async move {
            copy_bidirectional_client_compute(&mut client_proxy, &mut proxy_compute, |_, _| {})
                .await
                .unwrap();
        });

        // Assert correct transferred amounts
        let mut client_recv = String::new();
        let mut compute_recv = String::new();

        client.read_to_string(&mut client_recv).await.unwrap();
        compute.read_to_string(&mut compute_recv).await.unwrap();

        assert_eq!(compute_recv, "Neon Serverless ");
        assert_eq!(client_recv, "is amazing");

        copy.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_timeout() {
        let (mut client, mut client_proxy) = tokio::io::duplex(32); // Create a mock duplex stream
        let (mut proxy_compute, mut compute) = tokio::io::duplex(16); // Create a mock duplex stream

        // Try to send 24 bytes to compute, but compute only has space for 16 bytes.
        // Writes will not succeed.
        client.write_all(b"Neon Serverless Postgres").await.unwrap();
        client.shutdown().await.unwrap();

        let copy = tokio::spawn(async move {
            copy_bidirectional_client_compute(&mut client_proxy, &mut proxy_compute, |_, _| {})
                .await
                .unwrap_err()
        });

        tokio::time::advance(DISCONNECT_TIMEOUT).await;

        let res = copy.await.unwrap();
        assert!(matches!(res, ErrorSource::Timeout(_)));

        // Assert correct transferred amounts
        let mut compute_recv = String::new();
        compute.read_to_string(&mut compute_recv).await.unwrap();
        assert_eq!(compute_recv, "Neon Serverless ");
    }
}
