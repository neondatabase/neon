use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use std::future::poll_fn;
use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

#[derive(Debug)]
enum TransferState {
    Running(CopyBuffer),
    ShuttingDown(u64),
    Done(u64),
}

fn transfer_one_direction<A, B>(
    cx: &mut Context<'_>,
    state: &mut TransferState,
    r: &mut A,
    w: &mut B,
) -> Poll<io::Result<u64>>
where
    A: AsyncRead + AsyncWrite + Unpin + ?Sized,
    B: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut r = Pin::new(r);
    let mut w = Pin::new(w);
    loop {
        match state {
            TransferState::Running(buf) => {
                let count = ready!(buf.poll_copy(cx, r.as_mut(), w.as_mut()))?;
                *state = TransferState::ShuttingDown(count);
            }
            TransferState::ShuttingDown(count) => {
                ready!(w.as_mut().poll_shutdown(cx))?;
                *state = TransferState::Done(*count);
            }
            TransferState::Done(count) => return Poll::Ready(Ok(*count)),
        }
    }
}

pub(super) async fn copy_bidirectional<A, B>(
    a: &mut A,
    b: &mut B,
) -> Result<(u64, u64), std::io::Error>
where
    A: AsyncRead + AsyncWrite + Unpin + ?Sized,
    B: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut a_to_b = TransferState::Running(CopyBuffer::new());
    let mut b_to_a = TransferState::Running(CopyBuffer::new());

    poll_fn(|cx| {
        let mut a_to_b_result = transfer_one_direction(cx, &mut a_to_b, a, b)?;
        let mut b_to_a_result = transfer_one_direction(cx, &mut b_to_a, b, a)?;

        // Early termination checks
        if let TransferState::Done(_) = a_to_b {
            if let TransferState::Running(_) = b_to_a {
                // Initiate shutdown
                b_to_a = TransferState::ShuttingDown(0);
                b_to_a_result = transfer_one_direction(cx, &mut b_to_a, b, a)?;
            }
        }
        if let TransferState::Done(_) = b_to_a {
            if let TransferState::Running(_) = a_to_b {
                // Initiate shutdown
                a_to_b = TransferState::ShuttingDown(0);
                a_to_b_result = transfer_one_direction(cx, &mut a_to_b, a, b)?;
            }
        }

        // It is not a problem if ready! returns early ... (comment remains the same)
        let a_to_b = ready!(a_to_b_result);
        let b_to_a = ready!(b_to_a_result);

        Poll::Ready(Ok((a_to_b, b_to_a)))
    })
    .await
}

#[derive(Debug)]
pub(super) struct CopyBuffer {
    read_done: bool,
    need_flush: bool,
    pos: usize,
    cap: usize,
    amt: u64,
    buf: Box<[u8]>,
}
const DEFAULT_BUF_SIZE: usize = 8;

impl CopyBuffer {
    pub(super) fn new() -> Self {
        Self {
            read_done: false,
            need_flush: false,
            pos: 0,
            cap: 0,
            amt: 0,
            buf: vec![0; DEFAULT_BUF_SIZE].into_boxed_slice(),
        }
    }

    fn poll_fill_buf<R>(
        &mut self,
        cx: &mut Context<'_>,
        reader: Pin<&mut R>,
    ) -> Poll<io::Result<()>>
    where
        R: AsyncRead + ?Sized,
    {
        let me = &mut *self;
        let mut buf = ReadBuf::new(&mut me.buf);
        buf.set_filled(me.cap);

        let res = reader.poll_read(cx, &mut buf);
        if let Poll::Ready(Ok(())) = res {
            let filled_len = buf.filled().len();
            me.read_done = me.cap == filled_len;
            me.cap = filled_len;
        }
        res
    }

    fn poll_write_buf<R, W>(
        &mut self,
        cx: &mut Context<'_>,
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
    ) -> Poll<io::Result<usize>>
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
                    ready!(me.poll_fill_buf(cx, reader.as_mut()))?;
                }
                Poll::Pending
            }
            res => res,
        }
    }

    pub(super) fn poll_copy<R, W>(
        &mut self,
        cx: &mut Context<'_>,
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
    ) -> Poll<io::Result<u64>>
    where
        R: AsyncRead + ?Sized,
        W: AsyncWrite + ?Sized,
    {
        loop {
            // If our buffer is empty, then we need to read some data to
            // continue.
            if self.pos == self.cap && !self.read_done {
                self.pos = 0;
                self.cap = 0;

                match self.poll_fill_buf(cx, reader.as_mut()) {
                    Poll::Ready(Ok(())) => (),
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => {
                        // Try flushing when the reader has no progress to avoid deadlock
                        // when the reader depends on buffered writer.
                        if self.need_flush {
                            ready!(writer.as_mut().poll_flush(cx))?;
                            self.need_flush = false;
                        }

                        return Poll::Pending;
                    }
                }
            }

            // If our buffer has some data, let's write it out!
            while self.pos < self.cap {
                let i = ready!(self.poll_write_buf(cx, reader.as_mut(), writer.as_mut()))?;
                if i == 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "write zero byte into writer",
                    )));
                } else {
                    self.pos += i;
                    self.amt += i as u64;
                    self.need_flush = true;
                }
            }

            // If pos larger than cap, this loop will never stop.
            // In particular, user's wrong poll_write implementation returning
            // incorrect written length may lead to thread blocking.
            debug_assert!(
                self.pos <= self.cap,
                "writer returned length larger than input slice"
            );

            // If we've written all the data and we've seen EOF, flush out the
            // data and finish the transfer.
            if self.pos == self.cap && self.read_done {
                ready!(writer.as_mut().poll_flush(cx))?;
                return Poll::Ready(Ok(self.amt));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_early_termination_a_to_d() {
        let (mut a_mock, mut b_mock) = tokio::io::duplex(10); // Create a mock duplex stream
        let (mut c_mock, mut d_mock) = tokio::io::duplex(10); // Create a mock duplex stream

        // Simulate 'a' finishing while there's still data for 'b'
        a_mock.write_all(b"hello").await.unwrap();
        a_mock.shutdown().await.unwrap();
        d_mock.write_all(b"world").await.unwrap();

        let result = copy_bidirectional(&mut b_mock, &mut c_mock).await;
        assert!(result.is_ok());

        // Assert correct transferred amounts
        let (a_to_d_count, d_to_a_count) = result.unwrap();
        assert_eq!(a_to_d_count, 5); // 'hello' was transferred
        assert!(d_to_a_count < 5); // 'world' only partially transferred or not at all
    }

    #[tokio::test]
    async fn test_early_termination_d_to_a() {
        let (mut a_mock, mut b_mock) = tokio::io::duplex(10); // Create a mock duplex stream
        let (mut c_mock, mut d_mock) = tokio::io::duplex(10); // Create a mock duplex stream

        // Simulate 'a' finishing while there's still data for 'b'
        d_mock.write_all(b"hello").await.unwrap();
        d_mock.shutdown().await.unwrap();
        a_mock.write_all(b"world").await.unwrap();

        let result = copy_bidirectional(&mut b_mock, &mut c_mock).await;
        assert!(result.is_ok());

        // Assert correct transferred amounts
        let (a_to_d_count, d_to_a_count) = result.unwrap();
        assert_eq!(d_to_a_count, 5); // 'hello' was transferred
        assert!(a_to_d_count < 5); // 'world' only partially transferred or not at all
    }
}
