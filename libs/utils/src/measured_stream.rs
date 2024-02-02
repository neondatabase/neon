use pin_project_lite::pin_project;
use std::io::Read;
use std::pin::Pin;
use std::{io, task};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pin_project! {
    /// This stream tracks all writes and calls user provided
    /// callback when the underlying stream is flushed.
    pub struct MeasuredStream<S, R, W> {
        #[pin]
        stream: S,
        write_count: usize,
        inc_read_count: R,
        inc_write_count: W,
    }
}

impl<S, R, W> MeasuredStream<S, R, W> {
    pub fn new(stream: S, inc_read_count: R, inc_write_count: W) -> Self {
        Self {
            stream,
            write_count: 0,
            inc_read_count,
            inc_write_count,
        }
    }
}

impl<S: AsyncRead + Unpin, R: FnMut(usize), W> AsyncRead for MeasuredStream<S, R, W> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> task::Poll<io::Result<()>> {
        let this = self.project();
        let filled = buf.filled().len();
        this.stream.poll_read(context, buf).map_ok(|()| {
            let cnt = buf.filled().len() - filled;
            // Increment the read count.
            (this.inc_read_count)(cnt);
        })
    }
}

impl<S: AsyncWrite + Unpin, R, W: FnMut(usize)> AsyncWrite for MeasuredStream<S, R, W> {
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

/// Wrapper for a reader that counts bytes read.
///
/// Similar to MeasuredStream but it's one way and it's sync
pub struct MeasuredReader<R: Read> {
    inner: R,
    byte_count: usize,
}

impl<R: Read> MeasuredReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: reader,
            byte_count: 0,
        }
    }

    pub fn get_byte_count(&self) -> usize {
        self.byte_count
    }
}

impl<R: Read> Read for MeasuredReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let result = self.inner.read(buf);
        if let Ok(n_bytes) = result {
            self.byte_count += n_bytes
        }
        result
    }
}
