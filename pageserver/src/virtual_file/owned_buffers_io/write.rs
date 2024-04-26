use bytes::BytesMut;
use tokio_epoll_uring::{BoundedBuf, IoBuf, Slice};

/// A trait for doing owned-buffer write IO.
/// Think [`tokio::io::AsyncWrite`] but with owned buffers.
pub trait OwnedAsyncWriter {
    async fn write_all<B: BoundedBuf<Buf = Buf>, Buf: IoBuf + Send>(
        &mut self,
        buf: B,
    ) -> std::io::Result<(usize, B::Buf)>;
}

/// A wrapper aorund an [`OwnedAsyncWriter`] that uses a [`Buffer`] to batch
/// small writes into larger writes of size [`Buffer::cap`].
///
/// # Passthrough Of Large Writers
///
/// Calls to [`BufferedWriter::write_buffered`] that are larger than [`Buffer::cap`]
/// cause the internal buffer to be flushed prematurely so that the large
/// buffered write is passed through to the underlying [`OwnedAsyncWriter`].
///
/// This pass-through is generally beneficial for throughput, but if
/// the storage backend of the [`OwnedAsyncWriter`] is a shared resource,
/// unlimited large writes may cause latency or fairness issues.
///
/// In such cases, a different implementation that always buffers in memory
/// may be preferable.
pub struct BufferedWriter<B, W> {
    writer: W,
    /// invariant: always remains Some(buf) except
    /// - while IO is ongoing => goes back to Some() once the IO completed successfully
    /// - after an IO error => stays `None` forever
    /// In these exceptional cases, it's `None`.
    buf: Option<B>,
}

impl<B, Buf, W> BufferedWriter<B, W>
where
    B: Buffer<IoBuf = Buf> + Send,
    Buf: IoBuf + Send,
    W: OwnedAsyncWriter,
{
    pub fn new(writer: W, buf: B) -> Self {
        Self {
            writer,
            buf: Some(buf),
        }
    }

    pub async fn flush_and_into_inner(mut self) -> std::io::Result<W> {
        self.flush().await?;
        let Self { buf, writer } = self;
        assert!(buf.is_some());
        Ok(writer)
    }

    #[inline(always)]
    fn buf(&self) -> &B {
        self.buf
            .as_ref()
            .expect("must not use after we returned an error")
    }

    pub async fn write_buffered<S: IoBuf>(&mut self, chunk: Slice<S>) -> std::io::Result<(usize, S)>
    where
        S: IoBuf + Send,
    {
        let chunk_len = chunk.len();
        // avoid memcpy for the middle of the chunk
        if chunk.len() >= self.buf().cap() {
            self.flush().await?;
            // do a big write, bypassing `buf`
            assert_eq!(
                self.buf
                    .as_ref()
                    .expect("must not use after an error")
                    .pending(),
                0
            );
            let (nwritten, chunk) = self.writer.write_all(chunk).await?;
            assert_eq!(nwritten, chunk_len);
            return Ok((nwritten, chunk));
        }
        // in-memory copy the < BUFFER_SIZED tail of the chunk
        assert!(chunk.len() < self.buf().cap());
        let mut slice = &chunk[..];
        while !slice.is_empty() {
            let buf = self.buf.as_mut().expect("must not use after an error");
            let need = buf.cap() - buf.pending();
            let have = slice.len();
            let n = std::cmp::min(need, have);
            buf.extend_from_slice(&slice[..n]);
            slice = &slice[n..];
            if buf.pending() >= buf.cap() {
                assert_eq!(buf.pending(), buf.cap());
                self.flush().await?;
            }
        }
        assert!(slice.is_empty(), "by now we should have drained the chunk");
        Ok((chunk_len, chunk.into_inner()))
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        let buf = self.buf.take().expect("must not use after an error");
        let buf_len = buf.pending();
        if buf_len == 0 {
            self.buf = Some(buf);
            return Ok(());
        }
        let (nwritten, io_buf) = self.writer.write_all(buf.flush()).await?;
        assert_eq!(nwritten, buf_len);
        self.buf = Some(Buffer::reuse_after_flush(io_buf));
        Ok(())
    }
}

/// A [`Buffer`] is used by [`BufferedWriter`] to batch smaller writes into larger ones.
pub trait Buffer {
    type IoBuf: IoBuf;

    /// Capacity of the buffer. Must not change over the lifetime `self`.`
    fn cap(&self) -> usize;

    /// Add data to the buffer.
    /// Panics if there is not enough room to accomodate `other`'s content, i.e.,
    /// panics if `other.len() > self.cap() - self.pending()`.
    fn extend_from_slice(&mut self, other: &[u8]);

    /// Number of bytes in the buffer.
    fn pending(&self) -> usize;

    /// Turns `self` into a [`tokio_epoll_uring::Slice`] of the pending data
    /// so we can use [`tokio_epoll_uring`] to write it to disk.
    fn flush(self) -> Slice<Self::IoBuf>;

    /// After the write to disk is done and we have gotten back the slice,
    /// [`BufferedWriter`] uses this method to re-use the io buffer.
    fn reuse_after_flush(iobuf: Self::IoBuf) -> Self;
}

impl Buffer for BytesMut {
    type IoBuf = BytesMut;

    #[inline(always)]
    fn cap(&self) -> usize {
        self.capacity()
    }

    fn extend_from_slice(&mut self, other: &[u8]) {
        BytesMut::extend_from_slice(self, other)
    }

    #[inline(always)]
    fn pending(&self) -> usize {
        self.len()
    }

    fn flush(self) -> Slice<BytesMut> {
        if self.is_empty() {
            return self.slice_full();
        }
        let len = self.len();
        self.slice(0..len)
    }

    fn reuse_after_flush(mut iobuf: BytesMut) -> Self {
        iobuf.clear();
        iobuf
    }
}

impl OwnedAsyncWriter for Vec<u8> {
    async fn write_all<B: BoundedBuf<Buf = Buf>, Buf: IoBuf + Send>(
        &mut self,
        buf: B,
    ) -> std::io::Result<(usize, B::Buf)> {
        let nbytes = buf.bytes_init();
        if nbytes == 0 {
            return Ok((0, Slice::into_inner(buf.slice_full())));
        }
        let buf = buf.slice(0..nbytes);
        self.extend_from_slice(&buf[..]);
        Ok((buf.len(), Slice::into_inner(buf)))
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[derive(Default)]
    struct RecorderWriter {
        writes: Vec<Vec<u8>>,
    }
    impl OwnedAsyncWriter for RecorderWriter {
        async fn write_all<B: BoundedBuf<Buf = Buf>, Buf: IoBuf + Send>(
            &mut self,
            buf: B,
        ) -> std::io::Result<(usize, B::Buf)> {
            let nbytes = buf.bytes_init();
            if nbytes == 0 {
                self.writes.push(vec![]);
                return Ok((0, Slice::into_inner(buf.slice_full())));
            }
            let buf = buf.slice(0..nbytes);
            self.writes.push(Vec::from(&buf[..]));
            Ok((buf.len(), Slice::into_inner(buf)))
        }
    }

    macro_rules! write {
        ($writer:ident, $data:literal) => {{
            $writer
                .write_buffered(::bytes::Bytes::from_static($data).slice_full())
                .await?;
        }};
    }

    #[tokio::test]
    async fn test_buffered_writes_only() -> std::io::Result<()> {
        let recorder = RecorderWriter::default();
        let mut writer = BufferedWriter::new(recorder, BytesMut::with_capacity(2));
        write!(writer, b"a");
        write!(writer, b"b");
        write!(writer, b"c");
        write!(writer, b"d");
        write!(writer, b"e");
        let recorder = writer.flush_and_into_inner().await?;
        assert_eq!(
            recorder.writes,
            vec![Vec::from(b"ab"), Vec::from(b"cd"), Vec::from(b"e")]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_passthrough_writes_only() -> std::io::Result<()> {
        let recorder = RecorderWriter::default();
        let mut writer = BufferedWriter::new(recorder, BytesMut::with_capacity(2));
        write!(writer, b"abc");
        write!(writer, b"de");
        write!(writer, b"");
        write!(writer, b"fghijk");
        let recorder = writer.flush_and_into_inner().await?;
        assert_eq!(
            recorder.writes,
            vec![Vec::from(b"abc"), Vec::from(b"de"), Vec::from(b"fghijk")]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_passthrough_write_with_nonempty_buffer() -> std::io::Result<()> {
        let recorder = RecorderWriter::default();
        let mut writer = BufferedWriter::new(recorder, BytesMut::with_capacity(2));
        write!(writer, b"a");
        write!(writer, b"bc");
        write!(writer, b"d");
        write!(writer, b"e");
        let recorder = writer.flush_and_into_inner().await?;
        assert_eq!(
            recorder.writes,
            vec![Vec::from(b"a"), Vec::from(b"bc"), Vec::from(b"de")]
        );
        Ok(())
    }
}
