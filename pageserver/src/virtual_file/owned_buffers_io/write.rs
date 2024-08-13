use bytes::BytesMut;
use tokio_epoll_uring::{BoundedBuf, IoBuf, Slice};

use crate::context::RequestContext;

/// A trait for doing owned-buffer write IO.
/// Think [`tokio::io::AsyncWrite`] but with owned buffers.
pub trait OwnedAsyncWriter {
    async fn write_all<Buf: IoBuf + Send>(
        &mut self,
        buf: Slice<Buf>,
        ctx: &RequestContext,
    ) -> std::io::Result<(usize, Slice<Buf>)>;
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
    ///
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

    pub fn as_inner(&self) -> &W {
        &self.writer
    }

    /// Panics if used after any of the write paths returned an error
    pub fn inspect_buffer(&self) -> &B {
        self.buf()
    }

    #[cfg_attr(target_os = "macos", allow(dead_code))]
    pub async fn flush_and_into_inner(mut self, ctx: &RequestContext) -> std::io::Result<W> {
        self.flush(ctx).await?;

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

    #[cfg_attr(target_os = "macos", allow(dead_code))]
    pub async fn write_buffered<S: IoBuf + Send>(
        &mut self,
        chunk: Slice<S>,
        ctx: &RequestContext,
    ) -> std::io::Result<(usize, Slice<S>)> {
        assert_eq!(chunk.bytes_init(), chunk.bytes_total());

        let chunk_len = chunk.len();
        // avoid memcpy for the middle of the chunk
        if chunk.len() >= self.buf().cap() {
            self.flush(ctx).await?;
            // do a big write, bypassing `buf`
            assert_eq!(
                self.buf
                    .as_ref()
                    .expect("must not use after an error")
                    .pending(),
                0
            );
            let (nwritten, chunk) = self.writer.write_all(chunk, ctx).await?;
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
                self.flush(ctx).await?;
            }
        }
        assert!(slice.is_empty(), "by now we should have drained the chunk");
        Ok((chunk_len, chunk))
    }

    /// Strictly less performant variant of [`Self::write_buffered`] that allows writing borrowed data.
    ///
    /// It is less performant because we always have to copy the borrowed data into the internal buffer
    /// before we can do the IO. The [`Self::write_buffered`] can avoid this, which is more performant
    /// for large writes.
    pub async fn write_buffered_borrowed(
        &mut self,
        mut chunk: &[u8],
        ctx: &RequestContext,
    ) -> std::io::Result<usize> {
        let chunk_len = chunk.len();
        while !chunk.is_empty() {
            let buf = self.buf.as_mut().expect("must not use after an error");
            let need = buf.cap() - buf.pending();
            let have = chunk.len();
            let n = std::cmp::min(need, have);
            buf.extend_from_slice(&chunk[..n]);
            chunk = &chunk[n..];
            if buf.pending() >= buf.cap() {
                assert_eq!(buf.pending(), buf.cap());
                self.flush(ctx).await?;
            }
        }
        Ok(chunk_len)
    }

    async fn flush(&mut self, ctx: &RequestContext) -> std::io::Result<()> {
        let buf = self.buf.take().expect("must not use after an error");
        let buf_len = buf.pending();
        if buf_len == 0 {
            self.buf = Some(buf);
            return Ok(());
        }
        let slice = buf.flush();
        let (nwritten, slice) = self.writer.write_all(slice, ctx).await?;
        assert_eq!(nwritten, buf_len);
        self.buf = Some(Buffer::reuse_after_flush(slice.into_inner()));
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
    async fn write_all<Buf: IoBuf + Send>(
        &mut self,
        buf: Slice<Buf>,
        _: &RequestContext,
    ) -> std::io::Result<(usize, Slice<Buf>)> {
        assert_eq!(buf.bytes_init(), buf.bytes_total());
        self.extend_from_slice(&buf[..]);
        Ok((buf.len(), buf))
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;
    use crate::context::{DownloadBehavior, RequestContext};
    use crate::task_mgr::TaskKind;

    #[derive(Default)]
    struct RecorderWriter {
        writes: Vec<Vec<u8>>,
    }
    impl OwnedAsyncWriter for RecorderWriter {
        async fn write_all<Buf: IoBuf + Send>(
            &mut self,
            buf: Slice<Buf>,
            _: &RequestContext,
        ) -> std::io::Result<(usize, Slice<Buf>)> {
            assert_eq!(buf.bytes_init(), buf.bytes_total());
            self.writes.push(Vec::from(&buf[..]));
            Ok((buf.len(), buf))
        }
    }

    fn test_ctx() -> RequestContext {
        RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error)
    }

    macro_rules! write {
        ($writer:ident, $data:literal) => {{
            $writer
                .write_buffered(::bytes::Bytes::from_static($data).slice_full(), &test_ctx())
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
        let recorder = writer.flush_and_into_inner(&test_ctx()).await?;
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
        let recorder = writer.flush_and_into_inner(&test_ctx()).await?;
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
        let recorder = writer.flush_and_into_inner(&test_ctx()).await?;
        assert_eq!(
            recorder.writes,
            vec![Vec::from(b"a"), Vec::from(b"bc"), Vec::from(b"de")]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_write_all_borrowed_always_goes_through_buffer() -> std::io::Result<()> {
        let ctx = test_ctx();
        let ctx = &ctx;
        let recorder = RecorderWriter::default();
        let mut writer = BufferedWriter::new(recorder, BytesMut::with_capacity(2));

        writer.write_buffered_borrowed(b"abc", ctx).await?;
        writer.write_buffered_borrowed(b"d", ctx).await?;
        writer.write_buffered_borrowed(b"e", ctx).await?;
        writer.write_buffered_borrowed(b"fg", ctx).await?;
        writer.write_buffered_borrowed(b"hi", ctx).await?;
        writer.write_buffered_borrowed(b"j", ctx).await?;
        writer.write_buffered_borrowed(b"klmno", ctx).await?;

        let recorder = writer.flush_and_into_inner(ctx).await?;
        assert_eq!(
            recorder.writes,
            {
                let expect: &[&[u8]] = &[b"ab", b"cd", b"ef", b"gh", b"ij", b"kl", b"mn", b"o"];
                expect
            }
            .iter()
            .map(|v| v[..].to_vec())
            .collect::<Vec<_>>()
        );
        Ok(())
    }
}
