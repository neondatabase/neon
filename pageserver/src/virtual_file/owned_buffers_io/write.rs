mod flush;
use std::sync::Arc;

use bytes::BytesMut;
use flush::FlushHandle;
use tokio_epoll_uring::{BoundedBufMut, IoBuf};

use crate::{
    context::RequestContext,
    virtual_file::{IoBuffer, IoBufferMut},
};

use super::io_buf_ext::{FullSlice, IoBufExt};

/// A trait for doing owned-buffer write IO.
/// Think [`tokio::io::AsyncWrite`] but with owned buffers.
pub trait OwnedAsyncWriter {
    fn write_all_at<Buf: IoBuf + Send>(
        &self,
        buf: FullSlice<Buf>,
        offset: u64,
        ctx: &RequestContext,
    ) -> impl std::future::Future<Output = std::io::Result<FullSlice<Buf>>> + Send;
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
pub struct BufferedWriter<B: Buffer, W> {
    writer: Arc<W>,
    /// invariant: always remains Some(buf) except
    /// - while IO is ongoing => goes back to Some() once the IO completed successfully
    /// - after an IO error => stays `None` forever
    ///
    /// In these exceptional cases, it's `None`.
    mutable: Option<B>,
    flush_handle: FlushHandle<B::IoBuf, W>,
    bytes_amount: u64,
}

impl<B, Buf, W> BufferedWriter<B, W>
where
    B: Buffer<IoBuf = Buf> + Send + 'static,
    Buf: IoBuf + Send + Sync + Clone,
    W: OwnedAsyncWriter + Send + Sync + 'static + std::fmt::Debug,
{
    pub fn new(writer: Arc<W>, buf_new: impl Fn() -> B, ctx: &RequestContext) -> Self {
        Self {
            writer: writer.clone(),
            mutable: Some(buf_new()),
            flush_handle: FlushHandle::spawn_new(writer, buf_new(), ctx.attached_child()),
            bytes_amount: 0,
        }
    }

    pub fn as_inner(&self) -> &W {
        &self.writer
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_amount
    }

    /// Panics if used after any of the write paths returned an error
    pub fn inspect_mutable(&self) -> &B {
        self.mutable()
    }

    /// Gets a reference to the maybe flushed read-only buffer.
    pub fn inspect_maybe_flushed(&self) -> Option<&Buf> {
        self.flush_handle.maybe_flushed.as_ref()
    }

    #[cfg_attr(target_os = "macos", allow(dead_code))]
    pub async fn flush_and_into_inner(
        mut self,
        ctx: &RequestContext,
    ) -> std::io::Result<(u64, Arc<W>)> {
        self.flush(ctx).await?;

        let Self {
            mutable: buf,
            writer,
            mut flush_handle,
            bytes_amount,
        } = self;
        flush_handle.shutdown().await?;
        assert!(buf.is_some());
        Ok((bytes_amount, writer))
    }

    /// Gets a reference to the mutable in-memory buffer.
    #[inline(always)]
    fn mutable(&self) -> &B {
        self.mutable
            .as_ref()
            .expect("must not use after we returned an error")
    }

    /// Guarantees that if Ok() is returned, all bytes in `chunk` have been accepted.
    #[cfg_attr(target_os = "macos", allow(dead_code))]
    pub async fn write_buffered<S: IoBuf + Send>(
        &mut self,
        chunk: FullSlice<S>,
        ctx: &RequestContext,
    ) -> std::io::Result<(usize, FullSlice<S>)> {
        let chunk = chunk.into_raw_slice();

        let chunk_len = chunk.len();
        // avoid memcpy for the middle of the chunk
        if chunk.len() >= self.mutable().cap() {
            // TODO(yuchen): do we still want to keep the bypass path?
            self.flush(ctx).await?;
            // do a big write, bypassing `buf`
            assert_eq!(
                self.mutable
                    .as_ref()
                    .expect("must not use after an error")
                    .pending(),
                0
            );
            let chunk = self
                .writer
                .write_all_at(FullSlice::must_new(chunk), self.bytes_amount, ctx)
                .await?;
            self.bytes_amount += u64::try_from(chunk_len).unwrap();
            return Ok((chunk_len, chunk));
        }
        // in-memory copy the < BUFFER_SIZED tail of the chunk
        assert!(chunk.len() < self.mutable().cap());
        let mut slice = &chunk[..];
        while !slice.is_empty() {
            let buf = self.mutable.as_mut().expect("must not use after an error");
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
        Ok((chunk_len, FullSlice::must_new(chunk)))
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
            let buf = self.mutable.as_mut().expect("must not use after an error");
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

    async fn flush(&mut self, _ctx: &RequestContext) -> std::io::Result<()> {
        let buf = self.mutable.take().expect("must not use after an error");
        let buf_len = buf.pending();
        if buf_len == 0 {
            self.mutable = Some(buf);
            return Ok(());
        }
        let recycled = self.flush_handle.flush(buf, self.bytes_amount).await?;
        self.bytes_amount += u64::try_from(buf_len).unwrap();
        self.mutable = Some(recycled);
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

    /// Turns `self` into a [`FullSlice`] of the pending data
    /// so we can use [`tokio_epoll_uring`] to write it to disk.
    fn flush(self) -> FullSlice<Self::IoBuf>;

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

    fn flush(self) -> FullSlice<BytesMut> {
        self.slice_len()
    }

    fn reuse_after_flush(mut iobuf: BytesMut) -> Self {
        iobuf.clear();
        iobuf
    }
}

impl Buffer for IoBufferMut {
    type IoBuf = IoBuffer;

    fn cap(&self) -> usize {
        self.capacity()
    }

    fn extend_from_slice(&mut self, other: &[u8]) {
        self.reserve(other.len());
        BoundedBufMut::put_slice(self, other);
    }

    fn pending(&self) -> usize {
        self.len()
    }

    fn flush(self) -> FullSlice<Self::IoBuf> {
        self.freeze().slice_len()
    }

    fn reuse_after_flush(iobuf: Self::IoBuf) -> Self {
        let mut recycled = iobuf
            .into_mut()
            .expect("buffer should only have one strong reference");
        recycled.clear();
        recycled
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use bytes::BytesMut;

    use super::*;
    use crate::context::{DownloadBehavior, RequestContext};
    use crate::task_mgr::TaskKind;

    #[derive(Default, Debug)]
    struct RecorderWriter {
        /// record bytes and write offsets.
        writes: Mutex<Vec<(Vec<u8>, u64)>>,
    }

    impl RecorderWriter {
        /// Gets recorded bytes and write offsets.
        fn get_writes(&self) -> Vec<Vec<u8>> {
            self.writes
                .lock()
                .unwrap()
                .iter()
                .map(|(buf, _)| buf.clone())
                .collect()
        }
    }

    impl OwnedAsyncWriter for RecorderWriter {
        async fn write_all_at<Buf: IoBuf + Send>(
            &self,
            buf: FullSlice<Buf>,
            offset: u64,
            _: &RequestContext,
        ) -> std::io::Result<FullSlice<Buf>> {
            self.writes
                .lock()
                .unwrap()
                .push((Vec::from(&buf[..]), offset));
            Ok(buf)
        }
    }

    fn test_ctx() -> RequestContext {
        RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error)
    }

    macro_rules! write {
        ($writer:ident, $data:literal) => {{
            $writer
                .write_buffered(::bytes::Bytes::from_static($data).slice_len(), &test_ctx())
                .await?;
        }};
    }

    #[tokio::test]
    async fn test_buffered_writes_only() -> std::io::Result<()> {
        let recorder = Arc::new(RecorderWriter::default());
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);
        let mut writer = BufferedWriter::new(recorder, || BytesMut::with_capacity(2), &ctx);
        write!(writer, b"a");
        write!(writer, b"b");
        write!(writer, b"c");
        write!(writer, b"d");
        write!(writer, b"e");
        let (_, recorder) = writer.flush_and_into_inner(&test_ctx()).await?;
        assert_eq!(
            recorder.get_writes(),
            vec![Vec::from(b"ab"), Vec::from(b"cd"), Vec::from(b"e")]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_passthrough_writes_only() -> std::io::Result<()> {
        let recorder = Arc::new(RecorderWriter::default());
        let ctx = test_ctx();
        let mut writer = BufferedWriter::new(recorder, || BytesMut::with_capacity(2), &ctx);
        write!(writer, b"abc");
        write!(writer, b"de");
        write!(writer, b"");
        write!(writer, b"fghijk");
        let (_, recorder) = writer.flush_and_into_inner(&test_ctx()).await?;
        assert_eq!(
            recorder.get_writes(),
            vec![Vec::from(b"abc"), Vec::from(b"de"), Vec::from(b"fghijk")]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_passthrough_write_with_nonempty_buffer() -> std::io::Result<()> {
        let recorder = Arc::new(RecorderWriter::default());
        let ctx = test_ctx();
        let mut writer = BufferedWriter::new(recorder, || BytesMut::with_capacity(2), &ctx);
        write!(writer, b"a");
        write!(writer, b"bc");
        write!(writer, b"d");
        write!(writer, b"e");
        let (_, recorder) = writer.flush_and_into_inner(&test_ctx()).await?;
        assert_eq!(
            recorder.get_writes(),
            vec![Vec::from(b"a"), Vec::from(b"bc"), Vec::from(b"de")]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_write_all_borrowed_always_goes_through_buffer() -> std::io::Result<()> {
        let ctx = test_ctx();
        let ctx = &ctx;
        let recorder = Arc::new(RecorderWriter::default());
        let mut writer =
            BufferedWriter::<_, RecorderWriter>::new(recorder, || BytesMut::with_capacity(2), ctx);

        writer.write_buffered_borrowed(b"abc", ctx).await?;
        writer.write_buffered_borrowed(b"d", ctx).await?;
        writer.write_buffered_borrowed(b"e", ctx).await?;
        writer.write_buffered_borrowed(b"fg", ctx).await?;
        writer.write_buffered_borrowed(b"hi", ctx).await?;
        writer.write_buffered_borrowed(b"j", ctx).await?;
        writer.write_buffered_borrowed(b"klmno", ctx).await?;

        let (_, recorder) = writer.flush_and_into_inner(ctx).await?;
        assert_eq!(
            recorder.get_writes(),
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
