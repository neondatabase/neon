mod flush;
use std::sync::Arc;

pub(crate) use flush::FlushControl;
use flush::FlushHandle;
use tokio_epoll_uring::IoBuf;

use super::io_buf_aligned::IoBufAligned;
use super::io_buf_ext::{FullSlice, IoBufExt};
use crate::context::RequestContext;
use crate::virtual_file::{IoBuffer, IoBufferMut};

pub(crate) trait CheapCloneForRead {
    /// Returns a cheap clone of the buffer.
    fn cheap_clone(&self) -> Self;
}

impl CheapCloneForRead for IoBuffer {
    fn cheap_clone(&self) -> Self {
        // Cheap clone over an `Arc`.
        self.clone()
    }
}

/// A trait for doing owned-buffer write IO.
/// Think [`tokio::io::AsyncWrite`] but with owned buffers.
/// The owned buffers need to be aligned due to Direct IO requirements.
pub trait OwnedAsyncWriter {
    fn write_all_at<Buf: IoBufAligned + Send>(
        &self,
        buf: FullSlice<Buf>,
        offset: u64,
        ctx: &RequestContext,
    ) -> impl std::future::Future<Output = std::io::Result<FullSlice<Buf>>> + Send;
}

/// A wrapper aorund an [`OwnedAsyncWriter`] that uses a [`Buffer`] to batch
/// small writes into larger writes of size [`Buffer::cap`].
// TODO(yuchen): For large write, implementing buffer bypass for aligned parts of the write could be beneficial to throughput,
// since we would avoid copying majority of the data into the internal buffer.
pub struct BufferedWriter<B: Buffer, W> {
    writer: Arc<W>,
    /// invariant: always remains Some(buf) except
    /// - while IO is ongoing => goes back to Some() once the IO completed successfully
    /// - after an IO error => stays `None` forever
    ///
    /// In these exceptional cases, it's `None`.
    mutable: Option<B>,
    /// A handle to the background flush task for writting data to disk.
    flush_handle: FlushHandle<B::IoBuf, W>,
    /// The number of bytes submitted to the background task.
    bytes_submitted: u64,
}

impl<B, Buf, W> BufferedWriter<B, W>
where
    B: Buffer<IoBuf = Buf> + Send + 'static,
    Buf: IoBufAligned + Send + Sync + CheapCloneForRead,
    W: OwnedAsyncWriter + Send + Sync + 'static + std::fmt::Debug,
{
    /// Creates a new buffered writer.
    ///
    /// The `buf_new` function provides a way to initialize the owned buffers used by this writer.
    pub fn new(
        writer: Arc<W>,
        buf_new: impl Fn() -> B,
        gate_guard: utils::sync::gate::GateGuard,
        ctx: &RequestContext,
    ) -> Self {
        Self {
            writer: writer.clone(),
            mutable: Some(buf_new()),
            flush_handle: FlushHandle::spawn_new(
                writer,
                buf_new(),
                gate_guard,
                ctx.attached_child(),
            ),
            bytes_submitted: 0,
        }
    }

    pub fn as_inner(&self) -> &W {
        &self.writer
    }

    /// Returns the number of bytes submitted to the background flush task.
    pub fn bytes_submitted(&self) -> u64 {
        self.bytes_submitted
    }

    /// Panics if used after any of the write paths returned an error
    pub fn inspect_mutable(&self) -> &B {
        self.mutable()
    }

    /// Gets a reference to the maybe flushed read-only buffer.
    /// Returns `None` if the writer has not submitted any flush request.
    pub fn inspect_maybe_flushed(&self) -> Option<&FullSlice<Buf>> {
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
            bytes_submitted: bytes_amount,
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

    #[cfg_attr(target_os = "macos", allow(dead_code))]
    pub async fn write_buffered_borrowed(
        &mut self,
        chunk: &[u8],
        ctx: &RequestContext,
    ) -> std::io::Result<usize> {
        let (len, control) = self.write_buffered_borrowed_controlled(chunk, ctx).await?;
        if let Some(control) = control {
            control.release().await;
        }
        Ok(len)
    }

    /// In addition to bytes submitted in this write, also returns a handle that can control the flush behavior.
    pub(crate) async fn write_buffered_borrowed_controlled(
        &mut self,
        mut chunk: &[u8],
        ctx: &RequestContext,
    ) -> std::io::Result<(usize, Option<FlushControl>)> {
        let chunk_len = chunk.len();
        let mut control: Option<FlushControl> = None;
        while !chunk.is_empty() {
            let buf = self.mutable.as_mut().expect("must not use after an error");
            let need = buf.cap() - buf.pending();
            let have = chunk.len();
            let n = std::cmp::min(need, have);
            buf.extend_from_slice(&chunk[..n]);
            chunk = &chunk[n..];
            if buf.pending() >= buf.cap() {
                assert_eq!(buf.pending(), buf.cap());
                if let Some(control) = control.take() {
                    control.release().await;
                }
                control = self.flush(ctx).await?;
            }
        }
        Ok((chunk_len, control))
    }

    #[must_use = "caller must explcitly check the flush control"]
    async fn flush(&mut self, _ctx: &RequestContext) -> std::io::Result<Option<FlushControl>> {
        let buf = self.mutable.take().expect("must not use after an error");
        let buf_len = buf.pending();
        if buf_len == 0 {
            self.mutable = Some(buf);
            return Ok(None);
        }
        let (recycled, flush_control) = self.flush_handle.flush(buf, self.bytes_submitted).await?;
        self.bytes_submitted += u64::try_from(buf_len).unwrap();
        self.mutable = Some(recycled);
        Ok(Some(flush_control))
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

impl Buffer for IoBufferMut {
    type IoBuf = IoBuffer;

    fn cap(&self) -> usize {
        self.capacity()
    }

    fn extend_from_slice(&mut self, other: &[u8]) {
        if self.len() + other.len() > self.cap() {
            panic!("Buffer capacity exceeded");
        }

        IoBufferMut::extend_from_slice(self, other);
    }

    fn pending(&self) -> usize {
        self.len()
    }

    fn flush(self) -> FullSlice<Self::IoBuf> {
        self.freeze().slice_len()
    }

    /// Caller should make sure that `iobuf` only have one strong reference before invoking this method.
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
        async fn write_all_at<Buf: IoBufAligned + Send>(
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

    #[tokio::test]
    async fn test_write_all_borrowed_always_goes_through_buffer() -> anyhow::Result<()> {
        let ctx = test_ctx();
        let ctx = &ctx;
        let recorder = Arc::new(RecorderWriter::default());
        let gate = utils::sync::gate::Gate::default();
        let mut writer = BufferedWriter::<_, RecorderWriter>::new(
            recorder,
            || IoBufferMut::with_capacity(2),
            gate.enter()?,
            ctx,
        );

        writer.write_buffered_borrowed(b"abc", ctx).await?;
        writer.write_buffered_borrowed(b"", ctx).await?;
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
