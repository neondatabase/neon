mod flush;

use bytes::BufMut;
pub(crate) use flush::FlushControl;
use flush::FlushHandle;
pub(crate) use flush::FlushTaskError;
use flush::ShutdownRequest;
use tokio_epoll_uring::IoBuf;
use tokio_util::sync::CancellationToken;
use tracing::trace;

use super::io_buf_aligned::IoBufAligned;
use super::io_buf_aligned::IoBufAlignedMut;
use super::io_buf_ext::{FullSlice, IoBufExt};
use crate::context::RequestContext;
use crate::virtual_file::UsizeIsU64;
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
    ) -> impl std::future::Future<Output = (FullSlice<Buf>, std::io::Result<()>)> + Send;
    fn set_len(
        &self,
        len: u64,
        ctx: &RequestContext,
    ) -> impl Future<Output = std::io::Result<()>> + Send;
}

/// A wrapper aorund an [`OwnedAsyncWriter`] that uses a [`Buffer`] to batch
/// small writes into larger writes of size [`Buffer::cap`].
///
/// The buffer is flushed if and only if it is full ([`Buffer::pending`] == [`Buffer::cap`]).
/// This guarantees that writes to the filesystem happen
/// - at offsets that are multiples of [`Buffer::cap`]
/// - in lengths that are multiples of [`Buffer::cap`]
///
/// Above property is useful for Direct IO, where whatever the
/// effectively dominating disk-sector/filesystem-block/memory-page size
/// determines the requirements on
/// - the alignment of the pointer passed to the read/write operation
/// - the value of `count` (i.e., the length of the read/write operation)
///   which must be a multiple of the dominating sector/block/page size.
///
/// See [`BufferedWriter::shutdown`] / [`BufferedWriterShutdownMode`] for different
/// ways of dealing with the special case that the buffer is not full by the time
/// we are done writing.
///
/// The first flush to the underlying `W` happens at offset `start_offset` (arg of [`BufferedWriter::new`]).
/// The next flush is to offset `start_offset + Buffer::cap`. The one after at `start_offset + 2 * Buffer::cap` and so on.
///
/// TODO: decouple buffer capacity from alignment requirement.
/// Right now we assume [`Buffer::cap`] is the alignment requirement,
/// but actually [`Buffer::cap`] should only determine how often we flush
/// while writing, while a separate alignment requirement argument should
/// be passed to determine alignment requirement. This could be used by
/// [`BufferedWriterShutdownMode::PadThenTruncate`] to avoid excessive
/// padding of zeroes. For example, today, with a capacity of 64KiB, we
/// would pad up to 64KiB-1 bytes of zeroes, then truncate off 64KiB-1.
/// This is wasteful, e.g., if the alignment requirement is 4KiB, we only
/// need to pad & truncate up to 4KiB-1 bytes of zeroes
///
// TODO(yuchen): For large write, implementing buffer bypass for aligned parts of the write could be beneficial to throughput,
// since we would avoid copying majority of the data into the internal buffer.
// https://github.com/neondatabase/neon/issues/10101
pub struct BufferedWriter<B: Buffer, W> {
    /// Clone of the buffer that was last submitted to the flush loop.
    /// `None` if no flush request has been submitted, Some forever after.
    pub(super) maybe_flushed: Option<FullSlice<B::IoBuf>>,
    /// New writes are accumulated here.
    /// `None` only during submission while we wait for flush loop to accept
    /// the full dirty buffer in exchange for a clean buffer.
    /// If that exchange fails with an [`FlushTaskError`], the write path
    /// bails and leaves this as `None`.
    /// Subsequent writes will panic if attempted.
    /// The read path continues to work without error because [`Self::maybe_flushed`]
    /// and [`Self::bytes_submitted`] are advanced before the flush loop exchange starts,
    /// so, they will never try to read from [`Self::mutable`] anyway, because it's past
    /// the [`Self::maybe_flushed`] point.
    mutable: Option<B>,
    /// A handle to the background flush task for writting data to disk.
    flush_handle: FlushHandle<B::IoBuf, W>,
    /// The number of bytes submitted to the background task.
    bytes_submitted: u64,
}

/// How [`BufferedWriter::shutdown`] should deal with pending (=not-yet-flushed) data.
///
/// Cf the [`BufferedWriter`] comment's paragraph for context on why we need to think about this.
pub enum BufferedWriterShutdownMode {
    /// Drop pending data, don't write back to file.
    DropTail,
    /// Pad the pending data with zeroes (cf [`usize::next_multiple_of`]).
    ZeroPadToNextMultiple(usize),
    /// Fill the IO buffer with zeroes, flush to disk, the `ftruncate` the
    /// file to the exact number of bytes written to [`Self`].
    ///
    /// TODO: see in [`BufferedWriter`] comment about decoupling buffer capacity from alignment requirement.
    PadThenTruncate,
}

impl<B, Buf, W> BufferedWriter<B, W>
where
    B: IoBufAlignedMut + Buffer<IoBuf = Buf> + Send + 'static,
    Buf: IoBufAligned + Send + Sync + CheapCloneForRead,
    W: OwnedAsyncWriter + Send + Sync + 'static + std::fmt::Debug,
{
    /// Creates a new buffered writer.
    ///
    /// The `buf_new` function provides a way to initialize the owned buffers used by this writer.
    pub fn new(
        writer: W,
        start_offset: u64,
        buf_new: impl Fn() -> B,
        gate_guard: utils::sync::gate::GateGuard,
        cancel: CancellationToken,
        ctx: &RequestContext,
        flush_task_span: tracing::Span,
    ) -> Self {
        Self {
            mutable: Some(buf_new()),
            maybe_flushed: None,
            flush_handle: FlushHandle::spawn_new(
                writer,
                buf_new(),
                gate_guard,
                cancel,
                ctx.attached_child(),
                flush_task_span,
            ),
            bytes_submitted: start_offset,
        }
    }

    /// Returns the number of bytes submitted to the background flush task.
    pub fn bytes_submitted(&self) -> u64 {
        self.bytes_submitted
    }

    /// Panics if used after any of the write paths returned an error
    pub fn inspect_mutable(&self) -> Option<&B> {
        self.mutable.as_ref()
    }

    /// Gets a reference to the maybe flushed read-only buffer.
    /// Returns `None` if the writer has not submitted any flush request.
    pub fn inspect_maybe_flushed(&self) -> Option<&FullSlice<Buf>> {
        self.maybe_flushed.as_ref()
    }

    #[cfg_attr(target_os = "macos", allow(dead_code))]
    pub async fn shutdown(
        mut self,
        mode: BufferedWriterShutdownMode,
        ctx: &RequestContext,
    ) -> Result<(u64, W), FlushTaskError> {
        let mut mutable = self.mutable.take().expect("must not use after an error");
        let unpadded_pending = mutable.pending();
        let final_len: u64;
        let shutdown_req;
        match mode {
            BufferedWriterShutdownMode::DropTail => {
                trace!(pending=%mutable.pending(), "dropping pending data");
                drop(mutable);

                final_len = self.bytes_submitted;
                shutdown_req = ShutdownRequest { set_len: None };
            }
            BufferedWriterShutdownMode::ZeroPadToNextMultiple(next_multiple) => {
                let len = mutable.pending();
                let cap = mutable.cap();
                assert!(
                    len <= cap,
                    "buffer impl ensures this, but let's check because the extend_with below would panic if we go beyond"
                );
                let padded_len = len.next_multiple_of(next_multiple);
                assert!(
                    padded_len <= cap,
                    "caller specified a multiple that is larger than the buffer capacity"
                );
                let count = padded_len - len;
                mutable.extend_with(0, count);
                trace!(count, "padding with zeros");
                self.mutable = Some(mutable);

                final_len = self.bytes_submitted + padded_len.into_u64();
                shutdown_req = ShutdownRequest { set_len: None };
            }
            BufferedWriterShutdownMode::PadThenTruncate => {
                let len = mutable.pending();
                let cap = mutable.cap();
                // TODO: see struct comment TODO on decoupling buffer capacity from alignment requirement.
                let alignment_requirement = cap;
                assert!(len <= cap, "buffer impl should ensure this");
                let padding_end_offset = len.next_multiple_of(alignment_requirement);
                assert!(
                    padding_end_offset <= cap,
                    "{padding_end_offset} <= {cap}  ({alignment_requirement})"
                );
                let count = padding_end_offset - len;
                mutable.extend_with(0, count);
                trace!(count, "padding with zeros");
                self.mutable = Some(mutable);

                final_len = self.bytes_submitted + len.into_u64();
                shutdown_req = ShutdownRequest {
                    // Avoid set_len call if we didn't need to pad anything.
                    set_len: if count > 0 { Some(final_len) } else { None },
                };
            }
        };
        let padded_pending = self.mutable.as_ref().map(|b| b.pending());
        trace!(unpadded_pending, padded_pending, "padding done");
        if self.mutable.is_some() {
            self.flush(ctx).await?;
        }
        let Self {
            mutable: _,
            maybe_flushed: _,
            mut flush_handle,
            bytes_submitted: _,
        } = self;
        let writer = flush_handle.shutdown(shutdown_req).await?;

        Ok((final_len, writer))
    }

    #[cfg(test)]
    pub(crate) fn mutable(&self) -> &B {
        self.mutable.as_ref().expect("must not use after an error")
    }

    #[cfg_attr(target_os = "macos", allow(dead_code))]
    pub async fn write_buffered_borrowed(
        &mut self,
        chunk: &[u8],
        ctx: &RequestContext,
    ) -> Result<usize, FlushTaskError> {
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
    ) -> Result<(usize, Option<FlushControl>), FlushTaskError> {
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

    /// This function can only error if the flush task got cancelled.
    /// In that case, we leave [`Self::mutable`] intentionally as `None`.
    ///
    /// The read path continues to function correctly; it can read up to the
    /// point where it could read before, i.e., including what was in [`Self::mutable`]
    /// before the call to this function, because that's now stored in [`Self::maybe_flushed`].
    ///
    /// The write path becomes unavailable and will panic if used.
    /// The only correct solution to retry writes is to discard the entire [`BufferedWriter`],
    /// which upper layers of pageserver write path currently do not support.
    /// It is in fact quite hard to reason about what exactly happens in today's code.
    /// Best case we accumulate junk in the EphemeralFile, worst case is data corruption.
    #[must_use = "caller must explcitly check the flush control"]
    async fn flush(
        &mut self,
        _ctx: &RequestContext,
    ) -> Result<Option<FlushControl>, FlushTaskError> {
        let buf = self.mutable.take().expect("must not use after an error");
        let buf_len = buf.pending();
        if buf_len == 0 {
            self.mutable = Some(buf);
            return Ok(None);
        }
        // Prepare the buffer for read while flushing.
        let slice = buf.flush();
        // NB: this assignment also drops thereference to the old buffer, allowing us to re-own & make it mutable below.
        self.maybe_flushed = Some(slice.cheap_clone());
        let offset = self.bytes_submitted;
        self.bytes_submitted += u64::try_from(buf_len).unwrap();

        // If we return/panic here or later, we'll leave mutable = None, breaking further
        // writers, but the read path should still work.
        let (recycled, flush_control) = self.flush_handle.flush(slice, offset).await?;

        // The only other place that could hold a reference to the recycled buffer
        // is in `Self::maybe_flushed`, but we have already replace it with the new buffer.
        let recycled = Buffer::reuse_after_flush(recycled.into_raw_slice().into_inner());

        // We got back some recycled buffer, can open up for more writes again.
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

    /// Add `count` bytes `val` into `self`.
    /// Panics if `count > self.cap() - self.pending()`.
    fn extend_with(&mut self, val: u8, count: usize);

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

    fn extend_with(&mut self, val: u8, count: usize) {
        if self.len() + count > self.cap() {
            panic!("Buffer capacity exceeded");
        }

        IoBufferMut::put_bytes(self, val, count);
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

    use rstest::rstest;

    use super::*;
    use crate::context::{DownloadBehavior, RequestContext};
    use crate::task_mgr::TaskKind;

    #[derive(Debug, PartialEq, Eq)]
    enum Op {
        Write { buf: Vec<u8>, offset: u64 },
        SetLen { len: u64 },
    }

    #[derive(Default, Debug)]
    struct RecorderWriter {
        /// record bytes and write offsets.
        recording: Mutex<Vec<Op>>,
    }

    impl OwnedAsyncWriter for RecorderWriter {
        async fn write_all_at<Buf: IoBufAligned + Send>(
            &self,
            buf: FullSlice<Buf>,
            offset: u64,
            _: &RequestContext,
        ) -> (FullSlice<Buf>, std::io::Result<()>) {
            self.recording.lock().unwrap().push(Op::Write {
                buf: Vec::from(&buf[..]),
                offset,
            });
            (buf, Ok(()))
        }
        async fn set_len(&self, len: u64, _ctx: &RequestContext) -> std::io::Result<()> {
            self.recording.lock().unwrap().push(Op::SetLen { len });
            Ok(())
        }
    }

    fn test_ctx() -> RequestContext {
        RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error)
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_all_borrowed_always_goes_through_buffer(
        #[values(
            BufferedWriterShutdownMode::DropTail,
            BufferedWriterShutdownMode::ZeroPadToNextMultiple(2),
            BufferedWriterShutdownMode::PadThenTruncate
        )]
        mode: BufferedWriterShutdownMode,
    ) -> anyhow::Result<()> {
        let ctx = test_ctx();
        let ctx = &ctx;
        let recorder = RecorderWriter::default();
        let gate = utils::sync::gate::Gate::default();
        let cancel = CancellationToken::new();
        let cap = 4;
        let mut writer = BufferedWriter::<_, RecorderWriter>::new(
            recorder,
            0,
            || IoBufferMut::with_capacity(cap),
            gate.enter()?,
            cancel,
            ctx,
            tracing::Span::none(),
        );

        writer.write_buffered_borrowed(b"abc", ctx).await?;
        writer.write_buffered_borrowed(b"", ctx).await?;
        writer.write_buffered_borrowed(b"d", ctx).await?;
        writer.write_buffered_borrowed(b"efg", ctx).await?;
        writer.write_buffered_borrowed(b"hijklm", ctx).await?;

        let mut expect = {
            [(0, b"abcd"), (4, b"efgh"), (8, b"ijkl")]
                .into_iter()
                .map(|(offset, v)| Op::Write {
                    offset,
                    buf: v[..].to_vec(),
                })
                .collect::<Vec<_>>()
        };
        let expect_next_offset = 12;

        match &mode {
            BufferedWriterShutdownMode::DropTail => (),
            // We test the case with padding to next multiple of 2 so that it's different
            // from the alignment requirement of 4 inferred from buffer capacity.
            // See TODOs in the `BufferedWriter` struct comment on decoupling buffer capacity from alignment requirement.
            BufferedWriterShutdownMode::ZeroPadToNextMultiple(2) => {
                expect.push(Op::Write {
                    offset: expect_next_offset,
                    // it's legitimate for pad-to-next multiple 2 to be < alignment requirement 4 inferred from buffer capacity
                    buf: b"m\0".to_vec(),
                });
            }
            BufferedWriterShutdownMode::ZeroPadToNextMultiple(_) => unimplemented!(),
            BufferedWriterShutdownMode::PadThenTruncate => {
                expect.push(Op::Write {
                    offset: expect_next_offset,
                    buf: b"m\0\0\0".to_vec(),
                });
                expect.push(Op::SetLen { len: 13 });
            }
        }

        let (_, recorder) = writer.shutdown(mode, ctx).await?;
        assert_eq!(&*recorder.recording.lock().unwrap(), &expect);
        Ok(())
    }

    #[tokio::test]
    async fn test_set_len_is_skipped_if_not_needed() -> anyhow::Result<()> {
        let ctx = test_ctx();
        let ctx = &ctx;
        let recorder = RecorderWriter::default();
        let gate = utils::sync::gate::Gate::default();
        let cancel = CancellationToken::new();
        let cap = 4;
        let mut writer = BufferedWriter::<_, RecorderWriter>::new(
            recorder,
            0,
            || IoBufferMut::with_capacity(cap),
            gate.enter()?,
            cancel,
            ctx,
            tracing::Span::none(),
        );

        // write a multiple of `cap`
        writer.write_buffered_borrowed(b"abc", ctx).await?;
        writer.write_buffered_borrowed(b"defgh", ctx).await?;

        let (_, recorder) = writer
            .shutdown(BufferedWriterShutdownMode::PadThenTruncate, ctx)
            .await?;

        let expect = {
            [(0, b"abcd"), (4, b"efgh")]
                .into_iter()
                .map(|(offset, v)| Op::Write {
                    offset,
                    buf: v[..].to_vec(),
                })
                .collect::<Vec<_>>()
        };

        assert_eq!(
            &*recorder.recording.lock().unwrap(),
            &expect,
            "set_len should not be called if the buffer is already aligned"
        );

        Ok(())
    }
}
