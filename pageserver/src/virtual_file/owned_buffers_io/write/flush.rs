use std::ops::ControlFlow;

use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span, warn};
use utils::sync::duplex;

use super::{Buffer, BufferedWriterSink, CheapCloneForRead};
use crate::context::RequestContext;
use crate::virtual_file::MaybeFatalIo;
use crate::virtual_file::owned_buffers_io::io_buf_aligned::IoBufAligned;
use crate::virtual_file::owned_buffers_io::io_buf_ext::FullSlice;

/// A handle to the flush task.
pub struct FlushHandle<Buf, W> {
    inner: Option<FlushHandleInner<Buf, W>>,
}

pub struct FlushHandleInner<Buf, W> {
    /// A bi-directional channel that sends (buffer, offset) for writes,
    /// and receives recyled buffer.
    channel: duplex::mpsc::Duplex<Request<Buf>, FullSlice<Buf>>,
    /// Join handle for the background flush task.
    join_handle: tokio::task::JoinHandle<Result<W, FlushTaskError>>,
}

struct FlushRequest<Buf> {
    slice: FullSlice<Buf>,
    offset: u64,
}

pub struct ShutdownRequest {
    pub set_len: Option<u64>,
}

enum Request<Buf> {
    Flush(FlushRequest<Buf>),
    Shutdown(ShutdownRequest),
}

impl<Buf> Request<Buf> {
    fn op_str(&self) -> &'static str {
        match self {
            Request::Flush(_) => "flush",
            Request::Shutdown(_) => "shutdown",
        }
    }
}

impl<Buf, W> FlushHandle<Buf, W>
where
    Buf: IoBufAligned + Send + Sync + CheapCloneForRead,
    W: BufferedWriterSink + Send + Sync + 'static + std::fmt::Debug,
{
    /// Spawns a new background flush task and obtains a handle.
    ///
    /// Handle and background task are connected through a duplex channel.
    /// Dirty buffers are sent to the background task for flushing.
    /// Clean buffers are sent back to the handle for reuse.
    ///
    /// The queue depth is 1, and the passed-in `buf` seeds the queue depth.
    /// I.e., the passed-in buf is immediately available to the handle as a recycled buffer.
    pub fn spawn_new<B>(
        file: W,
        buf: B,
        gate_guard: utils::sync::gate::GateGuard,
        cancel: CancellationToken,
        ctx: RequestContext,
        span: tracing::Span,
    ) -> Self
    where
        B: Buffer<IoBuf = Buf> + Send + 'static,
    {
        let (front, back) = duplex::mpsc::channel(1);
        back.try_send(buf.flush())
            .expect("we just created it with capacity 1");

        let join_handle = tokio::spawn(
            FlushBackgroundTask::new(back, file, gate_guard, cancel, ctx)
                .run()
                .instrument(span),
        );

        FlushHandle {
            inner: Some(FlushHandleInner {
                channel: front,
                join_handle,
            }),
        }
    }

    /// Submits a buffer to be flushed in the background task.
    /// Returns a buffer that completed flushing for re-use, length reset to 0, capacity unchanged.
    /// If `save_buf_for_read` is true, then we save the buffer in `Self::maybe_flushed`, otherwise
    /// clear `maybe_flushed`.
    pub async fn flush(
        &mut self,
        slice: FullSlice<Buf>,
        offset: u64,
    ) -> Result<FullSlice<Buf>, FlushTaskError> {
        let request = FlushRequest { slice, offset };

        // Submits the buffer to the background task.
        self.send(Request::Flush(request)).await?;

        // Wait for an available buffer from the background flush task.
        // This is the BACKPRESSURE mechanism: if the flush task can't keep up,
        // then the write path will eventually wait for it here.
        let Some(recycled) = self.inner_mut().channel.recv().await else {
            return self.handle_error().await;
        };

        Ok(recycled)
    }

    /// Sends poison pill to flush task and waits for it to exit.
    pub async fn shutdown(&mut self, req: ShutdownRequest) -> Result<W, FlushTaskError> {
        self.send(Request::Shutdown(req)).await?;
        self.wait().await
    }

    async fn send(&mut self, request: Request<Buf>) -> Result<(), FlushTaskError> {
        let submit = self.inner_mut().channel.send(request).await;
        if submit.is_err() {
            return self.handle_error().await;
        }
        Ok(())
    }

    async fn handle_error<T>(&mut self) -> Result<T, FlushTaskError> {
        Err(self
            .wait()
            .await
            .expect_err("flush task only disconnects duplex if it exits with an error"))
    }

    async fn wait(&mut self) -> Result<W, FlushTaskError> {
        let handle = self
            .inner
            .take()
            .expect("must not use after we returned an error");
        drop(handle.channel.tx);
        handle.join_handle.await.unwrap()
    }

    /// Gets a mutable reference to the inner handle. Panics if [`Self::inner`] is `None`.
    /// This only happens if the handle is used after an error.
    fn inner_mut(&mut self) -> &mut FlushHandleInner<Buf, W> {
        self.inner
            .as_mut()
            .expect("must not use after we returned an error")
    }
}

/// A background task for flushing data to disk.
pub struct FlushBackgroundTask<Buf, W> {
    /// A bi-directional channel that receives (buffer, offset) for writes,
    /// and send back recycled buffer.
    channel: duplex::mpsc::Duplex<FullSlice<Buf>, Request<Buf>>,
    /// A writter for persisting data to disk.
    writer: W,
    ctx: RequestContext,
    cancel: CancellationToken,
    /// Prevent timeline from shuting down until the flush background task finishes flushing all remaining buffers to disk.
    _gate_guard: utils::sync::gate::GateGuard,
}

#[derive(Debug, thiserror::Error)]
pub enum FlushTaskError {
    #[error("flush task cancelled")]
    Cancelled,
}

impl<Buf, W> FlushBackgroundTask<Buf, W>
where
    Buf: IoBufAligned + Send + Sync,
    W: BufferedWriterSink + Sync + 'static,
{
    /// Creates a new background flush task.
    fn new(
        channel: duplex::mpsc::Duplex<FullSlice<Buf>, Request<Buf>>,
        file: W,
        gate_guard: utils::sync::gate::GateGuard,
        cancel: CancellationToken,
        ctx: RequestContext,
    ) -> Self {
        FlushBackgroundTask {
            channel,
            writer: file,
            _gate_guard: gate_guard,
            cancel,
            ctx,
        }
    }

    /// Runs the background flush task.
    async fn run(mut self) -> Result<W, FlushTaskError> {
        let writer = scopeguard::guard(self.writer, |writer| {
            writer.cleanup();
        });
        //  Exit condition: channel is closed and there is no remaining buffer to be flushed
        while let Some(request) = self.channel.recv().await {
            let op_kind = request.op_str();

            // Perform the requested operation.
            //
            // Error handling happens according to the current policy of crashing
            // on fatal IO errors and retrying in place otherwise (deeming all other errors retryable).
            // (The upper layers of the Pageserver write path are not equipped to retry write errors
            //  becasuse they often deallocate the buffers that were already written).
            //
            // TODO: use utils::backoff::retry once async closures are actually usable
            //
            let mut request_storage = Some(request);
            for attempt in 1.. {
                if self.cancel.is_cancelled() {
                    return Err(FlushTaskError::Cancelled);
                }
                let result = async {
                    let request: Request<Buf> = request_storage .take().expect(
                        "likely previous invocation of this future didn't get polled to completion",
                    );
                    match &request {
                        Request::Shutdown(ShutdownRequest { set_len: None }) => {
                            request_storage = Some(request);
                            return ControlFlow::Break(());
                        },
                        Request::Flush(_) | Request::Shutdown(ShutdownRequest { set_len: Some(_) }) => {
                        },
                    }
                    if attempt > 1 {
                        warn!(op=%request.op_str(), "retrying");
                    }
                    // borrows so we can async move the requests into async block while not moving these borrows here
                    let writer = &writer;
                    let request_storage = &mut request_storage;
                    let ctx = &self.ctx;
                    let io_fut = match request {
                        Request::Flush(FlushRequest { slice, offset }) => futures::future::Either::Left(async move {
                            let (slice, res) = writer.write_all_at(slice, offset, ctx).await;
                            *request_storage = Some(Request::Flush(FlushRequest {
                                slice,
                                offset,
                            }));
                            res
                        }),
                        Request::Shutdown(ShutdownRequest { set_len }) => futures::future::Either::Right(async move {
                            let set_len = set_len.expect("we filter out the None case above");
                            let res = writer.set_len(set_len, ctx).await;
                            *request_storage = Some(Request::Shutdown(ShutdownRequest {
                                set_len: Some(set_len),
                            }));
                            res
                        }),
                    };
                    // Don't cancel the io_fut by doing tokio::select with self.cancel.cancelled().
                    // The underlying tokio-epoll-uring slot / kernel operation is still ongoing and occupies resources.
                    // If we retry indefinitely, we'll deplete those resources.
                    // Future: teach tokio-epoll-uring io_uring operation cancellation, but still,
                    // wait for cancelled ops to complete and discard their error.
                    let res = io_fut.await;
                    let res = res.maybe_fatal_err("owned_buffers_io flush");
                    let Err(err) = res else {
                        if attempt > 1 {
                            warn!(op=%op_kind, "retry succeeded");
                        }
                        return ControlFlow::Break(());
                    };
                    warn!(%err, "error flushing buffered writer buffer to disk, retrying after backoff");
                    utils::backoff::exponential_backoff(attempt, 1.0, 10.0, &self.cancel).await;
                    ControlFlow::Continue(())
                }
                .instrument(info_span!("attempt", %attempt, %op_kind))
                .await;
                match result {
                    ControlFlow::Break(()) => break,
                    ControlFlow::Continue(()) => continue,
                }
            }
            let request = request_storage.expect("loop must have run at least once");

            let slice = match request {
                Request::Flush(FlushRequest { slice, .. }) => slice,
                Request::Shutdown(_) => {
                    // next iteration will observe recv() returning None
                    continue;
                }
            };

            // Sends the buffer back to the handle for reuse. The handle is in charged of cleaning the buffer.
            let send_res = self.channel.send(slice).await;
            if send_res.is_err() {
                // Although channel is closed. Still need to finish flushing the remaining buffers.
                continue;
            }
        }

        Ok(scopeguard::ScopeGuard::into_inner(writer))
    }
}
