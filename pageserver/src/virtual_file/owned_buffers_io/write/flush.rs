use std::ops::ControlFlow;

use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span, warn};
use utils::sync::duplex;

use super::{Buffer, CheapCloneForRead, OwnedAsyncWriter};
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
    #[cfg(test)]
    ready_to_flush_rx: Option<tokio::sync::oneshot::Receiver<()>>,
    #[cfg(test)]
    done_flush_tx: Option<tokio::sync::oneshot::Sender<()>>,
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

/// Constructs a request and a control object for a new flush operation.
#[cfg(not(test))]
fn new_flush_op<Buf>(slice: FullSlice<Buf>, offset: u64) -> (FlushRequest<Buf>, FlushControl) {
    let request = FlushRequest { slice, offset };
    let control = FlushControl::untracked();

    (request, control)
}

/// Constructs a request and a control object for a new flush operation.
#[cfg(test)]
fn new_flush_op<Buf>(slice: FullSlice<Buf>, offset: u64) -> (FlushRequest<Buf>, FlushControl) {
    let (ready_to_flush_tx, ready_to_flush_rx) = tokio::sync::oneshot::channel();
    let (done_flush_tx, done_flush_rx) = tokio::sync::oneshot::channel();
    let control = FlushControl::not_started(ready_to_flush_tx, done_flush_rx);

    let request = FlushRequest {
        slice,
        offset,
        ready_to_flush_rx: Some(ready_to_flush_rx),
        done_flush_tx: Some(done_flush_tx),
    };
    (request, control)
}

/// A handle to a `FlushRequest` that allows unit tests precise control over flush behavior.
#[cfg(test)]
pub(crate) struct FlushControl {
    not_started: FlushNotStarted,
}

#[cfg(not(test))]
pub(crate) struct FlushControl;

impl FlushControl {
    #[cfg(test)]
    fn not_started(
        ready_to_flush_tx: tokio::sync::oneshot::Sender<()>,
        done_flush_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Self {
        FlushControl {
            not_started: FlushNotStarted {
                ready_to_flush_tx,
                done_flush_rx,
            },
        }
    }

    #[cfg(not(test))]
    fn untracked() -> Self {
        FlushControl
    }

    /// In tests, turn flush control into a not started state.
    #[cfg(test)]
    pub(crate) fn into_not_started(self) -> FlushNotStarted {
        self.not_started
    }

    /// Release control to the submitted buffer.
    ///
    /// In `cfg(test)` environment, the buffer is guranteed to be flushed to disk after [`FlushControl::release`] is finishes execution.
    pub async fn release(self) {
        #[cfg(test)]
        {
            self.not_started
                .ready_to_flush()
                .wait_until_flush_is_done()
                .await;
        }
    }
}

impl<Buf, W> FlushHandle<Buf, W>
where
    Buf: IoBufAligned + Send + Sync + CheapCloneForRead,
    W: OwnedAsyncWriter + Send + Sync + 'static + std::fmt::Debug,
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
    ) -> Result<(FullSlice<Buf>, FlushControl), FlushTaskError> {
        let (request, flush_control) = new_flush_op(slice, offset);

        // Submits the buffer to the background task.
        self.send(Request::Flush(request)).await?;

        // Wait for an available buffer from the background flush task.
        // This is the BACKPRESSURE mechanism: if the flush task can't keep up,
        // then the write path will eventually wait for it here.
        let Some(recycled) = self.inner_mut().channel.recv().await else {
            return self.handle_error().await;
        };

        Ok((recycled, flush_control))
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

impl FlushTaskError {
    pub fn is_cancel(&self) -> bool {
        match self {
            FlushTaskError::Cancelled => true,
        }
    }
    pub fn into_anyhow(self) -> anyhow::Error {
        match self {
            FlushTaskError::Cancelled => anyhow::anyhow!(self),
        }
    }
}

impl<Buf, W> FlushBackgroundTask<Buf, W>
where
    Buf: IoBufAligned + Send + Sync,
    W: OwnedAsyncWriter + Sync + 'static,
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
                    let writer = &self.writer;
                    let request_storage = &mut request_storage;
                    let ctx = &self.ctx;
                    let io_fut = match request {
                        Request::Flush(FlushRequest { slice, offset, #[cfg(test)] ready_to_flush_rx, #[cfg(test)] done_flush_tx }) => futures::future::Either::Left(async move {
                            #[cfg(test)]
                            if let Some(ready_to_flush_rx) = ready_to_flush_rx {
                                {
                                    // In test, wait for control to signal that we are ready to flush.
                                    if ready_to_flush_rx.await.is_err() {
                                        tracing::debug!("control dropped");
                                    }
                                }
                            }
                            let (slice, res) = writer.write_all_at(slice, offset, ctx).await;
                            *request_storage = Some(Request::Flush(FlushRequest {
                                slice,
                                offset,
                                #[cfg(test)]
                                ready_to_flush_rx: None, // the contract is that we notify before first attempt
                                #[cfg(test)]
                                done_flush_tx
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
                Request::Flush(FlushRequest {
                    slice,
                    #[cfg(test)]
                    mut done_flush_tx,
                    ..
                }) => {
                    #[cfg(test)]
                    {
                        // In test, tell control we are done flushing buffer.
                        if done_flush_tx.take().expect("always Some").send(()).is_err() {
                            tracing::debug!("control dropped");
                        }
                    }
                    slice
                }
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

        Ok(self.writer)
    }
}

#[cfg(test)]
pub(crate) struct FlushNotStarted {
    ready_to_flush_tx: tokio::sync::oneshot::Sender<()>,
    done_flush_rx: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(test)]
pub(crate) struct FlushInProgress {
    done_flush_rx: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(test)]
pub(crate) struct FlushDone;

#[cfg(test)]
impl FlushNotStarted {
    /// Signals the background task the buffer is ready to flush to disk.
    pub fn ready_to_flush(self) -> FlushInProgress {
        self.ready_to_flush_tx
            .send(())
            .map(|_| FlushInProgress {
                done_flush_rx: self.done_flush_rx,
            })
            .unwrap()
    }
}

#[cfg(test)]
impl FlushInProgress {
    /// Waits until background flush is done.
    pub async fn wait_until_flush_is_done(self) -> FlushDone {
        self.done_flush_rx.await.unwrap();
        FlushDone
    }
}
