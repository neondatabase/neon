use std::ops::ControlFlow;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, info_span, warn};
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
    channel: duplex::mpsc::Duplex<FlushRequest<Buf>, FullSlice<Buf>>,
    /// Join handle for the background flush task.
    join_handle: tokio::task::JoinHandle<std::io::Result<Arc<W>>>,
}

struct FlushRequest<Buf> {
    slice: FullSlice<Buf>,
    offset: u64,
    #[cfg(test)]
    ready_to_flush_rx: tokio::sync::oneshot::Receiver<()>,
    #[cfg(test)]
    done_flush_tx: tokio::sync::oneshot::Sender<()>,
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
        ready_to_flush_rx,
        done_flush_tx,
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
    /// Note: The background task so we do not need to explicitly maintain a queue of buffers.
    pub fn spawn_new<B>(
        file: Arc<W>,
        buf: B,
        gate_guard: utils::sync::gate::GateGuard,
        cancel: CancellationToken,
        ctx: RequestContext,
        span: tracing::Span,
    ) -> Self
    where
        B: Buffer<IoBuf = Buf> + Send + 'static,
    {
        // It is fine to buffer up to only 1 message. We only 1 message in-flight at a time.
        let (front, back) = duplex::mpsc::channel(1);

        let join_handle = tokio::spawn(
            async move {
                FlushBackgroundTask::new(back, file, gate_guard, cancel, ctx)
                    .run(buf.flush())
                    .await
            }
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
    ) -> std::io::Result<(B, FlushControl)> {
        let (request, flush_control) = new_flush_op(slice, offset);

        // Submits the buffer to the background task.
        let submit = self.inner_mut().channel.send(request).await;
        if submit.is_err() {
            return self.handle_error().await;
        }

        // Wait for an available buffer from the background flush task.
        // This is the BACKPRESSURE mechanism: if the flush task can't keep up,
        // then the write path will eventually wait for it here.
        let Some(recycled) = self.inner_mut().channel.recv().await else {
            return self.handle_error().await;
        };

                Ok((recycled, flush_control))
    }

    async fn handle_error<T>(&mut self) -> std::io::Result<T> {
        Err(self
            .shutdown()
            .await
            .expect_err("flush task only disconnects duplex if it exits with an error"))
    }

    /// Cleans up the channel, join the flush task.
    pub async fn shutdown(&mut self) -> std::io::Result<Arc<W>> {
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
    channel: duplex::mpsc::Duplex<FullSlice<Buf>, FlushRequest<Buf>>,
    /// A writter for persisting data to disk.
    writer: Arc<W>,
    ctx: RequestContext,
    cancel: CancellationToken,
    /// Prevent timeline from shuting down until the flush background task finishes flushing all remaining buffers to disk.
    _gate_guard: utils::sync::gate::GateGuard,
}

impl<Buf, W> FlushBackgroundTask<Buf, W>
where
    Buf: IoBufAligned + Send + Sync,
    W: OwnedAsyncWriter + Sync + 'static,
{
    /// Creates a new background flush task.
    fn new(
        channel: duplex::mpsc::Duplex<FullSlice<Buf>, FlushRequest<Buf>>,
        file: Arc<W>,
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
    /// The passed in slice is immediately sent back to the flush handle through the duplex channel.
    async fn run(mut self, slice: FullSlice<Buf>) -> std::io::Result<Arc<W>> {
        // Sends the extra buffer back to the handle.
        // TODO: can this ever await and or fail? I think not.
        self.channel.send(slice).await.map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "flush handle closed early")
        })?;

        //  Exit condition: channel is closed and there is no remaining buffer to be flushed
        while let Some(request) = self.channel.recv().await {
            #[cfg(test)]
            {
                // In test, wait for control to signal that we are ready to flush.
                if request.ready_to_flush_rx.await.is_err() {
                    tracing::debug!("control dropped");
                }
            }

            // Write slice to disk at `offset`.
            //
            // Error handling happens according to the current policy of crashing
            // on fatal IO errors and retrying in place otherwise (deeming all other errors retryable).
            // (The upper layers of the Pageserver write path are not equipped to retry write errors
            //  becasuse they often deallocate the buffers that were already written).
            //
            // TODO: cancellation sensitiity.
            // Without it, if we hit a bug where retrying is never successful,
            // then we can't shut down the timeline/tenant/pageserver cleanly because
            // layers of the Pageserver write path are holding the gate open for EphemeralFile.
            //
            // TODO: use utils::backoff::retry once async closures are actually usable
            //
            let mut slice_storage = Some(request.slice);
            for attempt in 1.. {
                if self.cancel.is_cancelled() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "BufferedWriter cancellation token is cancelled",
                    ));
                }
                let result = async {
                    if attempt > 1 {
                        info!("retrying flush");
                    }
                    let slice = slice_storage.take().expect(
                        "likely previous invocation of this future didn't get polled to completion",
                    );
                    // Don't cancel this write by doing tokio::select with self.cancel.cancelled().
                    // The underlying tokio-epoll-uring slot / kernel operation is still ongoing and occupies resources.
                    // If we retry indefinitely, we'll deplete those resources.
                    // Future: teach tokio-epoll-uring io_uring operation cancellation, but still,
                    // wait for cancelled ops to complete and discard their error.
                    let (slice, res) = self.writer.write_all_at(slice, request.offset, &self.ctx).await;
                    slice_storage = Some(slice);
                    let res = res.maybe_fatal_err("owned_buffers_io flush");
                    let Err(err) = res else {
                        return ControlFlow::Break(());
                    };
                    warn!(%err, "error flushing buffered writer buffer to disk, retrying after backoff");
                    utils::backoff::exponential_backoff(attempt, 1.0, 10.0, &self.cancel).await;
                    ControlFlow::Continue(())
                }
                .instrument(info_span!("flush_attempt", %attempt))
                .await;
                match result {
                    ControlFlow::Break(()) => break,
                    ControlFlow::Continue(()) => continue,
                }
            }
            let slice = slice_storage.expect("loop must have run at least once");

            #[cfg(test)]
            {
                // In test, tell control we are done flushing buffer.
                if request.done_flush_tx.send(()).is_err() {
                    tracing::debug!("control dropped");
                }
            }

            // Sends the buffer back to the handle for reuse. The handle is in charged of cleaning the buffer.
            if self.channel.send(slice).await.is_err() {
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
