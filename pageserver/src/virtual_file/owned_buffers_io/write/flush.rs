use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_epoll_uring::IoBuf;

use crate::{context::RequestContext, virtual_file::owned_buffers_io::io_buf_ext::FullSlice};

use super::{Buffer, OwnedAsyncWriter};

/// A bi-directional channel.
pub struct Duplex<S, R> {
    pub tx: mpsc::Sender<S>,
    pub rx: mpsc::Receiver<R>,
}

/// Creates a bi-directional channel.
///
/// The channel will buffer up to the provided number of messages. Once the buffer is full,
/// attempts to send new messages will wait until a message is received from the channel.
/// The provided buffer capacity must be at least 1.
pub fn duplex_channel<A: Send, B: Send>(buffer: usize) -> (Duplex<A, B>, Duplex<B, A>) {
    let (tx_a, rx_a) = mpsc::channel::<A>(buffer);
    let (tx_b, rx_b) = mpsc::channel::<B>(buffer);

    (Duplex { tx: tx_a, rx: rx_b }, Duplex { tx: tx_b, rx: rx_a })
}

impl<S: Send, R: Send> Duplex<S, R> {
    /// Sends a value, waiting until there is capacity.
    ///
    /// A successful send occurs when it is determined that the other end of the channel has not hung up already.
    pub async fn send(&self, x: S) -> Result<(), mpsc::error::SendError<S>> {
        self.tx.send(x).await
    }

    /// Receives the next value for this receiver.
    ///
    /// This method returns `None` if the channel has been closed and there are
    /// no remaining messages in the channel's buffer.
    pub async fn recv(&mut self) -> Option<R> {
        self.rx.recv().await
    }
}

pub struct FlushHandleInner<Buf, W> {
    /// A bi-directional channel that sends (buffer, offset) for writes,
    /// and receives recyled buffer.
    channel: Duplex<(FullSlice<Buf>, u64), FullSlice<Buf>>,
    /// Join handle for the background flush task.
    join_handle: tokio::task::JoinHandle<std::io::Result<Arc<W>>>,
}

/// A handle to the flush task.
pub struct FlushHandle<Buf, W> {
    inner: Option<FlushHandleInner<Buf, W>>,
    /// Buffer for serving tail reads.
    pub(super) maybe_flushed: Option<Buf>,
}

pub struct FlushBackgroundTask<Buf, W> {
    /// A bi-directional channel that receives (buffer, offset) for writes,
    /// and send back recycled buffer.
    channel: Duplex<FullSlice<Buf>, (FullSlice<Buf>, u64)>,
    writer: Arc<W>,
    ctx: RequestContext,
}

impl<Buf, W> FlushBackgroundTask<Buf, W>
where
    Buf: IoBuf + Send + Sync,
    W: OwnedAsyncWriter + Sync + 'static,
{
    fn new(
        channel: Duplex<FullSlice<Buf>, (FullSlice<Buf>, u64)>,
        file: Arc<W>,
        ctx: RequestContext,
    ) -> Self {
        FlushBackgroundTask {
            channel,
            writer: file,
            ctx,
        }
    }

    /// Runs the background flush task.
    async fn run(mut self, slice: FullSlice<Buf>) -> std::io::Result<Arc<W>> {
        self.channel.send(slice).await.map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "flush handle closed early")
        })?;

        //  Exit condition: channel is closed and there is no remaining buffer to be flushed
        while let Some((slice, offset)) = self.channel.recv().await {
            let slice = self.writer.write_all_at(slice, offset, &self.ctx).await?;

            if self.channel.send(slice).await.is_err() {
                // Although channel is closed. Still need to finish flushing the remaining buffers.
                continue;
            }
        }

        Ok(self.writer)
    }
}

impl<Buf, W> FlushHandle<Buf, W>
where
    Buf: IoBuf + Send + Sync + Clone,
    W: OwnedAsyncWriter + Send + Sync + 'static + std::fmt::Debug,
{
    /// Spawns a new background flush task and obtains a handle.
    pub fn spawn_new<B>(file: Arc<W>, buf: B, ctx: RequestContext) -> Self
    where
        B: Buffer<IoBuf = Buf> + Send + 'static,
    {
        let (front, back) = duplex_channel(2);

        let bg = FlushBackgroundTask::new(back, file, ctx);
        let join_handle = tokio::spawn(async move { bg.run(buf.flush()).await });

        FlushHandle {
            inner: Some(FlushHandleInner {
                channel: front,
                join_handle,
            }),
            maybe_flushed: None,
        }
    }

    /// Submits a buffer to be flushed in the background task.
    /// Returns a buffer that completed flushing for re-use, length reset to 0, capacity unchanged.
    pub async fn flush<B>(&mut self, buf: B, offset: u64) -> std::io::Result<B>
    where
        B: Buffer<IoBuf = Buf> + Send + 'static,
    {
        let freezed = buf.flush();

        self.maybe_flushed
            .replace(freezed.as_raw_slice().get_ref().clone());

        let submit = self.inner_mut().channel.send((freezed, offset)).await;

        if submit.is_err() {
            return self.handle_error().await;
        }

        // Wait for an available buffer from the background flush task.
        let Some(recycled) = self.inner_mut().channel.recv().await else {
            return self.handle_error().await;
        };

        // The only other place that could hold a reference to the recycled buffer
        // is in `Self::maybe_flushed`, but we have already replace it with the new buffer.
        Ok(Buffer::reuse_after_flush(
            recycled.into_raw_slice().into_inner(),
        ))
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

    fn inner_mut(&mut self) -> &mut FlushHandleInner<Buf, W> {
        self.inner
            .as_mut()
            .expect("must not use after we returned an error")
    }

    async fn handle_error<T>(&mut self) -> std::io::Result<T> {
        Err(self.shutdown().await.unwrap_err())
    }
}
