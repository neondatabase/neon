use std::sync::Arc;

use tokio::sync::mpsc;

use crate::{
    context::RequestContext,
    virtual_file::{IoBuffer, IoBufferMut, VirtualFile},
};

use super::{IoBufExt, OwnedAsyncWriter};

pub struct Duplex<S, R> {
    pub tx: mpsc::Sender<S>,
    pub rx: mpsc::Receiver<R>,
}

pub fn duplex_channel<A: Send, B: Send>(buffer: usize) -> (Duplex<A, B>, Duplex<B, A>) {
    let (tx_a, rx_a) = mpsc::channel::<A>(buffer);
    let (tx_b, rx_b) = mpsc::channel::<B>(buffer);

    (Duplex { tx: tx_a, rx: rx_b }, Duplex { tx: tx_b, rx: rx_a })
}

impl<S: Send, R: Send> Duplex<S, R> {
    pub async fn send(&self, x: S) -> Result<(), mpsc::error::SendError<S>> {
        self.tx.send(x).await
    }

    pub async fn recv(&mut self) -> Option<R> {
        self.rx.recv().await
    }
}

/// A handle to the flush task.
pub struct FlushHandle {
    /// A bi-directional channel that sends (buffer, offset) for writes,
    /// and receives recyled buffer.
    channel: Duplex<(IoBuffer, u64), IoBuffer>,
    ///
    maybe_flushed: Option<IoBuffer>,
    join_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

pub struct FlushBackgroundTask {
    channel: Duplex<IoBuffer, (IoBuffer, u64)>,
    file: Arc<VirtualFile>,
    ctx: RequestContext,
}

impl FlushBackgroundTask {
    fn new(
        channel: Duplex<IoBuffer, (IoBuffer, u64)>,
        file: Arc<VirtualFile>,
        ctx: RequestContext,
    ) -> Self {
        FlushBackgroundTask { channel, file, ctx }
    }

    async fn run(mut self, buf: IoBufferMut) -> anyhow::Result<()> {
        self.channel.send(buf.freeze()).await?;

        while let Some((mut buf, offset)) = self.channel.recv().await {
            let slice = OwnedAsyncWriter::write_all_at(
                self.file.as_ref(),
                buf.slice_len(),
                offset,
                &self.ctx,
            )
            .await?;
            buf = slice.into_raw_slice().into_inner();

            if self.channel.send(buf).await.is_err() {
                // Channel closed, exit task.
                break;
            }
        }

        Ok(())
    }
}

impl FlushHandle {
    pub fn spawn_new(file: Arc<VirtualFile>, ctx: RequestContext) -> Self {
        let (front, back) = duplex_channel(1);

        let bg = FlushBackgroundTask::new(back, file, ctx);
        let buf = IoBufferMut::with_capacity(4096);
        let join_handle = tokio::spawn(async move { bg.run(buf).await });

        FlushHandle {
            channel: front,
            maybe_flushed: None,
            join_handle,
        }
    }
    /// Submits a buffer to be flushed in the background task.
    /// Returns a buffer that completed flushing for re-use, length reset to 0, capacity unchanged.
    async fn flush(&mut self, buf: IoBufferMut, offset: u64) -> IoBufferMut {
        // Send
        let freezed = buf.freeze();
        self.maybe_flushed.replace(freezed.clone());
        self.channel.send((freezed, offset)).await.unwrap();

        // Wait for an available buffer from the background flush task.
        let recycled = self.channel.recv().await.unwrap();

        // The only other place that could hold a reference to the recycled buffer
        // is in `Self::maybe_flushed`, but we have already replace it with the new buffer.
        let mut recycled = recycled
            .into_mut()
            .expect("buffer should only have one strong reference");

        recycled.clear();
        recycled
    }
}
