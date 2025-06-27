//! Batch processing system based on intrusive linked lists.
//!
//! Enqueuing a batch job requires no allocations, with
//! direct support for cancelling jobs early.
use std::collections::BTreeMap;
use std::pin::pin;
use std::sync::Mutex;

use scopeguard::ScopeGuard;
use tokio::sync::oneshot::error::TryRecvError;

use crate::ext::LockExt;

pub trait QueueProcessing: Send + 'static {
    type Req: Send + 'static;
    type Res: Send;

    /// Get the desired batch size.
    fn batch_size(&self, queue_size: usize) -> usize;

    /// This applies a full batch of events.
    /// Must respond with a full batch of replies.
    ///
    /// If this apply can error, it's expected that errors be forwarded to each Self::Res.
    ///
    /// Batching does not need to happen atomically.
    fn apply(&mut self, req: Vec<Self::Req>) -> impl Future<Output = Vec<Self::Res>> + Send;
}

pub struct BatchQueue<P: QueueProcessing> {
    processor: tokio::sync::Mutex<P>,
    inner: Mutex<BatchQueueInner<P>>,
}

struct BatchJob<P: QueueProcessing> {
    req: P::Req,
    res: tokio::sync::oneshot::Sender<P::Res>,
}

impl<P: QueueProcessing> BatchQueue<P> {
    pub fn new(p: P) -> Self {
        Self {
            processor: tokio::sync::Mutex::new(p),
            inner: Mutex::new(BatchQueueInner {
                version: 0,
                queue: BTreeMap::new(),
            }),
        }
    }

    /// Perform a single request-response process, this may be batched internally.
    ///
    /// This function is not cancel safe.
    pub async fn call<R>(
        &self,
        req: P::Req,
        cancelled: impl Future<Output = R>,
    ) -> Result<P::Res, R> {
        let (id, mut rx) = self.inner.lock_propagate_poison().register_job(req);

        let mut cancelled = pin!(cancelled);
        let resp = loop {
            // try become the leader, or try wait for success.
            let mut processor = tokio::select! {
                // try become leader.
                p = self.processor.lock() => p,
                // wait for success.
                resp = &mut rx => break resp.ok(),
                // wait for cancellation.
                cancel = cancelled.as_mut() => {
                    let mut inner = self.inner.lock_propagate_poison();
                    if inner.queue.remove(&id).is_some() {
                        tracing::warn!("batched task cancelled before completion");
                    }
                    return Err(cancel);
                },
            };

            tracing::debug!(id, "batch: became leader");
            let (reqs, resps) = self.inner.lock_propagate_poison().get_batch(&processor);

            // snitch incase the task gets cancelled.
            let cancel_safety = scopeguard::guard((), |()| {
                if !std::thread::panicking() {
                    tracing::error!(
                        id,
                        "batch: leader cancelled, despite not being cancellation safe"
                    );
                }
            });

            // apply a batch.
            // if this is cancelled, jobs will not be completed and will panic.
            let values = processor.apply(reqs).await;

            // good: we didn't get cancelled.
            ScopeGuard::into_inner(cancel_safety);

            if values.len() != resps.len() {
                tracing::error!(
                    "batch: invalid response size, expected={}, got={}",
                    resps.len(),
                    values.len()
                );
            }

            // send response values.
            for (tx, value) in std::iter::zip(resps, values) {
                if tx.send(value).is_err() {
                    // receiver hung up but that's fine.
                }
            }

            match rx.try_recv() {
                Ok(resp) => break Some(resp),
                Err(TryRecvError::Closed) => break None,
                // edge case - there was a race condition where
                // we became the leader but were not in the batch.
                //
                // Example:
                // thread 1: register job id=1
                // thread 2: register job id=2
                // thread 2: processor.lock().await
                // thread 1: processor.lock().await
                // thread 2: becomes leader, batch_size=1, jobs=[1].
                Err(TryRecvError::Empty) => {}
            }
        };

        tracing::debug!(id, "batch: job completed");

        Ok(resp.expect("no response found. batch processer should not panic"))
    }
}

struct BatchQueueInner<P: QueueProcessing> {
    version: u64,
    queue: BTreeMap<u64, BatchJob<P>>,
}

impl<P: QueueProcessing> BatchQueueInner<P> {
    fn register_job(&mut self, req: P::Req) -> (u64, tokio::sync::oneshot::Receiver<P::Res>) {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let id = self.version;

        // Overflow concern:
        // This is a u64, and we might enqueue 2^16 tasks per second.
        // This gives us 2^48 seconds (9 million years).
        // Even if this does overflow, it will not break, but some
        // jobs with the higher version might never get prioritised.
        self.version += 1;

        self.queue.insert(id, BatchJob { req, res: tx });

        tracing::debug!(id, "batch: registered job in the queue");

        (id, rx)
    }

    fn get_batch(&mut self, p: &P) -> (Vec<P::Req>, Vec<tokio::sync::oneshot::Sender<P::Res>>) {
        let batch_size = p.batch_size(self.queue.len());
        let mut reqs = Vec::with_capacity(batch_size);
        let mut resps = Vec::with_capacity(batch_size);
        let mut ids = Vec::with_capacity(batch_size);

        while reqs.len() < batch_size {
            let Some((id, job)) = self.queue.pop_first() else {
                break;
            };
            reqs.push(job.req);
            resps.push(job.res);
            ids.push(id);
        }

        tracing::debug!(ids=?ids, "batch: acquired jobs");

        (reqs, resps)
    }
}
