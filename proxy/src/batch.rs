//! Batch processing system based on intrusive linked lists.
//!
//! Enqueuing a batch job requires no allocations, with
//! direct support for cancelling jobs early.
use crate::ext::LockExt;
use std::{
    collections::BTreeMap,
    panic::{AssertUnwindSafe, UnwindSafe},
    pin::pin,
    sync::Mutex,
};

use futures::future::Either;
use scopeguard::ScopeGuard;

pub trait QueueProcessing: UnwindSafe + Send + 'static {
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

impl<P: QueueProcessing> QueueProcessing for AssertUnwindSafe<P> {
    type Req = P::Req;
    type Res = P::Res;

    fn batch_size(&self, queue_size: usize) -> usize {
        self.0.batch_size(queue_size)
    }

    fn apply(&mut self, req: Vec<Self::Req>) -> impl Future<Output = Vec<Self::Res>> + Send {
        self.0.apply(req)
    }
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

    pub async fn call(&self, req: P::Req) -> P::Res {
        let (id, rx) = self.inner.lock_propagate_poison().register_job(req);

        let guard = scopeguard::guard(&self.inner, move |inner| {
            let mut inner = inner.lock_propagate_poison();
            inner.queue.remove(&id);
        });

        // try become the leader, or try wait for success.
        let leader = pin!(self.processor.lock());
        let resp = match futures::future::select(pin!(rx), leader).await {
            // we got the resp.
            Either::Left((resp, _)) => resp,
            // we are the leader.
            Either::Right((mut processor, rx)) => {
                let (reqs, resps) = self.inner.lock_propagate_poison().get_batch(&processor);

                // apply a batch.
                let values = processor.apply(reqs).await;

                // send response values.
                for (tx, value) in std::iter::zip(resps, values) {
                    // sender hung up but that's fine.
                    drop(tx.send(value));
                }

                rx.await
            }
        };

        // already removed.
        let _ = ScopeGuard::into_inner(guard);

        resp.expect("no response found. batch processer should not panic")
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
        self.version += 1;

        self.queue.insert(id, BatchJob { req, res: tx });

        (id, rx)
    }

    fn get_batch(&mut self, p: &P) -> (Vec<P::Req>, Vec<tokio::sync::oneshot::Sender<P::Res>>) {
        let batch_size = p.batch_size(self.queue.len());
        let mut reqs = Vec::with_capacity(batch_size);
        let mut resps = Vec::with_capacity(batch_size);

        while reqs.len() < batch_size {
            let Some((_, job)) = self.queue.pop_first() else {
                break;
            };
            reqs.push(job.req);
            resps.push(job.res);
        }

        (reqs, resps)
    }
}
