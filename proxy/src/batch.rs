//! Batch processing system based on intrusive linked lists.
//!
//! Enqueuing a batch job requires no allocations, with
//! direct support for cancelling jobs early.
use crate::ext::LockExt;
use std::{
    future::poll_fn,
    panic::{AssertUnwindSafe, UnwindSafe},
    pin::{Pin, pin},
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use pin_list::PinList;
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
    inner: Mutex<BatchQueueInner<P>>,
}

impl<P: QueueProcessing> BatchQueue<P> {
    pub fn new(p: P) -> Self {
        Self {
            inner: Mutex::new(BatchQueueInner {
                processor: Some(p),
                version: 0,
                queue: PinList::new(pin_list::id::Checked::new()),
                len: 0,
            }),
        }
    }

    pub async fn call(self: &Arc<Self>, req: P::Req) -> P::Res {
        let node = pin!(pin_list::Node::<BatchQueueTypes<P>>::new());
        let mut node = self.initialise_node(node, req);

        // wait until completion.
        let res = poll_fn(|cx| self.poll_node(node.as_mut(), cx)).await;

        // we don't need the drop guard anymore. we have been removed already.
        let node = ScopeGuard::into_inner(node);
        debug_assert!(node.is_initial());

        res.expect("no response found. batch processer should not panic")
    }

    /// enqueue the node into the queue without allocating any extra memory
    fn initialise_node<'a, 'b>(
        &'a self,
        mut node: Node<'b, P>,
        req: P::Req,
    ) -> ScopeGuard<Node<'b, P>, impl FnOnce(Node<'b, P>), scopeguard::Always> {
        let inner = &mut *self.inner.lock_propagate_poison();
        inner.queue.push_back(
            node.as_mut(),
            (Waker::noop().clone(), Some(req)),
            inner.version,
        );

        // overflows are likely ok.
        inner.version += 1;
        inner.len += 1;

        // ensure node is reset on drop.
        scopeguard::guard(node, move |node| {
            let init = node.initialized_mut().expect("node was initialised");
            let mut inner = self.inner.lock_propagate_poison();

            let (data, _) = init.reset(&mut inner.queue);
            if let pin_list::NodeData::Linked(_) = data {
                inner.len -= 1;
            }
        })
    }

    /// check if the node has a result, or run a batch if this node is the leader.
    fn poll_node(self: &Arc<Self>, node: Node<P>, cx: &mut Context<'_>) -> Poll<Option<P::Res>> {
        let init = node.initialized_mut().expect("node was initialised");
        let inner = &mut *self.inner.lock_propagate_poison();

        // check if another task removed us.
        let init = match init.take_removed(&inner.queue) {
            Ok((res, _version)) => return Poll::Ready(res),
            Err(init) => init,
        };

        // check if we are the leader.
        let version = *init.unprotected();
        let leader = inner.queue.cursor_front().unprotected().copied();
        if Some(version) == leader {
            // we are the leader. get a batch if not already processing.
            if let Some(p) = inner.processor.take() {
                let (versions, batch) = inner.get_batch(&p);

                // run the batch in the background so it cannot be cancelled.
                let this = self.clone();
                tokio::task::spawn(async move {
                    // we need to return our processor, and also clean up the queue on panic.
                    let resps: Vec<P::Res> = vec![];
                    let (p, resps) = &mut *scopeguard::guard((p, resps), |(p, resps)| {
                        let inner = &mut *this.inner.lock_propagate_poison();
                        inner.processor = Some(p);

                        inner.put_responses(versions, resps);
                    });

                    // apply a batch.
                    *resps = p.apply(batch).await;
                });
            }
        }

        // update the waker.
        let entry = init
            .protected_mut(&mut inner.queue)
            .expect("node was not removed");

        entry.0.clone_from(cx.waker());
        Poll::Pending
    }
}

struct BatchQueueInner<P: QueueProcessing> {
    processor: Option<P>,
    version: u64,
    queue: PinList<BatchQueueTypes<P>>,
    len: usize,
}

impl<P: QueueProcessing> BatchQueueInner<P> {
    fn get_batch(&mut self, p: &P) -> (Vec<u64>, Vec<P::Req>) {
        let batch_size = p.batch_size(self.len);
        let mut versions = Vec::with_capacity(batch_size);
        let mut batch = Vec::with_capacity(batch_size);

        let mut cursor = self.queue.cursor_front_mut();
        while batch.len() < batch_size {
            let Some((_waker, req)) = cursor.protected_mut() else {
                break;
            };
            let Some(req) = req.take() else { break };

            let version = *cursor.unprotected().expect("we confirmed the node is init");
            versions.push(version);
            batch.push(req);

            cursor.move_next();
        }

        debug_assert!(!versions.is_empty());

        (versions, batch)
    }

    fn put_responses(&mut self, versions: Vec<u64>, resps: Vec<P::Res>) {
        let mut cursor = self.queue.cursor_front_mut();

        // important: we rely on this being empty if processor::apply panics.
        // resps.next() returning None is symbolic of that poisoned state.
        let mut resps = resps.into_iter();

        for version in versions {
            let resp = resps.next();

            // if this response doesn't match the req, then it means
            // the req was cancelled. drop the response and continue
            if Some(version) != cursor.unprotected().copied() {
                continue;
            }

            // remove the node, giving it the response.
            match cursor.remove_current(resp) {
                // wake the task so it can take the resp.
                Ok((waker, _req)) => waker.wake(),
                Err(_) => unreachable!("req node in queue must be linked"),
            }
            self.len -= 1;
        }

        // tell the next node that they are the new leader.
        if let Some((waker, _req)) = cursor.protected() {
            waker.wake_by_ref();
        }
    }
}

type BatchQueueTypes<P> = dyn pin_list::Types<
        Id = pin_list::id::Checked,
        Protected = (Waker, Option<<P as QueueProcessing>::Req>),
        Removed = Option<<P as QueueProcessing>::Res>,
        Unprotected = u64,
    >;
type Node<'a, P> = Pin<&'a mut pin_list::Node<BatchQueueTypes<P>>>;
