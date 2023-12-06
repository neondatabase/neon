//! Synchronization primitive to prevent starvation among concurrent tasks that do the same work.

use std::{
    collections::VecDeque,
    fmt,
    future::poll_fn,
    sync::Mutex,
    task::{Poll, Waker},
};

pub struct Queue<T> {
    inner: Mutex<Inner<T>>,
}

struct Inner<T> {
    waiters: VecDeque<usize>,
    free: VecDeque<usize>,
    slots: Vec<Option<(Option<Waker>, Option<T>)>>,
}

#[derive(Clone, Copy)]
pub struct Position<'q, T> {
    idx: usize,
    queue: &'q Queue<T>,
}

impl<T> fmt::Debug for Position<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Position").field("idx", &self.idx).finish()
    }
}

impl<T> Inner<T> {
    #[cfg(not(test))]
    #[inline]
    fn integrity_check(&self) {}

    #[cfg(test)]
    fn integrity_check(&self) {
        use std::collections::HashSet;
        let waiters = self.waiters.iter().copied().collect::<HashSet<_>>();
        let free = self.free.iter().copied().collect::<HashSet<_>>();
        for (slot_idx, slot) in self.slots.iter().enumerate() {
            match slot {
                None => {
                    assert!(!waiters.contains(&slot_idx));
                    assert!(free.contains(&slot_idx));
                }
                Some((None, None)) => {
                    assert!(waiters.contains(&slot_idx));
                    assert!(!free.contains(&slot_idx));
                }
                Some((Some(_), Some(_))) => {
                    assert!(!waiters.contains(&slot_idx));
                    assert!(!free.contains(&slot_idx));
                }
                Some((Some(_), None)) => {
                    assert!(waiters.contains(&slot_idx));
                    assert!(!free.contains(&slot_idx));
                }
                Some((None, Some(_))) => {
                    assert!(!waiters.contains(&slot_idx));
                    assert!(!free.contains(&slot_idx));
                }
            }
        }
    }
}

impl<T> Queue<T> {
    pub fn new(size: usize) -> Self {
        Queue {
            inner: Mutex::new(Inner {
                waiters: VecDeque::new(),
                free: (0..size).collect(),
                slots: {
                    let mut v = Vec::with_capacity(size);
                    v.resize_with(size, || None);
                    v
                },
            }),
        }
    }
    pub fn begin(&self) -> Result<Position<T>, ()> {
        #[cfg(test)]
        tracing::trace!("get in line locking inner");
        let mut inner = self.inner.lock().unwrap();
        inner.integrity_check();
        let my_waitslot_idx = inner
            .free
            .pop_front()
            .expect("can't happen, len(slots) = len(waiters");
        inner.waiters.push_back(my_waitslot_idx);
        let prev = inner.slots[my_waitslot_idx].replace((None, None));
        assert!(prev.is_none());
        inner.integrity_check();
        Ok(Position {
            idx: my_waitslot_idx,
            queue: &self,
        })
    }
}

impl<'q, T> Position<'q, T> {
    pub fn complete_and_wait(self, datum: T) -> impl std::future::Future<Output = T> + 'q {
        #[cfg(test)]
        tracing::trace!("found victim locking waiters");
        let mut inner = self.queue.inner.lock().unwrap();
        inner.integrity_check();
        let winner_idx = inner.waiters.pop_front().expect("we put ourselves in");
        #[cfg(test)]
        tracing::trace!(winner_idx, "putting victim into next waiters slot");
        let winner_slot = inner.slots[winner_idx].as_mut().unwrap();
        let prev = winner_slot.1.replace(datum);
        assert!(
            prev.is_none(),
            "ensure we didn't mess up this simple ring buffer structure"
        );
        if let Some(waker) = winner_slot.0.take() {
            #[cfg(test)]
            tracing::trace!(winner_idx, "waking up winner");
            waker.wake()
        }
        inner.integrity_check();
        drop(inner); // the poll_fn locks it again

        let mut poll_num = 0;
        let mut drop_guard = Some(scopeguard::guard((), |()| {
            panic!("must not drop this future until Ready");
        }));

        // take the victim that was found by someone else
        poll_fn(move |cx| {
            let my_waitslot_idx = self.idx;
            poll_num += 1;
            #[cfg(test)]
            tracing::trace!(poll_num, "poll_fn locking waiters");
            let mut inner = self.queue.inner.lock().unwrap();
            inner.integrity_check();
            let my_waitslot = inner.slots[self.idx].as_mut().unwrap();
            // assert!(
            //     poll_num <= 2,
            //     "once we place the waker in the slot, next wakeup should have a result: {}",
            //     my_waitslot.1.is_some()
            // );
            if let Some(res) = my_waitslot.1.take() {
                #[cfg(test)]
                tracing::trace!(poll_num, "have cache slot");
                // above .take() resets the waiters slot to None
                debug_assert!(my_waitslot.0.is_none());
                debug_assert!(my_waitslot.1.is_none());
                inner.slots[my_waitslot_idx] = None;
                inner.free.push_back(my_waitslot_idx);
                let _ = scopeguard::ScopeGuard::into_inner(drop_guard.take().unwrap());
                inner.integrity_check();
                return Poll::Ready(res);
            }
            // assert_eq!(poll_num, 1);
            if !my_waitslot
                .0
                .as_ref()
                .map(|existing| cx.waker().will_wake(existing))
                .unwrap_or(false)
            {
                let prev = my_waitslot.0.replace(cx.waker().clone());
                #[cfg(test)]
                tracing::trace!(poll_num, prev_is_some = prev.is_some(), "updating waker");
            }
            inner.integrity_check();
            #[cfg(test)]
            tracing::trace!(poll_num, "waiting to be woken up");
            Poll::Pending
        })
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        task::Poll,
        time::Duration,
    };

    use rand::RngCore;

    #[tokio::test]
    async fn in_order_completion_and_wait() {
        let queue = super::Queue::new(2);

        let q1 = queue.begin().unwrap();
        let q2 = queue.begin().unwrap();

        assert_eq!(q1.complete_and_wait(23).await, 23);
        assert_eq!(q2.complete_and_wait(42).await, 42);
    }

    #[tokio::test]
    async fn out_of_order_completion_and_wait() {
        let queue = super::Queue::new(2);

        let q1 = queue.begin().unwrap();
        let q2 = queue.begin().unwrap();

        let mut q2compfut = q2.complete_and_wait(23);

        match futures::poll!(&mut q2compfut) {
            Poll::Pending => {}
            Poll::Ready(_) => panic!("should not be ready yet, it's queued after q1"),
        }

        let q1res = q1.complete_and_wait(42).await;
        assert_eq!(q1res, 23);

        let q2res = q2compfut.await;
        assert_eq!(q2res, 42);
    }

    #[tokio::test]
    async fn in_order_completion_out_of_order_wait() {
        let queue = super::Queue::new(2);

        let q1 = queue.begin().unwrap();
        let q2 = queue.begin().unwrap();

        let mut q1compfut = q1.complete_and_wait(23);

        let mut q2compfut = q2.complete_and_wait(42);

        match futures::poll!(&mut q2compfut) {
            Poll::Pending => {
                unreachable!("q2 should be ready, it wasn't first but q1 is serviced already")
            }
            Poll::Ready(x) => assert_eq!(x, 42),
        }

        assert_eq!(futures::poll!(&mut q1compfut), Poll::Ready(23));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stress() {
        let ntasks = 8;
        let queue_size = 8;
        let queue = Arc::new(super::Queue::new(queue_size));

        let stop = Arc::new(AtomicBool::new(false));

        let mut tasks = vec![];
        for i in 0..ntasks {
            let jh = tokio::spawn({
                let queue = Arc::clone(&queue);
                let stop = Arc::clone(&stop);
                async move {
                    while !stop.load(Ordering::Relaxed) {
                        let q = queue.begin().unwrap();
                        for _ in 0..(rand::thread_rng().next_u32() % 10_000) {
                            std::hint::spin_loop();
                        }
                        q.complete_and_wait(i).await;
                        tokio::task::yield_now().await;
                    }
                }
            });
            tasks.push(jh);
        }

        tokio::time::sleep(Duration::from_secs(10)).await;

        stop.store(true, Ordering::Relaxed);

        for t in tasks {
            t.await.unwrap();
        }
    }

    #[test]
    fn stress_two_runtimes_shared_queue() {
        std::thread::scope(|s| {
            let ntasks = 8;
            let queue_size = 8;
            let queue = Arc::new(super::Queue::new(queue_size));

            let stop = Arc::new(AtomicBool::new(false));

            for i in 0..ntasks {
                s.spawn({
                    let queue = Arc::clone(&queue);
                    let stop = Arc::clone(&stop);
                    move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap();
                        rt.block_on(async move {
                            while !stop.load(Ordering::Relaxed) {
                                let q = queue.begin().unwrap();
                                for _ in 0..(rand::thread_rng().next_u32() % 10_000) {
                                    std::hint::spin_loop();
                                }
                                q.complete_and_wait(i).await;
                                tokio::task::yield_now().await;
                            }
                        });
                    }
                });
            }

            std::thread::sleep(Duration::from_secs(10));

            stop.store(true, Ordering::Relaxed);
        });
    }
}
