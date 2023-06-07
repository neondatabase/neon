#![warn(missing_docs)]

use either::Either;
use std::cmp::{Eq, Ordering, PartialOrd};
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot::{channel, Receiver, Sender};
use tokio::time::timeout;

/// An error happened while waiting for a number
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum SeqWaitError {
    /// The wait timeout was reached
    #[error("seqwait timeout was reached")]
    Timeout,

    /// [`SeqWait::shutdown`] was called
    #[error("SeqWait::shutdown was called")]
    Shutdown,
}

/// Monotonically increasing value
///
/// It is handy to store some other fields under the same mutex in SeqWait<S>
/// (e.g. store prev_record_lsn). So we allow SeqWait to be parametrized with
/// any type that can expose counter. <V> is the type of exposed counter.
pub trait MonotonicCounter<V> {
    /// Bump counter value and check that it goes forward
    /// N.B.: new_val is an actual new value, not a difference.
    fn cnt_advance(&mut self, new_val: V);

    /// Get counter value
    fn cnt_value(&self) -> V;
}

/// Internal components of a `SeqWait`
struct SeqWaitInt<S, V, T>
where
    S: MonotonicCounter<V>,
    V: Ord,
    T: Clone,
{
    waiters: BinaryHeap<Waiter<V, T>>,
    current: S,
    shutdown: bool,
    data: T,
}

struct Waiter<V, T>
where
    V: Ord,
    T: Clone,
{
    wake_num: V,             // wake me when this number arrives ...
    wake_channel: Sender<T>, // ... by sending a message to this channel
}

// BinaryHeap is a max-heap, and we want a min-heap. Reverse the ordering here
// to get that.
impl<V: Ord, T: Clone> PartialOrd for Waiter<V, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.wake_num.partial_cmp(&self.wake_num)
    }
}

impl<V: Ord, T: Clone> Ord for Waiter<V, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.wake_num.cmp(&self.wake_num)
    }
}

impl<V: Ord, T: Clone> PartialEq for Waiter<V, T> {
    fn eq(&self, other: &Self) -> bool {
        other.wake_num == self.wake_num
    }
}

impl<V: Ord, T: Clone> Eq for Waiter<V, T> {}

/// A tool for waiting on a sequence number
///
/// This provides a way to wait the arrival of a number.
/// As soon as the number arrives by another caller calling
/// [`advance`], then the waiter will be woken up.
///
/// This implementation takes a blocking Mutex on both [`wait_for`]
/// and [`advance`], meaning there may be unexpected executor blocking
/// due to thread scheduling unfairness. There are probably better
/// implementations, but we can probably live with this for now.
///
/// [`wait_for`]: SeqWait::wait_for
/// [`advance`]: SeqWait::advance
///
/// <S> means Storage, <V> is type of counter that this storage exposes.
///
pub struct SeqWait<S, V, T>
where
    S: MonotonicCounter<V>,
    V: Ord,
    T: Clone,
{
    internal: Mutex<SeqWaitInt<S, V, T>>,
}

impl<S, V, T> SeqWait<S, V, T>
where
    S: MonotonicCounter<V> + Copy,
    V: Ord + Copy,
    T: Clone,
{
    /// Create a new `SeqWait`, initialized to a particular number
    pub fn new(starting_num: S, data: T) -> Self {
        let internal = SeqWaitInt {
            waiters: BinaryHeap::new(),
            current: starting_num,
            shutdown: false,
            data,
        };
        SeqWait {
            internal: Mutex::new(internal),
        }
    }

    /// Shut down a `SeqWait`, causing all waiters (present and
    /// future) to return an error.
    pub fn shutdown(&self) {
        let waiters = {
            // Prevent new waiters; wake all those that exist.
            // Wake everyone with an error.
            let mut internal = self.internal.lock().unwrap();

            // This will steal the entire waiters map.
            // When we drop it all waiters will be woken.
            mem::take(&mut internal.waiters)

            // Drop the lock as we exit this scope.
        };

        // When we drop the waiters list, each Receiver will
        // be woken with an error.
        // This drop doesn't need to be explicit; it's done
        // here to make it easier to read the code and understand
        // the order of events.
        drop(waiters);
    }

    /// Wait for a number to arrive
    ///
    /// This call won't complete until someone has called `advance`
    /// with a number greater than or equal to the one we're waiting for.
    pub async fn wait_for(&self, num: V) -> Result<T, SeqWaitError> {
        match self.queue_for_wait(num, false) {
            Ok(Either::Left(data)) => Ok(data),
            Ok(Either::Right(rx)) => match rx.await {
                Err(_) => Err(SeqWaitError::Shutdown),
                Ok(data) => Ok(data),
            },
            Err(e) => Err(e),
        }
    }

    /// Wait for a number to arrive
    ///
    /// This call won't complete until someone has called `advance`
    /// with a number greater than or equal to the one we're waiting for.
    ///
    /// If that hasn't happened after the specified timeout duration,
    /// [`SeqWaitError::Timeout`] will be returned.
    ///
    /// Pass `timeout_duration.is_zero() == true` to guarantee that the
    /// future that is this function will never await.
    pub async fn wait_for_timeout(
        &self,
        num: V,
        timeout_duration: Duration,
    ) -> Result<T, SeqWaitError> {
        match self.queue_for_wait(num, timeout_duration.is_zero()) {
            Ok(Either::Left(data)) => Ok(data),
            Ok(Either::Right(rx)) => match timeout(timeout_duration, rx).await {
                Ok(Ok(data)) => Ok(data),
                Ok(Err(_)) => Err(SeqWaitError::Shutdown),
                Err(_) => Err(SeqWaitError::Timeout),
            },
            Err(e) => Err(e),
        }
    }

    /// Register and return a channel that will be notified when a number arrives,
    /// or None, if it has already arrived.
    fn queue_for_wait(&self, num: V, nowait: bool) -> Result<Either<T, Receiver<T>>, SeqWaitError> {
        let mut internal = self.internal.lock().unwrap();
        if internal.current.cnt_value() >= num {
            return Ok(Either::Left(internal.data.clone()));
        }
        if internal.shutdown {
            return Err(SeqWaitError::Shutdown);
        }
        if nowait {
            return Err(SeqWaitError::Timeout);
        }

        // Create a new channel.
        let (tx, rx) = channel();
        internal.waiters.push(Waiter {
            wake_num: num,
            wake_channel: tx,
        });
        // Drop the lock as we exit this scope.
        Ok(Either::Right(rx))
    }

    /// Announce a new number has arrived
    ///
    /// All waiters at this value or below will be woken.
    ///
    /// If `new_data` is Some(), it will update the internal data,
    /// even if `num` is smaller than the internal counter.
    /// It will not cause a wake-up though, in this case.
    ///
    /// Returns the old number.
    pub fn advance(&self, num: V, new_data: Option<T>) -> V {
        let old_value;
        let (wake_these, with_data) = {
            let mut internal = self.internal.lock().unwrap();
            if let Some(new_data) = new_data {
                internal.data = new_data;
            }

            old_value = internal.current.cnt_value();
            if old_value >= num {
                return old_value;
            }
            internal.current.cnt_advance(num);
            // Pop all waiters <= num from the heap. Collect them in a vector, and
            // wake them up after releasing the lock.
            let mut wake_these = Vec::new();
            while let Some(n) = internal.waiters.peek() {
                if n.wake_num > num {
                    break;
                }
                wake_these.push(internal.waiters.pop().unwrap().wake_channel);
            }
            (wake_these, internal.data.clone())
        };

        for tx in wake_these {
            // This can fail if there are no receivers.
            // We don't care; discard the error.
            let _ = tx.send(with_data.clone());
        }
        old_value
    }

    /// Read the current value, without waiting.
    pub fn load(&self) -> S {
        self.internal.lock().unwrap().current
    }

    /// Split the seqwait into a part than can only do wait,
    /// and another part that can do advance + wait.
    ///
    /// The wait-only part can be cloned, the advance part cannot be cloned.
    /// This provides a single-producer multi-consumer scheme.
    pub fn split_spmc(self) -> (Wait<S, V, T>, Advance<S, V, T>) {
        let inner = Arc::new(self);
        let w = Wait {
            inner: inner.clone(),
        };
        let a = Advance { inner };
        (w, a)
    }
}

/// See [`SeqWait::split_spmc`].
pub struct Wait<S, V, T>
where
    S: MonotonicCounter<V> + Copy,
    V: Ord + Copy,
    T: Clone,
{
    inner: Arc<SeqWait<S, V, T>>,
}

/// See [`SeqWait::split_spmc`].
pub struct Advance<S, V, T>
where
    S: MonotonicCounter<V> + Copy,
    V: Ord + Copy,
    T: Clone,
{
    inner: Arc<SeqWait<S, V, T>>,
}

impl<S, V, T> Wait<S, V, T>
where
    S: MonotonicCounter<V> + Copy,
    V: Ord + Copy,
    T: Clone,
{
    /// See [`SeqWait::wait_for`].
    pub async fn wait_for(&self, num: V) -> Result<T, SeqWaitError> {
        self.inner.wait_for(num).await
    }

    /// See [`SeqWait::wait_for_timeout`].
    pub async fn wait_for_timeout(
        &self,
        num: V,
        timeout_duration: Duration,
    ) -> Result<T, SeqWaitError> {
        self.inner.wait_for_timeout(num, timeout_duration).await
    }
}

impl<S, V, T> Advance<S, V, T>
where
    S: MonotonicCounter<V> + Copy,
    V: Ord + Copy,
    T: Clone,
{
    /// See [`SeqWait::advance`].
    pub fn advance(&self, num: V, new_data: Option<T>) -> V {
        self.inner.advance(num, new_data)
    }

    /// See [`SeqWait::wait_for`].
    pub async fn wait_for(&self, num: V) -> Result<T, SeqWaitError> {
        self.inner.wait_for(num).await
    }

    /// See [`SeqWait::wait_for_timeout`].
    pub async fn wait_for_timeout(
        &self,
        num: V,
        timeout_duration: Duration,
    ) -> Result<T, SeqWaitError> {
        self.inner.wait_for_timeout(num, timeout_duration).await
    }

    /// Get a `Clone::clone` of the current data inside the seqwait.
    pub fn get_current_data(&self) -> (V, T) {
        let inner = self.inner.internal.lock().unwrap();
        (inner.current.cnt_value(), inner.data.clone())
    }
}

impl<S, V, T> Clone for Wait<S, V, T>
where
    S: MonotonicCounter<V> + Copy,
    V: Ord + Copy,
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    impl MonotonicCounter<i32> for i32 {
        fn cnt_advance(&mut self, val: i32) {
            assert!(*self <= val);
            *self = val;
        }
        fn cnt_value(&self) -> i32 {
            *self
        }
    }

    #[tokio::test]
    async fn seqwait() {
        let seq = Arc::new(SeqWait::new(0, ()));
        let seq2 = Arc::clone(&seq);
        let seq3 = Arc::clone(&seq);
        let jh1 = tokio::task::spawn(async move {
            seq2.wait_for(42).await.expect("wait_for 42");
            let old = seq2.advance(100, None);
            assert_eq!(old, 99);
            seq2.wait_for_timeout(999, Duration::from_millis(100))
                .await
                .expect_err("no 999");
        });
        let jh2 = tokio::task::spawn(async move {
            seq3.wait_for(42).await.expect("wait_for 42");
            seq3.wait_for(0).await.expect("wait_for 0");
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        let old = seq.advance(99, None);
        assert_eq!(old, 0);
        seq.wait_for(100).await.expect("wait_for 100");

        // Calling advance with a smaller value is a no-op
        assert_eq!(seq.advance(98, None), 100);
        assert_eq!(seq.load(), 100);

        jh1.await.unwrap();
        jh2.await.unwrap();

        seq.shutdown();
    }

    #[tokio::test]
    async fn seqwait_timeout() {
        let seq = Arc::new(SeqWait::new(0, ()));
        let seq2 = Arc::clone(&seq);
        let jh = tokio::task::spawn(async move {
            let timeout = Duration::from_millis(1);
            let res = seq2.wait_for_timeout(42, timeout).await;
            assert_eq!(res, Err(SeqWaitError::Timeout));
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        // This will attempt to wake, but nothing will happen
        // because the waiter already dropped its Receiver.
        let old = seq.advance(99, None);
        assert_eq!(old, 0);
        jh.await.unwrap();

        seq.shutdown();
    }

    #[tokio::test]
    async fn data_basic() {
        let seq = Arc::new(SeqWait::new(0, "a"));
        let seq2 = Arc::clone(&seq);
        let jh = tokio::task::spawn(async move {
            let data = seq.wait_for(2).await.unwrap();
            assert_eq!(data, "b");
        });
        seq2.advance(1, Some("x"));
        seq2.advance(2, Some("b"));
        jh.await.unwrap();
    }

    #[test]
    fn data_always_most_recent() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let seq = Arc::new(SeqWait::new(0, "a"));
        let seq2 = Arc::clone(&seq);

        let jh = rt.spawn(async move {
            let data = seq.wait_for(2).await.unwrap();
            assert_eq!(data, "d");
        });

        // jh is not running until we poll it, thanks to current thread runtime

        rt.block_on(async move {
            seq2.advance(2, Some("b"));
            seq2.advance(3, Some("c"));
            seq2.advance(4, Some("d"));
        });

        rt.block_on(jh).unwrap();
    }

    #[tokio::test]
    async fn split_spmc_api_surface() {
        let seq = SeqWait::new(0, 1);
        let (w, a) = seq.split_spmc();

        let _ = w.wait_for(1);
        let _ = w.wait_for_timeout(0, Duration::from_secs(10));
        let _ = w.clone();

        let _ = a.advance(1, None);
        let _ = a.wait_for(1);
        let _ = a.wait_for_timeout(0, Duration::from_secs(10));

        // TODO would be nice to have must-not-compile tests for Advance not being clonable.
    }

    #[tokio::test]
    async fn new_data_same_lsn() {
        let seq = Arc::new(SeqWait::new(0, "a"));

        seq.advance(1, Some("b"));
        let data = seq.wait_for(1).await.unwrap();
        assert_eq!(data, "b", "the regular case where lsn and data advance");

        seq.advance(1, Some("c"));
        let data = seq.wait_for(1).await.unwrap();
        assert_eq!(
            data, "c",
            "no lsn advance still gives new data for old lsn wait_for's"
        );

        let (start_wait_for_sender, start_wait_for_receiver) = tokio::sync::oneshot::channel();
        // ensure we don't wake waiters for data-only change
        let jh = tokio::spawn({
            let seq = seq.clone();
            async move {
                start_wait_for_receiver.await.unwrap();
                match tokio::time::timeout(Duration::from_secs(2), seq.wait_for(2)).await {
                    Ok(_) => {
                        assert!(
                            false,
                            "advance should not wake waiters if data changes but LSN doesn't"
                        );
                    }
                    Err(_) => {
                        // Good, we weren't woken up.
                    }
                }
            }
        });

        seq.advance(1, Some("d"));
        start_wait_for_sender.send(()).unwrap();
        jh.await.unwrap();
    }
}
