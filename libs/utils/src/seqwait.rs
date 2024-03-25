#![warn(missing_docs)]

use std::cmp::{Eq, Ordering};
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::mem;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch::{channel, Receiver, Sender};
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
/// It is handy to store some other fields under the same mutex in `SeqWait<S>`
/// (e.g. store prev_record_lsn). So we allow SeqWait to be parametrized with
/// any type that can expose counter. `V` is the type of exposed counter.
pub trait MonotonicCounter<V> {
    /// Bump counter value and check that it goes forward
    /// N.B.: new_val is an actual new value, not a difference.
    fn cnt_advance(&mut self, new_val: V);

    /// Get counter value
    fn cnt_value(&self) -> V;
}

/// Internal components of a `SeqWait`
struct SeqWaitInt<S, V>
where
    S: MonotonicCounter<V>,
    V: Ord,
{
    waiters: BinaryHeap<Waiter<V>>,
    current: S,
    status_channel: Sender<Option<V>>,
    shutdown: bool,
}

impl<S, V> SeqWaitInt<S, V>
where
    S: MonotonicCounter<V>,
    V: Ord + Copy,
{
    /// `status_channel` contains the number of the first waiter in the queue.
    /// This function should be called whenever waiters queue changes.
    fn update_status(&self) {
        let first_waiter = self.waiters.peek().map(|w| w.wake_num);
        let _ = self.status_channel.send_replace(first_waiter);
    }
}

struct Waiter<T>
where
    T: Ord,
{
    wake_num: T,              // wake me when this number arrives ...
    wake_channel: Sender<()>, // ... by sending a message to this channel
}

// BinaryHeap is a max-heap, and we want a min-heap. Reverse the ordering here
// to get that.
impl<T: Ord> PartialOrd for Waiter<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> Ord for Waiter<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.wake_num.cmp(&self.wake_num)
    }
}

impl<T: Ord> PartialEq for Waiter<T> {
    fn eq(&self, other: &Self) -> bool {
        other.wake_num == self.wake_num
    }
}

impl<T: Ord> Eq for Waiter<T> {}

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
/// `S` means Storage, `V` is type of counter that this storage exposes.
///
pub struct SeqWait<S, V>
where
    S: MonotonicCounter<V>,
    V: Ord,
{
    internal: Mutex<SeqWaitInt<S, V>>,
}

impl<S, V> SeqWait<S, V>
where
    S: MonotonicCounter<V> + Copy,
    V: Ord + Copy,
{
    /// Create a new `SeqWait`, initialized to a particular number
    pub fn new(starting_num: S) -> Self {
        let internal = SeqWaitInt {
            waiters: BinaryHeap::new(),
            current: starting_num,
            status_channel: channel(None).0,
            shutdown: false,
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

            // Block any future waiters from starting
            internal.shutdown = true;

            // Update status: no active waiters.
            internal.status_channel.send_replace(None);

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
    ///
    /// This function is async cancellation-safe.
    pub async fn wait_for(&self, num: V) -> Result<(), SeqWaitError> {
        match self.queue_for_wait(num) {
            Ok(None) => Ok(()),
            Ok(Some(mut rx)) => rx.changed().await.map_err(|_| SeqWaitError::Shutdown),
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
    /// This function is async cancellation-safe.
    pub async fn wait_for_timeout(
        &self,
        num: V,
        timeout_duration: Duration,
    ) -> Result<(), SeqWaitError> {
        match self.queue_for_wait(num) {
            Ok(None) => Ok(()),
            Ok(Some(mut rx)) => match timeout(timeout_duration, rx.changed()).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) => Err(SeqWaitError::Shutdown),
                Err(_) => Err(SeqWaitError::Timeout),
            },
            Err(e) => Err(e),
        }
    }

    /// Check if [`Self::wait_for`] or [`Self::wait_for_timeout`] would wait if called with `num`.
    pub fn would_wait_for(&self, num: V) -> Result<(), V> {
        let internal = self.internal.lock().unwrap();
        let cnt = internal.current.cnt_value();
        drop(internal);
        if cnt >= num {
            Ok(())
        } else {
            Err(cnt)
        }
    }

    /// Register and return a channel that will be notified when a number arrives,
    /// or None, if it has already arrived.
    fn queue_for_wait(&self, num: V) -> Result<Option<Receiver<()>>, SeqWaitError> {
        let mut internal = self.internal.lock().unwrap();
        if internal.current.cnt_value() >= num {
            return Ok(None);
        }
        if internal.shutdown {
            return Err(SeqWaitError::Shutdown);
        }

        // Create a new channel.
        let (tx, rx) = channel(());
        internal.waiters.push(Waiter {
            wake_num: num,
            wake_channel: tx,
        });
        internal.update_status();
        // Drop the lock as we exit this scope.
        Ok(Some(rx))
    }

    /// Announce a new number has arrived
    ///
    /// All waiters at this value or below will be woken.
    ///
    /// Returns the old number.
    pub fn advance(&self, num: V) -> V {
        let old_value;
        let wake_these = {
            let mut internal = self.internal.lock().unwrap();

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
            internal.update_status();
            wake_these
        };

        for tx in wake_these {
            // This can fail if there are no receivers.
            // We don't care; discard the error.
            let _ = tx.send(());
        }
        old_value
    }

    /// Read the current value, without waiting.
    pub fn load(&self) -> S {
        self.internal.lock().unwrap().current
    }

    /// Get a Receiver for the current status.
    ///
    /// The current status is the number of the first waiter in the queue,
    /// or None if there are no waiters.
    ///
    /// This receiver will be notified whenever the status changes.
    /// It is useful for receiving notifications when the first waiter
    /// starts waiting for a number, or when there are no more waiters left.
    pub fn status_receiver(&self) -> Receiver<Option<V>> {
        self.internal.lock().unwrap().status_channel.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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
        let seq = Arc::new(SeqWait::new(0));
        let seq2 = Arc::clone(&seq);
        let seq3 = Arc::clone(&seq);
        let jh1 = tokio::task::spawn(async move {
            seq2.wait_for(42).await.expect("wait_for 42");
            let old = seq2.advance(100);
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
        let old = seq.advance(99);
        assert_eq!(old, 0);
        seq.wait_for(100).await.expect("wait_for 100");

        // Calling advance with a smaller value is a no-op
        assert_eq!(seq.advance(98), 100);
        assert_eq!(seq.load(), 100);

        jh1.await.unwrap();
        jh2.await.unwrap();

        seq.shutdown();
    }

    #[tokio::test]
    async fn seqwait_timeout() {
        let seq = Arc::new(SeqWait::new(0));
        let seq2 = Arc::clone(&seq);
        let jh = tokio::task::spawn(async move {
            let timeout = Duration::from_millis(1);
            let res = seq2.wait_for_timeout(42, timeout).await;
            assert_eq!(res, Err(SeqWaitError::Timeout));
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        // This will attempt to wake, but nothing will happen
        // because the waiter already dropped its Receiver.
        let old = seq.advance(99);
        assert_eq!(old, 0);
        jh.await.unwrap();

        seq.shutdown();
    }
}
