#![warn(missing_docs)]

use std::cmp::{Eq, Ordering, PartialOrd};
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::mem;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Mutex;
use std::time::Duration;

/// An error happened while waiting for a number
#[derive(Debug, PartialEq, thiserror::Error)]
#[error("SeqWaitError")]
pub enum SeqWaitError {
    /// The wait timeout was reached
    Timeout,
    /// [`SeqWait::shutdown`] was called
    Shutdown,
}

/// Internal components of a `SeqWait`
struct SeqWaitInt<T>
where
    T: Ord,
{
    waiters: BinaryHeap<Waiter<T>>,
    current: T,
    shutdown: bool,
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
        other.wake_num.partial_cmp(&self.wake_num)
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
pub struct SeqWait<T>
where
    T: Ord,
{
    internal: Mutex<SeqWaitInt<T>>,
}

impl<T> SeqWait<T>
where
    T: Ord + Debug + Copy,
{
    /// Create a new `SeqWait`, initialized to a particular number
    pub fn new(starting_num: T) -> Self {
        let internal = SeqWaitInt {
            waiters: BinaryHeap::new(),
            current: starting_num,
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
    pub fn wait_for(&self, num: T) -> Result<(), SeqWaitError> {
        match self.queue_for_wait(num) {
            Ok(None) => Ok(()),
            Ok(Some(rx)) => rx.recv().map_err(|_| SeqWaitError::Shutdown),
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
    pub fn wait_for_timeout(&self, num: T, timeout_duration: Duration) -> Result<(), SeqWaitError> {
        match self.queue_for_wait(num) {
            Ok(None) => Ok(()),
            Ok(Some(rx)) => rx.recv_timeout(timeout_duration).map_err(|e| match e {
                std::sync::mpsc::RecvTimeoutError::Timeout => SeqWaitError::Timeout,
                std::sync::mpsc::RecvTimeoutError::Disconnected => SeqWaitError::Shutdown,
            }),
            Err(e) => Err(e),
        }
    }

    /// Register and return a channel that will be notified when a number arrives,
    /// or None, if it has already arrived.
    fn queue_for_wait(&self, num: T) -> Result<Option<Receiver<()>>, SeqWaitError> {
        let mut internal = self.internal.lock().unwrap();
        if internal.current >= num {
            return Ok(None);
        }
        if internal.shutdown {
            return Err(SeqWaitError::Shutdown);
        }

        // Create a new channel.
        let (tx, rx) = channel();
        internal.waiters.push(Waiter {
            wake_num: num,
            wake_channel: tx,
        });
        // Drop the lock as we exit this scope.
        Ok(Some(rx))
    }

    /// Announce a new number has arrived
    ///
    /// All waiters at this value or below will be woken.
    ///
    /// `advance` will panic if you send it a lower number than
    /// a previous call.
    pub fn advance(&self, num: T) {
        let wake_these = {
            let mut internal = self.internal.lock().unwrap();

            if internal.current > num {
                panic!(
                    "tried to advance backwards, from {:?} to {:?}",
                    internal.current, num
                );
            }
            internal.current = num;

            // Pop all waiters <= num from the heap. Collect them in a vector, and
            // wake them up after releasing the lock.
            let mut wake_these = Vec::new();
            while let Some(n) = internal.waiters.peek() {
                if n.wake_num > num {
                    break;
                }
                wake_these.push(internal.waiters.pop().unwrap().wake_channel);
            }
            wake_these
        };

        for tx in wake_these {
            // This can fail if there are no receivers.
            // We don't care; discard the error.
            let _ = tx.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::thread::spawn;
    use std::time::Duration;

    #[test]
    fn seqwait() {
        let seq = Arc::new(SeqWait::new(0));
        let seq2 = Arc::clone(&seq);
        let seq3 = Arc::clone(&seq);
        spawn(move || {
            seq2.wait_for(42).expect("wait_for 42");
            seq2.advance(100);
            seq2.wait_for(999).expect_err("no 999");
        });
        spawn(move || {
            seq3.wait_for(42).expect("wait_for 42");
            seq3.wait_for(0).expect("wait_for 0");
        });
        sleep(Duration::from_secs(1));
        seq.advance(99);
        seq.wait_for(100).expect("wait_for 100");
        seq.shutdown();
    }

    #[test]
    fn seqwait_timeout() {
        let seq = Arc::new(SeqWait::new(0));
        let seq2 = Arc::clone(&seq);
        spawn(move || {
            let timeout = Duration::from_millis(1);
            let res = seq2.wait_for_timeout(42, timeout);
            assert_eq!(res, Err(SeqWaitError::Timeout));
        });
        sleep(Duration::from_secs(1));
        // This will attempt to wake, but nothing will happen
        // because the waiter already dropped its Receiver.
        seq.advance(99);
    }
}
