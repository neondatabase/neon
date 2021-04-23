use std::collections::BTreeMap;
use std::mem;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch::{channel, Receiver, Sender};
use tokio::time::timeout;

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
struct SeqWaitInt {
    waiters: BTreeMap<u64, (Sender<()>, Receiver<()>)>,
    current: u64,
    shutdown: bool,
}

/// A tool for waiting on a sequence number
///
/// This provides a way to await the arrival of a number.
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
pub struct SeqWait {
    internal: Mutex<SeqWaitInt>,
}

impl SeqWait {
    /// Create a new `SeqWait`, initialized to a particular number
    pub fn new(starting_num: u64) -> Self {
        let internal = SeqWaitInt {
            waiters: BTreeMap::new(),
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
    pub async fn wait_for(&self, num: u64) -> Result<(), SeqWaitError> {
        let mut rx = {
            let mut internal = self.internal.lock().unwrap();
            if internal.current >= num {
                return Ok(());
            }
            if internal.shutdown {
                return Err(SeqWaitError::Shutdown);
            }

            // If we already have a channel for waiting on this number, reuse it.
            if let Some((_, rx)) = internal.waiters.get_mut(&num) {
                // an Err from changed() means the sender was dropped.
                rx.clone()
            } else {
                // Create a new channel.
                let (tx, rx) = channel(());
                internal.waiters.insert(num, (tx, rx.clone()));
                rx
            }
            // Drop the lock as we exit this scope.
        };
        rx.changed().await.map_err(|_| SeqWaitError::Shutdown)
    }

    /// Wait for a number to arrive
    ///
    /// This call won't complete until someone has called `advance`
    /// with a number greater than or equal to the one we're waiting for.
    ///
    /// If that hasn't happened after the specified timeout duration,
    /// [`SeqWaitError::Timeout`] will be returned.
    pub async fn wait_for_timeout(
        &self,
        num: u64,
        timeout_duration: Duration,
    ) -> Result<(), SeqWaitError> {
        timeout(timeout_duration, self.wait_for(num))
            .await
            .unwrap_or(Err(SeqWaitError::Timeout))
    }

    /// Announce a new number has arrived
    ///
    /// All waiters at this value or below will be woken.
    ///
    /// `advance` will panic if you send it a lower number than
    /// a previous call.
    pub fn advance(&self, num: u64) {
        let wake_these = {
            let mut internal = self.internal.lock().unwrap();

            if internal.current > num {
                panic!(
                    "tried to advance backwards, from {} to {}",
                    internal.current, num
                );
            }
            internal.current = num;

            // split_off will give me all the high-numbered waiters,
            // so split and then swap. Everything at or above (num + 1)
            // gets to stay.
            let mut split = internal.waiters.split_off(&(num + 1));
            std::mem::swap(&mut split, &mut internal.waiters);
            split
        };

        for (_wake_num, (tx, _rx)) in wake_these {
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
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn seqwait() {
        let seq = Arc::new(SeqWait::new(0));
        let seq2 = Arc::clone(&seq);
        let seq3 = Arc::clone(&seq);
        tokio::spawn(async move {
            seq2.wait_for(42).await.expect("wait_for 42");
            seq2.advance(100);
            seq2.wait_for(999).await.expect_err("no 999");
        });
        tokio::spawn(async move {
            seq3.wait_for(42).await.expect("wait_for 42");
            seq3.wait_for(0).await.expect("wait_for 0");
        });
        sleep(Duration::from_secs(1)).await;
        seq.advance(99);
        seq.wait_for(100).await.expect("wait_for 100");
        seq.shutdown();
    }

    #[tokio::test]
    async fn seqwait_timeout() {
        let seq = Arc::new(SeqWait::new(0));
        let seq2 = Arc::clone(&seq);
        tokio::spawn(async move {
            let timeout = Duration::from_millis(1);
            let res = seq2.wait_for_timeout(42, timeout).await;
            assert_eq!(res, Err(SeqWaitError::Timeout));
        });
        sleep(Duration::from_secs(1)).await;
        // This will attempt to wake, but nothing will happen
        // because the waiter already dropped its Receiver.
        seq.advance(99);
    }
}
