use std::{sync::Mutex, time::Duration};

use tokio::{sync::Notify, time::Instant};

pub struct LeakyBucketConfig {
    /// "time cost" of a single request unit.
    /// should loosely represent how long it takes to handle a request unit in active resource time.
    pub cost: Duration,

    /// total size of the bucket
    pub bucket_width: Duration,
}

pub struct LeakyBucketState {
    /// Bucket is represented by `start..end` where `end = epoch + end` and `start = end - config.bucket_width`.
    ///
    /// At any given time, `end - now` represents the number of tokens in the bucket, multiplied by the "time_cost".
    /// Adding `n` tokens to the bucket is done by moving `end` forward by `n * config.time_cost`.
    /// If `now < start`, the bucket is considered filled and cannot accept any more tokens.
    /// Draining the bucket will happen naturally as `now` moves forward.
    ///
    /// Let `n` be some "time cost" for the request,
    /// If now is after end, the bucket is empty and the end is reset to now,
    /// If now is within the `bucket window + n`, we are within time budget.
    /// If now is before the `bucket window + n`, we have run out of budget.
    ///
    /// This is inspired by the generic cell rate algorithm (GCRA) and works
    /// exactly the same as a leaky-bucket.
    pub end: Instant,
}

impl LeakyBucketState {
    pub fn new(now: Instant) -> Self {
        Self { end: now }
    }

    pub fn bucket_is_empty(&self, now: Instant) -> bool {
        // if self.end is after now, the bucket is not empty
        self.end <= now
    }

    /// Immediately adds tokens to the bucket, if there is space.
    ///
    /// In a scenario where you are waiting for available rate,
    /// rather than just erroring immediately, `started` corresponds to when this waiting started.
    ///
    /// `n` is the number of tokens that will be filled in the bucket.
    ///
    /// # Errors
    ///
    /// If there is not enough space, no tokens are added. Instead, an error is returned with the time when
    /// there will be space again.
    pub fn add_tokens(
        &mut self,
        config: &LeakyBucketConfig,
        started: Instant,
        n: f64,
    ) -> Result<(), Instant> {
        let now = Instant::now();

        // invariant: started <= now
        debug_assert!(started <= now);

        // If the bucket was empty when we started our search, bump the end up accordingly.
        let mut end = self.end;
        if end < started {
            end = started;
        }

        let n = config.cost.mul_f64(n);
        let end_plus_n = end + n;
        let start_plus_n = end_plus_n.checked_sub(config.bucket_width);

        //       start          end
        //       |     start+n  |     end+n
        //       |   /          |   /
        // ------{o-[---------o-}--]----o----
        //   now1 ^      now2 ^         ^ now3
        //
        // at now1, the bucket would be completely filled if we add n tokens.
        // at now2, the bucket would be partially filled if we add n tokens.
        // at now3, the bucket would start completely empty before we add n tokens.

        match start_plus_n {
            Some(start_plus_n) if now < start_plus_n => Err(start_plus_n),
            _ => {
                self.end = end_plus_n;
                Ok(())
            }
        }
    }
}

pub struct RateLimiter {
    pub config: LeakyBucketConfig,
    pub state: Mutex<LeakyBucketState>,
    /// a queue to provide this fair ordering.
    pub queue: Notify,
}

struct Requeue<'a>(&'a Notify);

impl Drop for Requeue<'_> {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

impl RateLimiter {
    pub fn steady_rps(&self) -> f64 {
        self.config.cost.as_secs_f64().recip()
    }

    /// returns true if we did throttle
    pub async fn acquire(&self, count: usize) -> bool {
        let mut throttled = false;

        let start = tokio::time::Instant::now();

        // wait until we are the first in the queue
        let mut notified = std::pin::pin!(self.queue.notified());
        if !notified.as_mut().enable() {
            throttled = true;
            notified.await;
        }

        // notify the next waiter in the queue when we are done.
        let _guard = Requeue(&self.queue);

        loop {
            let res = self
                .state
                .lock()
                .unwrap()
                .add_tokens(&self.config, start, count as f64);
            match res {
                Ok(()) => return throttled,
                Err(ready_at) => {
                    throttled = true;
                    tokio::time::sleep_until(ready_at).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::{LeakyBucketConfig, LeakyBucketState};

    #[tokio::test(start_paused = true)]
    async fn check() {
        let config = LeakyBucketConfig {
            // average 100rps
            cost: Duration::from_millis(10),
            // burst up to 100 requests
            bucket_width: Duration::from_millis(1000),
        };

        let mut state = LeakyBucketState::new(Instant::now());

        // supports burst
        {
            // should work for 100 requests this instant
            for _ in 0..100 {
                state.add_tokens(&config, Instant::now(), 1.0).unwrap();
            }
            let ready = state.add_tokens(&config, Instant::now(), 1.0).unwrap_err();
            assert_eq!(ready - Instant::now(), Duration::from_millis(10));
        }

        // doesn't overfill
        {
            // after 1s we should have an empty bucket again.
            tokio::time::advance(Duration::from_secs(1)).await;
            assert!(state.bucket_is_empty(Instant::now()));

            // after 1s more, we should not over count the tokens and allow more than 200 requests.
            tokio::time::advance(Duration::from_secs(1)).await;
            for _ in 0..100 {
                state.add_tokens(&config, Instant::now(), 1.0).unwrap();
            }
            let ready = state.add_tokens(&config, Instant::now(), 1.0).unwrap_err();
            assert_eq!(ready - Instant::now(), Duration::from_millis(10));
        }

        // supports sustained rate over a long period
        {
            tokio::time::advance(Duration::from_secs(1)).await;

            // should sustain 100rps
            for _ in 0..2000 {
                tokio::time::advance(Duration::from_millis(10)).await;
                state.add_tokens(&config, Instant::now(), 1.0).unwrap();
            }
        }

        // supports requesting more tokens than can be stored in the bucket
        // we just wait a little bit longer upfront.
        {
            // start the bucket completely empty
            tokio::time::advance(Duration::from_secs(5)).await;
            assert!(state.bucket_is_empty(Instant::now()));

            // requesting 200 tokens of space should take 200*cost = 2s
            // but we already have 1s available, so we wait 1s from start.
            let start = Instant::now();

            let ready = state.add_tokens(&config, start, 200.0).unwrap_err();
            assert_eq!(ready - Instant::now(), Duration::from_secs(1));

            tokio::time::advance(Duration::from_millis(500)).await;
            let ready = state.add_tokens(&config, start, 200.0).unwrap_err();
            assert_eq!(ready - Instant::now(), Duration::from_millis(500));

            tokio::time::advance(Duration::from_millis(500)).await;
            state.add_tokens(&config, start, 200.0).unwrap();

            // bucket should be completely full now
            let ready = state.add_tokens(&config, Instant::now(), 1.0).unwrap_err();
            assert_eq!(ready - Instant::now(), Duration::from_millis(10));
        }
    }
}
