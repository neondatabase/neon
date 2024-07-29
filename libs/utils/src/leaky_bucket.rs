use std::time::Duration;

use tokio::time::Instant;

pub struct LeakyBucketConfig {
    /// Leaky buckets can drain at a fixed interval rate.
    /// We track all times as durations since this epoch so we can round down.
    pub epoch: Instant,

    /// How frequently we drain the bucket.
    /// If equal to 0, we drain continuously over time.
    /// If greater than 0, we drain at fixed intervals.
    pub drain_interval: Duration,

    /// "time cost" of a single request unit.
    /// should loosely represents how long it takes to handle a request unit in active resource time.
    pub cost: Duration,

    /// total size of the bucket
    pub bucket_width: Duration,
}

impl LeakyBucketConfig {
    fn prev_multiple_of_drain(&self, mut dur: Duration) -> Duration {
        if self.drain_interval > Duration::ZERO {
            let n = dur.div_duration_f64(self.drain_interval).floor();
            dur = self.drain_interval.mul_f64(n);
        }
        dur
    }

    fn next_multiple_of_drain(&self, mut dur: Duration) -> Duration {
        if self.drain_interval > Duration::ZERO {
            let n = dur.div_duration_f64(self.drain_interval).ceil();
            dur = self.drain_interval.mul_f64(n);
        }
        dur
    }
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
    pub end: Duration,
}

impl LeakyBucketState {
    pub fn new(now: Duration) -> Self {
        Self { end: now }
    }

    pub fn bucket_is_empty(&self, config: &LeakyBucketConfig, now: Instant) -> bool {
        // if self.end is after now, the bucket is not empty
        config.prev_multiple_of_drain(now - config.epoch) <= self.end
    }

    /// Immedaitely adds tokens to the bucket, if there is space.
    /// If there is not enough space, no tokens are added. Instead, an error is returned with the time when
    /// there will be space again.
    pub fn add_tokens(
        &mut self,
        config: &LeakyBucketConfig,
        now: Instant,
        n: f64,
    ) -> Result<(), Instant> {
        // round down to the last time we would have drained the bucket.
        let now = config.prev_multiple_of_drain(now - config.epoch);

        let n = config.cost.mul_f64(n);

        let end_plus_n = self.end + n;
        let start_plus_n = end_plus_n.saturating_sub(config.bucket_width);

        //       start          end
        //       |     start+n  |     end+n
        //       |   /          |   /
        // ------{o-[---------o-}--]----o----
        //   now1 ^      now2 ^         ^ now3
        //
        // at now1, the bucket would be completely filled if we add n tokens.
        // at now2, the bucket would be partially filled if we add n tokens.
        // at now3, the bucket would start completely empty before we add n tokens.

        if end_plus_n <= now {
            self.end = now + n;
            Ok(())
        } else if start_plus_n <= now {
            self.end = end_plus_n;
            Ok(())
        } else {
            let ready_at = config.next_multiple_of_drain(start_plus_n);
            Err(config.epoch + ready_at)
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
            epoch: Instant::now(),
            // drain the bucket every 0.5 seconds.
            drain_interval: Duration::from_millis(500),
            // average 100rps
            cost: Duration::from_millis(10),
            // burst up to 100 requests
            bucket_width: Duration::from_millis(1000),
        };

        let mut state = LeakyBucketState::new(Instant::now() - config.epoch);

        // supports burst
        {
            // should work for 100 requests this instant
            for _ in 0..100 {
                state.add_tokens(&config, Instant::now(), 1.0).unwrap();
            }
            let ready = state.add_tokens(&config, Instant::now(), 1.0).unwrap_err();
            assert_eq!(ready - Instant::now(), Duration::from_millis(500));
        }

        // quantized refill
        {
            // after 499ms we should not drain any tokens.
            tokio::time::advance(Duration::from_millis(499)).await;
            let ready = state.add_tokens(&config, Instant::now(), 1.0).unwrap_err();
            assert_eq!(ready - Instant::now(), Duration::from_millis(1));

            // after 500ms we should have drained 50 tokens.
            tokio::time::advance(Duration::from_millis(1)).await;
            for _ in 0..50 {
                state.add_tokens(&config, Instant::now(), 1.0).unwrap();
            }
            let ready = state.add_tokens(&config, Instant::now(), 1.0).unwrap_err();
            assert_eq!(ready - Instant::now(), Duration::from_millis(500));
        }

        // doesn't overfill
        {
            // after 1s we should have an empty bucket again.
            tokio::time::advance(Duration::from_secs(1)).await;
            assert!(state.bucket_is_empty(&config, Instant::now()));

            // after 1s more, we should not over count the tokens and allow more than 200 requests.
            tokio::time::advance(Duration::from_secs(1)).await;
            for _ in 0..100 {
                state.add_tokens(&config, Instant::now(), 1.0).unwrap();
            }
            let ready = state.add_tokens(&config, Instant::now(), 1.0).unwrap_err();
            assert_eq!(ready - Instant::now(), Duration::from_millis(500));
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
    }
}
