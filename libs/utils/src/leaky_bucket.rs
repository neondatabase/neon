use std::time::Duration;

use tokio::time::Instant;

pub struct LeakyBucketConfig {
    /// Leaky buckets can drain at a fixed interval rate.
    /// We track all times as durations since this epoch so we can round down.
    pub epoch: Instant,

    /// How frequently we drain the bucket.
    /// If equal to 0, we drain constantly over time.
    /// If greater than 0, we drain at fixed intervals.
    pub refill_rate: Duration,

    /// "time cost" of a single request unit.
    /// loosely represents how long it takes to handle a request unit in active CPU time.
    pub time_cost: Duration,

    /// total size of the bucket
    pub bucket_width: Duration,
}

impl LeakyBucketConfig {
    pub fn quantize_instant(&self, now: Instant) -> Duration {
        let mut now = now - self.epoch;

        if self.refill_rate > Duration::ZERO {
            // we only "add" new tokens on a fixed interval.
            // truncate to the most recent multiple of self.interval.
            now = self
                .refill_rate
                .mul_f64(now.div_duration_f64(self.refill_rate).trunc());
        }

        now
    }
}

// impl From<LeakyBucketConfig> for LeakyBucketConfig {
//     fn from(config: LeakyBucketConfig) -> Self {
//         // seconds_per_request = 1/(request_per_second)
//         let spr = config.rps.recip();
//         Self {
//             time_cost: Duration::from_secs_f64(spr),
//             bucket_width: Duration::from_secs_f64(config.max * spr),
//         }
//     }
// }

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
        config.quantize_instant(now) < self.end
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
        let now = config.quantize_instant(now);

        let start = self.end - config.bucket_width;

        let n = config.time_cost.mul_f64(n);

        //       start          end
        //       |     start+n  |     end+n
        //       |   /          |   /
        // ------{o-[---------o-}--]----o----
        //   now1 ^      now2 ^         ^ now3
        //
        // at now1, the bucket would be completely filled if we add n tokens.
        // at now2, the bucket would be partially filled if we add n tokens.
        // at now3, the bucket would start completely empty before we add n tokens.

        if self.end + n <= now {
            self.end = now + n;
            Ok(())
        } else if start + n <= now {
            self.end += n;
            Ok(())
        } else {
            Err(config.epoch + start + n)
        }
    }
}
