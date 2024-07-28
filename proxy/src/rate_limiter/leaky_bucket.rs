use std::{
    hash::Hash,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use ahash::RandomState;
use dashmap::DashMap;
use rand::{thread_rng, Rng};
use tokio::time::Instant;
use tracing::info;

use crate::intern::EndpointIdInt;

// Simple per-endpoint rate limiter.
pub type EndpointRateLimiter = LeakyBucketRateLimiter<EndpointIdInt>;

pub struct LeakyBucketRateLimiter<Key> {
    map: DashMap<Key, LeakyBucketState, RandomState>,
    config: LeakyBucketConfigInner,
    access_count: AtomicUsize,
}

impl<K: Hash + Eq> LeakyBucketRateLimiter<K> {
    pub const DEFAULT: LeakyBucketConfig = LeakyBucketConfig {
        rps: 600.0,
        max: 1500.0,
    };

    pub fn new_with_shards(config: LeakyBucketConfig, shards: usize) -> Self {
        Self {
            map: DashMap::with_hasher_and_shard_amount(RandomState::new(), shards),
            config: config.into(),
            access_count: AtomicUsize::new(0),
        }
    }

    /// Check that number of connections to the endpoint is below `max_rps` rps.
    pub fn check(&self, key: K, n: u32) -> bool {
        let now = Instant::now();

        if self.access_count.fetch_add(1, Ordering::AcqRel) % 2048 == 0 {
            self.do_gc(now);
        }

        let mut entry = self
            .map
            .entry(key)
            .or_insert_with(|| LeakyBucketState::new(now));

        entry.check(&self.config, now, n as f64)
    }

    fn do_gc(&self, now: Instant) {
        info!(
            "cleaning up bucket rate limiter, current size = {}",
            self.map.len()
        );
        let n = self.map.shards().len();
        let shard = thread_rng().gen_range(0..n);
        self.map.shards()[shard]
            .write()
            .retain(|_, value| value.get().should_retain(now));
    }
}

pub struct LeakyBucketConfig {
    pub rps: f64,
    pub max: f64,
}

impl LeakyBucketConfig {
    pub fn new(rps: f64, max: f64) -> Self {
        assert!(rps > 0.0, "rps must be positive");
        assert!(max > 0.0, "max must be positive");
        Self { rps, max }
    }
}

struct LeakyBucketConfigInner {
    /// "time cost" of a single request unit.
    /// loosely represents how long it takes to handle a request unit in active CPU time.
    time_cost: Duration,
    bucket_width: Duration,
}

impl From<LeakyBucketConfig> for LeakyBucketConfigInner {
    fn from(config: LeakyBucketConfig) -> Self {
        // seconds-per-request = 1/(request-per-second)
        let spr = config.rps.recip();
        Self {
            time_cost: Duration::from_secs_f64(spr),
            bucket_width: Duration::from_secs_f64(config.max * spr),
        }
    }
}

struct LeakyBucketState {
    /// Bucket is represented by `start..end` where `start = end - config.bucket_width`.
    ///
    /// At any given time, `end-now` represents the number of tokens in the bucket, multiplied by the "time_cost".
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
    end: Instant,
}

impl LeakyBucketState {
    fn new(now: Instant) -> Self {
        Self { end: now }
    }

    fn should_retain(&self, now: Instant) -> bool {
        // if self.end is after now, the bucket is not empty
        now < self.end
    }

    fn check(&mut self, config: &LeakyBucketConfigInner, now: Instant, n: f64) -> bool {
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
            true
        } else if start + n <= now {
            self.end += n;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::{LeakyBucketConfig, LeakyBucketConfigInner, LeakyBucketState};

    #[tokio::test(start_paused = true)]
    async fn check() {
        let config: LeakyBucketConfigInner = LeakyBucketConfig::new(500.0, 2000.0).into();
        assert_eq!(config.time_cost, Duration::from_millis(2));
        assert_eq!(config.bucket_width, Duration::from_secs(4));

        let mut bucket = LeakyBucketState::new(Instant::now());

        // should work for 2000 requests this second
        for _ in 0..2000 {
            assert!(bucket.check(&config, Instant::now(), 1.0));
        }
        assert!(!bucket.check(&config, Instant::now(), 1.0));
        assert_eq!(bucket.end - Instant::now(), config.bucket_width);

        // in 1ms we should drain 0.5 tokens.
        // make sure we don't lose any tokens
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(!bucket.check(&config, Instant::now(), 1.0));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(bucket.check(&config, Instant::now(), 1.0));

        // in 10ms we should drain 5 tokens
        tokio::time::advance(Duration::from_millis(10)).await;
        for _ in 0..5 {
            assert!(bucket.check(&config, Instant::now(), 1.0));
        }
        assert!(!bucket.check(&config, Instant::now(), 1.0));

        // in 10s we should drain 5000 tokens
        // but cap is only 2000
        tokio::time::advance(Duration::from_secs(10)).await;
        for _ in 0..2000 {
            assert!(bucket.check(&config, Instant::now(), 1.0));
        }
        assert!(!bucket.check(&config, Instant::now(), 1.0));

        // should sustain 500rps
        for _ in 0..2000 {
            tokio::time::advance(Duration::from_millis(10)).await;
            for _ in 0..5 {
                assert!(bucket.check(&config, Instant::now(), 1.0));
            }
        }
    }
}
