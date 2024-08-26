use std::{
    hash::Hash,
    sync::atomic::{AtomicUsize, Ordering},
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
    config: LeakyBucketConfig,
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
            config,
            access_count: AtomicUsize::new(0),
        }
    }

    /// Check that number of connections to the endpoint is below `max_rps` rps.
    pub fn check(&self, key: K, n: u32) -> bool {
        let now = Instant::now();

        if self.access_count.fetch_add(1, Ordering::AcqRel) % 2048 == 0 {
            self.do_gc(now);
        }

        let mut entry = self.map.entry(key).or_insert_with(|| LeakyBucketState {
            time: now,
            filled: 0.0,
        });

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
            .retain(|_, value| !value.get_mut().update(&self.config, now));
    }
}

pub struct LeakyBucketConfig {
    pub rps: f64,
    pub max: f64,
}

pub struct LeakyBucketState {
    filled: f64,
    time: Instant,
}

impl LeakyBucketConfig {
    pub fn new(rps: f64, max: f64) -> Self {
        assert!(rps > 0.0, "rps must be positive");
        assert!(max > 0.0, "max must be positive");
        Self { rps, max }
    }
}

impl LeakyBucketState {
    pub fn new() -> Self {
        Self {
            filled: 0.0,
            time: Instant::now(),
        }
    }

    /// updates the timer and returns true if the bucket is empty
    fn update(&mut self, info: &LeakyBucketConfig, now: Instant) -> bool {
        let drain = now.duration_since(self.time);
        let drain = drain.as_secs_f64() * info.rps;

        self.filled = (self.filled - drain).clamp(0.0, info.max);
        self.time = now;

        self.filled == 0.0
    }

    pub fn check(&mut self, info: &LeakyBucketConfig, now: Instant, n: f64) -> bool {
        self.update(info, now);

        if self.filled + n > info.max {
            return false;
        }
        self.filled += n;

        true
    }
}

impl Default for LeakyBucketState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::{LeakyBucketConfig, LeakyBucketState};

    #[tokio::test(start_paused = true)]
    async fn check() {
        let info = LeakyBucketConfig::new(500.0, 2000.0);
        let mut bucket = LeakyBucketState::new();

        // should work for 2000 requests this second
        for _ in 0..2000 {
            assert!(bucket.check(&info, Instant::now(), 1.0));
        }
        assert!(!bucket.check(&info, Instant::now(), 1.0));
        assert_eq!(bucket.filled, 2000.0);

        // in 1ms we should drain 0.5 tokens.
        // make sure we don't lose any tokens
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(!bucket.check(&info, Instant::now(), 1.0));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(bucket.check(&info, Instant::now(), 1.0));

        // in 10ms we should drain 5 tokens
        tokio::time::advance(Duration::from_millis(10)).await;
        for _ in 0..5 {
            assert!(bucket.check(&info, Instant::now(), 1.0));
        }
        assert!(!bucket.check(&info, Instant::now(), 1.0));

        // in 10s we should drain 5000 tokens
        // but cap is only 2000
        tokio::time::advance(Duration::from_secs(10)).await;
        for _ in 0..2000 {
            assert!(bucket.check(&info, Instant::now(), 1.0));
        }
        assert!(!bucket.check(&info, Instant::now(), 1.0));

        // should sustain 500rps
        for _ in 0..2000 {
            tokio::time::advance(Duration::from_millis(10)).await;
            for _ in 0..5 {
                assert!(bucket.check(&info, Instant::now(), 1.0));
            }
        }
    }
}
