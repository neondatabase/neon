use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};

use ahash::RandomState;
use clashmap::ClashMap;
use rand::{Rng, thread_rng};
use tokio::time::Instant;
use tracing::info;
use utils::leaky_bucket::LeakyBucketState;

use crate::intern::EndpointIdInt;

// Simple per-endpoint rate limiter.
pub type EndpointRateLimiter = LeakyBucketRateLimiter<EndpointIdInt>;

pub struct LeakyBucketRateLimiter<Key> {
    map: ClashMap<Key, LeakyBucketState, RandomState>,
    default_config: utils::leaky_bucket::LeakyBucketConfig,
    access_count: AtomicUsize,
}

impl<K: Hash + Eq> LeakyBucketRateLimiter<K> {
    pub const DEFAULT: LeakyBucketConfig = LeakyBucketConfig {
        rps: 600.0,
        max: 1500.0,
    };

    pub fn new_with_shards(config: LeakyBucketConfig, shards: usize) -> Self {
        Self {
            map: ClashMap::with_hasher_and_shard_amount(RandomState::new(), shards),
            default_config: config.into(),
            access_count: AtomicUsize::new(0),
        }
    }

    /// Check that number of connections to the endpoint is below `max_rps` rps.
    pub(crate) fn check(&self, key: K, config: Option<LeakyBucketConfig>, n: u32) -> bool {
        let now = Instant::now();

        let config = config.map_or(self.default_config, Into::into);

        if self.access_count.fetch_add(1, Ordering::AcqRel) % 2048 == 0 {
            self.do_gc(now);
        }

        let mut entry = self
            .map
            .entry(key)
            .or_insert_with(|| LeakyBucketState { empty_at: now });

        entry.add_tokens(&config, now, n as f64).is_ok()
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
            .retain(|(_, value)| !value.bucket_is_empty(now));
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

impl From<LeakyBucketConfig> for utils::leaky_bucket::LeakyBucketConfig {
    fn from(config: LeakyBucketConfig) -> Self {
        utils::leaky_bucket::LeakyBucketConfig::new(config.rps, config.max)
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;
    use utils::leaky_bucket::LeakyBucketState;

    use super::LeakyBucketConfig;

    #[tokio::test(start_paused = true)]
    async fn check() {
        let config: utils::leaky_bucket::LeakyBucketConfig =
            LeakyBucketConfig::new(500.0, 2000.0).into();
        assert_eq!(config.cost, Duration::from_millis(2));
        assert_eq!(config.bucket_width, Duration::from_secs(4));

        let mut bucket = LeakyBucketState {
            empty_at: Instant::now(),
        };

        // should work for 2000 requests this second
        for _ in 0..2000 {
            bucket.add_tokens(&config, Instant::now(), 1.0).unwrap();
        }
        bucket.add_tokens(&config, Instant::now(), 1.0).unwrap_err();
        assert_eq!(bucket.empty_at - Instant::now(), config.bucket_width);

        // in 1ms we should drain 0.5 tokens.
        // make sure we don't lose any tokens
        tokio::time::advance(Duration::from_millis(1)).await;
        bucket.add_tokens(&config, Instant::now(), 1.0).unwrap_err();
        tokio::time::advance(Duration::from_millis(1)).await;
        bucket.add_tokens(&config, Instant::now(), 1.0).unwrap();

        // in 10ms we should drain 5 tokens
        tokio::time::advance(Duration::from_millis(10)).await;
        for _ in 0..5 {
            bucket.add_tokens(&config, Instant::now(), 1.0).unwrap();
        }
        bucket.add_tokens(&config, Instant::now(), 1.0).unwrap_err();

        // in 10s we should drain 5000 tokens
        // but cap is only 2000
        tokio::time::advance(Duration::from_secs(10)).await;
        for _ in 0..2000 {
            bucket.add_tokens(&config, Instant::now(), 1.0).unwrap();
        }
        bucket.add_tokens(&config, Instant::now(), 1.0).unwrap_err();

        // should sustain 500rps
        for _ in 0..2000 {
            tokio::time::advance(Duration::from_millis(10)).await;
            for _ in 0..5 {
                bucket.add_tokens(&config, Instant::now(), 1.0).unwrap();
            }
        }
    }
}
