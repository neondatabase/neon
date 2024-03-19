use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash},
    net::IpAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use anyhow::bail;
use dashmap::DashMap;
use itertools::Itertools;
use rand::{rngs::StdRng, Rng, SeedableRng};
use tokio::sync::{Mutex as AsyncMutex, Semaphore, SemaphorePermit};
use tokio::time::{timeout, Duration, Instant};
use tracing::info;

use crate::{intern::EndpointIdInt, EndpointId};

use super::{
    limit_algorithm::{LimitAlgorithm, Sample},
    RateLimiterConfig,
};

pub struct RedisRateLimiter {
    data: Vec<RateBucket>,
    info: &'static [RateBucketInfo],
}

impl RedisRateLimiter {
    pub fn new(info: &'static [RateBucketInfo]) -> Self {
        Self {
            data: vec![
                RateBucket {
                    start: Instant::now(),
                    count: 0,
                };
                info.len()
            ],
            info,
        }
    }

    /// Check that number of connections is below `max_rps` rps.
    pub fn check(&mut self) -> bool {
        let now = Instant::now();

        let should_allow_request = self
            .data
            .iter_mut()
            .zip(self.info)
            .all(|(bucket, info)| bucket.should_allow_request(info, now, 1));

        if should_allow_request {
            // only increment the bucket counts if the request will actually be accepted
            self.data.iter_mut().for_each(|b| b.inc(1));
        }

        should_allow_request
    }
}

// Simple per-endpoint rate limiter.
//
// Check that number of connections to the endpoint is below `max_rps` rps.
// Purposefully ignore user name and database name as clients can reconnect
// with different names, so we'll end up sending some http requests to
// the control plane.
//
// We also may save quite a lot of CPU (I think) by bailing out right after we
// saw SNI, before doing TLS handshake. User-side error messages in that case
// does not look very nice (`SSL SYSCALL error: Undefined error: 0`), so for now
// I went with a more expensive way that yields user-friendlier error messages.
pub type EndpointRateLimiter = BucketRateLimiter<EndpointId, StdRng, RandomState>;

// This can't be just per IP because that would limit some PaaS that share IP addresses
pub type AuthRateLimiter = BucketRateLimiter<(EndpointIdInt, IpAddr), StdRng, RandomState>;

pub struct BucketRateLimiter<Key, Rand = StdRng, Hasher = RandomState> {
    map: DashMap<Key, Vec<RateBucket>, Hasher>,
    info: &'static [RateBucketInfo],
    access_count: AtomicUsize,
    rand: Mutex<Rand>,
}

#[derive(Clone, Copy)]
struct RateBucket {
    start: Instant,
    count: u32,
}

impl RateBucket {
    fn should_allow_request(&mut self, info: &RateBucketInfo, now: Instant, n: u32) -> bool {
        if now - self.start < info.interval {
            self.count + n <= info.max_rpi
        } else {
            // bucket expired, reset
            self.count = 0;
            self.start = now;

            true
        }
    }

    fn inc(&mut self, n: u32) {
        self.count += n;
    }
}

#[derive(Clone, Copy, PartialEq)]
pub struct RateBucketInfo {
    pub interval: Duration,
    // requests per interval
    pub max_rpi: u32,
}

impl std::fmt::Display for RateBucketInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rps = self.max_rpi * 1000 / self.interval.as_millis() as u32;
        write!(f, "{rps}@{}", humantime::format_duration(self.interval))
    }
}

impl std::fmt::Debug for RateBucketInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::str::FromStr for RateBucketInfo {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some((max_rps, interval)) = s.split_once('@') else {
            bail!("invalid rate info")
        };
        let max_rps = max_rps.parse()?;
        let interval = humantime::parse_duration(interval)?;
        Ok(Self::new(max_rps, interval))
    }
}

impl RateBucketInfo {
    pub const DEFAULT_ENDPOINT_SET: [Self; 3] = [
        Self::new(300, Duration::from_secs(1)),
        Self::new(200, Duration::from_secs(60)),
        Self::new(100, Duration::from_secs(600)),
    ];

    /// All of these are per endpoint-ip pair.
    /// Context: 4096 rounds of pbkdf2 take about 1ms of cpu time to execute (1 milli-cpu-second or 1mcpus).
    ///
    /// First bucket: 300mcpus total per endpoint-ip pair
    /// * 1228800 requests per second with 1 hash rounds. (endpoint rate limiter will catch this first)
    /// * 300 requests per second with 4096 hash rounds.
    /// * 2 requests per second with 600000 hash rounds.
    pub const DEFAULT_AUTH_SET: [Self; 3] = [
        Self::new(300 * 4096, Duration::from_secs(1)),
        Self::new(200 * 4096, Duration::from_secs(60)),
        Self::new(100 * 4096, Duration::from_secs(600)),
    ];

    pub fn validate(info: &mut [Self]) -> anyhow::Result<()> {
        info.sort_unstable_by_key(|info| info.interval);
        let invalid = info
            .iter()
            .tuple_windows()
            .find(|(a, b)| a.max_rpi > b.max_rpi);
        if let Some((a, b)) = invalid {
            bail!(
                "invalid bucket RPS limits. {b} allows fewer requests per bucket than {a} ({} vs {})",
                b.max_rpi,
                a.max_rpi,
            );
        }

        Ok(())
    }

    pub const fn new(max_rps: u32, interval: Duration) -> Self {
        Self {
            interval,
            max_rpi: ((max_rps as u64) * (interval.as_millis() as u64) / 1000) as u32,
        }
    }
}

impl<K: Hash + Eq> BucketRateLimiter<K> {
    pub fn new(info: &'static [RateBucketInfo]) -> Self {
        Self::new_with_rand_and_hasher(info, StdRng::from_entropy(), RandomState::new())
    }
}

impl<K: Hash + Eq, R: Rng, S: BuildHasher + Clone> BucketRateLimiter<K, R, S> {
    fn new_with_rand_and_hasher(info: &'static [RateBucketInfo], rand: R, hasher: S) -> Self {
        info!(buckets = ?info, "endpoint rate limiter");
        Self {
            info,
            map: DashMap::with_hasher_and_shard_amount(hasher, 64),
            access_count: AtomicUsize::new(1), // start from 1 to avoid GC on the first request
            rand: Mutex::new(rand),
        }
    }

    /// Check that number of connections to the endpoint is below `max_rps` rps.
    pub fn check(&self, key: K, n: u32) -> bool {
        // do a partial GC every 2k requests. This cleans up ~ 1/64th of the map.
        // worst case memory usage is about:
        //    = 2 * 2048 * 64 * (48B + 72B)
        //    = 30MB
        if self.access_count.fetch_add(1, Ordering::AcqRel) % 2048 == 0 {
            self.do_gc();
        }

        let now = Instant::now();
        let mut entry = self.map.entry(key).or_insert_with(|| {
            vec![
                RateBucket {
                    start: now,
                    count: 0,
                };
                self.info.len()
            ]
        });

        let should_allow_request = entry
            .iter_mut()
            .zip(self.info)
            .all(|(bucket, info)| bucket.should_allow_request(info, now, n));

        if should_allow_request {
            // only increment the bucket counts if the request will actually be accepted
            entry.iter_mut().for_each(|b| b.inc(n));
        }

        should_allow_request
    }

    /// Clean the map. Simple strategy: remove all entries in a random shard.
    /// At worst, we'll double the effective max_rps during the cleanup.
    /// But that way deletion does not aquire mutex on each entry access.
    pub fn do_gc(&self) {
        info!(
            "cleaning up bucket rate limiter, current size = {}",
            self.map.len()
        );
        let n = self.map.shards().len();
        // this lock is ok as the periodic cycle of do_gc makes this very unlikely to collide
        // (impossible, infact, unless we have 2048 threads)
        let shard = self.rand.lock().unwrap().gen_range(0..n);
        self.map.shards()[shard].write().clear();
    }
}

/// Limits the number of concurrent jobs.
///
/// Concurrency is limited through the use of [Token]s. Acquire a token to run a job, and release the
/// token once the job is finished.
///
/// The limit will be automatically adjusted based on observed latency (delay) and/or failures
/// caused by overload (loss).
pub struct Limiter {
    limit_algo: AsyncMutex<Box<dyn LimitAlgorithm>>,
    semaphore: std::sync::Arc<Semaphore>,
    config: RateLimiterConfig,

    // ONLY WRITE WHEN LIMIT_ALGO IS LOCKED
    limits: AtomicUsize,

    // ONLY USE ATOMIC ADD/SUB
    in_flight: Arc<AtomicUsize>,

    #[cfg(test)]
    notifier: Option<std::sync::Arc<tokio::sync::Notify>>,
}

/// A concurrency token, required to run a job.
///
/// Release the token back to the [Limiter] after the job is complete.
#[derive(Debug)]
pub struct Token<'t> {
    permit: Option<tokio::sync::SemaphorePermit<'t>>,
    start: Instant,
    in_flight: Arc<AtomicUsize>,
}

/// A snapshot of the state of the [Limiter].
///
/// Not guaranteed to be consistent under high concurrency.
#[derive(Debug, Clone, Copy)]
pub struct LimiterState {
    limit: usize,
    in_flight: usize,
}

/// Whether a job succeeded or failed as a result of congestion/overload.
///
/// Errors not considered to be caused by overload should be ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The job succeeded, or failed in a way unrelated to overload.
    Success,
    /// The job failed because of overload, e.g. it timed out or an explicit backpressure signal
    /// was observed.
    Overload,
}

impl Outcome {
    fn from_reqwest_error(error: &reqwest_middleware::Error) -> Self {
        match error {
            reqwest_middleware::Error::Middleware(_) => Outcome::Success,
            reqwest_middleware::Error::Reqwest(e) => {
                if let Some(status) = e.status() {
                    if status.is_server_error()
                        || reqwest::StatusCode::TOO_MANY_REQUESTS.as_u16() == status
                    {
                        Outcome::Overload
                    } else {
                        Outcome::Success
                    }
                } else {
                    Outcome::Success
                }
            }
        }
    }
    fn from_reqwest_response(response: &reqwest::Response) -> Self {
        if response.status().is_server_error()
            || response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
        {
            Outcome::Overload
        } else {
            Outcome::Success
        }
    }
}

impl Limiter {
    /// Create a limiter with a given limit control algorithm.
    pub fn new(config: RateLimiterConfig) -> Self {
        assert!(config.initial_limit > 0);
        Self {
            limit_algo: AsyncMutex::new(config.create_rate_limit_algorithm()),
            semaphore: Arc::new(Semaphore::new(config.initial_limit)),
            config,
            limits: AtomicUsize::new(config.initial_limit),
            in_flight: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            notifier: None,
        }
    }
    // pub fn new(limit_algorithm: T, timeout: Duration, initial_limit: usize) -> Self {
    //     assert!(initial_limit > 0);

    //     Self {
    //         limit_algo: AsyncMutex::new(limit_algorithm),
    //         semaphore: Arc::new(Semaphore::new(initial_limit)),
    //         timeout,
    //         limits: AtomicUsize::new(initial_limit),
    //         in_flight: Arc::new(AtomicUsize::new(0)),
    //         #[cfg(test)]
    //         notifier: None,
    //     }
    // }

    /// In some cases [Token]s are acquired asynchronously when updating the limit.
    #[cfg(test)]
    pub fn with_release_notifier(mut self, n: std::sync::Arc<tokio::sync::Notify>) -> Self {
        self.notifier = Some(n);
        self
    }

    /// Try to immediately acquire a concurrency [Token].
    ///
    /// Returns `None` if there are none available.
    pub fn try_acquire(&self) -> Option<Token> {
        let result = if self.config.disable {
            // If the rate limiter is disabled, we can always acquire a token.
            Some(Token::new(None, self.in_flight.clone()))
        } else {
            self.semaphore
                .try_acquire()
                .map(|permit| Token::new(Some(permit), self.in_flight.clone()))
                .ok()
        };
        if result.is_some() {
            self.in_flight.fetch_add(1, Ordering::AcqRel);
        }
        result
    }

    /// Try to acquire a concurrency [Token], waiting for `duration` if there are none available.
    ///
    /// Returns `None` if there are none available after `duration`.
    pub async fn acquire_timeout(&self, duration: Duration) -> Option<Token<'_>> {
        info!("acquiring token: {:?}", self.semaphore.available_permits());
        let result = if self.config.disable {
            // If the rate limiter is disabled, we can always acquire a token.
            Some(Token::new(None, self.in_flight.clone()))
        } else {
            match timeout(duration, self.semaphore.acquire()).await {
                Ok(maybe_permit) => maybe_permit
                    .map(|permit| Token::new(Some(permit), self.in_flight.clone()))
                    .ok(),
                Err(_) => None,
            }
        };
        if result.is_some() {
            self.in_flight.fetch_add(1, Ordering::AcqRel);
        }
        result
    }

    /// Return the concurrency [Token], along with the outcome of the job.
    ///
    /// The [Outcome] of the job, and the time taken to perform it, may be used
    /// to update the concurrency limit.
    ///
    /// Set the outcome to `None` to ignore the job.
    pub async fn release(&self, mut token: Token<'_>, outcome: Option<Outcome>) {
        tracing::info!("outcome is {:?}", outcome);
        let in_flight = self.in_flight.load(Ordering::Acquire);
        let old_limit = self.limits.load(Ordering::Acquire);
        let available = if self.config.disable {
            0 // This is not used in the algorithm and can be anything. If the config disable it makes sense to set it to 0.
        } else {
            self.semaphore.available_permits()
        };
        let total = in_flight + available;

        let mut algo = self.limit_algo.lock().await;

        let new_limit = if let Some(outcome) = outcome {
            let sample = Sample {
                latency: token.start.elapsed(),
                in_flight,
                outcome,
            };
            algo.update(old_limit, sample).await
        } else {
            old_limit
        };
        tracing::info!("new limit is {}", new_limit);
        let actual_limit = if new_limit < total {
            token.forget();
            total.saturating_sub(1)
        } else {
            if !self.config.disable {
                self.semaphore.add_permits(new_limit.saturating_sub(total));
            }
            new_limit
        };
        crate::metrics::RATE_LIMITER_LIMIT
            .with_label_values(&["expected"])
            .set(new_limit as i64);
        crate::metrics::RATE_LIMITER_LIMIT
            .with_label_values(&["actual"])
            .set(actual_limit as i64);
        self.limits.store(new_limit, Ordering::Release);
        #[cfg(test)]
        if let Some(n) = &self.notifier {
            n.notify_one();
        }
    }

    /// The current state of the limiter.
    pub fn state(&self) -> LimiterState {
        let limit = self.limits.load(Ordering::Relaxed);
        let in_flight = self.in_flight.load(Ordering::Relaxed);
        LimiterState { limit, in_flight }
    }
}

impl<'t> Token<'t> {
    fn new(permit: Option<SemaphorePermit<'t>>, in_flight: Arc<AtomicUsize>) -> Self {
        Self {
            permit,
            start: Instant::now(),
            in_flight,
        }
    }

    pub fn forget(&mut self) {
        if let Some(permit) = self.permit.take() {
            permit.forget();
        }
    }
}

impl Drop for Token<'_> {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

impl LimiterState {
    /// The current concurrency limit.
    pub fn limit(&self) -> usize {
        self.limit
    }
    /// The number of jobs in flight.
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }
}

#[async_trait::async_trait]
impl reqwest_middleware::Middleware for Limiter {
    async fn handle(
        &self,
        req: reqwest::Request,
        extensions: &mut task_local_extensions::Extensions,
        next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let start = Instant::now();
        let token = self
            .acquire_timeout(self.config.timeout)
            .await
            .ok_or_else(|| {
                reqwest_middleware::Error::Middleware(
                    // TODO: Should we map it into user facing errors?
                    crate::console::errors::ApiError::Console {
                        status: crate::http::StatusCode::TOO_MANY_REQUESTS,
                        text: "Too many requests".into(),
                    }
                    .into(),
                )
            })?;
        info!(duration = ?start.elapsed(), "waiting for token to connect to the control plane");
        crate::metrics::RATE_LIMITER_ACQUIRE_LATENCY.observe(start.elapsed().as_secs_f64());
        match next.run(req, extensions).await {
            Ok(response) => {
                self.release(token, Some(Outcome::from_reqwest_response(&response)))
                    .await;
                Ok(response)
            }
            Err(e) => {
                self.release(token, Some(Outcome::from_reqwest_error(&e)))
                    .await;
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{hash::BuildHasherDefault, pin::pin, task::Context, time::Duration};

    use futures::{task::noop_waker_ref, Future};
    use rand::SeedableRng;
    use rustc_hash::FxHasher;
    use tokio::time;

    use super::{BucketRateLimiter, EndpointRateLimiter, Limiter, Outcome};
    use crate::{
        rate_limiter::{RateBucketInfo, RateLimitAlgorithm},
        EndpointId,
    };

    #[tokio::test]
    async fn it_works() {
        let config = super::RateLimiterConfig {
            algorithm: RateLimitAlgorithm::Fixed,
            timeout: Duration::from_secs(1),
            initial_limit: 10,
            disable: false,
            ..Default::default()
        };
        let limiter = Limiter::new(config);

        let token = limiter.try_acquire().unwrap();

        limiter.release(token, Some(Outcome::Success)).await;

        assert_eq!(limiter.state().limit(), 10);
    }

    #[tokio::test]
    async fn is_fair() {
        let config = super::RateLimiterConfig {
            algorithm: RateLimitAlgorithm::Fixed,
            timeout: Duration::from_secs(1),
            initial_limit: 1,
            disable: false,
            ..Default::default()
        };
        let limiter = Limiter::new(config);

        // === TOKEN 1 ===
        let token1 = limiter.try_acquire().unwrap();

        let mut token2_fut = pin!(limiter.acquire_timeout(Duration::from_secs(1)));
        assert!(
            token2_fut
                .as_mut()
                .poll(&mut Context::from_waker(noop_waker_ref()))
                .is_pending(),
            "token is acquired by token1"
        );

        let mut token3_fut = pin!(limiter.acquire_timeout(Duration::from_secs(1)));
        assert!(
            token3_fut
                .as_mut()
                .poll(&mut Context::from_waker(noop_waker_ref()))
                .is_pending(),
            "token is acquired by token1"
        );

        limiter.release(token1, Some(Outcome::Success)).await;
        // === END TOKEN 1 ===

        // === TOKEN 2 ===
        assert!(
            limiter.try_acquire().is_none(),
            "token is acquired by token2"
        );

        assert!(
            token3_fut
                .as_mut()
                .poll(&mut Context::from_waker(noop_waker_ref()))
                .is_pending(),
            "token is acquired by token2"
        );

        let token2 = token2_fut.await.unwrap();

        limiter.release(token2, Some(Outcome::Success)).await;
        // === END TOKEN 2 ===

        // === TOKEN 3 ===
        assert!(
            limiter.try_acquire().is_none(),
            "token is acquired by token3"
        );

        let token3 = token3_fut.await.unwrap();
        limiter.release(token3, Some(Outcome::Success)).await;
        // === END TOKEN 3 ===

        // === TOKEN 4 ===
        let token4 = limiter.try_acquire().unwrap();
        limiter.release(token4, Some(Outcome::Success)).await;
    }

    #[tokio::test]
    async fn disable() {
        let config = super::RateLimiterConfig {
            algorithm: RateLimitAlgorithm::Fixed,
            timeout: Duration::from_secs(1),
            initial_limit: 1,
            disable: true,
            ..Default::default()
        };
        let limiter = Limiter::new(config);

        // === TOKEN 1 ===
        let token1 = limiter.try_acquire().unwrap();
        let token2 = limiter.try_acquire().unwrap();
        let state = limiter.state();
        assert_eq!(state.limit(), 1);
        assert_eq!(state.in_flight(), 2); // For disabled limiter, it's expected.
        limiter.release(token1, None).await;
        limiter.release(token2, None).await;
    }

    #[test]
    fn rate_bucket_rpi() {
        let rate_bucket = RateBucketInfo::new(50, Duration::from_secs(5));
        assert_eq!(rate_bucket.max_rpi, 50 * 5);

        let rate_bucket = RateBucketInfo::new(50, Duration::from_millis(500));
        assert_eq!(rate_bucket.max_rpi, 50 / 2);
    }

    #[test]
    fn rate_bucket_parse() {
        let rate_bucket: RateBucketInfo = "100@10s".parse().unwrap();
        assert_eq!(rate_bucket.interval, Duration::from_secs(10));
        assert_eq!(rate_bucket.max_rpi, 100 * 10);
        assert_eq!(rate_bucket.to_string(), "100@10s");

        let rate_bucket: RateBucketInfo = "100@1m".parse().unwrap();
        assert_eq!(rate_bucket.interval, Duration::from_secs(60));
        assert_eq!(rate_bucket.max_rpi, 100 * 60);
        assert_eq!(rate_bucket.to_string(), "100@1m");
    }

    #[test]
    fn default_rate_buckets() {
        let mut defaults = RateBucketInfo::DEFAULT_ENDPOINT_SET;
        RateBucketInfo::validate(&mut defaults[..]).unwrap();
    }

    #[test]
    #[should_panic = "invalid endpoint RPS limits. 10@10s allows fewer requests per bucket than 300@1s (100 vs 300)"]
    fn rate_buckets_validate() {
        let mut rates: Vec<RateBucketInfo> = ["300@1s", "10@10s"]
            .into_iter()
            .map(|s| s.parse().unwrap())
            .collect();
        RateBucketInfo::validate(&mut rates).unwrap();
    }

    #[tokio::test]
    async fn test_rate_limits() {
        let mut rates: Vec<RateBucketInfo> = ["100@1s", "20@30s"]
            .into_iter()
            .map(|s| s.parse().unwrap())
            .collect();
        RateBucketInfo::validate(&mut rates).unwrap();
        let limiter = EndpointRateLimiter::new(Vec::leak(rates));

        let endpoint = EndpointId::from("ep-my-endpoint-1234");

        time::pause();

        for _ in 0..100 {
            assert!(limiter.check(endpoint.clone(), 1));
        }
        // more connections fail
        assert!(!limiter.check(endpoint.clone(), 1));

        // fail even after 500ms as it's in the same bucket
        time::advance(time::Duration::from_millis(500)).await;
        assert!(!limiter.check(endpoint.clone(), 1));

        // after a full 1s, 100 requests are allowed again
        time::advance(time::Duration::from_millis(500)).await;
        for _ in 1..6 {
            for _ in 0..100 {
                assert!(limiter.check(endpoint.clone(), 1));
            }
            time::advance(time::Duration::from_millis(1000)).await;
        }

        // more connections after 600 will exceed the 20rps@30s limit
        assert!(!limiter.check(endpoint.clone(), 1));

        // will still fail before the 30 second limit
        time::advance(time::Duration::from_millis(30_000 - 6_000 - 1)).await;
        assert!(!limiter.check(endpoint.clone(), 1));

        // after the full 30 seconds, 100 requests are allowed again
        time::advance(time::Duration::from_millis(1)).await;
        for _ in 0..100 {
            assert!(limiter.check(endpoint.clone(), 1));
        }
    }

    #[tokio::test]
    async fn test_rate_limits_gc() {
        // fixed seeded random/hasher to ensure that the test is not flaky
        let rand = rand::rngs::StdRng::from_seed([1; 32]);
        let hasher = BuildHasherDefault::<FxHasher>::default();

        let limiter = BucketRateLimiter::new_with_rand_and_hasher(
            &RateBucketInfo::DEFAULT_ENDPOINT_SET,
            rand,
            hasher,
        );
        for i in 0..1_000_000 {
            limiter.check(i, 1);
        }
        assert!(limiter.map.len() < 150_000);
    }
}
