use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use enumset::EnumSet;
use tracing::{error, warn};
use utils::leaky_bucket::{LeakyBucketConfig, RateLimiter};

use crate::{context::RequestContext, task_mgr::TaskKind};

/// Throttle for `async` functions.
///
/// Runtime reconfigurable.
///
/// To share a throttle among multiple entities, wrap it in an [`Arc`].
///
/// The intial use case for this is tenant-wide throttling of getpage@lsn requests.
pub struct Throttle<M: Metric> {
    inner: ArcSwap<Inner>,
    metric: M,
    /// will be turned into [`Stats::count_accounted`]
    count_accounted: AtomicU64,
    /// will be turned into [`Stats::count_throttled`]
    count_throttled: AtomicU64,
    /// will be turned into [`Stats::sum_throttled_usecs`]
    sum_throttled_usecs: AtomicU64,
}

pub struct Inner {
    task_kinds: EnumSet<TaskKind>,
    rate_limiter: Arc<RateLimiter>,
}

pub type Config = pageserver_api::models::ThrottleConfig;

pub struct Observation {
    pub wait_time: Duration,
}
pub trait Metric {
    fn observe_throttling(&self, observation: &Observation);
}

/// See [`Throttle::reset_stats`].
pub struct Stats {
    // Number of requests that were subject to throttling, i.e., requests of the configured [`Config::task_kinds`].
    pub count_accounted: u64,
    // Subset of the `accounted` requests that were actually throttled.
    // Note that the numbers are stored as two independent atomics, so, there might be a slight drift.
    pub count_throttled: u64,
    // Sum of microseconds that throttled requests spent waiting for throttling.
    pub sum_throttled_usecs: u64,
}

impl<M> Throttle<M>
where
    M: Metric,
{
    pub fn new(config: Config, metric: M) -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(Self::new_inner(config))),
            metric,
            count_accounted: AtomicU64::new(0),
            count_throttled: AtomicU64::new(0),
            sum_throttled_usecs: AtomicU64::new(0),
        }
    }
    fn new_inner(config: Config) -> Inner {
        let Config {
            task_kinds,
            initial,
            refill_interval,
            refill_amount,
            max,
            fair: _,
        } = config;
        let task_kinds: EnumSet<TaskKind> = task_kinds
            .iter()
            .filter_map(|s| match TaskKind::from_str(s) {
                Ok(v) => Some(v),
                Err(e) => {
                    // TODO: avoid this failure mode
                    error!(
                        "cannot parse task kind, ignoring for rate limiting {}",
                        utils::error::report_compact_sources(&e)
                    );
                    None
                }
            })
            .collect();

        // steady rate, we expect `refill_amount` requests per `refill_interval`.
        // dividing gives us the rps.
        let rps = f64::from(refill_amount.get()) / refill_interval.as_secs_f64();
        let config = LeakyBucketConfig::new(rps, f64::from(max));

        // initial tracks how many tokens are available to put in the bucket
        // we want how many tokens are currently in the bucket
        let initial_tokens = max - initial;

        let rate_limiter = RateLimiter::with_initial_tokens(config, f64::from(initial_tokens));

        Inner {
            task_kinds,
            rate_limiter: Arc::new(rate_limiter),
        }
    }
    pub fn reconfigure(&self, config: Config) {
        self.inner.store(Arc::new(Self::new_inner(config)));
    }

    /// The [`Throttle`] keeps an internal flag that is true if there was ever any actual throttling.
    /// This method allows retrieving & resetting that flag.
    /// Useful for periodic reporting.
    pub fn reset_stats(&self) -> Stats {
        let count_accounted = self.count_accounted.swap(0, Ordering::Relaxed);
        let count_throttled = self.count_throttled.swap(0, Ordering::Relaxed);
        let sum_throttled_usecs = self.sum_throttled_usecs.swap(0, Ordering::Relaxed);
        Stats {
            count_accounted,
            count_throttled,
            sum_throttled_usecs,
        }
    }

    /// See [`Config::steady_rps`].
    pub fn steady_rps(&self) -> f64 {
        self.inner.load().rate_limiter.steady_rps()
    }

    pub async fn throttle(&self, ctx: &RequestContext, key_count: usize) -> Option<Duration> {
        let inner = self.inner.load_full(); // clones the `Inner` Arc
        if !inner.task_kinds.contains(ctx.task_kind()) {
            return None;
        };
        let start = std::time::Instant::now();

        let did_throttle = inner.rate_limiter.acquire(key_count).await;

        self.count_accounted.fetch_add(1, Ordering::Relaxed);
        if did_throttle {
            self.count_throttled.fetch_add(1, Ordering::Relaxed);
            let now = Instant::now();
            let wait_time = now - start;
            self.sum_throttled_usecs
                .fetch_add(wait_time.as_micros() as u64, Ordering::Relaxed);
            let observation = Observation { wait_time };
            self.metric.observe_throttling(&observation);
            match ctx.micros_spent_throttled.add(wait_time) {
                Ok(res) => res,
                Err(error) => {
                    use once_cell::sync::Lazy;
                    use utils::rate_limit::RateLimit;
                    static WARN_RATE_LIMIT: Lazy<Mutex<RateLimit>> =
                        Lazy::new(|| Mutex::new(RateLimit::new(Duration::from_secs(10))));
                    let mut guard = WARN_RATE_LIMIT.lock().unwrap();
                    guard.call(move || {
                        warn!(error, "error adding time spent throttled; this message is logged at a global rate limit");
                    });
                }
            }
            Some(wait_time)
        } else {
            None
        }
    }
}
