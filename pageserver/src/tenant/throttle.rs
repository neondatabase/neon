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
use tokio::sync::Notify;
use tracing::{error, warn};
use utils::leaky_bucket::{LeakyBucketConfig, LeakyBucketState};

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
            fair,
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

        // how frequently we drain a single token on average
        let time_cost = refill_interval / refill_amount.get() as u32;
        let bucket_width = time_cost * (max as u32);

        // initial tracks how many tokens are available to put in the bucket
        // we want how many tokens are currently in the bucket
        let initial_tokens = (max - initial) as u32;
        let end = time_cost * initial_tokens;

        let rate_limiter = RateLimiter {
            config: LeakyBucketConfig {
                epoch: tokio::time::Instant::now(),
                drain_interval: refill_interval,
                cost: time_cost,
                bucket_width,
            },
            state: Mutex::new(LeakyBucketState::new(end)),
            queue: fair.then(Notify::new),
        };

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

struct RateLimiter {
    config: LeakyBucketConfig,
    state: Mutex<LeakyBucketState>,

    /// if this rate limiter is fair,
    /// provide a queue to provide this fair ordering.
    queue: Option<Notify>,
}

impl RateLimiter {
    fn steady_rps(&self) -> f64 {
        self.config.cost.as_secs_f64().recip()
    }

    /// returns true if we did throttle
    async fn acquire(&self, count: usize) -> bool {
        let mut throttled = false;

        // wait until we are the first in the queue
        if let Some(queue) = &self.queue {
            let mut notified = std::pin::pin!(queue.notified());
            if !notified.as_mut().enable() {
                throttled = true;
                notified.await;
            }
        }

        // notify the next waiter in the queue when we are done.
        scopeguard::defer! {
            if let Some(queue) = &self.queue {
                queue.notify_one();
            }
        };

        loop {
            let now = tokio::time::Instant::now();

            let res = self
                .state
                .lock()
                .unwrap()
                .add_tokens(&self.config, now, count as f64);
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
