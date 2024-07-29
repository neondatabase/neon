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
        Inner {
            task_kinds,
            rate_limiter: Arc::new(
                RateLimiterBuilder {
                    initial,
                    refill_interval,
                    refill: refill_amount.get(),
                    max,
                    fair,
                }
                .build(),
            ),
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

        let did_throttle = !inner.rate_limiter.acquire(key_count).await;

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
    epoch: tokio::time::Instant,

    /// "time cost" of a single request unit.
    /// loosely represents how long it takes to handle a request unit in active CPU time.
    time_cost: Duration,

    bucket_width: Duration,

    interval: Duration,

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
    end: Mutex<Duration>,

    queue: Option<Notify>,
}

struct RateLimiterBuilder {
    /// The max number of tokens.
    max: usize,
    /// The initial count of tokens.
    initial: usize,
    /// Tokens to add every `per` duration.
    refill: usize,
    /// Interval to add tokens in milliseconds.
    refill_interval: Duration,
    /// If the rate limiter is fair or not.
    fair: bool,
}

impl RateLimiterBuilder {
    fn build(self) -> RateLimiter {
        let queue = self.fair.then(Notify::new);

        let time_cost = self.refill_interval / self.refill as u32;
        let bucket_width = time_cost * (self.max as u32);
        let initial_allow = time_cost * (self.initial as u32);
        let end = bucket_width - initial_allow;

        RateLimiter {
            epoch: tokio::time::Instant::now(),
            time_cost,
            bucket_width,
            interval: self.refill_interval,
            end: Mutex::new(end),
            queue,
        }
    }
}

impl RateLimiter {
    fn steady_rps(&self) -> f64 {
        self.time_cost.as_secs_f64().recip()
    }

    /// returns true if not throttled
    async fn acquire(&self, count: usize) -> bool {
        let mut not_throttled = true;

        let n = self.time_cost.mul_f64(count as f64);

        // wait until we are the first in the queue
        if let Some(queue) = &self.queue {
            let mut notified = std::pin::pin!(queue.notified());
            if !notified.as_mut().enable() {
                not_throttled = false;
                notified.await;
            }
        }

        // notify the next waiter in the queue
        scopeguard::defer! {
            if let Some(queue) = &self.queue {
                queue.notify_one();
            }
        };

        loop {
            let now = tokio::time::Instant::now();
            let now = now - self.epoch;

            // we only "add" new tokens on a fixed interval.
            // truncate to the most recent multiple of self.interval.
            let now = self
                .interval
                .mul_f64(now.div_duration_f64(self.interval).trunc());

            let ready_at = {
                //       start          end
                //       |     start+n  |     end+n
                //       |   /          |   /
                // ------{o-[---------o-}--]----o----
                //   now1 ^      now2 ^         ^ now3
                //
                // at now1, the bucket would be completely filled if we add n tokens.
                // at now2, the bucket would be partially filled if we add n tokens.
                // at now3, the bucket would start completely empty before we add n tokens.

                let mut end = self.end.lock().unwrap();
                let start = *end - self.bucket_width;
                let ready_at = start + n;

                if *end + n <= now {
                    *end = now + n;
                    return not_throttled;
                } else if ready_at <= now {
                    *end += n;
                    return not_throttled;
                }

                ready_at
            };

            not_throttled = false;
            tokio::time::sleep_until(self.epoch + ready_at).await;
        }
    }
}
