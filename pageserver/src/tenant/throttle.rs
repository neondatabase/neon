use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use enumset::EnumSet;
use tracing::error;

use crate::{context::RequestContext, task_mgr::TaskKind};

pub struct Throttle<M: Metric> {
    inner: ArcSwap<Inner>,
    metric: M,
    throttled: AtomicBool,
}

pub struct Inner {
    task_kinds: EnumSet<TaskKind>,
    rate_limiter: Arc<leaky_bucket::RateLimiter>,
}

pub type Config = pageserver_api::models::ThrottleConfig;

pub struct Observation {
    pub wait_time: Duration,
}
pub trait Metric {
    fn observe(&self, observation: &Observation);
}

/// See [`Throttle::reset_was_throttled`].
pub enum ResetWasThrottledResult {
    WasNotThrottled,
    WasThrottled,
}

impl<M> Throttle<M>
where
    M: Metric,
{
    pub fn new(config: Config, metric: M) -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(Self::new_inner(config))),
            metric,
            throttled: AtomicBool::new(false),
        }
    }
    fn new_inner(config: Config) -> Inner {
        let Config {
            task_kinds,
            initial,
            interval_millis,
            interval_refill,
            max,
            fair,
        } = config;
        let task_kinds: EnumSet<TaskKind> = task_kinds
            .into_iter()
            .filter_map(|s| match TaskKind::from_str(&s) {
                Ok(v) => Some(v),
                Err(e) => {
                    // TODO: avoid this failure mode
                    error!(
                        "canont parse task kind, ignoring for rate limiting {}",
                        utils::error::report_compact_sources(&e)
                    );
                    None
                }
            })
            .collect();
        Inner {
            task_kinds,
            rate_limiter: Arc::new(
                leaky_bucket::RateLimiter::builder()
                    .initial(initial)
                    .interval(Duration::from_millis(interval_millis.get()))
                    .refill(interval_refill.get())
                    .max(max)
                    .fair(fair)
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
    pub fn reset_was_throttled(&self) -> ResetWasThrottledResult {
        match self.throttled.swap(false, Ordering::Relaxed) {
            false => ResetWasThrottledResult::WasNotThrottled,
            true => ResetWasThrottledResult::WasThrottled,
        }
    }

    pub async fn throttle(&self, ctx: &RequestContext, key_count: usize) {
        let inner = self.inner.load_full(); // clones the `Inner` Arc
        if !inner.task_kinds.contains(ctx.task_kind()) {
            return;
        };
        let start = std::time::Instant::now();
        let mut did_throttle = false;
        let mut acquire_fut = Arc::clone(&inner.rate_limiter).acquire_owned(key_count);
        let mut acquire_fut = std::pin::pin!(acquire_fut);
        std::future::poll_fn(|cx| {
            use std::future::Future;
            let poll = acquire_fut.as_mut().poll(cx);
            did_throttle = did_throttle || poll.is_pending();
            poll
        })
        .await;
        if did_throttle {
            self.throttled.swap(true, Ordering::Relaxed);
            let now = Instant::now();
            let wait_time = now - start;
            let observation = Observation { wait_time };
            self.metric.observe(&observation);
        }
    }
}
