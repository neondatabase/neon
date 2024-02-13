use std::{
    str::FromStr,
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use enumset::EnumSet;
use tracing::error;

use crate::{context::RequestContext, task_mgr::TaskKind};

pub struct Throttle<M: Metric> {
    inner: ArcSwap<Inner>,
    metric: M,
    last_throttled_at: std::sync::Mutex<Option<Instant>>,
}

pub struct Inner {
    task_kinds: EnumSet<TaskKind>,
    rate_limiter: Arc<leaky_bucket::RateLimiter>,
}

pub type Config = pageserver_api::models::ThrottleConfig;

pub struct Observation {
    pub wait_time: Duration,
    pub unthrottled_for: bool,
}
pub trait Metric {
    fn observe(&self, observation: &Observation);
}

impl<M> Throttle<M>
where
    M: Metric,
{
    pub fn new(config: Config, metric: M) -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(Self::new_inner(config))),
            metric,
            last_throttled_at: std::sync::Mutex::new(None),
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
            use std::task::Poll;
            let poll = acquire_fut.as_mut().poll(cx);
            did_throttle = matches!(Poll::<()>::Pending, poll);
            poll
        })
        .await;
        let now = Instant::now();
        let wait_time = now - start;
        if did_throttle {
            let unthrottled_for = self
                .last_throttled_at
                .lock()
                .unwrap()
                .replace(now)
                .map(|prev| now - prev);
            std::mem::forget(unthrottled_for);
            let observation = Observation {
                wait_time,
                unthrottled_for: false,
            };
            self.metric.observe(&observation);
        }
    }
}
