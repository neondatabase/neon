use std::{str::FromStr, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use enumset::EnumSet;
use tracing::error;

use crate::{context::RequestContext, task_mgr::TaskKind};

pub struct Throttle<C: DurationSum> {
    inner: ArcSwap<Inner>,
    wait_time_micros: C,
}

pub struct Inner {
    task_kinds: EnumSet<TaskKind>,
    rate_limiter: Arc<leaky_bucket::RateLimiter>,
}

pub type Config = pageserver_api::models::ThrottleConfig;

pub trait DurationSum {
    fn add(&self, duration: Duration);
}

impl<C> Throttle<C>
where
    C: DurationSum,
{
    pub fn new(config: Config, metric: C) -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(Self::new_inner(config))),
            wait_time_micros: metric,
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
        Arc::clone(&inner.rate_limiter)
            .acquire_owned(key_count)
            .await;
        let elapsed = start.elapsed();
        self.wait_time_micros.add(elapsed);
    }
}
