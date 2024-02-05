use std::{sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use enumset::EnumSet;
use tracing::error;

use crate::{context::RequestContext, task_mgr::TaskKind};

pub struct Throttle {
    inner: ArcSwap<Inner>,
}

pub struct Inner {
    task_kinds: EnumSet<TaskKind>,
    rate_limiter: Arc<leaky_bucket::RateLimiter>,
}

pub type Config = pageserver_api::models::ThrottleConfig;

impl Throttle {
    pub fn new(config: Config) -> Self {
        let Config {
            task_kinds,
            initial,
            interval_millis,
            max,
            fair,
        } = config;
        let task_kinds: EnumSet<TaskKind> = task_kinds
            .into_iter()
            .filter_map(|s| match serde_json::from_str::<'_, TaskKind>(&s) {
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
        Self {
            inner: ArcSwap::new(Arc::new(Inner {
                task_kinds,
                rate_limiter: Arc::new(
                    leaky_bucket::RateLimiter::builder()
                        .initial(initial)
                        .interval(Duration::from_millis(interval_millis.get()))
                        .max(max)
                        .fair(fair)
                        .build(),
                ),
            })),
        }
    }
    pub fn reconfigure(&self, config: Config) {
        let new = Self::new(config);
        self.inner.store(ArcSwap::into_inner(new.inner));
    }

    #[inline(always)]
    pub async fn throttle(&self, ctx: &RequestContext) {
        let inner = self.inner.load_full(); // clones the `Inner` Arc
        if !inner.task_kinds.contains(ctx.task_kind()) {
            return;
        };
        // weird api...
        Arc::clone(&inner.rate_limiter).acquire_owned(1).await;
    }
}
