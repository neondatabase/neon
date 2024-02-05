use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use enumset::EnumSet;

use crate::task_mgr::TaskKind;

pub struct Throttle {
    inner: ArcSwap<Inner>,
}

pub struct Inner {
    task_kinds: EnumSet<TaskKind>,
    throttle: leaky_bucket::RateLimiter,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub task_kinds: EnumSet<TaskKind>, // empty set disables the rate limit
    pub initial: usize,
    pub interval_millis: NonZeroUsize,
    pub max: usize,
    pub fair: bool,
}

impl Config {
    pub fn disabled() -> Config {
        Config {
            task_kinds: EnumSet::empty(),
            inital: 0,
            interval_millis: NonZeroUsize::try_from(1).unwrap(),
            max: 0,
            fair: true,
        }
    }
}

impl Throttle {
    pub fn new(config: Config) -> Self {
        let Config {
            task_kinds,
            initial,
            interval_millis,
            max,
            fair,
        } = config;
        Self {
            inner: Arc::new(Inner {
                task_kinds: config.task_kinds,
                throttle: leaky_bucket::RateLimiter::builder()
                    .initial(initial)
                    .interval(Duration::from_millis(interval_millis.get()))
                    .max(max)
                    .fair(fair),
            }),
        }
    }
    pub fn reconfigure(&self, config: Config) {
        let new = Self::new(config);
        self.inner.store(new);
    }

    pub fn acquire(&self) -> Permit<'_> {
        let this = self.inner.load();
        this.acquire()
    }
}

struct Permit<'a> {
    inner: leaky_bucket::Acquire<'a>,
}
