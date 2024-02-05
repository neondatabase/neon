use std::{num::NonZeroU64, sync::Arc, time::Duration};

use itertools::Itertools;

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
    pub interval_millis: NonZeroU64,
    pub max: usize,
    pub fair: bool,
}

impl Config {
    pub fn disabled() -> Config {
        Config {
            task_kinds: EnumSet::empty(),
            initial: 0,
            interval_millis: NonZeroU64::try_from(1).unwrap(),
            max: 0,
            fair: true,
        }
    }
}

impl TryFrom<pageserver_api::models::ThrottleConfig> for Config {
    type Error = String;

    fn try_from(value: pageserver_api::models::ThrottleConfig) -> Result<Self, Self::Error> {
        let pageserver_api::models::ThrottleConfig {
            task_kinds,
            initial,
            interval_millis,
            max,
            fair,
        } = value;

        let task_kinds: EnumSet<TaskKind> = task_kinds
            .into_iter()
            .map(|s| {
                serde_json::from_str::<'_, TaskKind>(&s).map_err(|e| {
                    format!(
                        "canont parse task kind: {}",
                        utils::error::report_compact_sources(&e)
                    )
                })
            })
            .try_collect()?;

        Ok(Self {
            task_kinds,
            initial,
            interval_millis,
            max,
            fair,
        })
    }
}

impl From<Config> for pageserver_api::models::ThrottleConfig {
    fn from(value: Config) -> Self {
        let Config {
            task_kinds,
            initial,
            interval_millis,
            max,
            fair,
        } = value;
        pageserver_api::models::ThrottleConfig {
            task_kinds: task_kinds
                .iter()
                .map(|k| serde_json::to_string(&k))
                .try_collect()
                .expect("TaskKind serialization cannot fail"),
            initial,
            interval_millis,
            max,
            fair,
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
            inner: ArcSwap::new(Arc::new(Inner {
                task_kinds: config.task_kinds,
                throttle: leaky_bucket::RateLimiter::builder()
                    .initial(initial)
                    .interval(Duration::from_millis(interval_millis.get()))
                    .max(max)
                    .fair(fair)
                    .build(),
            })),
        }
    }
    pub fn reconfigure(&self, config: Config) {
        todo!()
        // let new = Self::new(config);
        // self.inner.store(new);
    }

    pub fn acquire_one(&self) -> Permit<'_> {
        todo!()
        // let this = self.inner.load();
        // this.acquire()
    }
}

pub(crate) struct Permit<'a> {
    inner: leaky_bucket::Acquire<'a>,
}
