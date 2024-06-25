use std::{num::NonZeroUsize, sync::Arc};

#[derive(Default, Debug, PartialEq, Eq)]
pub enum L0FlushConfig {
    #[default]
    PageCached,
    Direct {
        max_memory: NonZeroUsize,
        max_concurrency: NonZeroUsize,
    },
}

pub struct L0FlushGlobalState(Arc<Inner>);

pub(crate) enum Inner {
    PageCached,
    Direct {
        config: L0FlushConfig,
        semaphore: tokio::sync::Semaphore,
    },
}

impl L0FlushGlobalState {
    pub fn new(config: L0FlushConfig) -> Self {
        match config {
            L0FlushConfig::PageCached => Self(Inner::PageCached),
            L0FlushConfig::Direct {
                max_memory,
                max_concurrency,
            } => {
                let semaphore = tokio::sync::Semaphore::new(max_concurrency.get());
                Self(Inner::Direct { config, semaphore })
            }
        }
    }

    pub(crate) fn inner(&self) -> &Inner {
        &self.0
    }
}
