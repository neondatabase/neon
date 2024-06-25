use std::{num::NonZeroUsize, sync::Arc};

#[derive(Default)]
pub enum L0FlushConfig {
    #[default]
    PageCached,
    Direct {
        /// Concurrent L0 flushes are limited to consume at most `max_memory` bytes of memory.
        /// If there are a lot of small L0s that need to be flushed, a lot of flushes can happen in parallel.
        /// If there is a large L0 to be flushed, it might have to wait until preceding flushes are done.
        max_memory: MaxMemory,
    },
}

/// Deserializer guarantees that that initializing the `tokio::sync::Semaphore` will succeed.
pub struct MaxMemory(NonZeroUsize);

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
            L0FlushConfig::Direct { max_memory } => {
                let semaphore = tokio::sync::Semaphore::new(max_memory.0.get());
                Self(Inner::Direct { config, semaphore })
            }
        }
    }

    pub(crate) fn inner(&self) -> &Inner {
        &self.0
    }
}
