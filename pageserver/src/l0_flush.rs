use std::{num::NonZeroUsize, sync::Arc};

use crate::tenant::ephemeral_file;

#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum L0FlushConfig {
    PageCached,
    #[serde(rename_all = "snake_case")]
    Direct {
        max_concurrency: NonZeroUsize,
    },
}

impl Default for L0FlushConfig {
    fn default() -> Self {
        Self::Direct {
            // TODO: using num_cpus results in different peak memory usage on different instance types.
            max_concurrency: NonZeroUsize::new(usize::max(1, num_cpus::get())).unwrap(),
        }
    }
}

#[derive(Clone)]
pub struct L0FlushGlobalState(Arc<Inner>);

pub enum Inner {
    PageCached,
    Direct { semaphore: tokio::sync::Semaphore },
}

impl L0FlushGlobalState {
    pub fn new(config: L0FlushConfig) -> Self {
        match config {
            L0FlushConfig::PageCached => Self(Arc::new(Inner::PageCached)),
            L0FlushConfig::Direct { max_concurrency } => {
                let semaphore = tokio::sync::Semaphore::new(max_concurrency.get());
                Self(Arc::new(Inner::Direct { semaphore }))
            }
        }
    }

    pub fn inner(&self) -> &Arc<Inner> {
        &self.0
    }
}

impl L0FlushConfig {
    pub(crate) fn prewarm_on_write(&self) -> ephemeral_file::PrewarmPageCacheOnWrite {
        use L0FlushConfig::*;
        match self {
            PageCached => ephemeral_file::PrewarmPageCacheOnWrite::Yes,
            Direct { .. } => ephemeral_file::PrewarmPageCacheOnWrite::No,
        }
    }
}
