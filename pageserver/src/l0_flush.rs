use std::{num::NonZeroUsize, sync::Arc};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum L0FlushConfig {
    Direct { max_concurrency: NonZeroUsize },
}

impl Default for L0FlushConfig {
    fn default() -> Self {
        Self::Direct {
            // TODO: using num_cpus results in different peak memory usage on different instance types.
            max_concurrency: NonZeroUsize::new(usize::max(1, num_cpus::get())).unwrap(),
        }
    }
}

impl From<pageserver_api::models::L0FlushConfig> for L0FlushConfig {
    fn from(config: pageserver_api::models::L0FlushConfig) -> Self {
        match config {
            pageserver_api::models::L0FlushConfig::Direct { max_concurrency } => {
                Self::Direct { max_concurrency }
            }
        }
    }
}

#[derive(Clone)]
pub struct L0FlushGlobalState(Arc<Inner>);

pub enum Inner {
    Direct { semaphore: tokio::sync::Semaphore },
}

impl L0FlushGlobalState {
    pub fn new(config: L0FlushConfig) -> Self {
        match config {
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
