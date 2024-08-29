use std::sync::Arc;

use crate::tenant::ephemeral_file;

use pageserver_api::models::L0FlushConfig;

#[derive(Clone)]
pub struct L0FlushGlobalState(Arc<Inner>);

pub(crate) enum Inner {
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

    pub(crate) fn inner(&self) -> &Arc<Inner> {
        &self.0
    }
}

pub(crate) fn prewarm_on_write(config: &L0FlushConfig) -> ephemeral_file::PrewarmPageCacheOnWrite {
    use L0FlushConfig::*;
    match config {
        PageCached => ephemeral_file::PrewarmPageCacheOnWrite::Yes,
        Direct { .. } => ephemeral_file::PrewarmPageCacheOnWrite::No,
    }
}
