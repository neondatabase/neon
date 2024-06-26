use std::{num::NonZeroUsize, sync::Arc};

#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub enum L0FlushConfig {
    #[default]
    PageCached,
    Direct {
        max_concurrency: NonZeroUsize,
    },
    Fail(&'static str),
}

#[derive(Clone)]
pub struct L0FlushGlobalState(Arc<Inner>);

pub(crate) enum Inner {
    PageCached,
    Direct { semaphore: tokio::sync::Semaphore },
    Fail(&'static str),
}

impl L0FlushGlobalState {
    pub fn new(config: L0FlushConfig) -> Self {
        match config {
            L0FlushConfig::PageCached => Self(Arc::new(Inner::PageCached)),
            L0FlushConfig::Direct { max_concurrency } => {
                let semaphore = tokio::sync::Semaphore::new(max_concurrency.get());
                Self(Arc::new(Inner::Direct { semaphore }))
            }
            L0FlushConfig::Fail(msg) => Self(Arc::new(Inner::Fail(msg))),
        }
    }

    pub(crate) fn inner(&self) -> &Arc<Inner> {
        &self.0
    }
}
