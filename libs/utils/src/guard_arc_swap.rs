//! A wrapper around `ArcSwap` that ensures there is only one writer at a time and writes
//! don't block reads.

use arc_swap::ArcSwap;
use std::sync::Arc;
use tokio::sync::TryLockError;

pub struct GuardArcSwap<T> {
    inner: ArcSwap<T>,
    guard: tokio::sync::Mutex<()>,
}

pub struct Guard<'a, T> {
    _guard: tokio::sync::MutexGuard<'a, ()>,
    inner: &'a ArcSwap<T>,
}

impl<T> GuardArcSwap<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(inner)),
            guard: tokio::sync::Mutex::new(()),
        }
    }

    pub fn read(&self) -> Arc<T> {
        self.inner.load_full()
    }

    pub async fn write_guard(&self) -> Guard<'_, T> {
        Guard {
            _guard: self.guard.lock().await,
            inner: &self.inner,
        }
    }

    pub fn try_write_guard(&self) -> Result<Guard<'_, T>, TryLockError> {
        let guard = self.guard.try_lock()?;
        Ok(Guard {
            _guard: guard,
            inner: &self.inner,
        })
    }
}

impl<T> Guard<'_, T> {
    pub fn read(&self) -> Arc<T> {
        self.inner.load_full()
    }

    pub fn write(&mut self, value: T) {
        self.inner.store(Arc::new(value));
    }
}
