use std::pin::Pin;
use std::task;

use hashbrown::HashMap;
use parking_lot::Mutex;
use pin_project_lite::pin_project;
use thiserror::Error;
use tokio::sync::oneshot;

#[derive(Debug, Error)]
pub(crate) enum RegisterError {
    #[error("Waiter `{0}` already registered")]
    Occupied(String),
}

#[derive(Debug, Error)]
pub(crate) enum NotifyError {
    #[error("Notify failed: waiter `{0}` not registered")]
    NotFound(String),

    #[error("Notify failed: channel hangup")]
    Hangup,
}

#[derive(Debug, Error)]
pub(crate) enum WaitError {
    #[error("Wait failed: channel hangup")]
    Hangup,
}

pub(crate) struct Waiters<T>(pub(self) Mutex<HashMap<String, oneshot::Sender<T>>>);

impl<T> Default for Waiters<T> {
    fn default() -> Self {
        Waiters(Mutex::default())
    }
}

impl<T> Waiters<T> {
    pub(crate) fn register(&self, key: String) -> Result<Waiter<'_, T>, RegisterError> {
        let (tx, rx) = oneshot::channel();

        self.0
            .lock()
            .try_insert(key.clone(), tx)
            .map_err(|e| RegisterError::Occupied(e.entry.key().clone()))?;

        Ok(Waiter {
            receiver: rx,
            guard: DropKey {
                registry: self,
                key,
            },
        })
    }

    pub(crate) fn notify(&self, key: &str, value: T) -> Result<(), NotifyError>
    where
        T: Send + Sync,
    {
        let tx = self
            .0
            .lock()
            .remove(key)
            .ok_or_else(|| NotifyError::NotFound(key.to_string()))?;

        tx.send(value).map_err(|_| NotifyError::Hangup)
    }
}

struct DropKey<'a, T> {
    key: String,
    registry: &'a Waiters<T>,
}

impl<T> Drop for DropKey<'_, T> {
    fn drop(&mut self) {
        self.registry.0.lock().remove(&self.key);
    }
}

pin_project! {
    pub(crate) struct Waiter<'a, T> {
        #[pin]
        receiver: oneshot::Receiver<T>,
        guard: DropKey<'a, T>,
    }
}

impl<T> std::future::Future for Waiter<'_, T> {
    type Output = Result<T, WaitError>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        self.project()
            .receiver
            .poll(cx)
            .map_err(|_| WaitError::Hangup)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn test_waiter() -> anyhow::Result<()> {
        let waiters = Arc::new(Waiters::default());

        let key = "Key";
        let waiter = waiters.register(key.to_owned())?;

        let waiters = Arc::clone(&waiters);
        let notifier = tokio::spawn(async move {
            waiters.notify(key, ())?;
            Ok(())
        });

        waiter.await?;
        notifier.await?
    }
}
