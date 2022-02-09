use anyhow::Context;
use futures::Future;
use std::{collections::HashMap, pin::Pin, task};
use tokio::sync::mpsc;
use parking_lot::Mutex;

pub struct Waiters<T>(pub(self) Mutex<HashMap<String, mpsc::Sender<T>>>);

impl<T> Default for Waiters<T> {
    fn default() -> Self {
        Waiters(Default::default())
    }
}

impl<T> Waiters<T> {
    pub fn register(&self, key: String) -> Waiter<T> {
        let (tx, rx) = mpsc::channel(3);

        // TODO: use `try_insert` (unstable)
        let prev = self.0.lock().insert(key.clone(), tx);
        assert!(matches!(prev, None)); // assert_matches! is nightly-only

        Waiter {
            receiver: rx,
            registry: self,
            key,
        }
    }

    pub async fn notify(&self, key: &str, value: T) -> anyhow::Result<()>
    where
        T: Send + Sync + 'static,
    {
        let tx = self
            .0
            .lock()
            .remove(key)
            .with_context(|| format!("key {} not found", key))?;
        tx.send(value).await.map_err(|err| anyhow::anyhow!("channel hangup: {}", err))
    }
}

pub struct Waiter<'a, T> {
    receiver: mpsc::Receiver<T>,
    registry: &'a Waiters<T>,
    key: String,
}

impl<T> Future for Waiter<'_, T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        self.receiver.poll_recv(cx).map(|t| t.expect("sender dropped"))
    }
}

impl<T> Drop for Waiter<'_, T> {
    fn drop(&mut self) {
        self.registry.0.lock().remove(&self.key);
    }
}
