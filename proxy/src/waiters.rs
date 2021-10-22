use anyhow::{anyhow, Context};
use std::collections::HashMap;
use std::sync::{mpsc, Mutex};

pub struct Waiters<T>(pub(self) Mutex<HashMap<String, mpsc::Sender<T>>>);

impl<T> Default for Waiters<T> {
    fn default() -> Self {
        Waiters(Default::default())
    }
}

impl<T> Waiters<T> {
    pub fn register(&self, key: String) -> Waiter<T> {
        let (tx, rx) = mpsc::channel();

        // TODO: use `try_insert` (unstable)
        let prev = self.0.lock().unwrap().insert(key.clone(), tx);
        assert!(matches!(prev, None)); // assert_matches! is nightly-only

        Waiter {
            receiver: rx,
            registry: self,
            key,
        }
    }

    pub fn notify(&self, key: &str, value: T) -> anyhow::Result<()>
    where
        T: Send + Sync + 'static,
    {
        let tx = self
            .0
            .lock()
            .unwrap()
            .remove(key)
            .ok_or_else(|| anyhow!("key {} not found", key))?;
        tx.send(value).context("channel hangup")
    }
}

pub struct Waiter<'a, T> {
    receiver: mpsc::Receiver<T>,
    registry: &'a Waiters<T>,
    key: String,
}

impl<T> Waiter<'_, T> {
    pub fn wait(self) -> anyhow::Result<T> {
        self.receiver.recv().context("channel hangup")
    }
}

impl<T> Drop for Waiter<'_, T> {
    fn drop(&mut self) {
        self.registry.0.lock().unwrap().remove(&self.key);
    }
}
