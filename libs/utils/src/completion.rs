use std::sync::{atomic::AtomicI32, Arc};

use tokio::sync::{mpsc, Mutex};

/// While a reference is kept around, the associated [`Barrier::wait`] will wait.
///
/// Can be cloned, moved and kept around in futures as "guard objects".
pub struct Completion {
    sender: mpsc::Sender<()>,
    refcount: Arc<AtomicI32>,
}

impl Clone for Completion {
    fn clone(&self) -> Self {
        let i = self
            .refcount
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        tracing::info!("Completion::clone[{:p}]: {i}", &(*self.refcount));
        Self {
            sender: self.sender.clone(),
            refcount: self.refcount.clone(),
        }
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        let i = self
            .refcount
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        tracing::info!("Completion::drop[{:p}]: {i}", &(*self.refcount));
    }
}

/// Barrier will wait until all clones of [`Completion`] have been dropped.
#[derive(Clone)]
pub struct Barrier(Arc<Mutex<mpsc::Receiver<()>>>);

impl Default for Barrier {
    fn default() -> Self {
        let (_, rx) = channel();
        rx
    }
}

impl Barrier {
    pub async fn wait(self) {
        self.0.lock().await.recv().await;
    }

    pub async fn maybe_wait(barrier: Option<Barrier>) {
        if let Some(b) = barrier {
            b.wait().await
        }
    }
}

impl PartialEq for Barrier {
    fn eq(&self, other: &Self) -> bool {
        // we don't use dyn so this is good
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Barrier {}

/// Create new Guard and Barrier pair.
pub fn channel() -> (Completion, Barrier) {
    let (tx, rx) = mpsc::channel::<()>(1);
    let rx = Mutex::new(rx);
    let rx = Arc::new(rx);
    (
        Completion {
            sender: tx,
            refcount: Arc::new(AtomicI32::new(1)),
        },
        Barrier(rx),
    )
}
