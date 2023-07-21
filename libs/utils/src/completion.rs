use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};

/// While a reference is kept around, the associated [`Barrier::wait`] will wait.
///
/// Can be cloned, moved and kept around in futures as "guard objects".
#[derive(Clone)]
pub struct Completion(mpsc::Sender<()>);

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
    (Completion(tx), Barrier(rx))
}
