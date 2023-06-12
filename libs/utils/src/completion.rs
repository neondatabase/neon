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

/// Create new Guard and Barrier pair.
pub fn channel() -> (Completion, Barrier) {
    let (tx, rx) = mpsc::channel::<()>(1);
    let rx = Mutex::new(rx);
    let rx = Arc::new(rx);
    (Completion(tx), Barrier(rx))
}
