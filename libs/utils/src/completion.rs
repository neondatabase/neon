use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// While a reference is kept around, the associated [`Barrier::wait`] will wait.
///
/// Can be cloned, moved and kept around in futures as "guard objects".
pub struct Completion(Arc<Shared>);

impl Clone for Completion {
    #[track_caller]
    fn clone(&self) -> Self {
        let in_progress = self
            .0
            .completions_in_progress
            .fetch_add(1, Ordering::Release);
        assert!(in_progress < usize::MAX - 1);
        let waiters = Arc::strong_count(&self.0)
            .checked_sub(in_progress)
            .unwrap_or(0);
        let id = self.0.id;
        let location = std::panic::Location::caller();
        tracing::info!(id, waiters, in_progress, %location, "cloning");
        Self(self.0.clone())
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        let now = self
            .0
            .completions_in_progress
            .fetch_update(Ordering::Release, Ordering::Relaxed, |x| x.checked_sub(1))
            .expect("should not have underflown");

        if now == 0 {
            let count = Arc::strong_count(&self.0);
            let id = self.0.id;
            tracing::info!(id, waiters = count - 1, "notifying waiters");
            self.0.notify.notify_waiters();
        }
    }
}

/// Barrier will wait until all clones of [`Completion`] have been dropped.
#[derive(Clone)]
pub struct Barrier(Arc<Shared>);

struct Shared {
    id: usize,
    notify: tokio::sync::Notify,
    completions_in_progress: AtomicUsize,
}

impl Default for Barrier {
    fn default() -> Self {
        let (_, rx) = channel();
        rx
    }
}

impl Barrier {
    pub async fn wait(self) {
        loop {
            let in_progress = self.0.completions_in_progress.load(Ordering::Acquire);

            if in_progress == 0 {
                tracing::info!(id = self.0.id, "wait complete!");
                break;
            } else {
                let waiters = Arc::strong_count(&self.0)
                    .checked_sub(in_progress)
                    // there might be drift between the two, but we are still waiting
                    .unwrap_or(1);
                tracing::info!(id = self.0.id, waiters, in_progress, "waiting");
                drop(
                    tokio::time::timeout(
                        std::time::Duration::from_millis(100),
                        self.0.notify.notified(),
                    )
                    .await,
                );
            }
        }
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
#[track_caller]
pub fn channel() -> (Completion, Barrier) {
    static ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

    let shared = Arc::new(Shared {
        id: ID_COUNTER.fetch_add(1, Ordering::Relaxed),
        notify: Default::default(),
        completions_in_progress: AtomicUsize::new(1),
    });

    let location = std::panic::Location::caller();

    tracing::info!(id = shared.id, %location, "created");

    (Completion(shared.clone()), Barrier(shared))
}
