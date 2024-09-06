use tokio_util::task::{task_tracker::TaskTrackerToken, TaskTracker};

/// While a reference is kept around, the associated [`Barrier::wait`] will wait.
///
/// Can be cloned, moved and kept around in futures as "guard objects".
#[derive(Clone)]
pub struct Completion {
    token: TaskTrackerToken,
}

impl std::fmt::Debug for Completion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Completion")
            .field("siblings", &self.token.task_tracker().len())
            .finish()
    }
}

impl Completion {
    /// Returns true if this completion is associated with the given barrier.
    pub fn blocks(&self, barrier: &Barrier) -> bool {
        TaskTracker::ptr_eq(self.token.task_tracker(), &barrier.0)
    }

    pub fn barrier(&self) -> Barrier {
        Barrier(self.token.task_tracker().clone())
    }
}

/// Barrier will wait until all clones of [`Completion`] have been dropped.
#[derive(Clone)]
pub struct Barrier(TaskTracker);

impl std::fmt::Debug for Barrier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Barrier")
            .field("remaining", &self.0.len())
            .finish()
    }
}

impl Default for Barrier {
    fn default() -> Self {
        let (_, rx) = channel();
        rx
    }
}

impl Barrier {
    pub async fn wait(self) {
        self.0.wait().await;
    }

    pub async fn maybe_wait(barrier: Option<Barrier>) {
        if let Some(b) = barrier {
            b.wait().await
        }
    }

    /// Return true if a call to wait() would complete immediately
    pub fn is_ready(&self) -> bool {
        futures::future::FutureExt::now_or_never(self.0.wait()).is_some()
    }
}

impl PartialEq for Barrier {
    fn eq(&self, other: &Self) -> bool {
        TaskTracker::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Barrier {}

/// Create new Guard and Barrier pair.
pub fn channel() -> (Completion, Barrier) {
    let tracker = TaskTracker::new();
    // otherwise wait never exits
    tracker.close();

    let token = tracker.token();
    (Completion { token }, Barrier(tracker))
}
