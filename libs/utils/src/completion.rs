use tokio_util::task::{task_tracker::TaskTrackerToken, TaskTracker};

/// While a reference is kept around, the associated [`Barrier::wait`] will wait.
///
/// Can be cloned, moved and kept around in futures as "guard objects".
#[derive(Clone)]
pub struct Completion(TaskTrackerToken);

/// Barrier will wait until all clones of [`Completion`] have been dropped.
#[derive(Clone)]
pub struct Barrier(TaskTracker);

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
    (Completion(token), Barrier(tracker))
}
