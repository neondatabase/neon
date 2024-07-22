use std::sync::Arc;

use rand::Rng;

use crate::metrics::MISC_OPERATION_SECONDS;

/// Global rate limiter for background tasks.
#[derive(Clone)]
pub struct RateLimiter {
    partial_backup: Arc<tokio::sync::Semaphore>,
    eviction: Arc<tokio::sync::Semaphore>,
}

impl RateLimiter {
    /// Create a new rate limiter.
    /// - `partial_backup_max`: maximum number of concurrent partial backups.
    /// - `eviction_max`: maximum number of concurrent timeline evictions.
    pub fn new(partial_backup_max: usize, eviction_max: usize) -> Self {
        Self {
            partial_backup: Arc::new(tokio::sync::Semaphore::new(partial_backup_max)),
            eviction: Arc::new(tokio::sync::Semaphore::new(eviction_max)),
        }
    }

    /// Get a permit for partial backup. This will block if the maximum number of concurrent
    /// partial backups is reached.
    pub async fn acquire_partial_backup(&self) -> tokio::sync::OwnedSemaphorePermit {
        let _timer = MISC_OPERATION_SECONDS
            .with_label_values(&["partial_permit_acquire"])
            .start_timer();
        self.partial_backup
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is closed")
    }

    /// Try to get a permit for timeline eviction. This will return None if the maximum number of
    /// concurrent timeline evictions is reached.
    pub fn try_acquire_eviction(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.eviction.clone().try_acquire_owned().ok()
    }
}

/// Generate a random duration that is a fraction of the given duration.
pub fn rand_duration(duration: &std::time::Duration) -> std::time::Duration {
    let randf64 = rand::thread_rng().gen_range(0.0..1.0);
    duration.mul_f64(randf64)
}
