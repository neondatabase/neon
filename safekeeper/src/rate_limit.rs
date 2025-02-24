use std::sync::Arc;

use rand::Rng;

use crate::metrics::MISC_OPERATION_SECONDS;

/// Global rate limiter for background tasks.
#[derive(Clone)]
pub struct RateLimiter {
    partial_upload: Arc<tokio::sync::Semaphore>,
    eviction: Arc<tokio::sync::Semaphore>,
}

impl RateLimiter {
    /// Create a new rate limiter.
    /// - `partial_upload_max`: maximum number of concurrent partial uploads.
    /// - `eviction_max`: maximum number of concurrent timeline evictions.
    pub fn new(partial_upload_max: usize, eviction_max: usize) -> Self {
        Self {
            partial_upload: Arc::new(tokio::sync::Semaphore::new(partial_upload_max)),
            eviction: Arc::new(tokio::sync::Semaphore::new(eviction_max)),
        }
    }

    /// Get a permit for partial upload. This will block if the maximum number of concurrent
    /// partial uploads is reached.
    pub async fn acquire_partial_upload(&self) -> tokio::sync::OwnedSemaphorePermit {
        let _timer = MISC_OPERATION_SECONDS
            .with_label_values(&["partial_permit_acquire"])
            .start_timer();
        self.partial_upload
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
