use std::io;

use tokio::time;

use crate::compute;
use crate::config::RetryConfig;

pub(crate) trait CouldRetry {
    /// Returns true if the error could be retried
    fn could_retry(&self) -> bool;
}

pub(crate) trait ShouldRetryWakeCompute {
    /// Returns true if we need to invalidate the cache for this node.
    /// If false, we can continue retrying with the current node cache.
    fn should_retry_wake_compute(&self) -> bool;
}

pub(crate) fn should_retry(err: &impl CouldRetry, num_retries: u32, config: RetryConfig) -> bool {
    num_retries < config.max_retries && err.could_retry()
}

impl CouldRetry for io::Error {
    fn could_retry(&self) -> bool {
        use std::io::ErrorKind;
        matches!(
            self.kind(),
            ErrorKind::ConnectionRefused | ErrorKind::AddrNotAvailable | ErrorKind::TimedOut
        )
    }
}

impl CouldRetry for compute::ConnectionError {
    fn could_retry(&self) -> bool {
        match self {
            compute::ConnectionError::TlsError(err) => err.could_retry(),
            compute::ConnectionError::WakeComputeError(err) => err.could_retry(),
            compute::ConnectionError::TooManyConnectionAttempts(_) => false,
            #[cfg(test)]
            compute::ConnectionError::TestError { retryable, .. } => *retryable,
        }
    }
}

impl ShouldRetryWakeCompute for compute::ConnectionError {
    fn should_retry_wake_compute(&self) -> bool {
        match self {
            // the cache entry was not checked for validity
            compute::ConnectionError::TooManyConnectionAttempts(_) => false,
            #[cfg(test)]
            compute::ConnectionError::TestError { wakeable, .. } => *wakeable,
            _ => true,
        }
    }
}

pub(crate) fn retry_after(num_retries: u32, config: RetryConfig) -> time::Duration {
    config
        .base_delay
        .mul_f64(config.backoff_factor.powi((num_retries as i32) - 1))
}
