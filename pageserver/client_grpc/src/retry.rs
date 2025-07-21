use std::time::Duration;

use futures::future::pending;
use tokio::time::Instant;
use tracing::{error, info, warn};

use utils::backoff::exponential_backoff_duration;

/// A retry handler for Pageserver gRPC requests.
///
/// This is used instead of backoff::retry for better control and observability.
pub struct Retry {
    /// Timeout across all retry attempts. If None, retries forever.
    pub timeout: Option<Duration>,
    /// The initial backoff duration. The first retry does not use a backoff.
    pub base_backoff: Duration,
    /// The maximum backoff duration.
    pub max_backoff: Duration,
}

impl Retry {
    /// Runs the given async closure with timeouts and retries (exponential backoff). Logs errors,
    /// using the current tracing span for context.
    ///
    /// Only certain gRPC status codes are retried, see [`Self::should_retry`].
    pub async fn with<T, F, O>(&self, mut f: F) -> tonic::Result<T>
    where
        F: FnMut(usize) -> O, // pass attempt number, starting at 0
        O: Future<Output = tonic::Result<T>>,
    {
        let started = Instant::now();
        let deadline = self.timeout.map(|timeout| started + timeout);
        let mut last_error = None;
        let mut retries = 0;
        loop {
            // Set up a future to wait for the backoff, if any, and run the closure.
            let backoff_and_try = async {
                // NB: sleep() always sleeps 1ms, even when given a 0 argument. See:
                // https://github.com/tokio-rs/tokio/issues/6866
                if let Some(backoff) = self.backoff_duration(retries) {
                    tokio::time::sleep(backoff).await;
                }

                f(retries).await
            };

            // Set up a future for the timeout, if any.
            let timeout = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => pending().await,
                }
            };

            // Wait for the backoff and request, or bail out if the timeout is exceeded.
            let result = tokio::select! {
                result = backoff_and_try => result,

                _ = timeout => {
                    let last_error = last_error.unwrap_or_else(|| {
                        tonic::Status::deadline_exceeded(format!(
                            "request timed out after {:.3}s",
                            started.elapsed().as_secs_f64()
                        ))
                    });
                    error!(
                        "giving up after {:.3}s and {retries} retries, last error {:?}: {}",
                        started.elapsed().as_secs_f64(), last_error.code(), last_error.message(),
                    );
                    return Err(last_error);
                }
            };

            match result {
                // Success, return the result.
                Ok(result) => {
                    if retries > 0 {
                        info!(
                            "request succeeded after {retries} retries in {:.3}s",
                            started.elapsed().as_secs_f64(),
                        );
                    }

                    return Ok(result);
                }

                // Error, retry or bail out.
                Err(status) => {
                    let (code, message) = (status.code(), status.message());
                    let attempt = retries + 1;

                    if !Self::should_retry(code) {
                        // NB: include the attempt here too. This isn't necessarily the first
                        // attempt, because the error may change between attempts.
                        error!(
                            "request failed with {code:?}: {message}, not retrying (attempt {attempt})"
                        );
                        return Err(status);
                    }

                    warn!("request failed with {code:?}: {message}, retrying (attempt {attempt})");

                    retries += 1;
                    last_error = Some(status);
                }
            }
        }
    }

    /// Returns the backoff duration for the given retry attempt, or None for no backoff. The first
    /// attempt and first retry never backs off, so this returns None for 0 and 1 retries.
    fn backoff_duration(&self, retries: usize) -> Option<Duration> {
        let backoff = exponential_backoff_duration(
            (retries as u32).saturating_sub(1), // first retry does not back off
            self.base_backoff.as_secs_f64(),
            self.max_backoff.as_secs_f64(),
        );
        (!backoff.is_zero()).then_some(backoff)
    }

    /// Returns true if the given status code should be retries.
    fn should_retry(code: tonic::Code) -> bool {
        match code {
            tonic::Code::Ok => panic!("unexpected Ok status code"),

            // These codes are transient, so retry them.
            tonic::Code::Aborted => true,
            tonic::Code::Cancelled => true,
            tonic::Code::DeadlineExceeded => true, // maybe transient slowness
            tonic::Code::ResourceExhausted => true,
            tonic::Code::Unavailable => true,

            // The following codes will like continue to fail, so don't retry.
            tonic::Code::AlreadyExists => false,
            tonic::Code::DataLoss => false,
            tonic::Code::FailedPrecondition => false,
            // NB: don't retry Internal. It is intended for serious errors such as invariant
            // violations, and is also used for client-side invariant checks that would otherwise
            // result in retry loops.
            tonic::Code::Internal => false,
            tonic::Code::InvalidArgument => false,
            tonic::Code::NotFound => false,
            tonic::Code::OutOfRange => false,
            tonic::Code::PermissionDenied => false,
            tonic::Code::Unauthenticated => false,
            tonic::Code::Unimplemented => false,
            tonic::Code::Unknown => false,
        }
    }
}
