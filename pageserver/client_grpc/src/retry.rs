use std::time::Duration;

use tokio::time::Instant;
use tracing::{error, info, warn};

use utils::backoff::exponential_backoff_duration;

/// A retry handler for Pageserver gRPC requests.
///
/// This is used instead of backoff::retry for better control and observability.
pub struct Retry;

impl Retry {
    /// The per-request timeout.
    // TODO: tune these, and/or make them configurable. Should we retry forever?
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
    /// The total timeout across all attempts
    const TOTAL_TIMEOUT: Duration = Duration::from_secs(60);
    /// The initial backoff duration.
    const BASE_BACKOFF: Duration = Duration::from_millis(10);
    /// The maximum backoff duration.
    const MAX_BACKOFF: Duration = Duration::from_secs(10);
    /// If true, log successful requests. For debugging.
    const LOG_SUCCESS: bool = false;

    /// Runs the given async closure with timeouts and retries (exponential backoff). Logs errors,
    /// using the current tracing span for context.
    ///
    /// Only certain gRPC status codes are retried, see [`Self::should_retry`]. For default
    /// timeouts, see [`Self::REQUEST_TIMEOUT`] and [`Self::TOTAL_TIMEOUT`].
    pub async fn with<T, F, O>(&self, mut f: F) -> tonic::Result<T>
    where
        F: FnMut() -> O,
        O: Future<Output = tonic::Result<T>>,
    {
        let started = Instant::now();
        let deadline = started + Self::TOTAL_TIMEOUT;
        let mut last_error = None;
        let mut retries = 0;
        loop {
            // Set up a future to wait for the backoff (if any) and run the request with a timeout.
            let backoff_and_try = async {
                // NB: sleep() always sleeps 1ms, even when given a 0 argument. See:
                // https://github.com/tokio-rs/tokio/issues/6866
                if let Some(backoff) = Self::backoff_duration(retries) {
                    tokio::time::sleep(backoff).await;
                }

                let request_started = Instant::now();
                tokio::time::timeout(Self::REQUEST_TIMEOUT, f())
                    .await
                    .map_err(|_| {
                        tonic::Status::deadline_exceeded(format!(
                            "request timed out after {:.3}s",
                            request_started.elapsed().as_secs_f64()
                        ))
                    })?
            };

            // Wait for the backoff and request, or bail out if the total timeout is exceeded.
            let result = tokio::select! {
                result = backoff_and_try => result,

                _ = tokio::time::sleep_until(deadline) => {
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
                    if retries > 0 || Self::LOG_SUCCESS {
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

    /// Returns the backoff duration for the given retry attempt, or None for no backoff.
    fn backoff_duration(retry: usize) -> Option<Duration> {
        let backoff = exponential_backoff_duration(
            retry as u32,
            Self::BASE_BACKOFF.as_secs_f64(),
            Self::MAX_BACKOFF.as_secs_f64(),
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
