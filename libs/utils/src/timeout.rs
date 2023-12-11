use std::time::Duration;

use tokio_util::sync::CancellationToken;

#[derive(thiserror::Error, Debug)]
pub enum TimeoutCancellableError {
    #[error("Timed out")]
    Timeout,
    #[error("Cancelled")]
    Cancelled,
}

/// Wrap [`tokio::time::timeout`] with a CancellationToken.
///
/// This wrapper is appropriate for any long running operation in a task
/// that ought to respect a CancellationToken (which means most tasks).
///
/// The only time you should use a bare tokio::timeout is when the future `F`
/// itself respects a CancellationToken: otherwise, always use this wrapper
/// with your CancellationToken to ensure that your task does not hold up
/// graceful shutdown.
pub async fn timeout_cancellable<F>(
    duration: Duration,
    cancel: &CancellationToken,
    future: F,
) -> Result<F::Output, TimeoutCancellableError>
where
    F: std::future::Future,
{
    tokio::select!(
        r = tokio::time::timeout(duration, future) => {
            r.map_err(|_| TimeoutCancellableError::Timeout)

        },
        _ = cancel.cancelled() => {
            Err(TimeoutCancellableError::Cancelled)

        }
    )
}
