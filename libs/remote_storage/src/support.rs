use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures_util::Stream;
use tokio_util::sync::CancellationToken;

pin_project_lite::pin_project! {
    /// An `AsyncRead` adapter which carries a permit for the lifetime of the value.
    pub(crate) struct PermitCarrying<S> {
        permit: tokio::sync::OwnedSemaphorePermit,
        #[pin]
        inner: S,
    }
}

impl<S> PermitCarrying<S> {
    pub(crate) fn new(permit: tokio::sync::OwnedSemaphorePermit, inner: S) -> Self {
        Self { permit, inner }
    }
}

impl<S: Stream> Stream for PermitCarrying<S> {
    type Item = <S as Stream>::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

pin_project_lite::pin_project! {
    pub(crate) struct DownloadStream<F, S> {
        hit: bool,
        #[pin]
        cancellation: F,
        #[pin]
        inner: S,
    }
}

impl<F, S> DownloadStream<F, S> {
    pub(crate) fn new(cancellation: F, inner: S) -> Self {
        Self {
            cancellation,
            hit: false,
            inner,
        }
    }
}

/// See documentation on [`crate::DownloadStream`] on rationale why `std::io::Error` is used.
impl<E, F, S> Stream for DownloadStream<F, S>
where
    std::io::Error: From<E>,
    F: Future<Output = E>,
    S: Stream<Item = std::io::Result<Bytes>>,
{
    type Item = <S as Stream>::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if !*this.hit {
            if let Poll::Ready(e) = this.cancellation.poll(cx) {
                *this.hit = true;
                let e = Err(std::io::Error::from(e));
                return Poll::Ready(Some(e));
            }
        }

        this.inner.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// This type is used at as the root cause for timeouts and cancellations with anyhow returning
/// RemoteStorage methods.
#[derive(Debug)]
pub enum TimeoutOrCancel {
    Timeout,
    Cancel,
}

impl std::fmt::Display for TimeoutOrCancel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use TimeoutOrCancel::*;
        match self {
            Timeout => write!(f, "timeout"),
            Cancel => write!(f, "cancel"),
        }
    }
}

impl std::error::Error for TimeoutOrCancel {}

impl TimeoutOrCancel {
    pub fn caused(error: &anyhow::Error) -> Option<&Self> {
        error.root_cause().downcast_ref()
    }

    /// Returns true if the error was caused by [`TimeoutOrCancel::Cancel`].
    pub fn caused_by_cancel(error: &anyhow::Error) -> bool {
        Self::caused(error).is_some_and(Self::is_cancel)
    }

    pub fn is_cancel(&self) -> bool {
        matches!(self, TimeoutOrCancel::Cancel)
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, TimeoutOrCancel::Timeout)
    }
}

// Sadly the only way `tokio::io::copy_buf` helpers work is that if the error type is
// `std::io::Error`.
impl From<TimeoutOrCancel> for std::io::Error {
    fn from(value: TimeoutOrCancel) -> Self {
        use TimeoutOrCancel::*;

        let e = match value {
            Timeout => crate::DownloadError::Timeout,
            Cancel => crate::DownloadError::Cancelled,
        };

        std::io::Error::other(e)
    }
}

/// Fires only on the first cancel or timeout, not on both.
pub(crate) async fn cancel_or_timeout(
    timeout: Duration,
    cancel: &CancellationToken,
) -> TimeoutOrCancel {
    tokio::select! {
        _ = tokio::time::sleep(timeout) => TimeoutOrCancel::Timeout,
        _ = cancel.cancelled() => TimeoutOrCancel::Cancel,
    }
}
