use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures_util::Stream;
use tokio_util::sync::CancellationToken;

use crate::DownloadError;

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
        let e = DownloadError::from(value);
        std::io::Error::other(e)
    }
}

impl From<TimeoutOrCancel> for DownloadError {
    fn from(value: TimeoutOrCancel) -> Self {
        use TimeoutOrCancel::*;

        match value {
            Timeout => DownloadError::Timeout,
            Cancel => DownloadError::Cancelled,
        }
    }
}

/// Fires only on the first cancel or timeout, not on both.
pub(crate) async fn cancel_or_timeout(
    timeout: Duration,
    cancel: CancellationToken,
) -> TimeoutOrCancel {
    tokio::select! {
        _ = tokio::time::sleep(timeout) => TimeoutOrCancel::Timeout,
        _ = cancel.cancelled() => TimeoutOrCancel::Cancel,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DownloadError;
    use futures::stream::StreamExt;

    #[tokio::test(start_paused = true)]
    async fn cancelled_download_stream() {
        let inner = futures::stream::pending();
        let timeout = Duration::from_secs(120);
        let cancel = CancellationToken::new();

        let stream = DownloadStream::new(cancel_or_timeout(timeout, cancel.clone()), inner);
        let mut stream = std::pin::pin!(stream);

        let mut first = stream.next();

        tokio::select! {
            _ = &mut first => unreachable!("we haven't yet cancelled nor is timeout passed"),
            _ = tokio::time::sleep(Duration::from_secs(1)) => {},
        }

        cancel.cancel();

        let e = first.await.expect("there must be some").unwrap_err();
        assert!(matches!(e.kind(), std::io::ErrorKind::Other), "{e:?}");
        let inner = e.get_ref().expect("inner should be set");
        assert!(
            inner
                .downcast_ref::<DownloadError>()
                .is_some_and(|e| matches!(e, DownloadError::Cancelled)),
            "{inner:?}"
        );

        tokio::select! {
            _ = stream.next() => unreachable!("no timeout ever happens as we were already cancelled"),
            _ = tokio::time::sleep(Duration::from_secs(121)) => {},
        }
    }

    #[tokio::test(start_paused = true)]
    async fn timeouted_download_stream() {
        let inner = futures::stream::pending();
        let timeout = Duration::from_secs(120);
        let cancel = CancellationToken::new();

        let stream = DownloadStream::new(cancel_or_timeout(timeout, cancel.clone()), inner);
        let mut stream = std::pin::pin!(stream);

        // because the stream uses 120s timeout we are paused, we advance to 120s right away.
        let first = stream.next();

        let e = first.await.expect("there must be some").unwrap_err();
        assert!(matches!(e.kind(), std::io::ErrorKind::Other), "{e:?}");
        let inner = e.get_ref().expect("inner should be set");
        assert!(
            inner
                .downcast_ref::<DownloadError>()
                .is_some_and(|e| matches!(e, DownloadError::Timeout)),
            "{inner:?}"
        );

        cancel.cancel();

        tokio::select! {
            _ = stream.next() => unreachable!("no cancellation ever happens because we already timed out"),
            _ = tokio::time::sleep(Duration::from_secs(121)) => {},
        }
    }
}
