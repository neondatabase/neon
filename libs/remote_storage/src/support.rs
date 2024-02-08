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

/// TODO: This is currently returning `std::io::Error` which is not really required, the From bound is
/// going to the opposite direction.
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
                // FIXME: does this make any sense? quite ackward type
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

pub(crate) enum TimeoutOrCancel {
    Timeout,
    Cancel,
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
