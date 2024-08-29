use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures_util::Stream;
use tokio_util::sync::CancellationToken;

use crate::TimeoutOrCancel;

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

                // most likely this will be a std::io::Error wrapping a DownloadError
                let e = Err(std::io::Error::from(e));
                return Poll::Ready(Some(e));
            }
        } else {
            // this would be perfectly valid behaviour for doing a graceful completion on the
            // download for example, but not one we expect to do right now.
            tracing::warn!("continuing polling after having cancelled or timeouted");
        }

        this.inner.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// Fires only on the first cancel or timeout, not on both.
pub(crate) fn cancel_or_timeout(
    timeout: Duration,
    cancel: CancellationToken,
) -> impl std::future::Future<Output = TimeoutOrCancel> + 'static {
    // futures are lazy, they don't do anything before being polled.
    //
    // "precalculate" the wanted deadline before returning the future, so that we can use pause
    // failpoint to trigger a timeout in test.
    let deadline = tokio::time::Instant::now() + timeout;
    async move {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => TimeoutOrCancel::Timeout,
            _ = cancel.cancelled() => {
                TimeoutOrCancel::Cancel
            },
        }
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
        let e = DownloadError::from(e);
        assert!(matches!(e, DownloadError::Cancelled), "{e:?}");

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

        // because the stream uses 120s timeout and we are paused, we advance to 120s right away.
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
        let e = DownloadError::from(e);
        assert!(matches!(e, DownloadError::Timeout), "{e:?}");

        cancel.cancel();

        tokio::select! {
            _ = stream.next() => unreachable!("no cancellation ever happens because we already timed out"),
            _ = tokio::time::sleep(Duration::from_secs(121)) => {},
        }
    }

    #[tokio::test]
    async fn notified_but_pollable_after() {
        let inner = futures::stream::once(futures::future::ready(Ok(bytes::Bytes::from_static(
            b"hello world",
        ))));
        let timeout = Duration::from_secs(120);
        let cancel = CancellationToken::new();

        cancel.cancel();
        let stream = DownloadStream::new(cancel_or_timeout(timeout, cancel.clone()), inner);
        let mut stream = std::pin::pin!(stream);

        let next = stream.next().await;
        let ioe = next.unwrap().unwrap_err();
        assert!(
            matches!(
                ioe.get_ref().unwrap().downcast_ref::<DownloadError>(),
                Some(&DownloadError::Cancelled)
            ),
            "{ioe:?}"
        );

        let next = stream.next().await;
        let bytes = next.unwrap().unwrap();
        assert_eq!(&b"hello world"[..], bytes);
    }
}
