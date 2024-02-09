use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::Stream;

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
