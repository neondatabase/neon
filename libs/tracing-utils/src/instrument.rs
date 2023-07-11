use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use tracing::{instrument::Instrumented, Level};

pub trait InstrumentCancel: Sized {
    type Inner;
    fn with_cancel_info(self) -> CancelLog<Self::Inner> {
        self.with_cancel_log(Level::INFO)
    }
    fn with_cancel_log(self, level: Level) -> CancelLog<Self::Inner>;
}

impl<T: Sized> InstrumentCancel for Instrumented<T> {
    type Inner = T;
    fn with_cancel_log(self, level: Level) -> CancelLog<T> {
        CancelLog {
            inner: self,
            cancel_level: Some(level),
        }
    }
}

pin_project! {
    /// A [`Future`] that has been instrumented with a `tracing` [`Span`]. It will emit a log on cancellation
    ///
    /// This type is returned by the [`Instrument`] extension trait. See that
    /// trait's documentation for details.
    ///
    /// [`Future`]: std::future::Future
    /// [`Span`]: crate::Span
    #[derive(Debug, Clone)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct CancelLog<T> {
        #[pin]
        inner: Instrumented<T>,
        cancel_level: Option<Level>,
    }

    impl<T> PinnedDrop for CancelLog<T> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let Some(level) = this.cancel_level.take() {
                let _enter = this.inner.span().enter();
                match level {
                    Level::TRACE => tracing::event!(Level::TRACE, "task was cancelled"),
                    Level::DEBUG => tracing::event!(Level::DEBUG, "task was cancelled"),
                    Level::INFO => tracing::event!(Level::INFO, "task was cancelled"),
                    Level::WARN => tracing::event!(Level::WARN, "task was cancelled"),
                    Level::ERROR => tracing::event!(Level::ERROR, "task was cancelled"),
                }
            }
        }
    }
}

impl<T: Future> Future for CancelLog<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = this.inner.poll(cx);
        if res.is_ready() {
            *this.cancel_level = None;
        }
        res
    }
}
