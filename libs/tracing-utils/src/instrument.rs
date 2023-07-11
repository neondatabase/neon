use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use tracing::{Level, Span};

pub trait InstrumentCancel: Sized {
    fn instrument_with_cancel(self, span: Span) -> InstrumentedCancel<Self> {
        InstrumentedCancel {
            inner: self,
            span,
            cancel_level: Some(Level::INFO),
        }
    }
}
impl<T: Sized> InstrumentCancel for T {}

impl<T> InstrumentedCancel<T> {
    pub fn with_level(mut self, level: Level) -> Self {
        self.cancel_level = Some(level);
        self
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
    pub struct InstrumentedCancel<T> {
        #[pin]
        inner: T,
        span: Span,
        cancel_level: Option<Level>,
    }

    impl<T> PinnedDrop for InstrumentedCancel<T> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let Some(level) = this.cancel_level.take() {
                let _enter = this.span.enter();
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

impl<T: Future> Future for InstrumentedCancel<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _enter = this.span.enter();
        let res = this.inner.poll(cx);
        if res.is_ready() {
            *this.cancel_level = None;
        }
        res
    }
}
