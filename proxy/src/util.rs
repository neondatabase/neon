use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::FutureExt;
use tokio_util::sync::CancellationToken;

pub fn run_until_cancelled<F: Future>(
    f: F,
    cancellation_token: &CancellationToken,
) -> impl Future<Output = Option<F::Output>> {
    run_until(f, cancellation_token.cancelled()).map(|r| r.ok())
}

/// Runs the future `f` unless interrupted by future `condition`.
pub fn run_until<F1: Future, F2: Future>(
    f: F1,
    condition: F2,
) -> impl Future<Output = Result<F1::Output, F2::Output>> {
    RunUntil { a: f, b: condition }
}

pin_project_lite::pin_project! {
    struct RunUntil<A, B> {
        #[pin] a: A,
        #[pin] b: B,
    }
}

impl<A, B> Future for RunUntil<A, B>
where
    A: Future,
    B: Future,
{
    type Output = Result<A::Output, B::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if let Poll::Ready(a) = this.a.poll(cx) {
            return Poll::Ready(Ok(a));
        }
        if let Poll::Ready(b) = this.b.poll(cx) {
            return Poll::Ready(Err(b));
        }
        Poll::Pending
    }
}

pub fn deserialize_json_string<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: for<'de2> serde::Deserialize<'de2>,
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let s = String::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(<D::Error as serde::de::Error>::custom)
}
