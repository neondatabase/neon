use std::pin::pin;

use futures::future::{Either, select};
use tokio_util::sync::CancellationToken;

pub async fn run_until_cancelled<F: Future>(
    f: F,
    cancellation_token: &CancellationToken,
) -> Option<F::Output> {
    run_until(f, cancellation_token.cancelled()).await.ok()
}

/// Runs the future `f` unless interrupted by future `condition`.
pub async fn run_until<F1: Future, F2: Future>(
    f: F1,
    condition: F2,
) -> Result<F1::Output, F2::Output> {
    match select(pin!(f), pin!(condition)).await {
        Either::Left((f1, _)) => Ok(f1),
        Either::Right((f2, _)) => Err(f2),
    }
}
