use std::pin::pin;

use futures::future::{Either, select};
use tokio_util::sync::CancellationToken;

pub async fn run_until_cancelled<F: Future>(
    f: F,
    cancellation_token: &CancellationToken,
) -> Option<F::Output> {
    match select(pin!(f), pin!(cancellation_token.cancelled())).await {
        Either::Left((f, _)) => Some(f),
        Either::Right(((), _)) => None,
    }
}
