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

pub fn deserialize_json_string<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: for<'de2> serde::Deserialize<'de2>,
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let s = String::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(<D::Error as serde::de::Error>::custom)
}
