//! Helpers for observing duration on `HistogramVec` / `CounterVec` / `GaugeVec` / `MetricVec<T>`.

use std::{future::Future, time::Instant};

pub trait DurationResultObserver {
    fn observe_result<T, E>(&self, res: &Result<T, E>, duration: std::time::Duration);
}

pub async fn observe_async_block_duration_by_result<
    T,
    E,
    F: Future<Output = Result<T, E>>,
    O: DurationResultObserver,
>(
    observer: &O,
    block: F,
) -> Result<T, E> {
    let start = Instant::now();
    let result = block.await;
    let duration = start.elapsed();
    observer.observe_result(&result, duration);
    result
}
