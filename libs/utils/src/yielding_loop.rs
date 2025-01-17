use tokio_util::sync::CancellationToken;

#[derive(thiserror::Error, Debug)]
pub enum YieldingLoopError {
    #[error("Cancelled")]
    Cancelled,
}

/// Helper for long synchronous loops, e.g. over all tenants in the system.
///
/// Periodically yields to avoid blocking the executor, and after resuming
/// checks the provided cancellation token to drop out promptly on shutdown.
#[inline(always)]
pub async fn yielding_loop<I, T, F>(
    interval: usize,
    cancel: &CancellationToken,
    iter: I,
    mut visitor: F,
) -> Result<(), YieldingLoopError>
where
    I: Iterator<Item = T>,
    F: FnMut(T),
{
    for (i, item) in iter.enumerate() {
        visitor(item);

        if (i + 1) % interval == 0 {
            tokio::task::yield_now().await;
            if cancel.is_cancelled() {
                return Err(YieldingLoopError::Cancelled);
            }
        }
    }

    Ok(())
}
