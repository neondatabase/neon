use std::{sync::Arc, time::Duration};

/// Gates are a concurrency helper, primarily used for implementing safe shutdown.
///
/// Users of a resource call `enter()` to acquire a GateGuard, and the owner of
/// the resource calls `close()` when they want to ensure that all holders of guards
/// have released them, and that no future guards will be issued.
#[derive(Debug)]
pub struct Gate {
    /// Each caller of enter() takes one unit from the semaphore. In close(), we
    /// take all the units to ensure all GateGuards are destroyed.
    sem: Arc<tokio::sync::Semaphore>,

    /// For observability only: a name that will be used to log warnings if a particular
    /// gate is holding up shutdown
    name: String,
}

/// RAII guard for a [`Gate`]: as long as this exists, calls to [`Gate::close`] will
/// not complete.
#[derive(Debug)]
pub struct GateGuard(tokio::sync::OwnedSemaphorePermit);

/// Observability helper: every `warn_period`, emit a log warning that we're still waiting on this gate
async fn warn_if_stuck<Fut: std::future::Future>(
    fut: Fut,
    name: &str,
    warn_period: std::time::Duration,
) -> <Fut as std::future::Future>::Output {
    let started = std::time::Instant::now();

    let mut fut = std::pin::pin!(fut);

    loop {
        match tokio::time::timeout(warn_period, &mut fut).await {
            Ok(ret) => return ret,
            Err(_) => {
                tracing::warn!(
                    gate = name,
                    elapsed_ms = started.elapsed().as_millis(),
                    "still waiting, taking longer than expected..."
                );
            }
        }
    }
}

#[derive(Debug)]
pub enum GateError {
    GateClosed,
}

impl Gate {
    const MAX_UNITS: u32 = u32::MAX;

    pub fn new(name: String) -> Self {
        Self {
            sem: Arc::new(tokio::sync::Semaphore::new(Self::MAX_UNITS as usize)),
            name,
        }
    }

    /// Acquire a guard that will prevent close() calls from completing. If close()
    /// was already called, this will return an error which should be interpreted
    /// as "shutting down".
    ///
    /// This function would typically be used from e.g. request handlers. While holding
    /// the guard returned from this function, it is important to respect a CancellationToken
    /// to avoid blocking close() indefinitely: typically types that contain a Gate will
    /// also contain a CancellationToken.
    pub fn enter(&self) -> Result<GateGuard, GateError> {
        self.sem
            .clone()
            .try_acquire_owned()
            .map(GateGuard)
            .map_err(|_| GateError::GateClosed)
    }

    /// Types with a shutdown() method and a gate should call this method at the
    /// end of shutdown, to ensure that all GateGuard holders are done.
    ///
    /// This will wait for all guards to be destroyed.  For this to complete promptly, it is
    /// important that the holders of such guards are respecting a CancellationToken which has
    /// been cancelled before entering this function.
    pub async fn close(&self) {
        warn_if_stuck(self.do_close(), &self.name, Duration::from_millis(1000)).await
    }

    /// Check if [`Self::close()`] has finished waiting for all [`Self::enter()`] users to finish.  This
    /// is usually analoguous for "Did shutdown finish?" for types that include a Gate, whereas checking
    /// the CancellationToken on such types is analogous to "Did shutdown start?"
    pub fn close_complete(&self) -> bool {
        self.sem.is_closed()
    }

    async fn do_close(&self) {
        tracing::debug!(gate = self.name, "Closing Gate...");
        match self.sem.acquire_many(Self::MAX_UNITS).await {
            Ok(_units) => {
                // While holding all units, close the semaphore.  All subsequent calls to enter() will fail.
                self.sem.close();
            }
            Err(_) => {
                // Semaphore closed: we are the only function that can do this, so it indicates a double-call.
                // This is legal.  Timeline::shutdown for example is not protected from being called more than
                // once.
                tracing::debug!(gate = self.name, "Double close")
            }
        }
        tracing::debug!(gate = self.name, "Closed Gate.")
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;

    use super::*;

    #[tokio::test]
    async fn test_idle_gate() {
        // Having taken no gates, we should not be blocked in close
        let gate = Gate::new("test".to_string());
        gate.close().await;

        // If a guard is dropped before entering, close should not be blocked
        let gate = Gate::new("test".to_string());
        let guard = gate.enter().unwrap();
        drop(guard);
        gate.close().await;

        // Entering a closed guard fails
        gate.enter().expect_err("enter should fail after close");
    }

    #[tokio::test]
    async fn test_busy_gate() {
        let gate = Gate::new("test".to_string());

        let guard = gate.enter().unwrap();

        let mut close_fut = std::pin::pin!(gate.close());

        // Close should be blocked
        assert!(close_fut.as_mut().now_or_never().is_none());

        // Attempting to enter() should fail, even though close isn't done yet.
        gate.enter()
            .expect_err("enter should fail after entering close");

        drop(guard);

        // Guard is gone, close should finish
        assert!(close_fut.as_mut().now_or_never().is_some());

        // Attempting to enter() is still forbidden
        gate.enter().expect_err("enter should fail finishing close");
    }
}
