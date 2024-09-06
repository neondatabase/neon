use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

/// Gates are a concurrency helper, primarily used for implementing safe shutdown.
///
/// Users of a resource call `enter()` to acquire a GateGuard, and the owner of
/// the resource calls `close()` when they want to ensure that all holders of guards
/// have released them, and that no future guards will be issued.
pub struct Gate {
    inner: Arc<GateInner>,
}

impl std::fmt::Debug for Gate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gate")
            // use this for identification
            .field("ptr", &Arc::as_ptr(&self.inner))
            .field("inner", &self.inner)
            .finish()
    }
}

struct GateInner {
    sem: tokio::sync::Semaphore,
    closing: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for GateInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let avail = self.sem.available_permits();

        let guards = u32::try_from(avail)
            .ok()
            // the sem only supports 32-bit ish amount, but lets play it safe
            .and_then(|x| Gate::MAX_UNITS.checked_sub(x));

        let closing = self.closing.load(Ordering::Relaxed);

        if let Some(guards) = guards {
            f.debug_struct("Gate")
                .field("remaining_guards", &guards)
                .field("closing", &closing)
                .finish()
        } else {
            f.debug_struct("Gate")
                .field("avail_permits", &avail)
                .field("closing", &closing)
                .finish()
        }
    }
}

/// RAII guard for a [`Gate`]: as long as this exists, calls to [`Gate::close`] will
/// not complete.
#[derive(Debug)]
pub struct GateGuard {
    // Record the span where the gate was entered, so that we can identify who was blocking Gate::close
    span_at_enter: tracing::Span,
    gate: Arc<GateInner>,
}

impl Drop for GateGuard {
    fn drop(&mut self) {
        if self.gate.closing.load(Ordering::Relaxed) {
            self.span_at_enter.in_scope(
                || tracing::info!(gate = ?Arc::as_ptr(&self.gate), "kept the gate from closing"),
            );
        }

        // when the permit was acquired, it was forgotten to allow us to manage it's lifecycle
        // manually, so "return" the permit now.
        self.gate.sem.add_permits(1);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GateError {
    #[error("gate is closed")]
    GateClosed,
}

impl Default for Gate {
    fn default() -> Self {
        Self {
            inner: Arc::new(GateInner {
                sem: tokio::sync::Semaphore::new(Self::MAX_UNITS as usize),
                closing: AtomicBool::new(false),
            }),
        }
    }
}

impl Gate {
    const MAX_UNITS: u32 = u32::MAX;

    /// Acquire a guard that will prevent close() calls from completing. If close()
    /// was already called, this will return an error which should be interpreted
    /// as "shutting down".
    ///
    /// This function would typically be used from e.g. request handlers. While holding
    /// the guard returned from this function, it is important to respect a CancellationToken
    /// to avoid blocking close() indefinitely: typically types that contain a Gate will
    /// also contain a CancellationToken.
    pub fn enter(&self) -> Result<GateGuard, GateError> {
        let permit = self
            .inner
            .sem
            .try_acquire()
            .map_err(|_| GateError::GateClosed)?;

        // we now have the permit, let's disable the normal raii functionality and leave
        // "returning" the permit to our GateGuard::drop.
        //
        // this is done to avoid the need for multiple Arcs (one for semaphore, next for other
        // fields).
        permit.forget();

        Ok(GateGuard {
            span_at_enter: tracing::Span::current(),
            gate: self.inner.clone(),
        })
    }

    /// Types with a shutdown() method and a gate should call this method at the
    /// end of shutdown, to ensure that all GateGuard holders are done.
    ///
    /// This will wait for all guards to be destroyed.  For this to complete promptly, it is
    /// important that the holders of such guards are respecting a CancellationToken which has
    /// been cancelled before entering this function.
    pub async fn close(&self) {
        let started_at = std::time::Instant::now();
        let mut do_close = std::pin::pin!(self.do_close());

        // with 1s we rarely saw anything, let's try if we get more gate closing reasons with 100ms
        let nag_after = Duration::from_millis(100);

        let Err(_timeout) = tokio::time::timeout(nag_after, &mut do_close).await else {
            return;
        };

        tracing::info!(
            gate = ?self.as_ptr(),
            elapsed_ms = started_at.elapsed().as_millis(),
            "closing is taking longer than expected"
        );

        // close operation is not trying to be cancellation safe as pageserver does not need it.
        //
        // note: "closing" is not checked in Gate::enter -- it exists just for observability,
        // dropping of GateGuard after this will log who they were.
        self.inner.closing.store(true, Ordering::Relaxed);

        do_close.await;

        tracing::info!(
            gate = ?self.as_ptr(),
            elapsed_ms = started_at.elapsed().as_millis(),
            "close completed"
        );
    }

    /// Used as an identity of a gate. This identity will be resolved to something useful when
    /// it's actually closed in a hopefully sensible `tracing::Span` which will describe it even
    /// more.
    ///
    /// `GateGuard::drop` also logs this pointer when it has realized it has been keeping the gate
    /// open for too long.
    fn as_ptr(&self) -> *const GateInner {
        Arc::as_ptr(&self.inner)
    }

    /// Check if [`Self::close()`] has finished waiting for all [`Self::enter()`] users to finish.  This
    /// is usually analoguous for "Did shutdown finish?" for types that include a Gate, whereas checking
    /// the CancellationToken on such types is analogous to "Did shutdown start?"
    pub fn close_complete(&self) -> bool {
        self.inner.sem.is_closed()
    }

    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all, fields(gate = ?self.as_ptr()))]
    async fn do_close(&self) {
        tracing::debug!("Closing Gate...");

        match self.inner.sem.acquire_many(Self::MAX_UNITS).await {
            Ok(_permit) => {
                // While holding all units, close the semaphore.  All subsequent calls to enter() will fail.
                self.inner.sem.close();
            }
            Err(_closed) => {
                // Semaphore closed: we are the only function that can do this, so it indicates a double-call.
                // This is legal.  Timeline::shutdown for example is not protected from being called more than
                // once.
                tracing::debug!("Double close")
            }
        }
        tracing::debug!("Closed Gate.")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn close_unused() {
        // Having taken no guards, we should not be blocked in close
        let gate = Gate::default();
        gate.close().await;
    }

    #[tokio::test]
    async fn close_idle() {
        // If a guard is dropped before entering, close should not be blocked
        let gate = Gate::default();
        let guard = gate.enter().unwrap();
        drop(guard);
        gate.close().await;

        // Entering a closed guard fails
        gate.enter().expect_err("enter should fail after close");
    }

    #[tokio::test(start_paused = true)]
    async fn close_busy_gate() {
        let gate = Gate::default();
        let forever = Duration::from_secs(24 * 7 * 365);

        let guard =
            tracing::info_span!("i am holding back the gate").in_scope(|| gate.enter().unwrap());

        let mut close_fut = std::pin::pin!(gate.close());

        // Close should be waiting for guards to drop
        tokio::time::timeout(forever, &mut close_fut)
            .await
            .unwrap_err();

        // Attempting to enter() should fail, even though close isn't done yet.
        gate.enter()
            .expect_err("enter should fail after entering close");

        // this will now log, which we cannot verify except manually
        drop(guard);

        // Guard is gone, close should finish
        close_fut.await;

        // Attempting to enter() is still forbidden
        gate.enter().expect_err("enter should fail finishing close");
    }
}
