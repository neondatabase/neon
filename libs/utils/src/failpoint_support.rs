//! Failpoint support code shared between pageserver and safekeepers.
//! 
//! This module provides a compatibility layer over the new neon_failpoint crate.

use tokio_util::sync::CancellationToken;
pub use neon_failpoint::{configure_failpoint as apply_failpoint, init, has_failpoints};

/// Declare a failpoint that can use to `pause` failpoint action.
/// This is now a compatibility wrapper around the new neon_failpoint crate.
///
/// Optionally pass a cancellation token, and this failpoint will drop out of
/// its pause when the cancellation token fires. This is useful for testing
/// cases where we would like to block something, but test its clean shutdown behavior.
/// The macro evaluates to a Result in that case, where Ok(()) is the case
/// where the failpoint was not paused, and Err() is the case where cancellation
/// token fired while evaluating the failpoint.
#[macro_export]
macro_rules! pausable_failpoint {
    ($name:literal) => {{
        if cfg!(feature = "testing") {
            let cancel = ::tokio_util::sync::CancellationToken::new();
            let _ = $crate::pausable_failpoint!($name, &cancel);
        }
    }};
    ($name:literal, $cancel:expr) => {{
        if cfg!(feature = "testing") {
            match ::neon_failpoint::failpoint_with_cancellation($name, None, $cancel).await {
                ::neon_failpoint::FailpointResult::Continue => Ok(()),
                ::neon_failpoint::FailpointResult::Return(_) => Ok(()),
                ::neon_failpoint::FailpointResult::Cancelled => Err(()),
            }
        } else {
            Ok(())
        }
    }};
}

pub use pausable_failpoint;

/// use with neon_failpoint::configure_failpoint("$name", "sleep(2000)")
///
/// The effect is similar to a "sleep(2000)" action, i.e. we sleep for the
/// specified time (in milliseconds). The main difference is that we use async
/// tokio sleep function. Another difference is that we print lines to the log,
/// which can be useful in tests to check that the failpoint was hit.
///
/// Optionally pass a cancellation token, and this failpoint will drop out of
/// its sleep when the cancellation token fires.  This is useful for testing
/// cases where we would like to block something, but test its clean shutdown behavior.
#[macro_export]
macro_rules! __failpoint_sleep_millis_async {
    ($name:literal) => {{
        if cfg!(feature = "testing") {
            ::neon_failpoint::failpoint($name, None).await;
        }
    }};
    ($name:literal, $cancel:expr) => {{
        if cfg!(feature = "testing") {
            ::neon_failpoint::failpoint_with_cancellation($name, None, $cancel).await;
        }
    }};
}
pub use __failpoint_sleep_millis_async as sleep_millis_async;

// Helper functions are no longer needed as the new implementation handles this internally
// but we keep them for backward compatibility
#[doc(hidden)]
pub async fn failpoint_sleep_helper(_name: &'static str, _duration_str: String) {
    // This is now handled by the neon_failpoint crate internally
    tracing::warn!("failpoint_sleep_helper is deprecated, use neon_failpoint directly");
}

#[doc(hidden)]
pub async fn failpoint_sleep_cancellable_helper(
    _name: &'static str,
    _duration_str: String,
    _cancel: &CancellationToken,
) {
    // This is now handled by the neon_failpoint crate internally
    tracing::warn!("failpoint_sleep_cancellable_helper is deprecated, use neon_failpoint directly");
}
