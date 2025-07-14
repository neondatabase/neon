//! Failpoint support code shared between pageserver and safekeepers.
//!
//! This module provides a compatibility layer over the new neon_failpoint crate.

pub use neon_failpoint::{configure_failpoint as apply_failpoint, has_failpoints, init};
use tokio_util::sync::CancellationToken;

/// Mere forward to neon_failpoint::pausable_failpoint
#[macro_export]
macro_rules! pausable_failpoint {
    ($name:literal) => {
        ::neon_failpoint::pausable_failpoint!($name)
    };
    ($name:literal, $cancel:expr) => {
        ::neon_failpoint::pausable_failpoint!($name, $cancel)
    };
}

/// DEPRECATED! - use with fail::cfg("$name", "return(2000)")
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
        // If the failpoint is used with a "return" action, set should_sleep to the
        // returned value (as string). Otherwise it's set to None.
        let should_sleep = (|| {
            ::neon_failpoint::fail_point_sync!($name, |x| x);
            ::std::option::Option::None
        })();

        // Sleep if the action was a returned value
        if let ::std::option::Option::Some(duration_str) = should_sleep {
            $crate::failpoint_support::failpoint_sleep_helper($name, duration_str).await
        }
    }};
    ($name:literal, $cancel:expr) => {{
        // If the failpoint is used with a "return" action, set should_sleep to the
        // returned value (as string). Otherwise it's set to None.
        let should_sleep = (|| {
            ::neon_failpoint::fail_point_sync!($name, |x| x);
            ::std::option::Option::None
        })();

        // Sleep if the action was a returned value
        if let ::std::option::Option::Some(duration_str) = should_sleep {
            $crate::failpoint_support::failpoint_sleep_cancellable_helper(
                $name,
                duration_str,
                $cancel,
            )
            .await
        }
    }};
}
pub use __failpoint_sleep_millis_async as sleep_millis_async;

// Helper function used by the macro. (A function has nicer scoping so we
// don't need to decorate everything with "::")
#[doc(hidden)]
pub async fn failpoint_sleep_helper(name: &'static str, duration_str: String) {
    let millis = duration_str.parse::<u64>().unwrap();
    let d = std::time::Duration::from_millis(millis);

    tracing::info!("failpoint {:?}: sleeping for {:?}", name, d);
    tokio::time::sleep(d).await;
    tracing::info!("failpoint {:?}: sleep done", name);
}

// Helper function used by the macro. (A function has nicer scoping so we
// don't need to decorate everything with "::")
#[doc(hidden)]
pub async fn failpoint_sleep_cancellable_helper(
    name: &'static str,
    duration_str: String,
    cancel: &CancellationToken,
) {
    let millis = duration_str.parse::<u64>().unwrap();
    let d = std::time::Duration::from_millis(millis);

    tracing::info!("failpoint {:?}: sleeping for {:?}", name, d);
    tokio::time::timeout(d, cancel.cancelled()).await.ok();
    tracing::info!("failpoint {:?}: sleep done", name);
}
