//! Failpoint support code shared between pageserver and safekeepers.

use tokio_util::sync::CancellationToken;

/// Declare a failpoint that can use to `pause` failpoint action.
/// We don't want to block the executor thread, hence, spawn_blocking + await.
///
/// Optionally pass a cancellation token, and this failpoint will drop out of
/// its pause when the cancellation token fires. This is useful for testing
/// cases where we would like to block something, but test its clean shutdown behavior.
/// The macro evaluates to a Result in that case, where Ok(()) is the case
/// where the failpoint was not paused, and Err() is the case where cancellation
/// token fired while evaluating the failpoint.
///
/// Remember to unpause the failpoint in the test; until that happens, one of the
/// limited number of spawn_blocking thread pool threads is leaked.
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
            let failpoint_fut = ::tokio::task::spawn_blocking({
                let current = ::tracing::Span::current();
                move || {
                    let _entered = current.entered();
                    ::tracing::info!("at failpoint {}", $name);
                    ::fail::fail_point!($name);
                }
            });
            let cancel_fut = async move {
                $cancel.cancelled().await;
            };
            ::tokio::select! {
                res = failpoint_fut => {
                    res.expect("spawn_blocking");
                    // continue with execution
                    Ok(())
                },
                _ = cancel_fut => {
                    Err(())
                }
            }
        } else {
            Ok(())
        }
    }};
}

pub use pausable_failpoint;

/// use with fail::cfg("$name", "return(2000)")
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
            ::fail::fail_point!($name, |x| x);
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
            ::fail::fail_point!($name, |x| x);
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

pub fn init() -> fail::FailScenario<'static> {
    // The failpoints lib provides support for parsing the `FAILPOINTS` env var.
    // We want non-default behavior for `exit`, though, so, we handle it separately.
    //
    // Format for FAILPOINTS is "name=actions" separated by ";".
    let actions = std::env::var("FAILPOINTS");
    if actions.is_ok() {
        std::env::remove_var("FAILPOINTS");
    } else {
        // let the library handle non-utf8, or nothing for not present
    }

    let scenario = fail::FailScenario::setup();

    if let Ok(val) = actions {
        val.split(';')
            .enumerate()
            .map(|(i, s)| s.split_once('=').ok_or((i, s)))
            .for_each(|res| {
                let (name, actions) = match res {
                    Ok(t) => t,
                    Err((i, s)) => {
                        panic!(
                            "startup failpoints: missing action on the {}th failpoint; try `{s}=return`",
                            i + 1,
                        );
                    }
                };
                if let Err(e) = apply_failpoint(name, actions) {
                    panic!("startup failpoints: failed to apply failpoint {name}={actions}: {e}");
                }
            });
    }

    scenario
}

pub fn apply_failpoint(name: &str, actions: &str) -> Result<(), String> {
    if actions == "exit" {
        fail::cfg_callback(name, exit_failpoint)
    } else {
        fail::cfg(name, actions)
    }
}

#[inline(never)]
fn exit_failpoint() {
    tracing::info!("Exit requested by failpoint");
    std::process::exit(1);
}
