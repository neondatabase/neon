/// use with fail::cfg("$name", "return(2000)")
///
/// The effect is similar to a "sleep(2000)" action, i.e. we sleep for the
/// specified time (in milliseconds). The main difference is that we use async
/// tokio sleep function. Another difference is that we print lines to the log,
/// which can be useful in tests to check that the failpoint was hit.
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
}
pub use __failpoint_sleep_millis_async as sleep_millis_async;

// Helper function used by the macro. (A function has nicer scoping so we
// don't need to decorate everything with "::")
#[doc(hidden)]
pub(crate) async fn failpoint_sleep_helper(name: &'static str, duration_str: String) {
    let millis = duration_str.parse::<u64>().unwrap();
    let d = std::time::Duration::from_millis(millis);

    tracing::info!("failpoint {:?}: sleeping for {:?}", name, d);
    tokio::time::sleep(d).await;
    tracing::info!("failpoint {:?}: sleep done", name);
}

pub fn init() -> fail::FailScenario<'static> {
    let mut exits = Vec::new();

    if let Ok(val) = std::env::var("FAILPOINTS") {
        // pre-process to allow using "exit" from `env.pageserver.start(extra_env_vars={"FAILPOINTS"=...})`
        let parsed = val.split(';').map(|s| {
            s.split_once('=')
                .map(|tuple| (tuple.0, Some(tuple.1)))
                .unwrap_or((s, None))
        });

        let mut s = String::new();

        for (name, action) in parsed {
            if action == Some("exit") {
                // we'll need to handle this separatedly
                exits.push(String::from(name));
                continue;
            }

            if !s.is_empty() {
                s.push(';');
            }
            s.push_str(name);
            if let Some(action) = action {
                s.push('=');
                s.push_str(action);
            }
        }
        std::env::set_var("FAILPOINTS", s);
    };

    let scenario = fail::FailScenario::setup();

    for name in exits {
        fail::cfg_callback(name, exit_failpoint).unwrap();
    }

    scenario
}

pub(crate) fn apply_failpoint(name: &str, actions: &str) -> Result<(), String> {
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
