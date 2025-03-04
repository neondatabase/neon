use std::{future::Future, panic::AssertUnwindSafe};

use futures::FutureExt;
use tracing::error;

pub async fn exit_on_panic_or_error<T, E>(
    task_name: &'static str,
    future: impl Future<Output = Result<T, E>>,
) -> T
where
    E: std::fmt::Debug,
{
    // We use AssertUnwindSafe here so that the payload function
    // doesn't need to be UnwindSafe. We don't do anything after the
    // unwinding that would expose us to unwind-unsafe behavior.
    let result = AssertUnwindSafe(future).catch_unwind().await;
    match result {
        Ok(Ok(val)) => val,
        Ok(Err(err)) => {
            error!(
                task_name,
                "Task exited with error, exiting process: {err:?}"
            );
            std::process::exit(1);
        }
        Err(panic_obj) => {
            error!(task_name, "Task panicked, exiting process: {panic_obj:?}");
            std::process::exit(1);
        }
    }
}
