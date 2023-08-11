use std::fmt::{Debug, Display};

use futures::Future;

pub const DEFAULT_BASE_BACKOFF_SECONDS: f64 = 0.1;
pub const DEFAULT_MAX_BACKOFF_SECONDS: f64 = 3.0;

pub async fn exponential_backoff(n: u32, base_increment: f64, max_seconds: f64) {
    let backoff_duration_seconds =
        exponential_backoff_duration_seconds(n, base_increment, max_seconds);
    if backoff_duration_seconds > 0.0 {
        tracing::info!(
            "Backoff: waiting {backoff_duration_seconds} seconds before processing with the task",
        );
        tokio::time::sleep(std::time::Duration::from_secs_f64(backoff_duration_seconds)).await;
    }
}

pub fn exponential_backoff_duration_seconds(n: u32, base_increment: f64, max_seconds: f64) -> f64 {
    if n == 0 {
        0.0
    } else {
        (1.0 + base_increment).powf(f64::from(n)).min(max_seconds)
    }
}

/// retries passed operation until one of the following conditions are met:
/// Encountered error is considered as permanent (non-retryable)
/// Retries have been exhausted.
/// `is_permanent` closure should be used to provide distinction between permanent/non-permanent errors
/// When attempts cross `warn_threshold` function starts to emit log warnings.
/// `description` argument is added to log messages. Its value should identify the `op` is doing
pub async fn retry<T, O, F, E>(
    mut op: O,
    is_permanent: impl Fn(&E) -> bool,
    warn_threshold: u32,
    max_attempts: u32,
    description: &str,
) -> Result<T, E>
where
    // Not std::error::Error because anyhow::Error doesnt implement it.
    // For context see https://github.com/dtolnay/anyhow/issues/63
    E: Display + Debug,
    O: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
{
    let mut attempts = 0;
    loop {
        let result = op().await;
        match result {
            Ok(_) => {
                if attempts > 0 {
                    tracing::info!("{description} succeeded after {attempts} retries");
                }
                return result;
            }

            // These are "permanent" errors that should not be retried.
            Err(ref e) if is_permanent(e) => {
                return result;
            }
            // Assume that any other failure might be transient, and the operation might
            // succeed if we just keep trying.
            Err(err) if attempts < warn_threshold => {
                tracing::info!("{description} failed, will retry (attempt {attempts}): {err:#}");
            }
            Err(err) if attempts < max_attempts => {
                tracing::warn!("{description} failed, will retry (attempt {attempts}): {err:#}");
            }
            Err(ref err) => {
                // Operation failed `max_attempts` times. Time to give up.
                tracing::warn!(
                    "{description} still failed after {attempts} retries, giving up: {err:?}"
                );
                return result;
            }
        }
        // sleep and retry
        exponential_backoff(
            attempts,
            DEFAULT_BASE_BACKOFF_SECONDS,
            DEFAULT_MAX_BACKOFF_SECONDS,
        )
        .await;
        attempts += 1;
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use tokio::sync::Mutex;

    use super::*;

    #[test]
    fn backoff_defaults_produce_growing_backoff_sequence() {
        let mut current_backoff_value = None;

        for i in 0..10_000 {
            let new_backoff_value = exponential_backoff_duration_seconds(
                i,
                DEFAULT_BASE_BACKOFF_SECONDS,
                DEFAULT_MAX_BACKOFF_SECONDS,
            );

            if let Some(old_backoff_value) = current_backoff_value.replace(new_backoff_value) {
                assert!(
                    old_backoff_value <= new_backoff_value,
                    "{i}th backoff value {new_backoff_value} is smaller than the previous one {old_backoff_value}"
                )
            }
        }

        assert_eq!(
            current_backoff_value.expect("Should have produced backoff values to compare"),
            DEFAULT_MAX_BACKOFF_SECONDS,
            "Given big enough of retries, backoff should reach its allowed max value"
        );
    }

    #[tokio::test]
    async fn retry_retries() {
        let count = Mutex::new(0);
        let err_result = retry(
            || async {
                *count.lock().await += 1;
                Result::<(), io::Error>::Err(io::Error::from(io::ErrorKind::Other))
            },
            |_e| false,
            1,
            1,
            "work",
        )
        .await;

        assert!(err_result.is_err());

        assert_eq!(*count.lock().await, 2);

        let count = Mutex::new(0);
        retry(
            || async {
                let mut locked = count.lock().await;
                if *locked > 1 {
                    Ok(())
                } else {
                    *locked += 1;
                    Err(io::Error::from(io::ErrorKind::Other))
                }
            },
            |_e| false,
            2,
            2,
            "work",
        )
        .await
        .unwrap();

        // permanent errors are not retried
        let count = Mutex::new(0);
        let _ = retry(
            || async {
                let mut locked = count.lock().await;
                if *locked > 1 {
                    Ok(())
                } else {
                    *locked += 1;
                    Err(io::Error::from(io::ErrorKind::Other))
                }
            },
            |_e| true,
            2,
            2,
            "work",
        )
        .await
        .unwrap_err();

        assert_eq!(*count.lock().await, 1);
    }
}
