use std::error::Error;
use std::io;

use tokio::time;

use crate::compute;
use crate::config::RetryConfig;

pub(crate) trait CouldRetry {
    /// Returns true if the error could be retried
    fn could_retry(&self) -> bool;
}

pub(crate) trait ShouldRetryWakeCompute {
    /// Returns true if we need to invalidate the cache for this node.
    /// If false, we can continue retrying with the current node cache.
    fn should_retry_wake_compute(&self) -> bool;
}

pub(crate) fn should_retry(err: &impl CouldRetry, num_retries: u32, config: RetryConfig) -> bool {
    num_retries < config.max_retries && err.could_retry()
}

impl CouldRetry for io::Error {
    fn could_retry(&self) -> bool {
        use std::io::ErrorKind;
        matches!(
            self.kind(),
            ErrorKind::ConnectionRefused | ErrorKind::AddrNotAvailable | ErrorKind::TimedOut
        )
    }
}

impl CouldRetry for postgres_client::error::DbError {
    fn could_retry(&self) -> bool {
        use postgres_client::error::SqlState;
        matches!(
            self.code(),
            &SqlState::CONNECTION_FAILURE
                | &SqlState::CONNECTION_EXCEPTION
                | &SqlState::CONNECTION_DOES_NOT_EXIST
                | &SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
        )
    }
}
impl ShouldRetryWakeCompute for postgres_client::error::DbError {
    fn should_retry_wake_compute(&self) -> bool {
        use postgres_client::error::SqlState;
        // Here are errors that happens after the user successfully authenticated to the database.
        // TODO: there are pgbouncer errors that should be retried, but they are not listed here.
        let non_retriable_pg_errors = matches!(
            self.code(),
            &SqlState::TOO_MANY_CONNECTIONS
                | &SqlState::OUT_OF_MEMORY
                | &SqlState::SYNTAX_ERROR
                | &SqlState::T_R_SERIALIZATION_FAILURE
                | &SqlState::INVALID_CATALOG_NAME
                | &SqlState::INVALID_SCHEMA_NAME
                | &SqlState::INVALID_PARAMETER_VALUE,
        );
        if non_retriable_pg_errors {
            return false;
        }
        // PGBouncer errors that should not trigger a wake_compute retry.
        if self.code() == &SqlState::PROTOCOL_VIOLATION {
            // Source for the error message:
            // https://github.com/pgbouncer/pgbouncer/blob/f15997fe3effe3a94ba8bcc1ea562e6117d1a131/src/client.c#L1070
            return !self
                .message()
                .contains("no more connections allowed (max_client_conn)");
        }
        true
    }
}

impl CouldRetry for postgres_client::Error {
    fn could_retry(&self) -> bool {
        if let Some(io_err) = self.source().and_then(|x| x.downcast_ref()) {
            io::Error::could_retry(io_err)
        } else if let Some(db_err) = self.source().and_then(|x| x.downcast_ref()) {
            postgres_client::error::DbError::could_retry(db_err)
        } else {
            false
        }
    }
}
impl ShouldRetryWakeCompute for postgres_client::Error {
    fn should_retry_wake_compute(&self) -> bool {
        if let Some(db_err) = self.source().and_then(|x| x.downcast_ref()) {
            postgres_client::error::DbError::should_retry_wake_compute(db_err)
        } else {
            // likely an IO error. Possible the compute has shutdown and the
            // cache is stale.
            true
        }
    }
}

impl CouldRetry for compute::ConnectionError {
    fn could_retry(&self) -> bool {
        match self {
            compute::ConnectionError::TlsError(err) => err.could_retry(),
            compute::ConnectionError::WakeComputeError(err) => err.could_retry(),
            compute::ConnectionError::TooManyConnectionAttempts(_) => false,
        }
    }
}
impl ShouldRetryWakeCompute for compute::ConnectionError {
    fn should_retry_wake_compute(&self) -> bool {
        match self {
            // the cache entry was not checked for validity
            compute::ConnectionError::TooManyConnectionAttempts(_) => false,
            _ => true,
        }
    }
}

pub(crate) fn retry_after(num_retries: u32, config: RetryConfig) -> time::Duration {
    config
        .base_delay
        .mul_f64(config.backoff_factor.powi((num_retries as i32) - 1))
}

#[cfg(test)]
mod tests {
    use postgres_client::error::{DbError, SqlState};

    use super::ShouldRetryWakeCompute;

    #[test]
    fn should_retry_wake_compute_for_db_error() {
        // These SQLStates should NOT trigger a wake_compute retry.
        let non_retry_states = [
            SqlState::TOO_MANY_CONNECTIONS,
            SqlState::OUT_OF_MEMORY,
            SqlState::SYNTAX_ERROR,
            SqlState::T_R_SERIALIZATION_FAILURE,
            SqlState::INVALID_CATALOG_NAME,
            SqlState::INVALID_SCHEMA_NAME,
            SqlState::INVALID_PARAMETER_VALUE,
        ];
        for state in non_retry_states {
            let err = DbError::new_test_error(state.clone(), "oops".to_string());
            assert!(
                !err.should_retry_wake_compute(),
                "State {state:?} unexpectedly retried"
            );
        }

        // Errors coming from pgbouncer should not trigger a wake_compute retry
        let non_retry_pgbouncer_errors = ["no more connections allowed (max_client_conn)"];
        for error in non_retry_pgbouncer_errors {
            let err = DbError::new_test_error(SqlState::PROTOCOL_VIOLATION, error.to_string());
            assert!(
                !err.should_retry_wake_compute(),
                "PGBouncer error {error:?} unexpectedly retried"
            );
        }

        // These SQLStates should trigger a wake_compute retry.
        let retry_states = [
            SqlState::CONNECTION_FAILURE,
            SqlState::CONNECTION_EXCEPTION,
            SqlState::CONNECTION_DOES_NOT_EXIST,
            SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
        ];
        for state in retry_states {
            let err = DbError::new_test_error(state.clone(), "oops".to_string());
            assert!(
                err.should_retry_wake_compute(),
                "State {state:?} unexpectedly skipped retry"
            );
        }
    }
}
