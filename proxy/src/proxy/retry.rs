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
        !matches!(
            self.code(),
            &SqlState::TOO_MANY_CONNECTIONS
                | &SqlState::OUT_OF_MEMORY
                | &SqlState::SYNTAX_ERROR
                | &SqlState::T_R_SERIALIZATION_FAILURE
                | &SqlState::INVALID_CATALOG_NAME
                | &SqlState::INVALID_SCHEMA_NAME
                | &SqlState::INVALID_PARAMETER_VALUE
        )
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
            compute::ConnectionError::Postgres(err) => err.could_retry(),
            compute::ConnectionError::CouldNotConnect(err) => err.could_retry(),
            compute::ConnectionError::WakeComputeError(err) => err.could_retry(),
            _ => false,
        }
    }
}
impl ShouldRetryWakeCompute for compute::ConnectionError {
    fn should_retry_wake_compute(&self) -> bool {
        match self {
            compute::ConnectionError::Postgres(err) => err.should_retry_wake_compute(),
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
