use crate::{compute, config::RetryConfig};
use std::{error::Error, io};
use tokio::time;

pub trait CouldRetry {
    fn could_retry(&self) -> bool;
}

pub trait CouldRetry2 {
    fn should_retry_database_address(&self) -> bool;
}

pub fn should_retry(err: &impl CouldRetry, num_retries: u32, config: RetryConfig) -> bool {
    num_retries < config.max_retries && err.could_retry()
}

impl CouldRetry for io::Error {
    fn could_retry(&self) -> bool {
        use std::io::ErrorKind;
        match self.kind() {
            ErrorKind::ConnectionRefused | ErrorKind::AddrNotAvailable | ErrorKind::TimedOut => {
                true
            }
            _ => false,
        }
    }
}

impl CouldRetry for tokio_postgres::error::DbError {
    fn could_retry(&self) -> bool {
        use tokio_postgres::error::SqlState;
        match *self.code() {
            SqlState::CONNECTION_FAILURE
            | SqlState::CONNECTION_EXCEPTION
            | SqlState::CONNECTION_DOES_NOT_EXIST
            | SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION => true,
            _ => false,
        }
    }
}
impl CouldRetry2 for tokio_postgres::error::DbError {
    fn should_retry_database_address(&self) -> bool {
        use tokio_postgres::error::SqlState;
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

impl CouldRetry for tokio_postgres::Error {
    fn could_retry(&self) -> bool {
        if let Some(io_err) = self.source().and_then(|x| x.downcast_ref()) {
            io::Error::could_retry(io_err)
        } else if let Some(db_err) = self.source().and_then(|x| x.downcast_ref()) {
            tokio_postgres::error::DbError::could_retry(db_err)
        } else {
            false
        }
    }
}
impl CouldRetry2 for tokio_postgres::Error {
    fn should_retry_database_address(&self) -> bool {
        if let Some(db_err) = self.source().and_then(|x| x.downcast_ref()) {
            tokio_postgres::error::DbError::should_retry_database_address(db_err)
        } else {
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
impl CouldRetry2 for compute::ConnectionError {
    fn should_retry_database_address(&self) -> bool {
        match self {
            compute::ConnectionError::Postgres(err) => err.should_retry_database_address(),
            // the cache entry was not checked for validity
            compute::ConnectionError::TooManyConnectionAttempts(_) => false,
            _ => true,
        }
    }
}

pub fn retry_after(num_retries: u32, config: RetryConfig) -> time::Duration {
    config
        .base_delay
        .mul_f64(config.backoff_factor.powi((num_retries as i32) - 1))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{config::RetryConfig, proxy::retry::retry_after};

    #[test]
    fn connect_compute_total_wait() {
        let mut total_wait = tokio::time::Duration::ZERO;
        let config = RetryConfig {
            base_delay: Duration::from_secs(1),
            max_retries: 5,
            backoff_factor: 2.0,
        };
        for num_retries in 1..config.max_retries {
            total_wait += retry_after(num_retries, config);
        }
        assert!(f64::abs(total_wait.as_secs_f64() - 15.0) < 0.1);
    }
}
