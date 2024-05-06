use crate::{compute, config::RetryConfig};
use std::{error::Error, io};
use tokio::time;

pub trait ShouldRetry {
    fn could_retry(&self) -> bool;
    fn should_retry(&self, num_retries: u32, config: RetryConfig) -> bool {
        match self {
            _ if num_retries >= config.max_retries => false,
            err => err.could_retry(),
        }
    }
    fn should_retry_database_address(&self) -> bool {
        true
    }
}

impl ShouldRetry for io::Error {
    fn could_retry(&self) -> bool {
        use std::io::ErrorKind;
        matches!(
            self.kind(),
            ErrorKind::ConnectionRefused | ErrorKind::AddrNotAvailable | ErrorKind::TimedOut
        )
    }
}

impl ShouldRetry for tokio_postgres::error::DbError {
    fn could_retry(&self) -> bool {
        use tokio_postgres::error::SqlState;
        matches!(
            self.code(),
            &SqlState::CONNECTION_FAILURE
                | &SqlState::CONNECTION_EXCEPTION
                | &SqlState::CONNECTION_DOES_NOT_EXIST
                | &SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
        )
    }
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

impl ShouldRetry for tokio_postgres::Error {
    fn could_retry(&self) -> bool {
        if let Some(io_err) = self.source().and_then(|x| x.downcast_ref()) {
            io::Error::could_retry(io_err)
        } else if let Some(db_err) = self.source().and_then(|x| x.downcast_ref()) {
            tokio_postgres::error::DbError::could_retry(db_err)
        } else {
            false
        }
    }
    fn should_retry_database_address(&self) -> bool {
        if let Some(io_err) = self.source().and_then(|x| x.downcast_ref()) {
            io::Error::should_retry_database_address(io_err)
        } else if let Some(db_err) = self.source().and_then(|x| x.downcast_ref()) {
            tokio_postgres::error::DbError::should_retry_database_address(db_err)
        } else {
            true
        }
    }
}

impl ShouldRetry for compute::ConnectionError {
    fn could_retry(&self) -> bool {
        match self {
            compute::ConnectionError::Postgres(err) => err.could_retry(),
            compute::ConnectionError::CouldNotConnect(err) => err.could_retry(),
            _ => false,
        }
    }
    fn should_retry_database_address(&self) -> bool {
        match self {
            compute::ConnectionError::Postgres(err) => err.should_retry_database_address(),
            compute::ConnectionError::CouldNotConnect(err) => err.should_retry_database_address(),
            _ => true,
        }
    }
}

pub fn retry_after(num_retries: u32, config: RetryConfig) -> time::Duration {
    config
        .base_delay
        .mul_f64(config.backoff_factor.powi((num_retries as i32) - 1))
}
