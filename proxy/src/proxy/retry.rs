use crate::compute;
use std::{error::Error, io};
use tokio::time;

/// Number of times we should retry the `/proxy_wake_compute` http request.
/// Retry duration is BASE_RETRY_WAIT_DURATION * RETRY_WAIT_EXPONENT_BASE ^ n, where n starts at 0
pub const NUM_RETRIES_CONNECT: u32 = 16;
const BASE_RETRY_WAIT_DURATION: time::Duration = time::Duration::from_millis(25);
const RETRY_WAIT_EXPONENT_BASE: f64 = std::f64::consts::SQRT_2;

pub trait ShouldRetry {
    fn could_retry(&self) -> bool;
    fn should_retry(&self, num_retries: u32) -> bool {
        match self {
            _ if num_retries >= NUM_RETRIES_CONNECT => false,
            err => err.could_retry(),
        }
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
}

impl ShouldRetry for compute::ConnectionError {
    fn could_retry(&self) -> bool {
        match self {
            compute::ConnectionError::Postgres(err) => err.could_retry(),
            compute::ConnectionError::CouldNotConnect(err) => err.could_retry(),
            _ => false,
        }
    }
}

pub fn retry_after(num_retries: u32) -> time::Duration {
    BASE_RETRY_WAIT_DURATION.mul_f64(RETRY_WAIT_EXPONENT_BASE.powi((num_retries as i32) - 1))
}
