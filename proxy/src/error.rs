use std::{error::Error as StdError, fmt, io};

/// Upcast (almost) any error into an opaque [`io::Error`].
pub fn io_error(e: impl Into<Box<dyn StdError + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

/// A small combinator for pluggable error logging.
pub fn log_error<E: fmt::Display>(e: E) -> E {
    tracing::error!("{e}");
    e
}

/// Marks errors that may be safely shown to a client.
/// This trait can be seen as a specialized version of [`ToString`].
///
/// NOTE: This trait should not be implemented for [`anyhow::Error`], since it
/// is way too convenient and tends to proliferate all across the codebase,
/// ultimately leading to accidental leaks of sensitive data.
pub trait UserFacingError: fmt::Display {
    /// Format the error for client, stripping all sensitive info.
    ///
    /// Although this might be a no-op for many types, it's highly
    /// recommended to override the default impl in case error type
    /// contains anything sensitive: various IDs, IP addresses etc.
    #[inline(always)]
    fn to_string_client(&self) -> String {
        self.to_string()
    }
}
