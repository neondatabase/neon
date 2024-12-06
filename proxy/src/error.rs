use std::error::Error as StdError;
use std::{fmt, io};

use anyhow::Context;
use measured::FixedCardinalityLabel;
use tokio::task::JoinError;

/// Upcast (almost) any error into an opaque [`io::Error`].
pub(crate) fn io_error(e: impl Into<Box<dyn StdError + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

/// Marks errors that may be safely shown to a client.
/// This trait can be seen as a specialized version of [`ToString`].
///
/// NOTE: This trait should not be implemented for [`anyhow::Error`], since it
/// is way too convenient and tends to proliferate all across the codebase,
/// ultimately leading to accidental leaks of sensitive data.
pub(crate) trait UserFacingError: ReportableError {
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, FixedCardinalityLabel)]
#[label(singleton = "type")]
pub enum ErrorKind {
    /// Wrong password, unknown endpoint, protocol violation, etc...
    User,

    /// Network error between user and proxy. Not necessarily user error
    #[label(rename = "clientdisconnect")]
    ClientDisconnect,

    /// Proxy self-imposed user rate limits
    #[label(rename = "ratelimit")]
    RateLimit,

    /// Proxy self-imposed service-wise rate limits
    #[label(rename = "serviceratelimit")]
    ServiceRateLimit,

    /// Proxy quota limit violation
    #[label(rename = "quota")]
    Quota,

    /// internal errors
    Service,

    /// Error communicating with control plane
    #[label(rename = "controlplane")]
    ControlPlane,

    /// Postgres error
    Postgres,

    /// Error communicating with compute
    Compute,
}

impl ErrorKind {
    pub(crate) fn to_metric_label(self) -> &'static str {
        match self {
            ErrorKind::User => "user",
            ErrorKind::ClientDisconnect => "clientdisconnect",
            ErrorKind::RateLimit => "ratelimit",
            ErrorKind::ServiceRateLimit => "serviceratelimit",
            ErrorKind::Quota => "quota",
            ErrorKind::Service => "service",
            ErrorKind::ControlPlane => "controlplane",
            ErrorKind::Postgres => "postgres",
            ErrorKind::Compute => "compute",
        }
    }
}

pub(crate) trait ReportableError: fmt::Display + Send + 'static {
    fn get_error_kind(&self) -> ErrorKind;
}

impl ReportableError for postgres_client::error::Error {
    fn get_error_kind(&self) -> ErrorKind {
        if self.as_db_error().is_some() {
            ErrorKind::Postgres
        } else {
            ErrorKind::Compute
        }
    }
}

/// Flattens `Result<Result<T>>` into `Result<T>`.
pub fn flatten_err<T>(r: Result<anyhow::Result<T>, JoinError>) -> anyhow::Result<T> {
    r.context("join error").and_then(|x| x)
}
