//! Simple Authentication and Security Layer.
//!
//! RFC: <https://datatracker.ietf.org/doc/html/rfc4422>.
//!
//! Reference implementation:
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/backend/libpq/auth-sasl.c>
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/interfaces/libpq/fe-auth.c>

mod channel_binding;
mod messages;
mod stream;

use std::io;

pub(crate) use channel_binding::ChannelBinding;
pub(crate) use messages::FirstMessage;
pub(crate) use stream::{Outcome, SaslStream};
use thiserror::Error;

use crate::error::{ReportableError, UserFacingError};

/// Fine-grained auth errors help in writing tests.
#[derive(Error, Debug)]
pub(crate) enum Error {
    #[error("Channel binding failed: {0}")]
    ChannelBindingFailed(&'static str),

    #[error("Unsupported channel binding method: {0}")]
    ChannelBindingBadMethod(Box<str>),

    #[error("Bad client message: {0}")]
    BadClientMessage(&'static str),

    #[error("Internal error: missing digest")]
    MissingBinding,

    #[error("could not decode salt: {0}")]
    Base64(#[from] base64::DecodeError),

    #[error(transparent)]
    Io(#[from] io::Error),
}

impl UserFacingError for Error {
    fn to_string_client(&self) -> String {
        match self {
            Self::ChannelBindingFailed(m) => (*m).to_string(),
            Self::ChannelBindingBadMethod(m) => format!("unsupported channel binding method {m}"),
            _ => "authentication protocol violation".to_string(),
        }
    }
}

impl ReportableError for Error {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            Error::ChannelBindingFailed(_) => crate::error::ErrorKind::User,
            Error::ChannelBindingBadMethod(_) => crate::error::ErrorKind::User,
            Error::BadClientMessage(_) => crate::error::ErrorKind::User,
            Error::MissingBinding => crate::error::ErrorKind::Service,
            Error::Base64(_) => crate::error::ErrorKind::ControlPlane,
            Error::Io(_) => crate::error::ErrorKind::ClientDisconnect,
        }
    }
}

/// A convenient result type for SASL exchange.
pub(crate) type Result<T> = std::result::Result<T, Error>;

/// A result of one SASL exchange.
#[must_use]
pub(crate) enum Step<T, R> {
    /// We should continue exchanging messages.
    Continue(T, String),
    /// The client has been authenticated successfully.
    Success(R, String),
    /// Authentication failed (reason attached).
    Failure(&'static str),
}

/// Every SASL mechanism (e.g. [SCRAM](crate::scram)) is expected to implement this trait.
pub(crate) trait Mechanism: Sized {
    /// What's produced as a result of successful authentication.
    type Output;

    /// Produce a server challenge to be sent to the client.
    /// This is how this method is called in PostgreSQL (`libpq/sasl.h`).
    fn exchange(self, input: &str) -> Result<Step<Self, Self::Output>>;
}
