//! Simple Authentication and Security Layer.
//!
//! RFC: <https://datatracker.ietf.org/doc/html/rfc4422>.
//!
//! Reference implementation:
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/backend/libpq/auth-sasl.c>
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/interfaces/libpq/fe-auth.c>

use crate::parse::{split_at_const, split_cstr};
use anyhow::Context;
use thiserror::Error;

/// SASL-specific payload of [`PasswordMessage`](zenith_utils::pq_proto::FeMessage::PasswordMessage).
#[derive(Debug)]
pub struct SaslFirstMessage<'a> {
    /// Authentication method, e.g. `"SCRAM-SHA-256"`.
    pub method: &'a str,
    /// Initial client message.
    pub message: &'a [u8],
}

impl<'a> SaslFirstMessage<'a> {
    // NB: FromStr doesn't work with lifetimes
    pub fn parse(bytes: &'a [u8]) -> Option<Self> {
        let (method_cstr, tail) = split_cstr(bytes)?;
        let method = method_cstr.to_str().ok()?;

        let (len_bytes, message) = split_at_const(tail)?;
        let len = u32::from_be_bytes(*len_bytes) as usize;
        if len != message.len() {
            return None;
        }

        Some(Self { method, message })
    }
}

/// A single SASL message.
/// This struct is deliberately decoupled from lower-level
/// [`BeAuthenticationSaslMessage`](zenith_utils::pq_proto::BeAuthenticationSaslMessage).
#[derive(Debug)]
pub enum SaslMessage<T> {
    /// We expect to see more steps.
    Continue(T),
    /// This is the final step.
    Final(T),
}

/// This specialized trait provides capabilities akin to
/// [`std::io::Read`]+[`std::io::Write`] in oder to
/// abstract away underlying stream implementations.
pub trait SaslStream {
    /// We'd like to use `AsRef<[str]>` here, but afaik there's
    /// no cheap way to make [`String`] out of [`bytes::Bytes`];
    /// On the other hand, byte slices are a decent middle ground.
    type In: AsRef<[u8]>;

    /// Receive a [SASL](crate::sasl) message from a client.
    fn recv(&mut self) -> anyhow::Result<Self::In>;

    /// Send a [SASL](crate::sasl) message to a client.
    fn send(&mut self, data: &SaslMessage<impl AsRef<[u8]>>) -> anyhow::Result<()>;
}

impl<S: SaslStream> SaslStream for &mut S {
    type In = S::In;

    #[inline(always)]
    fn recv(&mut self) -> anyhow::Result<Self::In> {
        S::recv(self)
    }

    #[inline(always)]
    fn send(&mut self, data: &SaslMessage<impl AsRef<[u8]>>) -> anyhow::Result<()> {
        S::send(self, data)
    }
}

/// Sometimes it's necessary to mix in a message we got from somewhere else.
impl<'a, V: AsRef<[u8]>, S: SaslStream<In = V>> SaslStream for (Option<V>, S) {
    type In = S::In;

    #[inline(always)]
    fn recv(&mut self) -> anyhow::Result<Self::In> {
        // Try returning a stashed message first
        match self.0.take() {
            Some(value) => Ok(value),
            None => self.1.recv(),
        }
    }

    #[inline(always)]
    fn send(&mut self, data: &SaslMessage<impl AsRef<[u8]>>) -> anyhow::Result<()> {
        self.1.send(data)
    }
}

/// Fine-grained auth errors help in writing tests.
#[derive(Error, Debug)]
pub enum SaslError {
    #[error("failed to authenticate client: {0}")]
    AuthenticationFailed(&'static str),
    #[error("bad client message")]
    BadClientMessage,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// A convenient result type for SASL exchange.
pub type Result<T> = std::result::Result<T, SaslError>;

pub enum SaslStep<T, R> {
    Transition(T),
    Authenticated(R),
}

/// Every SASL mechanism (e.g. [SCRAM](crate::scram)) is expected to implement this trait.
pub trait SaslMechanism: Sized {
    /// What's produced as a result of successful authentication.
    type Outcome;

    /// Produce a server challenge to be sent to the client.
    /// This is how this method is called in PostgreSQL (libpq/sasl.h).
    fn exchange(self, input: &str) -> Result<(SaslStep<Self, Self::Outcome>, String)>;

    /// Perform SASL message exchange according to the underlying algorithm
    /// until user is either authenticated or denied access.
    fn authenticate(mut self, mut stream: impl SaslStream) -> Result<Self::Outcome> {
        loop {
            let msg = stream.recv()?;
            let input = std::str::from_utf8(msg.as_ref()).context("bad encoding")?;

            let (this, reply) = self.exchange(input)?;
            match this {
                SaslStep::Transition(this) => {
                    stream.send(&SaslMessage::Continue(reply))?;
                    self = this;
                }
                SaslStep::Authenticated(outcome) => {
                    stream.send(&SaslMessage::Final(reply))?;
                    return Ok(outcome);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    #[test]
    fn parse_sasl_first_message() {
        let proto = CStr::from_bytes_with_nul(b"SCRAM-SHA-256\0").unwrap();
        let sasl = "n,,n=,r=KHQ2Gjc7NptyB8aov5/TnUy4".as_bytes();
        let sasl_len = (sasl.len() as u32).to_be_bytes();
        let bytes = [proto.to_bytes_with_nul(), sasl_len.as_ref(), sasl].concat();

        let password = SaslFirstMessage::parse(&bytes).unwrap();
        assert_eq!(password.method, proto.to_str().unwrap());
        assert_eq!(password.message, sasl);
    }
}
