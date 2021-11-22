//! Simple Authentication and Security Layer.
//!
//! RFC: <https://datatracker.ietf.org/doc/html/rfc4422>.
//!
//! Reference implementation:
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/backend/libpq/auth-sasl.c>
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/interfaces/libpq/fe-auth.c>

use crate::parse::{split_at_const, split_cstr};
use crate::stream::PqStream;
use std::io;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use zenith_utils::pq_proto::{BeAuthenticationSaslMessage, BeMessage};

/// SASL-specific payload of [`PasswordMessage`](zenith_utils::pq_proto::FeMessage::PasswordMessage).
#[derive(Debug)]
pub struct SaslFirstMessage<'a> {
    /// Authentication method, e.g. `"SCRAM-SHA-256"`.
    pub method: &'a str,
    /// Initial client message.
    pub message: &'a str,
}

impl<'a> SaslFirstMessage<'a> {
    // NB: FromStr doesn't work with lifetimes
    pub fn parse(bytes: &'a [u8]) -> Option<Self> {
        let (method_cstr, tail) = split_cstr(bytes)?;
        let method = method_cstr.to_str().ok()?;

        let (len_bytes, bytes) = split_at_const(tail)?;
        let len = u32::from_be_bytes(*len_bytes) as usize;
        if len != bytes.len() {
            return None;
        }

        let message = std::str::from_utf8(bytes).ok()?;
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

impl<'a> SaslMessage<&'a str> {
    fn to_reply(&self) -> BeMessage<'a> {
        use BeAuthenticationSaslMessage::*;
        BeMessage::AuthenticationSasl(match self {
            SaslMessage::Continue(s) => Continue(s.as_bytes()),
            SaslMessage::Final(s) => Final(s.as_bytes()),
        })
    }
}

/// Abstracts away all peculiarities of the libpq's protocol.
pub struct SaslStream<'a, S> {
    /// The underlying stream.
    stream: &'a mut PqStream<S>,
    /// Current password message we received from client.
    current: bytes::Bytes,
    /// First SASL message produced by client.
    first: Option<&'a str>,
}

impl<'a, S> SaslStream<'a, S> {
    pub fn new(stream: &'a mut PqStream<S>, first: &'a str) -> Self {
        Self {
            stream,
            current: bytes::Bytes::new(),
            first: Some(first),
        }
    }
}

impl<S: AsyncRead + Unpin> SaslStream<'_, S> {
    // Receive a new SASL message from the client.
    async fn recv(&mut self) -> io::Result<&str> {
        self.current = self.stream.read_password_message().await?;
        let s = std::str::from_utf8(&self.current)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad encoding"))?;
        Ok(s)
    }
}

impl<S: AsyncWrite + Unpin> SaslStream<'_, S> {
    // Send a SASL message to the client.
    async fn send(&mut self, msg: &SaslMessage<&str>) -> io::Result<()> {
        self.stream.write_message(&msg.to_reply()).await?;
        Ok(())
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> SaslStream<'_, S> {
    /// Perform SASL message exchange according to the underlying algorithm
    /// until user is either authenticated or denied access.
    pub async fn authenticate(mut self, mut mechanism: impl SaslMechanism) -> Result<()> {
        loop {
            let input = self.recv().await?;
            let (moved, reply) = mechanism.exchange(&input)?;
            match moved {
                Some(moved) => {
                    self.send(&SaslMessage::Continue(&reply)).await?;
                    mechanism = moved;
                }
                None => {
                    self.send(&SaslMessage::Final(&reply)).await?;
                    return Ok(());
                }
            }
        }
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
    Io(#[from] io::Error),
}

/// A convenient result type for SASL exchange.
pub type Result<T> = std::result::Result<T, SaslError>;

/// Every SASL mechanism (e.g. [SCRAM](crate::scram)) is expected to implement this trait.
pub trait SaslMechanism: Sized {
    /// Produce a server challenge to be sent to the client.
    /// This is how this method is called in PostgreSQL (libpq/sasl.h).
    fn exchange(self, input: &str) -> Result<(Option<Self>, String)>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sasl_first_message() {
        let proto = "SCRAM-SHA-256";
        let sasl = "n,,n=,r=KHQ2Gjc7NptyB8aov5/TnUy4";
        let sasl_len = (sasl.len() as u32).to_be_bytes();
        let bytes = [proto.as_bytes(), &[0], sasl_len.as_ref(), sasl.as_bytes()].concat();

        let password = SaslFirstMessage::parse(&bytes).unwrap();
        assert_eq!(password.method, proto);
        assert_eq!(password.message, sasl);
    }
}
