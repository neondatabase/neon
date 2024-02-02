//! Abstraction for the string-oriented SASL protocols.

use super::{messages::ServerMessage, Mechanism};
use crate::stream::PqStream;
use std::io;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;

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
        if let Some(first) = self.first.take() {
            return Ok(first);
        }

        self.current = self.stream.read_password_message().await?;
        let s = std::str::from_utf8(&self.current)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad encoding"))?;

        Ok(s)
    }
}

impl<S: AsyncWrite + Unpin> SaslStream<'_, S> {
    // Send a SASL message to the client.
    async fn send(&mut self, msg: &ServerMessage<&str>) -> io::Result<()> {
        self.stream.write_message(&msg.to_reply()).await?;
        Ok(())
    }
}

/// SASL authentication outcome.
/// It's much easier to match on those two variants
/// than to peek into a noisy protocol error type.
#[must_use = "caller must explicitly check for success"]
pub enum Outcome<R> {
    /// Authentication succeeded and produced some value.
    Success(R),
    /// Authentication failed (reason attached).
    Failure(&'static str),
}

impl<S: AsyncRead + AsyncWrite + Unpin> SaslStream<'_, S> {
    /// Perform SASL message exchange according to the underlying algorithm
    /// until user is either authenticated or denied access.
    pub async fn authenticate<M: Mechanism>(
        mut self,
        mut mechanism: M,
    ) -> super::Result<Outcome<M::Output>> {
        loop {
            let input = self.recv().await?;
            let step = mechanism.exchange(input).map_err(|error| {
                info!(?error, "error during SASL exchange");
                error
            })?;

            use super::Step;
            return Ok(match step {
                Step::Continue(moved_mechanism, reply) => {
                    self.send(&ServerMessage::Continue(&reply)).await?;
                    mechanism = moved_mechanism;
                    continue;
                }
                Step::Success(result, reply) => {
                    self.send(&ServerMessage::Final(&reply)).await?;
                    Outcome::Success(result)
                }
                Step::Failure(reason) => Outcome::Failure(reason),
            });
        }
    }
}
