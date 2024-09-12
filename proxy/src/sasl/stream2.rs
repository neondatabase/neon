//! Abstraction for the string-oriented SASL protocols.

use crate::{
    auth_proxy::AuthProxyStream,
    sasl::{messages::ServerMessage, Mechanism},
    stream::AuthProxyStreamExt,
};
use std::io;
use tracing::info;

use super::Outcome;

/// Abstracts away all peculiarities of the libpq's protocol.
pub(crate) struct SaslStream2<'a> {
    /// The underlying stream.
    stream: &'a mut AuthProxyStream,
    /// Current password message we received from client.
    current: bytes::Bytes,
    /// First SASL message produced by client.
    first: Option<&'a str>,
}

impl<'a> SaslStream2<'a> {
    pub(crate) fn new(stream: &'a mut AuthProxyStream, first: &'a str) -> Self {
        Self {
            stream,
            current: bytes::Bytes::new(),
            first: Some(first),
        }
    }
}

impl SaslStream2<'_> {
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

impl SaslStream2<'_> {
    // Send a SASL message to the client.
    async fn send(&mut self, msg: &ServerMessage<&str>) -> io::Result<()> {
        self.stream.write_message(&msg.to_reply()).await?;
        Ok(())
    }
}

impl SaslStream2<'_> {
    /// Perform SASL message exchange according to the underlying algorithm
    /// until user is either authenticated or denied access.
    pub(crate) async fn authenticate<M: Mechanism>(
        mut self,
        mut mechanism: M,
    ) -> crate::sasl::Result<Outcome<M::Output>> {
        loop {
            let input = self.recv().await?;
            let step = mechanism.exchange(input).map_err(|error| {
                info!(?error, "error during SASL exchange");
                error
            })?;

            use crate::sasl::Step;
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
