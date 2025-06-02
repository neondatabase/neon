//! Abstraction for the string-oriented SASL protocols.

use std::io;

use tokio::io::{AsyncRead, AsyncWrite};

use super::{Mechanism, Step};
use crate::context::RequestContext;
use crate::pqproto::{BeAuthenticationSaslMessage, BeMessage};
use crate::stream::PqStream;

/// SASL authentication outcome.
/// It's much easier to match on those two variants
/// than to peek into a noisy protocol error type.
#[must_use = "caller must explicitly check for success"]
pub(crate) enum Outcome<R> {
    /// Authentication succeeded and produced some value.
    Success(R),
    /// Authentication failed (reason attached).
    Failure(&'static str),
}

pub async fn authenticate<S, F, M>(
    ctx: &RequestContext,
    stream: &mut PqStream<S>,
    mechanism: F,
) -> super::Result<Outcome<M::Output>>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnOnce(&str) -> super::Result<M>,
    M: Mechanism,
{
    let (mut mechanism, mut input) = {
        // pause the timer while we communicate with the client
        let _paused = ctx.latency_timer_pause(crate::metrics::Waiting::Client);

        // Initial client message contains the chosen auth method's name.
        let msg = stream.read_password_message().await?;

        let sasl = super::FirstMessage::parse(msg)
            .ok_or(super::Error::BadClientMessage("bad sasl message"))?;

        (mechanism(sasl.method)?, sasl.message)
    };

    loop {
        match mechanism.exchange(input) {
            Ok(Step::Continue(moved_mechanism, reply)) => {
                mechanism = moved_mechanism;

                // write reply
                let sasl_msg = BeAuthenticationSaslMessage::Continue(reply.as_bytes());
                stream.write_message(BeMessage::AuthenticationSasl(sasl_msg));
                drop(reply);
            }
            Ok(Step::Success(result, reply)) => {
                // write reply
                let sasl_msg = BeAuthenticationSaslMessage::Final(reply.as_bytes());
                stream.write_message(BeMessage::AuthenticationSasl(sasl_msg));
                stream.write_message(BeMessage::AuthenticationOk);

                // exit with success
                break Ok(Outcome::Success(result));
            }
            // exit with failure
            Ok(Step::Failure(reason)) => break Ok(Outcome::Failure(reason)),
            Err(error) => {
                tracing::info!(?error, "error during SASL exchange");
                return Err(error);
            }
        }

        // pause the timer while we communicate with the client
        let _paused = ctx.latency_timer_pause(crate::metrics::Waiting::Client);

        // get next input
        stream.flush().await?;
        let msg = stream.read_password_message().await?;
        input = std::str::from_utf8(msg)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad encoding"))?;
    }
}
