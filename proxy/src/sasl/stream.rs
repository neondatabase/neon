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
    let sasl = {
        // pause the timer while we communicate with the client
        let _paused = ctx.latency_timer_pause(crate::metrics::Waiting::Client);

        // Initial client message contains the chosen auth method's name.
        let msg = stream.read_password_message().await?;
        super::FirstMessage::parse(msg).ok_or(super::Error::BadClientMessage("bad sasl message"))?
    };

    let mut mechanism = mechanism(sasl.method)?;
    let mut input = sasl.message;
    loop {
        let step = mechanism
            .exchange(input)
            .inspect_err(|error| tracing::info!(?error, "error during SASL exchange"))?;

        match step {
            Step::Continue(moved_mechanism, reply) => {
                mechanism = moved_mechanism;

                // pause the timer while we communicate with the client
                let _paused = ctx.latency_timer_pause(crate::metrics::Waiting::Client);

                // write reply
                let sasl_msg = BeAuthenticationSaslMessage::Continue(reply.as_bytes());
                stream.write_message(BeMessage::AuthenticationSasl(sasl_msg));

                // get next input
                stream.flush().await?;
                let msg = stream.read_password_message().await?;
                input = std::str::from_utf8(msg)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad encoding"))?;
            }
            Step::Success(result, reply) => {
                // pause the timer while we communicate with the client
                let _paused = ctx.latency_timer_pause(crate::metrics::Waiting::Client);

                // write reply
                let sasl_msg = BeAuthenticationSaslMessage::Final(reply.as_bytes());
                stream.write_message(BeMessage::AuthenticationSasl(sasl_msg));
                stream.write_message(BeMessage::AuthenticationOk);
                // exit with success
                break Ok(Outcome::Success(result));
            }
            // exit with failure
            Step::Failure(reason) => break Ok(Outcome::Failure(reason)),
        }
    }
}
