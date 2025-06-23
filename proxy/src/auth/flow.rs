//! Main authentication flow.

use std::sync::Arc;

use postgres_protocol::authentication::sasl::{SCRAM_SHA_256, SCRAM_SHA_256_PLUS};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;

use super::backend::ComputeCredentialKeys;
use super::{AuthError, PasswordHackPayload};
use crate::context::RequestContext;
use crate::control_plane::AuthSecret;
use crate::intern::EndpointIdInt;
use crate::pqproto::{BeAuthenticationSaslMessage, BeMessage};
use crate::sasl;
use crate::scram::threadpool::ThreadPool;
use crate::scram::{self};
use crate::stream::{PqStream, Stream};
use crate::tls::TlsServerEndPoint;

/// Use [SCRAM](crate::scram)-based auth in [`AuthFlow`].
pub(crate) struct Scram<'a>(
    pub(crate) &'a scram::ServerSecret,
    pub(crate) &'a RequestContext,
);

impl Scram<'_> {
    #[inline(always)]
    fn first_message(&self, channel_binding: bool) -> BeMessage<'_> {
        if channel_binding {
            BeMessage::AuthenticationSasl(BeAuthenticationSaslMessage::Methods(scram::METHODS))
        } else {
            BeMessage::AuthenticationSasl(BeAuthenticationSaslMessage::Methods(
                scram::METHODS_WITHOUT_PLUS,
            ))
        }
    }
}

/// Use an ad hoc auth flow (for clients which don't support SNI) proposed in
/// <https://github.com/neondatabase/cloud/issues/1620#issuecomment-1165332290>.
pub(crate) struct PasswordHack;

/// Use clear-text password auth called `password` in docs
/// <https://www.postgresql.org/docs/current/auth-password.html>
pub(crate) struct CleartextPassword {
    pub(crate) pool: Arc<ThreadPool>,
    pub(crate) endpoint: EndpointIdInt,
    pub(crate) secret: AuthSecret,
}

/// This wrapper for [`PqStream`] performs client authentication.
#[must_use]
pub(crate) struct AuthFlow<'a, S, State> {
    /// The underlying stream which implements libpq's protocol.
    stream: &'a mut PqStream<Stream<S>>,
    /// State might contain ancillary data.
    state: State,
    tls_server_end_point: TlsServerEndPoint,
}

/// Initial state of the stream wrapper.
impl<'a, S: AsyncRead + AsyncWrite + Unpin, M> AuthFlow<'a, S, M> {
    /// Create a new wrapper for client authentication.
    pub(crate) fn new(stream: &'a mut PqStream<Stream<S>>, method: M) -> Self {
        let tls_server_end_point = stream.get_ref().tls_server_end_point();

        Self {
            stream,
            state: method,
            tls_server_end_point,
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AuthFlow<'_, S, PasswordHack> {
    /// Perform user authentication. Raise an error in case authentication failed.
    pub(crate) async fn get_password(self) -> super::Result<PasswordHackPayload> {
        self.stream
            .write_message(BeMessage::AuthenticationCleartextPassword);
        self.stream.flush().await?;

        let msg = self.stream.read_password_message().await?;
        let password = msg
            .strip_suffix(&[0])
            .ok_or(AuthError::MalformedPassword("missing terminator"))?;

        let payload = PasswordHackPayload::parse(password)
            // If we ended up here and the payload is malformed, it means that
            // the user neither enabled SNI nor resorted to any other method
            // for passing the project name we rely on. We should show them
            // the most helpful error message and point to the documentation.
            .ok_or(AuthError::MissingEndpointName)?;

        Ok(payload)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AuthFlow<'_, S, CleartextPassword> {
    /// Perform user authentication. Raise an error in case authentication failed.
    pub(crate) async fn authenticate(self) -> super::Result<sasl::Outcome<ComputeCredentialKeys>> {
        self.stream
            .write_message(BeMessage::AuthenticationCleartextPassword);
        self.stream.flush().await?;

        let msg = self.stream.read_password_message().await?;
        let password = msg
            .strip_suffix(&[0])
            .ok_or(AuthError::MalformedPassword("missing terminator"))?;

        let outcome = validate_password_and_exchange(
            &self.state.pool,
            self.state.endpoint,
            password,
            self.state.secret,
        )
        .await?;

        if let sasl::Outcome::Success(_) = &outcome {
            self.stream.write_message(BeMessage::AuthenticationOk);
        }

        Ok(outcome)
    }
}

/// Stream wrapper for handling [SCRAM](crate::scram) auth.
impl<S: AsyncRead + AsyncWrite + Unpin> AuthFlow<'_, S, Scram<'_>> {
    /// Perform user authentication. Raise an error in case authentication failed.
    pub(crate) async fn authenticate(self) -> super::Result<sasl::Outcome<scram::ScramKey>> {
        let Scram(secret, ctx) = self.state;
        let channel_binding = self.tls_server_end_point;

        // send sasl message.
        {
            // pause the timer while we communicate with the client
            let _paused = ctx.latency_timer_pause(crate::metrics::Waiting::Client);

            let sasl = self.state.first_message(channel_binding.supported());
            self.stream.write_message(sasl);
            self.stream.flush().await?;
        }

        // complete sasl handshake.
        sasl::authenticate(ctx, self.stream, |method| {
            // Currently, the only supported SASL method is SCRAM.
            match method {
                SCRAM_SHA_256 => ctx.set_auth_method(crate::context::AuthMethod::ScramSha256),
                SCRAM_SHA_256_PLUS => {
                    ctx.set_auth_method(crate::context::AuthMethod::ScramSha256Plus);
                }
                method => return Err(sasl::Error::BadAuthMethod(method.into())),
            }

            // TODO: make this a metric instead
            info!("client chooses {}", method);

            Ok(scram::Exchange::new(secret, rand::random, channel_binding))
        })
        .await
        .map_err(AuthError::Sasl)
    }
}

pub(crate) async fn validate_password_and_exchange(
    pool: &ThreadPool,
    endpoint: EndpointIdInt,
    password: &[u8],
    secret: AuthSecret,
) -> super::Result<sasl::Outcome<ComputeCredentialKeys>> {
    match secret {
        // perform scram authentication as both client and server to validate the keys
        AuthSecret::Scram(scram_secret) => {
            let outcome = crate::scram::exchange(pool, endpoint, &scram_secret, password).await?;

            let client_key = match outcome {
                sasl::Outcome::Success(client_key) => client_key,
                sasl::Outcome::Failure(reason) => return Ok(sasl::Outcome::Failure(reason)),
            };

            let keys = crate::compute::ScramKeys {
                client_key: client_key.as_bytes(),
                server_key: scram_secret.server_key.as_bytes(),
            };

            Ok(sasl::Outcome::Success(ComputeCredentialKeys::AuthKeys(
                postgres_client::config::AuthKeys::ScramSha256(keys),
            )))
        }
    }
}
