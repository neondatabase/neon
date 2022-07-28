//! Main authentication flow.

use super::{AuthErrorImpl, PasswordHackPayload};
use crate::{sasl, scram, stream::PqStream};
use std::io;
use tokio::io::{AsyncRead, AsyncWrite};
use utils::pq_proto::{BeAuthenticationSaslMessage, BeMessage, BeMessage as Be};

/// Every authentication selector is supposed to implement this trait.
pub trait AuthMethod {
    /// Any authentication selector should provide initial backend message
    /// containing auth method name and parameters, e.g. md5 salt.
    fn first_message(&self) -> BeMessage<'_>;
}

/// Initial state of [`AuthFlow`].
pub struct Begin;

/// Use [SCRAM](crate::scram)-based auth in [`AuthFlow`].
pub struct Scram<'a>(pub &'a scram::ServerSecret);

impl AuthMethod for Scram<'_> {
    #[inline(always)]
    fn first_message(&self) -> BeMessage<'_> {
        Be::AuthenticationSasl(BeAuthenticationSaslMessage::Methods(scram::METHODS))
    }
}

/// Use an ad hoc auth flow (for clients which don't support SNI) proposed in
/// <https://github.com/neondatabase/cloud/issues/1620#issuecomment-1165332290>.
pub struct PasswordHack;

impl AuthMethod for PasswordHack {
    #[inline(always)]
    fn first_message(&self) -> BeMessage<'_> {
        Be::AuthenticationCleartextPassword
    }
}

/// This wrapper for [`PqStream`] performs client authentication.
#[must_use]
pub struct AuthFlow<'a, Stream, State> {
    /// The underlying stream which implements libpq's protocol.
    stream: &'a mut PqStream<Stream>,
    /// State might contain ancillary data (see [`Self::begin`]).
    state: State,
}

/// Initial state of the stream wrapper.
impl<'a, S: AsyncWrite + Unpin> AuthFlow<'a, S, Begin> {
    /// Create a new wrapper for client authentication.
    pub fn new(stream: &'a mut PqStream<S>) -> Self {
        Self {
            stream,
            state: Begin,
        }
    }

    /// Move to the next step by sending auth method's name & params to client.
    pub async fn begin<M: AuthMethod>(self, method: M) -> io::Result<AuthFlow<'a, S, M>> {
        self.stream.write_message(&method.first_message()).await?;

        Ok(AuthFlow {
            stream: self.stream,
            state: method,
        })
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AuthFlow<'_, S, PasswordHack> {
    /// Perform user authentication. Raise an error in case authentication failed.
    pub async fn authenticate(self) -> super::Result<PasswordHackPayload> {
        let msg = self.stream.read_password_message().await?;
        let password = msg
            .strip_suffix(&[0])
            .ok_or(AuthErrorImpl::MalformedPassword("missing terminator"))?;

        // The so-called "password" should contain a base64-encoded json.
        // We will use it later to route the client to their project.
        let bytes = base64::decode(password)
            .map_err(|_| AuthErrorImpl::MalformedPassword("bad encoding"))?;

        let payload = serde_json::from_slice(&bytes)
            .map_err(|_| AuthErrorImpl::MalformedPassword("invalid payload"))?;

        Ok(payload)
    }
}

/// Stream wrapper for handling [SCRAM](crate::scram) auth.
impl<S: AsyncRead + AsyncWrite + Unpin> AuthFlow<'_, S, Scram<'_>> {
    /// Perform user authentication. Raise an error in case authentication failed.
    pub async fn authenticate(self) -> super::Result<scram::ScramKey> {
        // Initial client message contains the chosen auth method's name.
        let msg = self.stream.read_password_message().await?;
        let sasl = sasl::FirstMessage::parse(&msg)
            .ok_or(AuthErrorImpl::MalformedPassword("bad sasl message"))?;

        // Currently, the only supported SASL method is SCRAM.
        if !scram::METHODS.contains(&sasl.method) {
            return Err(AuthErrorImpl::auth_failed("method not supported").into());
        }

        let secret = self.state.0;
        let key = sasl::SaslStream::new(self.stream, sasl.message)
            .authenticate(scram::Exchange::new(secret, rand::random, None))
            .await?;

        Ok(key)
    }
}
