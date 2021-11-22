use super::{AuthError, AuthErrorImpl};
use crate::stream::PqStream;
use crate::{sasl, scram};
use std::io;
use tokio::io::{AsyncRead, AsyncWrite};
use zenith_utils::pq_proto::{BeAuthenticationSaslMessage, BeMessage, BeMessage as Be};

// TODO: add SCRAM-SHA-256-PLUS
/// A list of supported SCRAM methods.
const SCRAM_METHODS: &[&str] = &["SCRAM-SHA-256"];

/// Every authentication selector is supposed to implement this trait.
pub trait AuthMethod {
    /// Any authentication selector should provide initial backend message
    /// containing auth method name and parameters, e.g. md5 salt.
    fn first_message(&self) -> BeMessage<'_>;
}

/// Initial state of [`AuthStream`].
pub struct Begin;

/// Use [SCRAM](crate::scram)-based auth in [`AuthStream`].
pub struct Scram<'a>(pub &'a scram::ServerSecret);

impl AuthMethod for Scram<'_> {
    #[inline(always)]
    fn first_message(&self) -> BeMessage<'_> {
        Be::AuthenticationSasl(BeAuthenticationSaslMessage::Methods(SCRAM_METHODS))
    }
}

/// Use password-based auth in [`AuthStream`].
pub struct Md5(
    /// Salt for client.
    pub [u8; 4],
);

impl AuthMethod for Md5 {
    #[inline(always)]
    fn first_message(&self) -> BeMessage<'_> {
        Be::AuthenticationMD5Password(self.0)
    }
}

/// This wrapper for [`PqStream`] performs client authentication.
#[must_use]
pub struct AuthStream<'a, Stream, State> {
    /// The underlying stream which implements libpq's protocol.
    stream: &'a mut PqStream<Stream>,
    /// State might contain ancillary data (see [`AuthStream::begin`]).
    state: State,
}

/// Initial state of the stream wrapper.
impl<'a, S: AsyncWrite + Unpin> AuthStream<'a, S, Begin> {
    /// Create a new wrapper for client authentication.
    pub fn new(stream: &'a mut PqStream<S>) -> Self {
        Self {
            stream,
            state: Begin,
        }
    }

    /// Move to the next step by sending auth method's name & params to client.
    pub async fn begin<M: AuthMethod>(self, method: M) -> io::Result<AuthStream<'a, S, M>> {
        self.stream.write_message(&method.first_message()).await?;

        Ok(AuthStream {
            stream: self.stream,
            state: method,
        })
    }
}

/// Stream wrapper for handling simple MD5 password auth.
impl<S: AsyncRead + AsyncWrite + Unpin> AuthStream<'_, S, Md5> {
    /// Perform user authentication. Raise an error in case authentication failed.
    pub async fn authenticate(self) -> Result<(), AuthError> {
        unimplemented!("mew MD5 auth flow is yet to be implemented");
    }
}

/// Stream wrapper for handling [SCRAM](crate::scram) auth.
impl<S: AsyncRead + AsyncWrite + Unpin> AuthStream<'_, S, Scram<'_>> {
    /// Perform user authentication. Raise an error in case authentication failed.
    pub async fn authenticate(self) -> Result<(), AuthError> {
        // Initial client message contains the chosen auth method's name.
        let msg = self.stream.read_password_message().await?;
        let sasl = sasl::SaslFirstMessage::parse(&msg).ok_or(AuthErrorImpl::MalformedPassword)?;

        // Currently, the only supported SASL method is SCRAM.
        if !SCRAM_METHODS.contains(&sasl.method) {
            return Err(AuthErrorImpl::auth_failed("method not supported").into());
        }

        let secret = self.state.0;
        sasl::SaslStream::new(self.stream, sasl.message)
            .authenticate(scram::Exchange::new(secret))
            .await?;

        Ok(())
    }
}
