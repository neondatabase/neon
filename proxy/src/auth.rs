//! Authentication machinery.

use crate::sasl::{SaslFirstMessage, SaslMechanism, SaslMessage, SaslStream};
use crate::scram::key::ScramKey;
use crate::scram::{ScramExchangeServer, ScramSecret};
use anyhow::{bail, Context};
use zenith_utils::postgres_backend::{PostgresBackend, ProtoState};
use zenith_utils::pq_proto::{
    BeAuthenticationSaslMessage as BeSaslMessage, BeMessage as Be, FeMessage as Fe, *,
};

// TODO: add SCRAM-SHA-256-PLUS
/// A list of supported SCRAM methods.
const SCRAM_METHODS: &[&str] = &["SCRAM-SHA-256"];

/// Initial state of [`AuthStream`].
pub struct Begin;

/// Use [SCRAM](crate::scram)-based auth in [`AuthStream`].
pub struct Scram<'a>(pub &'a ScramSecret);

/// Use password-based auth in [`AuthStream`].
pub struct Md5(
    /// Salt for client.
    pub [u8; 4],
);

/// Every authentication selector is supposed to implement this trait.
pub trait AuthMethod {
    /// Any authentication selector should provide initial backend message
    /// containing auth method name and parameters, e.g. md5 salt.
    fn first_message(&self) -> BeMessage<'_>;
}

impl AuthMethod for Scram<'_> {
    #[inline(always)]
    fn first_message(&self) -> BeMessage<'_> {
        Be::AuthenticationSasl(BeSaslMessage::Methods(SCRAM_METHODS))
    }
}

impl AuthMethod for Md5 {
    #[inline(always)]
    fn first_message(&self) -> BeMessage<'_> {
        Be::AuthenticationMD5Password(self.0)
    }
}

/// This wrapper for [`PostgresBackend`] performs client authentication.
#[must_use]
pub struct AuthStream<'a, State> {
    /// The underlying stream which implements libpq's protocol.
    pgb: &'a mut PostgresBackend,
    /// State might contain ancillary data (see [`AuthStream::begin`]).
    state: State,
}

/// Initial state of the stream wrapper.
impl<'a> AuthStream<'a, Begin> {
    /// Create a new wrapper for client authentication.
    pub fn new(pgb: &'a mut PostgresBackend) -> Self {
        Self { pgb, state: Begin }
    }

    /// Move to the next step by sending auth method's name & params to client.
    pub fn begin<M: AuthMethod>(self, method: M) -> anyhow::Result<AuthStream<'a, M>> {
        self.pgb.write_message(&method.first_message())?;
        self.pgb.state = ProtoState::Authentication;

        Ok(AuthStream {
            pgb: self.pgb,
            state: method,
        })
    }
}

/// Stream wrapper for handling simple MD5 password auth.
impl AuthStream<'_, Md5> {
    /// Perform user authentication; Raise an error in case authentication failed.
    pub fn authenticate(mut self) -> anyhow::Result<()> {
        let msg = self.read_password_message()?;
        let (_trailing_null, _md5_response) = msg.split_last().context("bad password message")?;

        Ok(())
    }
}

/// Stream wrapper for handling [SCRAM](crate::scram) auth.
impl AuthStream<'_, Scram<'_>> {
    /// Perform user authentication; Raise an error in case authentication failed.
    pub fn authenticate(mut self) -> anyhow::Result<ScramKey> {
        // Initial client message contains the chosen auth method's name
        let msg = self.read_password_message()?;
        let sasl = SaslFirstMessage::parse(&msg).context("bad SASL message")?;

        // Currently, the only supported SASL method is SCRAM
        if !SCRAM_METHODS.contains(&sasl.method) {
            bail!("unsupported SASL method: {}", sasl.method);
        }

        let secret = self.state.0;
        let stream = (Some(msg.slice_ref(sasl.message)), &mut self);
        let client_key = ScramExchangeServer::new(secret).authenticate(stream)?;

        Ok(client_key)
    }
}

/// Only [`AuthMethod`] states should receive password messages.
impl<M: AuthMethod> AuthStream<'_, M> {
    /// Receive a new [`PasswordMessage`](FeMessage::PasswordMessage) and extract its payload.
    fn read_password_message(&mut self) -> anyhow::Result<bytes::Bytes> {
        match self.pgb.read_message()? {
            Some(Fe::PasswordMessage(msg)) => Ok(msg),
            None => bail!("connection is lost"),
            bad => bail!("unexpected message type: {:?}", bad),
        }
    }
}

/// Abstract away all intricacies of [`PostgresBackend`],
/// since [SASL](crate::sasl) protocols are text-based.
impl SaslStream for AuthStream<'_, Scram<'_>> {
    type In = bytes::Bytes;

    fn recv(&mut self) -> anyhow::Result<Self::In> {
        self.read_password_message()
    }

    fn send(&mut self, data: &SaslMessage<impl AsRef<[u8]>>) -> anyhow::Result<()> {
        let reply = match data {
            SaslMessage::Continue(reply) => BeSaslMessage::Continue(reply.as_ref()),
            SaslMessage::Final(reply) => BeSaslMessage::Final(reply.as_ref()),
        };

        self.pgb.write_message(&Be::AuthenticationSasl(reply))?;

        Ok(())
    }
}
