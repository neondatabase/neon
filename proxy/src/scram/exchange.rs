//! Implementation of the SCRAM authentication algorithm.

use super::messages::{
    ClientFinalMessage, ClientFirstMessage, OwnedServerFirstMessage, SCRAM_RAW_NONCE_LEN,
};
use super::secret::ServerSecret;
use super::signature::SignatureBuilder;
use crate::sasl::{self, ChannelBinding, Error as SaslError};

/// The only channel binding mode we currently support.
struct TlsServerEndPoint;

impl std::fmt::Display for TlsServerEndPoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tls-server-end-point")
    }
}

impl std::str::FromStr for TlsServerEndPoint {
    type Err = sasl::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "tls-server-end-point" => Ok(TlsServerEndPoint),
            _ => Err(sasl::Error::ChannelBindingBadMethod(s.into())),
        }
    }
}

enum ExchangeState {
    /// Waiting for [`ClientFirstMessage`].
    Initial,
    /// Waiting for [`ClientFinalMessage`].
    SaltSent {
        cbind_flag: ChannelBinding<TlsServerEndPoint>,
        client_first_message_bare: String,
        server_first_message: OwnedServerFirstMessage,
    },
}

/// Server's side of SCRAM auth algorithm.
pub struct Exchange<'a> {
    state: ExchangeState,
    secret: &'a ServerSecret,
    nonce: fn() -> [u8; SCRAM_RAW_NONCE_LEN],
    cert_digest: Option<&'a [u8]>,
}

impl<'a> Exchange<'a> {
    pub fn new(
        secret: &'a ServerSecret,
        nonce: fn() -> [u8; SCRAM_RAW_NONCE_LEN],
        cert_digest: Option<&'a [u8]>,
    ) -> Self {
        Self {
            state: ExchangeState::Initial,
            secret,
            nonce,
            cert_digest,
        }
    }
}

impl sasl::Mechanism for Exchange<'_> {
    type Output = super::ScramKey;

    fn exchange(mut self, input: &str) -> sasl::Result<(sasl::Step<Self, Self::Output>, String)> {
        use {sasl::Step::*, ExchangeState::*};
        match &self.state {
            Initial => {
                let client_first_message =
                    ClientFirstMessage::parse(input).ok_or(SaslError::BadClientMessage)?;

                let server_first_message = client_first_message.build_server_first_message(
                    &(self.nonce)(),
                    &self.secret.salt_base64,
                    self.secret.iterations,
                );
                let msg = server_first_message.as_str().to_owned();

                self.state = SaltSent {
                    cbind_flag: client_first_message.cbind_flag.and_then(str::parse)?,
                    client_first_message_bare: client_first_message.bare.to_owned(),
                    server_first_message,
                };

                Ok((Continue(self), msg))
            }
            SaltSent {
                cbind_flag,
                client_first_message_bare,
                server_first_message,
            } => {
                let client_final_message =
                    ClientFinalMessage::parse(input).ok_or(SaslError::BadClientMessage)?;

                let channel_binding = cbind_flag.encode(|_| {
                    self.cert_digest
                        .map(base64::encode)
                        .ok_or(SaslError::ChannelBindingFailed("no cert digest provided"))
                })?;

                // This might've been caused by a MITM attack
                if client_final_message.channel_binding != channel_binding {
                    return Err(SaslError::ChannelBindingFailed("data mismatch"));
                }

                if client_final_message.nonce != server_first_message.nonce() {
                    return Err(SaslError::AuthenticationFailed(
                        "combined nonce doesn't match",
                    ));
                }

                let signature_builder = SignatureBuilder {
                    client_first_message_bare,
                    server_first_message: server_first_message.as_str(),
                    client_final_message_without_proof: client_final_message.without_proof,
                };

                let client_key = signature_builder
                    .build(&self.secret.stored_key)
                    .derive_client_key(&client_final_message.proof);

                if client_key.sha256() != self.secret.stored_key {
                    return Err(SaslError::AuthenticationFailed("password doesn't match"));
                }

                let msg = client_final_message
                    .build_server_final_message(signature_builder, &self.secret.server_key);

                Ok((Authenticated(client_key), msg))
            }
        }
    }
}
