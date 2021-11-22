//! Salted Challenge Response Authentication Mechanism.
//!
//! RFC: <https://datatracker.ietf.org/doc/html/rfc5802>.
//!
//! Reference implementation:
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/backend/libpq/auth-scram.c>
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/interfaces/libpq/fe-auth-scram.c>

mod channel_binding;
mod key;
mod messages;
mod secret;
mod signature;

pub use channel_binding::*;
pub use secret::*;

use crate::sasl::{self, SaslError, SaslMechanism};
use messages::{ClientFinalMessage, ClientFirstMessage, OwnedServerFirstMessage};
use signature::SignatureBuilder;

pub use self::secret::ScramSecret;

/// Decode base64 into array without any heap allocations
fn base64_decode_array<const N: usize>(input: impl AsRef<[u8]>) -> Option<[u8; N]> {
    let mut bytes = [0u8; N];

    let size = base64::decode_config_slice(input, base64::STANDARD, &mut bytes).ok()?;
    if size != N {
        return None;
    }

    Some(bytes)
}

#[derive(Debug)]
enum ScramExchangeServerState {
    /// Waiting for [`ClientFirstMessage`].
    Initial,
    /// Waiting for [`ClientFinalMessage`].
    SaltSent {
        cbind_flag: ChannelBinding<String>,
        client_first_message_bare: String,
        server_first_message: OwnedServerFirstMessage,
    },
}

/// Server's side of SCRAM auth algorithm.
#[derive(Debug)]
pub struct ScramExchangeServer<'a> {
    state: ScramExchangeServerState,
    secret: &'a ScramSecret,
}

impl<'a> ScramExchangeServer<'a> {
    pub fn new(secret: &'a ScramSecret) -> Self {
        Self {
            state: ScramExchangeServerState::Initial,
            secret,
        }
    }
}

impl SaslMechanism for ScramExchangeServer<'_> {
    fn exchange(mut self, input: &str) -> sasl::Result<(Option<Self>, String)> {
        use ScramExchangeServerState::*;
        match &self.state {
            Initial => {
                let client_first_message =
                    ClientFirstMessage::parse(input).ok_or(SaslError::BadClientMessage)?;

                let server_first_message = client_first_message.build_server_first_message(
                    // TODO: use secure random
                    &rand::random(),
                    &self.secret.salt_base64,
                    self.secret.iterations,
                );
                let msg = server_first_message.as_str().to_owned();

                self.state = SaltSent {
                    cbind_flag: client_first_message.cbind_flag.map(str::to_owned),
                    client_first_message_bare: client_first_message.bare.to_owned(),
                    server_first_message,
                };

                Ok((Some(self), msg))
            }
            SaltSent {
                cbind_flag,
                client_first_message_bare,
                server_first_message,
            } => {
                let client_final_message =
                    ClientFinalMessage::parse(input).ok_or(SaslError::BadClientMessage)?;

                let channel_binding = cbind_flag.encode(|_| {
                    // TODO: make global design decision regarding the certificate
                    todo!("fetch TLS certificate data")
                });

                // This might've been caused by a MITM attack
                if client_final_message.channel_binding != channel_binding {
                    return Err(SaslError::AuthenticationFailed("channel binding failed"));
                }

                if client_final_message.nonce != server_first_message.nonce() {
                    return Err(SaslError::AuthenticationFailed("bad nonce"));
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
                    return Err(SaslError::AuthenticationFailed("keys don't match"));
                }

                let msg = client_final_message
                    .build_server_final_message(signature_builder, &self.secret.server_key);

                Ok((None, msg))
            }
        }
    }
}
