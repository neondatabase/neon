use super::channel_binding::ChannelBinding;
use super::messages::{ClientFinalMessage, ClientFirstMessage, OwnedServerFirstMessage};
use super::secret::ServerSecret;
use super::signature::SignatureBuilder;
use crate::sasl::{self, SaslError, SaslMechanism};

#[derive(Debug)]
enum ExchangeState {
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
pub struct Exchange<'a> {
    state: ExchangeState,
    secret: &'a ServerSecret,
}

impl<'a> Exchange<'a> {
    pub fn new(secret: &'a ServerSecret) -> Self {
        Self {
            state: ExchangeState::Initial,
            secret,
        }
    }
}

impl SaslMechanism for Exchange<'_> {
    fn exchange(mut self, input: &str) -> sasl::Result<(Option<Self>, String)> {
        use ExchangeState::*;
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
