//! Implementation of the SCRAM authentication algorithm.

use std::convert::Infallible;

use postgres_protocol::authentication::sasl::ScramSha256;

use super::messages::{
    ClientFinalMessage, ClientFirstMessage, OwnedServerFirstMessage, SCRAM_RAW_NONCE_LEN,
};
use super::secret::ServerSecret;
use super::signature::SignatureBuilder;
use crate::config;
use crate::sasl::{self, ChannelBinding, Error as SaslError};

/// The only channel binding mode we currently support.
#[derive(Debug)]
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

struct SaslSentInner {
    cbind_flag: ChannelBinding<TlsServerEndPoint>,
    client_first_message_bare: String,
    server_first_message: OwnedServerFirstMessage,
}

struct SaslInitial {
    nonce: fn() -> [u8; SCRAM_RAW_NONCE_LEN],
}

enum ExchangeState {
    /// Waiting for [`ClientFirstMessage`].
    Initial(SaslInitial),
    /// Waiting for [`ClientFinalMessage`].
    SaltSent(SaslSentInner),
}

/// Server's side of SCRAM auth algorithm.
pub struct Exchange<'a> {
    state: ExchangeState,
    secret: &'a ServerSecret,
    tls_server_end_point: config::TlsServerEndPoint,
}

impl<'a> Exchange<'a> {
    pub fn new(
        secret: &'a ServerSecret,
        nonce: fn() -> [u8; SCRAM_RAW_NONCE_LEN],
        tls_server_end_point: config::TlsServerEndPoint,
    ) -> Self {
        Self {
            state: ExchangeState::Initial(SaslInitial { nonce }),
            secret,
            tls_server_end_point,
        }
    }
}

pub async fn exchange(
    secret: &ServerSecret,
    mut client: ScramSha256,
    tls_server_end_point: config::TlsServerEndPoint,
) -> sasl::Result<sasl::Outcome<super::ScramKey>> {
    use sasl::Step::*;

    let init = SaslInitial {
        nonce: rand::random,
    };

    let client_first = std::str::from_utf8(client.message())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let sent = match init.transition(secret, &tls_server_end_point, client_first)? {
        Continue(sent, server_first) => {
            // `client.update` might perform `pbkdf2(pw)`, best to spawn it in a blocking thread.
            // TODO(conrad): take this code from tokio-postgres and make an async-aware pbkdf2 impl
            client = tokio::task::spawn_blocking(move || {
                client.update(server_first.as_bytes())?;
                Ok::<ScramSha256, std::io::Error>(client)
            })
            .await
            .expect("should not panic while performing password hash")?;
            sent
        }
        Success(x, _) => match x {},
        Failure(msg) => return Ok(sasl::Outcome::Failure(msg)),
    };

    let client_final = std::str::from_utf8(client.message())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let keys = match sent.transition(secret, &tls_server_end_point, client_final)? {
        Success(keys, server_final) => {
            client.finish(server_final.as_bytes())?;
            keys
        }
        Continue(x, _) => match x {},
        Failure(msg) => return Ok(sasl::Outcome::Failure(msg)),
    };

    Ok(sasl::Outcome::Success(keys))
}

impl SaslInitial {
    fn transition(
        &self,
        secret: &ServerSecret,
        tls_server_end_point: &config::TlsServerEndPoint,
        input: &str,
    ) -> sasl::Result<sasl::Step<SaslSentInner, Infallible>> {
        let client_first_message = ClientFirstMessage::parse(input)
            .ok_or(SaslError::BadClientMessage("invalid client-first-message"))?;

        // If the flag is set to "y" and the server supports channel
        // binding, the server MUST fail authentication
        if client_first_message.cbind_flag == ChannelBinding::NotSupportedServer
            && tls_server_end_point.supported()
        {
            return Err(SaslError::ChannelBindingFailed("SCRAM-PLUS not used"));
        }

        let server_first_message = client_first_message.build_server_first_message(
            &(self.nonce)(),
            &secret.salt_base64,
            secret.iterations,
        );
        let msg = server_first_message.as_str().to_owned();

        let next = SaslSentInner {
            cbind_flag: client_first_message.cbind_flag.and_then(str::parse)?,
            client_first_message_bare: client_first_message.bare.to_owned(),
            server_first_message,
        };

        Ok(sasl::Step::Continue(next, msg))
    }
}

impl SaslSentInner {
    fn transition(
        &self,
        secret: &ServerSecret,
        tls_server_end_point: &config::TlsServerEndPoint,
        input: &str,
    ) -> sasl::Result<sasl::Step<Infallible, super::ScramKey>> {
        let Self {
            cbind_flag,
            client_first_message_bare,
            server_first_message,
        } = self;

        let client_final_message = ClientFinalMessage::parse(input)
            .ok_or(SaslError::BadClientMessage("invalid client-final-message"))?;

        let channel_binding = cbind_flag.encode(|_| match tls_server_end_point {
            config::TlsServerEndPoint::Sha256(x) => Ok(x),
            config::TlsServerEndPoint::Undefined => Err(SaslError::MissingBinding),
        })?;

        // This might've been caused by a MITM attack
        if client_final_message.channel_binding != channel_binding {
            return Err(SaslError::ChannelBindingFailed(
                "insecure connection: secure channel data mismatch",
            ));
        }

        if client_final_message.nonce != server_first_message.nonce() {
            return Err(SaslError::BadClientMessage("combined nonce doesn't match"));
        }

        let signature_builder = SignatureBuilder {
            client_first_message_bare,
            server_first_message: server_first_message.as_str(),
            client_final_message_without_proof: client_final_message.without_proof,
        };

        let client_key = signature_builder
            .build(&secret.stored_key)
            .derive_client_key(&client_final_message.proof);

        // Auth fails either if keys don't match or it's pre-determined to fail.
        if client_key.sha256() != secret.stored_key || secret.doomed {
            return Ok(sasl::Step::Failure("password doesn't match"));
        }

        let msg =
            client_final_message.build_server_final_message(signature_builder, &secret.server_key);

        Ok(sasl::Step::Success(client_key, msg))
    }
}

impl sasl::Mechanism for Exchange<'_> {
    type Output = super::ScramKey;

    fn exchange(mut self, input: &str) -> sasl::Result<sasl::Step<Self, Self::Output>> {
        use {sasl::Step::*, ExchangeState::*};
        match &self.state {
            Initial(init) => {
                match init.transition(self.secret, &self.tls_server_end_point, input)? {
                    Continue(sent, msg) => {
                        self.state = SaltSent(sent);
                        Ok(Continue(self, msg))
                    }
                    Success(x, _) => match x {},
                    Failure(msg) => Ok(Failure(msg)),
                }
            }
            SaltSent(sent) => {
                match sent.transition(self.secret, &self.tls_server_end_point, input)? {
                    Success(keys, msg) => Ok(Success(keys, msg)),
                    Continue(x, _) => match x {},
                    Failure(msg) => Ok(Failure(msg)),
                }
            }
        }
    }
}
