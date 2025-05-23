//! Implementation of the SCRAM authentication algorithm.

use std::convert::Infallible;

use hmac::Mac;
use rand::RngCore;
use tracing::{debug, trace};
use x509_cert::der::zeroize::Zeroize;

use super::ScramKey;
use super::messages::{
    ClientFinalMessage, ClientFirstMessage, OwnedServerFirstMessage, SCRAM_RAW_NONCE_LEN,
};
use super::pbkdf2::Pbkdf2;
use super::secret::ServerSecret;
use super::signature::SignatureBuilder;
use super::threadpool::ThreadPool;
use crate::intern::{EndpointIdInt, RoleNameInt};
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
pub(crate) struct Exchange<'a> {
    state: ExchangeState,
    secret: &'a ServerSecret,
    tls_server_end_point: crate::tls::TlsServerEndPoint,
}

impl<'a> Exchange<'a> {
    pub(crate) fn new(
        secret: &'a ServerSecret,
        nonce: fn() -> [u8; SCRAM_RAW_NONCE_LEN],
        tls_server_end_point: crate::tls::TlsServerEndPoint,
    ) -> Self {
        Self {
            state: ExchangeState::Initial(SaslInitial { nonce }),
            secret,
            tls_server_end_point,
        }
    }
}

async fn derive_client_key(
    pool: &ThreadPool,
    endpoint: EndpointIdInt,
    password: &[u8],
    salt: &[u8],
    iterations: u32,
) -> ScramKey {
    pool.spawn_job(endpoint, Pbkdf2::start(password, salt, iterations))
        .await
}

pub(crate) async fn exchange(
    pool: &ThreadPool,
    endpoint: EndpointIdInt,
    role: RoleNameInt,
    secret: &ServerSecret,
    password: &[u8],
) -> sasl::Result<sasl::Outcome<super::ScramKey>> {
    // hot path: let's check the threadpool cache
    if let Some(cached) = pool.cache.get(&(endpoint, role)) {
        // cached key is no longer valid.
        if secret.is_password_invalid(&cached.1).into() {
            debug!("invalidating cached password");
            cached.invalidate();
        } else if cached.0.verify(password) {
            trace!("password validated from cache");
            return Ok(sasl::Outcome::Success(cached.take_value().1.1));
        }
    }

    // slow path: full password verify.
    let salt = base64::decode(&secret.salt_base64)?;
    let client_key = derive_client_key(pool, endpoint, password, &salt, secret.iterations).await;

    if secret.is_password_invalid(&client_key).into() {
        Ok(sasl::Outcome::Failure("password doesn't match"))
    } else {
        trace!("storing cached password");
        pool.cache.insert_unit(
            (endpoint, role),
            (ClientSecretEntry::new(password), client_key.clone()),
        );

        Ok(sasl::Outcome::Success(client_key))
    }
}

#[derive(Clone)]
pub struct ClientSecretEntry {
    salt: [u8; 64],
    hash: [u8; 64],
}

impl Drop for ClientSecretEntry {
    fn drop(&mut self) {
        self.salt.zeroize();
        self.hash.zeroize();
    }
}

impl ClientSecretEntry {
    fn new(password: &[u8]) -> Self {
        let mut salt = [0; 64];
        rand::thread_rng().fill_bytes(&mut salt);

        let mut hmac = hmac::Hmac::<sha2::Sha512>::new_from_slice(password)
            .expect("HMAC is able to accept all key sizes");
        hmac.update(&salt);
        let hash = hmac.finalize().into_bytes().into();

        Self { salt, hash }
    }

    fn verify(&self, password: &[u8]) -> bool {
        let mut hmac = hmac::Hmac::<sha2::Sha512>::new_from_slice(password)
            .expect("HMAC is able to accept all key sizes");
        hmac.update(&self.salt);

        hmac.verify_slice(&self.hash).is_ok()
    }
}

impl SaslInitial {
    fn transition(
        &self,
        secret: &ServerSecret,
        tls_server_end_point: &crate::tls::TlsServerEndPoint,
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
        tls_server_end_point: &crate::tls::TlsServerEndPoint,
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
            crate::tls::TlsServerEndPoint::Sha256(x) => Ok(x),
            crate::tls::TlsServerEndPoint::Undefined => Err(SaslError::MissingBinding),
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
        if secret.is_password_invalid(&client_key).into() {
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
        use ExchangeState;
        use sasl::Step;
        match &self.state {
            ExchangeState::Initial(init) => {
                match init.transition(self.secret, &self.tls_server_end_point, input)? {
                    Step::Continue(sent, msg) => {
                        self.state = ExchangeState::SaltSent(sent);
                        Ok(Step::Continue(self, msg))
                    }
                    Step::Failure(msg) => Ok(Step::Failure(msg)),
                }
            }
            ExchangeState::SaltSent(sent) => {
                match sent.transition(self.secret, &self.tls_server_end_point, input)? {
                    Step::Success(keys, msg) => Ok(Step::Success(keys, msg)),
                    Step::Failure(msg) => Ok(Step::Failure(msg)),
                }
            }
        }
    }
}
