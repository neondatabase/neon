//! Implementation of the SCRAM authentication algorithm.

use std::convert::Infallible;
use std::time::Instant;

use tracing::{debug, trace};
use x509_cert::der::zeroize::Zeroize;

use super::messages::{
    ClientFinalMessage, ClientFirstMessage, OwnedServerFirstMessage, SCRAM_RAW_NONCE_LEN,
};
use super::pbkdf2::Pbkdf2;
use super::secret::ServerSecret;
use super::signature::SignatureBuilder;
use super::threadpool::ThreadPool;
use super::{ScramKey, pbkdf2};
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
) -> pbkdf2::Block {
    pool.spawn_job(endpoint, Pbkdf2::start(password, salt, iterations))
        .await
}

/// For cleartext flow, we need to derive the client key to
/// 1. authenticate the client.
/// 2. authenticate with compute.
pub(crate) async fn exchange(
    pool: &ThreadPool,
    endpoint: EndpointIdInt,
    role: RoleNameInt,
    secret: &ServerSecret,
    password: &[u8],
) -> sasl::Result<sasl::Outcome<super::ScramKey>> {
    if secret.iterations > CACHED_ROUNDS {
        exchange_with_cache(pool, endpoint, role, secret, password).await
    } else {
        let salt = base64::decode(&secret.salt_base64)?;
        let hash = derive_client_key(pool, endpoint, password, &salt, secret.iterations).await;
        Ok(validate_pbkdf2(secret, &hash))
    }
}

/// Compute the client key using a cache. We cache the suffix of the pbkdf2 result only,
/// which is not enough by itself to perform an offline brute force.
async fn exchange_with_cache(
    pool: &ThreadPool,
    endpoint: EndpointIdInt,
    role: RoleNameInt,
    secret: &ServerSecret,
    password: &[u8],
) -> sasl::Result<sasl::Outcome<super::ScramKey>> {
    let salt = base64::decode(&secret.salt_base64)?;

    debug_assert!(
        secret.iterations > CACHED_ROUNDS,
        "we should not cache password data if there isn't enough rounds needed"
    );

    // compute the prefix of the pbkdf2 output.
    let prefix = derive_client_key(pool, endpoint, password, &salt, CACHED_ROUNDS).await;

    if let Some(cached) = pool.cache.get(&(endpoint, role)) {
        // hot path: let's check the threadpool cache
        if secret.cached_at == cached.cached_from {
            // cache is valid. compute the full hash by adding the prefix to the suffix.
            let mut hash = prefix;
            pbkdf2::xor(&mut hash, &cached.suffix);
            let outcome = validate_pbkdf2(secret, &hash);

            if matches!(outcome, sasl::Outcome::Success(_)) {
                trace!("password validated from cache");
            }

            return Ok(outcome);
        }

        // cached key is no longer valid.
        debug!("invalidating cached password");
        cached.invalidate();
    }

    // slow path: full password hash.
    let hash = derive_client_key(pool, endpoint, password, &salt, secret.iterations).await;
    let outcome = validate_pbkdf2(secret, &hash);

    let client_key = match outcome {
        sasl::Outcome::Success(client_key) => client_key,
        sasl::Outcome::Failure(_) => return Ok(outcome),
    };

    trace!("storing cached password");

    // time to cache, compute the suffix by subtracting the prefix from the hash.
    let mut suffix = hash;
    pbkdf2::xor(&mut suffix, &prefix);

    pool.cache.insert_unit(
        (endpoint, role),
        Pbkdf2CacheEntry {
            cached_from: secret.cached_at,
            suffix,
        },
    );

    Ok(sasl::Outcome::Success(client_key))
}

fn validate_pbkdf2(secret: &ServerSecret, hash: &pbkdf2::Block) -> sasl::Outcome<ScramKey> {
    let client_key = super::ScramKey::client_key(&(*hash).into());
    if secret.is_password_invalid(&client_key).into() {
        sasl::Outcome::Failure("password doesn't match")
    } else {
        sasl::Outcome::Success(client_key)
    }
}

const CACHED_ROUNDS: u32 = 16;

/// To speed up password hashing for more active customers, we store the tail results of the
/// PBKDF2 algorithm. If the output of PBKDF2 is U1 ^ U2 ^ ⋯ ^ Uc, then we store
/// suffix = U17 ^ U18 ^ ⋯ ^ Uc. We only need to calculate U1 ^ U2 ^ ⋯ ^ U15 ^ U16
/// to determine the final result.
///
/// The suffix alone isn't enough to crack the password. The stored_key is still required.
/// While both are cached in memory, given they're in different locations is makes it much
/// harder to exploit, even if any such memory exploit exists in proxy.
#[derive(Clone)]
pub struct Pbkdf2CacheEntry {
    /// corresponds to [`ServerSecret::cached_at`]
    cached_from: Instant,
    suffix: pbkdf2::Block,
}

impl Drop for Pbkdf2CacheEntry {
    fn drop(&mut self) {
        self.suffix.zeroize();
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
