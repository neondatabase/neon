use std::{sync::Arc, time::Duration};

use anyhow::{bail, ensure, Context};
use arc_swap::{access::Access, ArcSwap, Guard};
use dashmap::{mapref::entry::Entry, DashMap};
use hmac::digest::generic_array::GenericArray;
// use jose_jwa::S;
use jose_jwk::crypto::KeyInfo;
use tokio::time::Instant;

use crate::intern::EndpointIdInt;

#[derive(Default)]
pub struct JWKCache {
    client: reqwest::Client,

    map: DashMap<EndpointIdInt, Arc<JWKCacheEntryLock>>,
}

pub struct JWKCacheEntryLock {
    cached: ArcSwap<JWKCacheEntry>,
    lookup: tokio::sync::Mutex<()>,
}

pub struct JWKCacheEntry {
    /// Should refetch at least every hour to verify when old keys have been removed.
    /// Should refetch when new key IDs are seen only every 5 minutes or so
    last_retrieved: Instant,

    // /// jwks urls
    // urls: Vec<url::Url>,
    /// cplane will return multiple JWKs urls that we need to scrape.
    key_sets: Vec<jose_jwk::JwkSet>,
}

impl JWKCacheEntryLock {
    async fn renew_jwks(
        &self,
        client: &reqwest::Client,
        endpoint: EndpointIdInt,
    ) -> Arc<JWKCacheEntry> {
        let _guard = self.lookup.lock().await;

        // double check that no one beat us to updating the cache.
        let now = Instant::now();
        let guard = self.cached.load_full();
        let last_update = now.duration_since(guard.last_retrieved);
        if last_update < Duration::from_secs(300) {
            return guard;
        }

        todo!("refetch jwks from cplane");

        // fetch from cplane
        // fetch jwks
        // entry.last_retrieved = now;

        let x = Arc::new(JWKCacheEntry {
            last_retrieved: now,
            key_sets: vec![],
        });
        self.cached.swap(x.clone());
        x
    }
}

const MIN_RENEW: Duration = Duration::from_secs(300);
const MAX_RENEW: Duration = Duration::from_secs(3600);

impl JWKCache {
    pub async fn check_jwt(
        &self,
        endpoint: EndpointIdInt,
        jwt: String,
    ) -> Result<(), anyhow::Error> {
        // JWT compact form is defined to be
        // <B64(Header)> || . || <B64(Payload)> || . || <B64(Signature)>
        // where Signature = alg(<B64(Header)> || . || <B64(Payload)>);

        let (header_payload, signature) = jwt
            .rsplit_once(".")
            .context("not a valid compact JWT encoding")?;
        let (header, _payload) = header_payload
            .split_once(".")
            .context("not a valid compact JWT encoding")?;

        let header = base64::decode_config(header, base64::URL_SAFE_NO_PAD)
            .context("not a valid compact JWT encoding")?;
        let header = serde_json::from_slice::<JWTHeader>(&header)
            .context("not a valid compact JWT encoding")?;

        ensure!(header.typ == "JWT");
        let kid = header.kid.context("missing key id")?;

        let entry = match self.map.get(&endpoint) {
            Some(entry) => Arc::clone(&*entry),
            None => match self.map.entry(endpoint) {
                Entry::Occupied(entry) => Arc::clone(&*entry.into_ref()),
                Entry::Vacant(_) => todo!("fetch jwks from cplane"),
            },
        };

        // check if the cached JWKs need updating.
        let mut guard = {
            let now = Instant::now();
            let guard = entry.cached.load();
            let last_update = now.duration_since(guard.last_retrieved);

            if last_update > MAX_RENEW {
                // it's been too long since we checked the keys. wait for them to update.
                entry.renew_jwks(&self.client, endpoint).await
            } else if last_update > MIN_RENEW {
                // every 5 minutes we should spawn a job to eagerly update the token.

                let entry = entry.clone();
                let client = self.client.clone();
                tokio::spawn(async move {
                    entry.renew_jwks(&client, endpoint).await;
                });

                Guard::into_inner(guard)
            } else {
                Guard::into_inner(guard)
            }
        };

        // get the key from the JWKs if possible. If not, wait for the keys to update.
        let jwk = loop {
            let jwk = guard
                .key_sets
                .iter()
                .flat_map(|jwks| &jwks.keys)
                .find(|jwk| {
                    jwk.prm.kid.as_deref() == Some(kid) && jwk.key.is_supported(&header.alg)
                });

            match jwk {
                Some(jwk) => break jwk,
                None if guard.last_retrieved.elapsed() > MIN_RENEW => {
                    guard = entry.renew_jwks(&self.client, endpoint).await
                }
                _ => {
                    bail!("jwk not found");
                }
            }
        };

        let sig = base64::decode_config(signature, base64::URL_SAFE_NO_PAD)
            .context("not a valid compact JWT encoding")?;
        match &jwk.key {
            jose_jwk::Key::Ec(key) => {
                verify_ec_signature(header_payload.as_bytes(), &sig, key)?;
            }
            jose_jwk::Key::Rsa(key) => {
                verify_rsa_signature(header_payload.as_bytes(), &sig, key, &jwk.prm.alg)?;
            }
            jose_jwk::Key::Okp(key) => {
                verify_okp_signature(header_payload.as_bytes(), &sig, key)?;
            }
            key => bail!("unsupported key type {key:?}"),
        };

        // TODO: verify iss, exp, nbf, etc...

        Ok(())
    }
}

fn verify_ec_signature(data: &[u8], sig: &[u8], key: &jose_jwk::Ec) -> anyhow::Result<()> {
    use ecdsa::Signature;
    use signature::Verifier;

    match key.crv {
        jose_jwk::EcCurves::P256 => {
            let pk =
                p256::PublicKey::try_from(key).map_err(|_| anyhow::anyhow!("invalid P256 key"))?;
            let key = p256::ecdsa::VerifyingKey::from(&pk);
            let sig = Signature::from_slice(sig)?;
            key.verify(data, &sig)?;
        }
        jose_jwk::EcCurves::P384 => {
            let pk =
                p384::PublicKey::try_from(key).map_err(|_| anyhow::anyhow!("invalid P384 key"))?;
            let key = p384::ecdsa::VerifyingKey::from(&pk);
            let sig = Signature::from_slice(sig)?;
            key.verify(data, &sig)?;
        }
        jose_jwk::EcCurves::P521 => {
            ensure!(key.x.len() == 66 && key.y.len() == 66);
            let x = GenericArray::from_slice(&key.x);
            let y = GenericArray::from_slice(&key.y);

            let encoded = p521::EncodedPoint::from_affine_coordinates(x, y, false);
            let key = p521::ecdsa::VerifyingKey::from_encoded_point(&encoded)?;
            let sig = Signature::from_slice(sig)?;
            key.verify(data, &sig)?;
        }
        jose_jwk::EcCurves::P256K => {
            ensure!(key.x.len() == 32 && key.y.len() == 32);
            let x = GenericArray::from_slice(&key.x);
            let y = GenericArray::from_slice(&key.y);

            let encoded = k256::EncodedPoint::from_affine_coordinates(x, y, false);
            let key = k256::ecdsa::VerifyingKey::from_encoded_point(&encoded)?;
            let sig = Signature::from_slice(sig)?;
            key.verify(data, &sig)?;
        }
        key => bail!("unsupported ec key type {key:?}"),
    }

    Ok(())
}

fn verify_okp_signature(data: &[u8], sig: &[u8], key: &jose_jwk::Okp) -> anyhow::Result<()> {
    use ed25519_dalek::Signature;
    use signature::Verifier;

    match key.crv {
        jose_jwk::OkpCurves::Ed25519 => {
            let x = <&[u8; 32]>::try_from(&**key.x)?;
            let key = ed25519_dalek::VerifyingKey::from_bytes(x)?;
            let sig = Signature::from_slice(sig)?;
            key.verify(data, &sig)?;
        }
        // jose_jwk::OkpCurves::Ed448 => todo!(),
        // jose_jwk::OkpCurves::X25519 => todo!(),
        // jose_jwk::OkpCurves::X448 => todo!(),
        key => bail!("unsupported octet key pair curve type {key:?}"),
    }

    Ok(())
}

fn verify_rsa_signature(
    data: &[u8],
    sig: &[u8],
    key: &jose_jwk::Rsa,
    alg: &Option<jose_jwa::Algorithm>,
) -> anyhow::Result<()> {
    use jose_jwa::{Algorithm, Signing};
    use rsa::{Pkcs1v15Sign, Pss, RsaPublicKey};
    use sha2::Digest;

    let key = RsaPublicKey::try_from(key).map_err(|_| anyhow::anyhow!("invalid RSA key"))?;

    match alg {
        Some(Algorithm::Signing(Signing::Ps256)) => {
            let hashed = sha2::Sha256::digest(data);
            let scheme = Pss::new::<sha2::Sha256>();
            key.verify(scheme, &hashed, sig)?;
        }
        Some(Algorithm::Signing(Signing::Ps384)) => {
            let hashed = sha2::Sha384::digest(data);
            let scheme = Pss::new::<sha2::Sha384>();
            key.verify(scheme, &hashed, sig)?;
        }
        Some(Algorithm::Signing(Signing::Ps512)) => {
            let hashed = sha2::Sha512::digest(data);
            let scheme = Pss::new::<sha2::Sha512>();
            key.verify(scheme, &hashed, sig)?;
        }
        Some(Algorithm::Signing(Signing::Rs256)) => {
            let hashed = sha2::Sha256::digest(data);
            let scheme = Pkcs1v15Sign::new::<sha2::Sha256>();
            key.verify(scheme, &hashed, sig)?;
        }
        Some(Algorithm::Signing(Signing::Rs384)) => {
            let hashed = sha2::Sha384::digest(data);
            let scheme = Pkcs1v15Sign::new::<sha2::Sha384>();
            key.verify(scheme, &hashed, sig)?;
        }
        Some(Algorithm::Signing(Signing::Rs512)) => {
            let hashed = sha2::Sha512::digest(data);
            let scheme = Pkcs1v15Sign::new::<sha2::Sha512>();
            key.verify(scheme, &hashed, sig)?;
        }
        _ => bail!("invalid RSA signing algorithm"),
    };

    Ok(())
}

/// <https://datatracker.ietf.org/doc/html/rfc7515#section-4.1>
#[derive(serde::Deserialize)]
struct JWTHeader<'a> {
    /// must be "JWT"
    typ: &'a str,
    /// must be a supported alg
    alg: jose_jwa::Algorithm,
    /// key id, must be provided for our usecase
    kid: Option<&'a str>,
}
