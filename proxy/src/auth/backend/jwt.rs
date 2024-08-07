use std::{sync::Arc, time::Duration};

use anyhow::{bail, ensure, Context};
use arc_swap::ArcSwapOption;
use dashmap::DashMap;
use jose_jwk::crypto::KeyInfo;
use tokio::time::Instant;

use crate::intern::EndpointIdInt;

#[derive(Default)]
pub struct JWKCache {
    client: reqwest::Client,

    map: DashMap<EndpointIdInt, Arc<JWKCacheEntryLock>>,
}

pub struct JWKCacheEntryLock {
    cached: ArcSwapOption<JWKCacheEntry>,
    lookup: tokio::sync::Semaphore,
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

/// How to get the JWT auth rules
#[allow(async_fn_in_trait)]
pub trait FetchAuthRules {
    async fn fetch_auth_rules(&self) -> anyhow::Result<AuthRules>;
}

#[derive(Clone)]
struct FetchAuthFromCplane(EndpointIdInt);

impl FetchAuthRules for FetchAuthFromCplane {
    async fn fetch_auth_rules(&self) -> anyhow::Result<AuthRules> {
        bail!("not yet implemented")
    }
}

pub struct AuthRules {
    jwks_urls: Vec<url::Url>,
}

impl JWKCacheEntryLock {
    async fn acquire_permit(&self) -> DetachedPermit {
        match self.lookup.acquire().await {
            Ok(permit) => {
                permit.forget();
                DetachedPermit
            }
            Err(_) => panic!("semaphore should not be closed"),
        }
    }

    fn try_acquire_permit(&self) -> Option<DetachedPermit> {
        match self.lookup.try_acquire() {
            Ok(permit) => {
                permit.forget();
                Some(DetachedPermit)
            }
            Err(tokio::sync::TryAcquireError::NoPermits) => None,
            Err(tokio::sync::TryAcquireError::Closed) => panic!("semaphore should not be closed"),
        }
    }

    async fn renew_jwks<F: FetchAuthRules>(
        &self,
        permit: DetachedPermit,
        client: &reqwest::Client,
        auth_rules: &F,
    ) -> anyhow::Result<Arc<JWKCacheEntry>> {
        let _permit = permit.attach(&self.lookup);

        // double check that no one beat us to updating the cache.
        let now = Instant::now();
        let guard = self.cached.load_full();
        if let Some(cached) = guard {
            let last_update = now.duration_since(cached.last_retrieved);
            if last_update < Duration::from_secs(300) {
                return Ok(cached);
            }
        }

        let rules = auth_rules.fetch_auth_rules().await?;
        let mut key_sets = vec![];
        for url in rules.jwks_urls {
            match client
                .get(url.clone())
                .send()
                .await
                .and_then(|r| r.error_for_status())
            {
                Err(e) => tracing::warn!(?url, error=?e, "could not fetch JWKs"),
                Ok(r) => match r.json::<jose_jwk::JwkSet>().await {
                    Err(e) => tracing::warn!(?url, error=?e, "could not decode JWKs"),
                    Ok(jwks) => key_sets.push(jwks),
                },
            }
        }
        if key_sets.is_empty() {
            bail!("no JWKs found")
        }

        let x = Arc::new(JWKCacheEntry {
            last_retrieved: now,
            key_sets,
        });
        self.cached.swap(Some(x.clone()));

        Ok(x)
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

        // try with just a read lock first
        let entry = self.map.get(&endpoint).as_deref().map(Arc::clone);
        let entry = match entry {
            Some(entry) => entry,
            None => {
                // acquire a write lock after to insert.
                let entry = self.map.entry(endpoint).or_insert_with(|| {
                    Arc::new(JWKCacheEntryLock {
                        cached: ArcSwapOption::empty(),
                        lookup: tokio::sync::Semaphore::new(1),
                    })
                });
                Arc::clone(&*entry)
            }
        };

        let fetch = FetchAuthFromCplane(endpoint);

        // check if the cached JWKs need updating.
        let mut guard = {
            let now = Instant::now();
            let guard = entry.cached.load_full();

            if let Some(cached) = guard {
                let last_update = now.duration_since(cached.last_retrieved);

                if last_update > MAX_RENEW {
                    let permit = entry.acquire_permit().await;

                    // it's been too long since we checked the keys. wait for them to update.
                    entry.renew_jwks(permit, &self.client, &fetch).await?
                } else if last_update > MIN_RENEW {
                    // every 5 minutes we should spawn a job to eagerly update the token.
                    if let Some(permit) = entry.try_acquire_permit() {
                        let entry = entry.clone();
                        let client = self.client.clone();
                        let fetch = fetch.clone();
                        tokio::spawn(async move {
                            if let Err(e) = entry.renew_jwks(permit, &client, &fetch).await {
                                tracing::warn!(error=?e, "could not fetch JWKs in background job");
                            }
                        });
                    }

                    cached
                } else {
                    cached
                }
            } else {
                let permit = entry.acquire_permit().await;
                entry.renew_jwks(permit, &self.client, &fetch).await?
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
                    let permit = entry.acquire_permit().await;
                    guard = entry.renew_jwks(permit, &self.client, &fetch).await?;
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
        key => bail!("unsupported ec key type {key:?}"),
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
    use rsa::{Pkcs1v15Sign, RsaPublicKey};
    use sha2::Digest;

    let key = RsaPublicKey::try_from(key).map_err(|_| anyhow::anyhow!("invalid RSA key"))?;

    match alg {
        Some(Algorithm::Signing(Signing::Rs256)) => {
            let hashed = sha2::Sha256::digest(data);
            let scheme = Pkcs1v15Sign::new::<sha2::Sha256>();
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

// semaphore trickery
struct DetachedPermit;

impl DetachedPermit {
    fn attach(self, semaphore: &tokio::sync::Semaphore) -> AttachedPermit {
        AttachedPermit(semaphore)
    }
}

struct AttachedPermit<'a>(&'a tokio::sync::Semaphore);

impl Drop for AttachedPermit<'_> {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}
