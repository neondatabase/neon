use std::{future::Future, sync::Arc, time::Duration};

use anyhow::{bail, ensure, Context};
use arc_swap::ArcSwapOption;
use dashmap::DashMap;
use jose_jwk::crypto::KeyInfo;
use signature::Verifier;
use tokio::time::Instant;

use crate::{http::parse_json_body_with_limit, intern::EndpointIdInt};

// TODO(conrad): make these configurable.
const MIN_RENEW: Duration = Duration::from_secs(30);
const AUTO_RENEW: Duration = Duration::from_secs(300);
const MAX_RENEW: Duration = Duration::from_secs(3600);
const MAX_JWK_BODY_SIZE: usize = 64 * 1024;

/// How to get the JWT auth rules
pub trait FetchAuthRules: Clone + Send + Sync + 'static {
    fn fetch_auth_rules(&self) -> impl Future<Output = anyhow::Result<AuthRules>> + Send;
}

#[derive(Clone)]
struct FetchAuthRulesFromCplane {
    #[allow(dead_code)]
    endpoint: EndpointIdInt,
}

impl FetchAuthRules for FetchAuthRulesFromCplane {
    async fn fetch_auth_rules(&self) -> anyhow::Result<AuthRules> {
        Err(anyhow::anyhow!("not yet implemented"))
    }
}

pub struct AuthRules {
    jwks_urls: Vec<url::Url>,
}

#[derive(Default)]
pub struct JwkCache {
    client: reqwest::Client,

    map: DashMap<EndpointIdInt, Arc<JwkCacheEntryLock>>,
}

pub struct JwkCacheEntryLock {
    cached: ArcSwapOption<JwkCacheEntry>,
    lookup: tokio::sync::Semaphore,
}

impl Default for JwkCacheEntryLock {
    fn default() -> Self {
        JwkCacheEntryLock {
            cached: ArcSwapOption::empty(),
            lookup: tokio::sync::Semaphore::new(1),
        }
    }
}

pub struct JwkCacheEntry {
    /// Should refetch at least every hour to verify when old keys have been removed.
    /// Should refetch when new key IDs are seen only every 5 minutes or so
    last_retrieved: Instant,

    /// cplane will return multiple JWKs urls that we need to scrape.
    key_sets: ahash::HashMap<url::Url, jose_jwk::JwkSet>,
}

impl JwkCacheEntryLock {
    async fn acquire_permit<'a>(self: &'a Arc<Self>) -> JwkRenewalPermit<'a> {
        JwkRenewalPermit::acquire_permit(self).await
    }

    fn try_acquire_permit<'a>(self: &'a Arc<Self>) -> Option<JwkRenewalPermit<'a>> {
        JwkRenewalPermit::try_acquire_permit(self)
    }

    async fn renew_jwks<F: FetchAuthRules>(
        &self,
        _permit: JwkRenewalPermit<'_>,
        client: &reqwest::Client,
        auth_rules: &F,
    ) -> anyhow::Result<Arc<JwkCacheEntry>> {
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
        let mut key_sets = ahash::HashMap::with_capacity_and_hasher(
            rules.jwks_urls.len(),
            ahash::RandomState::new(),
        );
        // TODO(conrad): run concurrently
        // TODO(conrad): strip the JWKs urls (should be checked by cplane as well - cloud#16284)
        for url in rules.jwks_urls {
            let req = client.get(url.clone());
            // TODO(conrad): eventually switch to using reqwest_middleware/`new_client_with_timeout`.
            match req.send().await.and_then(|r| r.error_for_status()) {
                // todo: should we re-insert JWKs if we want to keep this JWKs URL?
                // I expect these failures would be quite sparse.
                Err(e) => tracing::warn!(?url, error=?e, "could not fetch JWKs"),
                Ok(r) => {
                    let resp: http::Response<reqwest::Body> = r.into();
                    match parse_json_body_with_limit::<jose_jwk::JwkSet>(
                        resp.into_body(),
                        MAX_JWK_BODY_SIZE,
                    )
                    .await
                    {
                        Err(e) => tracing::warn!(?url, error=?e, "could not decode JWKs"),
                        Ok(jwks) => {
                            key_sets.insert(url, jwks);
                        }
                    }
                }
            }
        }

        let entry = Arc::new(JwkCacheEntry {
            last_retrieved: now,
            key_sets,
        });
        self.cached.swap(Some(Arc::clone(&entry)));

        Ok(entry)
    }

    async fn get_or_update_jwk_cache<F: FetchAuthRules>(
        self: &Arc<Self>,
        client: &reqwest::Client,
        fetch: &F,
    ) -> Result<Arc<JwkCacheEntry>, anyhow::Error> {
        let now = Instant::now();
        let guard = self.cached.load_full();

        // if we have no cached JWKs, try and get some
        let Some(cached) = guard else {
            let permit = self.acquire_permit().await;
            return self.renew_jwks(permit, client, fetch).await;
        };

        let last_update = now.duration_since(cached.last_retrieved);

        // check if the cached JWKs need updating.
        if last_update > MAX_RENEW {
            let permit = self.acquire_permit().await;

            // it's been too long since we checked the keys. wait for them to update.
            return self.renew_jwks(permit, client, fetch).await;
        }

        // every 5 minutes we should spawn a job to eagerly update the token.
        if last_update > AUTO_RENEW {
            if let Some(permit) = self.try_acquire_permit() {
                tracing::debug!("JWKs should be renewed. Renewal permit acquired");
                let permit = permit.into_owned();
                let entry = self.clone();
                let client = client.clone();
                let fetch = fetch.clone();
                tokio::spawn(async move {
                    if let Err(e) = entry.renew_jwks(permit, &client, &fetch).await {
                        tracing::warn!(error=?e, "could not fetch JWKs in background job");
                    }
                });
            } else {
                tracing::debug!("JWKs should be renewed. Renewal permit already taken, skipping");
            }
        }

        Ok(cached)
    }

    async fn check_jwt<F: FetchAuthRules>(
        self: &Arc<Self>,
        jwt: String,
        client: &reqwest::Client,
        fetch: &F,
    ) -> Result<(), anyhow::Error> {
        // JWT compact form is defined to be
        // <B64(Header)> || . || <B64(Payload)> || . || <B64(Signature)>
        // where Signature = alg(<B64(Header)> || . || <B64(Payload)>);

        let (header_payload, signature) = jwt
            .rsplit_once(".")
            .context("Provided authentication token is not a valid JWT encoding")?;
        let (header, _payload) = header_payload
            .split_once(".")
            .context("Provided authentication token is not a valid JWT encoding")?;

        let header = base64::decode_config(header, base64::URL_SAFE_NO_PAD)
            .context("Provided authentication token is not a valid JWT encoding")?;
        let header = serde_json::from_slice::<JWTHeader>(&header)
            .context("Provided authentication token is not a valid JWT encoding")?;

        let sig = base64::decode_config(signature, base64::URL_SAFE_NO_PAD)
            .context("Provided authentication token is not a valid JWT encoding")?;

        ensure!(header.typ == "JWT");
        let kid = header.kid.context("missing key id")?;

        let mut guard = self.get_or_update_jwk_cache(client, fetch).await?;

        // get the key from the JWKs if possible. If not, wait for the keys to update.
        let jwk = loop {
            let jwk = guard
                .key_sets
                .values()
                .flat_map(|jwks| &jwks.keys)
                .find(|jwk| jwk.prm.kid.as_deref() == Some(kid));

            match jwk {
                Some(jwk) => break jwk,
                None if guard.last_retrieved.elapsed() > MIN_RENEW => {
                    let permit = self.acquire_permit().await;
                    guard = self.renew_jwks(permit, client, fetch).await?;
                }
                _ => {
                    bail!("jwk not found");
                }
            }
        };

        ensure!(
            jwk.is_supported(&header.alg),
            "signature algorithm not supported"
        );

        match &jwk.key {
            jose_jwk::Key::Ec(key) => {
                verify_ec_signature(header_payload.as_bytes(), &sig, key)?;
            }
            jose_jwk::Key::Rsa(key) => {
                verify_rsa_signature(header_payload.as_bytes(), &sig, key, &jwk.prm.alg)?;
            }
            key => bail!("unsupported key type {key:?}"),
        };

        // TODO(conrad): verify iss, exp, nbf, etc...

        Ok(())
    }
}

impl JwkCache {
    pub async fn check_jwt(
        &self,
        endpoint: EndpointIdInt,
        jwt: String,
    ) -> Result<(), anyhow::Error> {
        // try with just a read lock first
        let entry = self.map.get(&endpoint).as_deref().map(Arc::clone);
        let entry = match entry {
            Some(entry) => entry,
            None => {
                // acquire a write lock after to insert.
                let entry = self.map.entry(endpoint).or_default();
                Arc::clone(&*entry)
            }
        };

        let fetch = FetchAuthRulesFromCplane { endpoint };
        entry.check_jwt(jwt, &self.client, &fetch).await
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
    use rsa::{
        pkcs1v15::{Signature, VerifyingKey},
        RsaPublicKey,
    };

    let key = RsaPublicKey::try_from(key).map_err(|_| anyhow::anyhow!("invalid RSA key"))?;

    match alg {
        Some(Algorithm::Signing(Signing::Rs256)) => {
            let key = VerifyingKey::<sha2::Sha256>::new(key);
            let sig = Signature::try_from(sig)?;
            key.verify(data, &sig)?;
        }
        _ => bail!("invalid RSA signing algorithm"),
    };

    Ok(())
}

/// <https://datatracker.ietf.org/doc/html/rfc7515#section-4.1>
#[derive(serde::Deserialize, serde::Serialize)]
struct JWTHeader<'a> {
    /// must be "JWT"
    typ: &'a str,
    /// must be a supported alg
    alg: jose_jwa::Algorithm,
    /// key id, must be provided for our usecase
    kid: Option<&'a str>,
}

struct JwkRenewalPermit<'a> {
    inner: Option<JwkRenewalPermitInner<'a>>,
}

enum JwkRenewalPermitInner<'a> {
    Owned(Arc<JwkCacheEntryLock>),
    Borrowed(&'a Arc<JwkCacheEntryLock>),
}

impl JwkRenewalPermit<'_> {
    fn into_owned(mut self) -> JwkRenewalPermit<'static> {
        JwkRenewalPermit {
            inner: self.inner.take().map(JwkRenewalPermitInner::into_owned),
        }
    }

    async fn acquire_permit(from: &Arc<JwkCacheEntryLock>) -> JwkRenewalPermit {
        match from.lookup.acquire().await {
            Ok(permit) => {
                permit.forget();
                JwkRenewalPermit {
                    inner: Some(JwkRenewalPermitInner::Borrowed(from)),
                }
            }
            Err(_) => panic!("semaphore should not be closed"),
        }
    }

    fn try_acquire_permit(from: &Arc<JwkCacheEntryLock>) -> Option<JwkRenewalPermit> {
        match from.lookup.try_acquire() {
            Ok(permit) => {
                permit.forget();
                Some(JwkRenewalPermit {
                    inner: Some(JwkRenewalPermitInner::Borrowed(from)),
                })
            }
            Err(tokio::sync::TryAcquireError::NoPermits) => None,
            Err(tokio::sync::TryAcquireError::Closed) => panic!("semaphore should not be closed"),
        }
    }
}

impl JwkRenewalPermitInner<'_> {
    fn into_owned(self) -> JwkRenewalPermitInner<'static> {
        match self {
            JwkRenewalPermitInner::Owned(p) => JwkRenewalPermitInner::Owned(p),
            JwkRenewalPermitInner::Borrowed(p) => JwkRenewalPermitInner::Owned(Arc::clone(p)),
        }
    }
}

impl Drop for JwkRenewalPermit<'_> {
    fn drop(&mut self) {
        let entry = match &self.inner {
            None => return,
            Some(JwkRenewalPermitInner::Owned(p)) => p,
            Some(JwkRenewalPermitInner::Borrowed(p)) => *p,
        };
        entry.lookup.add_permits(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{future::IntoFuture, net::SocketAddr, time::SystemTime};

    use base64::URL_SAFE_NO_PAD;
    use bytes::Bytes;
    use http::Response;
    use http_body_util::Full;
    use hyper1::service::service_fn;
    use hyper_util::rt::TokioIo;
    use rand::rngs::OsRng;
    use signature::Signer;
    use tokio::net::TcpListener;

    fn new_ec_jwk(kid: String) -> (p256::SecretKey, jose_jwk::Jwk) {
        let sk = p256::SecretKey::random(&mut OsRng);
        let pk = sk.public_key().into();
        let jwk = jose_jwk::Jwk {
            key: jose_jwk::Key::Ec(pk),
            prm: jose_jwk::Parameters {
                kid: Some(kid),
                alg: Some(jose_jwa::Algorithm::Signing(jose_jwa::Signing::Es256)),
                ..Default::default()
            },
        };
        (sk, jwk)
    }

    fn new_rsa_jwk(kid: String) -> (rsa::RsaPrivateKey, jose_jwk::Jwk) {
        let sk = rsa::RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let pk = sk.to_public_key().into();
        let jwk = jose_jwk::Jwk {
            key: jose_jwk::Key::Rsa(pk),
            prm: jose_jwk::Parameters {
                kid: Some(kid),
                alg: Some(jose_jwa::Algorithm::Signing(jose_jwa::Signing::Rs256)),
                ..Default::default()
            },
        };
        (sk, jwk)
    }

    fn build_jwt_payload(kid: String, sig: jose_jwa::Signing) -> String {
        let header = JWTHeader {
            typ: "JWT",
            alg: jose_jwa::Algorithm::Signing(sig),
            kid: Some(&kid),
        };
        let body = typed_json::json! {{
            "exp": SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() + 3600,
        }};

        let header =
            base64::encode_config(serde_json::to_string(&header).unwrap(), URL_SAFE_NO_PAD);
        let body = base64::encode_config(body.to_string(), URL_SAFE_NO_PAD);

        format!("{header}.{body}")
    }

    fn new_ec_jwt(kid: String, key: p256::SecretKey) -> String {
        use p256::ecdsa::{Signature, SigningKey};

        let payload = build_jwt_payload(kid, jose_jwa::Signing::Es256);
        let sig: Signature = SigningKey::from(key).sign(payload.as_bytes());
        let sig = base64::encode_config(sig.to_bytes(), URL_SAFE_NO_PAD);

        format!("{payload}.{sig}")
    }

    fn new_rsa_jwt(kid: String, key: rsa::RsaPrivateKey) -> String {
        use rsa::pkcs1v15::SigningKey;
        use rsa::signature::SignatureEncoding;

        let payload = build_jwt_payload(kid, jose_jwa::Signing::Rs256);
        let sig = SigningKey::<sha2::Sha256>::new(key).sign(payload.as_bytes());
        let sig = base64::encode_config(sig.to_bytes(), URL_SAFE_NO_PAD);

        format!("{payload}.{sig}")
    }

    #[tokio::test]
    async fn renew() {
        let (rs1, jwk1) = new_rsa_jwk("1".into());
        let (rs2, jwk2) = new_rsa_jwk("2".into());
        let (ec1, jwk3) = new_ec_jwk("3".into());
        let (ec2, jwk4) = new_ec_jwk("4".into());

        let jwt1 = new_rsa_jwt("1".into(), rs1);
        let jwt2 = new_rsa_jwt("2".into(), rs2);
        let jwt3 = new_ec_jwt("3".into(), ec1);
        let jwt4 = new_ec_jwt("4".into(), ec2);

        let foo_jwks = jose_jwk::JwkSet {
            keys: vec![jwk1, jwk3],
        };
        let bar_jwks = jose_jwk::JwkSet {
            keys: vec![jwk2, jwk4],
        };

        let service = service_fn(move |req| {
            let foo_jwks = foo_jwks.clone();
            let bar_jwks = bar_jwks.clone();
            async move {
                let jwks = match req.uri().path() {
                    "/foo" => &foo_jwks,
                    "/bar" => &bar_jwks,
                    _ => {
                        return Response::builder()
                            .status(404)
                            .body(Full::new(Bytes::new()));
                    }
                };
                let body = serde_json::to_vec(jwks).unwrap();
                Response::builder()
                    .status(200)
                    .body(Full::new(Bytes::from(body)))
            }
        });

        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let server = hyper1::server::conn::http1::Builder::new();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (s, _) = listener.accept().await.unwrap();
                let serve = server.serve_connection(TokioIo::new(s), service.clone());
                tokio::spawn(serve.into_future());
            }
        });

        let client = reqwest::Client::new();

        #[derive(Clone)]
        struct Fetch(SocketAddr);

        impl FetchAuthRules for Fetch {
            async fn fetch_auth_rules(&self) -> anyhow::Result<AuthRules> {
                Ok(AuthRules {
                    jwks_urls: vec![
                        format!("http://{}/foo", self.0).parse().unwrap(),
                        format!("http://{}/bar", self.0).parse().unwrap(),
                    ],
                })
            }
        }

        let jwk_cache = Arc::new(JwkCacheEntryLock::default());

        jwk_cache
            .check_jwt(jwt1, &client, &Fetch(addr))
            .await
            .unwrap();
        jwk_cache
            .check_jwt(jwt2, &client, &Fetch(addr))
            .await
            .unwrap();
        jwk_cache
            .check_jwt(jwt3, &client, &Fetch(addr))
            .await
            .unwrap();
        jwk_cache
            .check_jwt(jwt4, &client, &Fetch(addr))
            .await
            .unwrap();
    }
}
