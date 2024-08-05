use std::time::Duration;

use anyhow::{bail, Context};
use dashmap::{mapref::entry::Entry, DashMap};
use jsonwebtoken::{jwk::JwkSet, Algorithm, DecodingKey, TokenData, Validation};
use tokio::time::Instant;

use crate::intern::EndpointIdInt;

pub struct JWKCache {
    client: reqwest::Client,

    // I don't like this. jsonwebtoken crate is horrendous with the amount of string allocations.. oh well.
    map: DashMap<EndpointIdInt, JWKCacheEntry>,
}

pub struct JWKCacheEntry {
    /// Should refetch at least every hour to verify when old keys have been removed.
    /// Should refetch when new key IDs are seen only every 5 minutes or so
    last_retrieved: Instant,

    /// jwks urls
    urls: Vec<url::Url>,

    /// cplane will return multiple JWKs urls that we need to scrape.
    key_sets: Vec<jose_jwk::JwkSet>,
}

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

        // let header = jsonwebtoken::decode_header(&jwt).context("could not parse JWT")?;
        let kid = header.kid.context("missing key id")?;

        let entry = match self.map.get(&endpoint) {
            Some(entry) => entry,
            None => match self.map.entry(endpoint) {
                Entry::Occupied(entry) => entry.into_ref().downgrade(),
                Entry::Vacant(_) => todo!("fetch jwks from cplane"),
            },
        };

        let now = Instant::now();
        if now.duration_since(entry.last_retrieved) > Duration::from_secs(3600) {
            todo!("refetch jwks from cplane")
            // drop(entry) - don't want to hold the lock while running long network operations
            // fetch from cplane
            // fetch jwks
            // entry.last_retrieved = now;

            // can we use arcswap compare_and_swap here?
        }

        let jwk = entry.key_sets.iter().flat_map(|jwks| &jwks.keys).find(|jwk| {
            jwk.prm.kid.as_deref() == Some(kid) && jwk.prm.alg == Some(header.alg)
        });

        let key = match jwk {
            Some(jwk) if jwk.is_supported() => DecodingKey::from_jwk(jwk).context("invalid key")?,
            None if now.duration_since(entry.last_retrieved) > Duration::from_secs(300) => {
                // refetch jwks
                todo!("fetch jwks again");
            }
            _ => {
                bail!("jwk not found");
            }
        };

        let mut validation = Validation::new(Algorithm::ES256);
        // support all public key algorithms
        validation.algorithms = vec![
            Algorithm::ES256,
            Algorithm::ES384,
            Algorithm::RS256,
            Algorithm::RS384,
            Algorithm::RS512,
            Algorithm::PS256,
            Algorithm::PS384,
            Algorithm::PS512,
            Algorithm::EdDSA,
        ];

        // should we require some set of common claims?
        // eg `exp`?
        validation.required_spec_claims.clear();
        validation.leeway = 60;

        validation.validate_exp = true;
        validation.validate_nbf = true;
        // we don't have an audience - I want cplane to add this field
        validation.validate_aud = false;

        // subject can be anything - we support all users
        validation.sub = None;
        // don't know the audience - I want cplane to add this field
        validation.aud = None;
        // don't know the issuer - I want cplane to add this field
        validation.iss = None;

        // ew...
        let token: TokenData<serde_json::Value> =
            jsonwebtoken::decode(&jwt, &key, &validation).context("invalid token")?;

        Ok(())
    }
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

enum JwsAlg {
    // we do not support symmetric key algorithms such as HMAC.

    // RSASSA PKCS1-v1_5: <https://www.rfc-editor.org/rfc/rfc7518#section-3.3>
    /// RSASSA-PKCS1-v1_5 using SHA-256
    RS256,
    /// RSASSA-PKCS1-v1_5 using SHA-384
    RS384,
    /// RSASSA-PKCS1-v1_5 using SHA-512
    RS512,

    // ECDSA: <https://www.rfc-editor.org/rfc/rfc7518#section-3.4>
    /// ECDSA using P-256 and SHA-256
    ES256,
    /// ECDSA using P-384 and SHA-384
    ES384,
    /// ECDSA using P-521 and SHA-512
    ES512,

    // RSASSA-PSS: <https://www.rfc-editor.org/rfc/rfc7518#section-3.5>
    /// RSASSA-PSS using SHA-256 and MGF1 with SHA-256
    PS256,
    /// RSASSA-PSS using SHA-384 and MGF1 with SHA-384
    PS384,
    /// RSASSA-PSS using SHA-512 and MGF1 with SHA-512
    PS512,

    // EdDSA: <https://datatracker.ietf.org/doc/html/rfc8037#section-3.1>
    /// Edwards-curve Digital Signature Algorithm (EdDSA)
    EdDSA,
}
