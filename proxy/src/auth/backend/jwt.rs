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
    key_sets: Vec<JwkSet>,
}

impl JWKCache {
    pub async fn check_jwt(
        &self,
        endpoint: EndpointIdInt,
        jwt: String,
    ) -> Result<(), anyhow::Error> {
        let header = jsonwebtoken::decode_header(&jwt).context("could not parse JWT")?;
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

        let mut jwk = None;
        for jwks in &entry.key_sets {
            jwk = jwks.find(&kid);
            if jwk.is_some_and(|k| k.is_supported()) {
                break;
            }
        }

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
