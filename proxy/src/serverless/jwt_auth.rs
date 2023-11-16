// https://adapted-gorilla-88.clerk.accounts.dev/.well-known/jwks.json

use std::sync::Arc;

use anyhow::{bail, Context};
use biscuit::{
    jwk::{JWKSet, JWK},
    jws, CompactPart,
};
use dashmap::DashMap;
use reqwest::{IntoUrl, Url};
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::RwLock;

pub struct JWKSetCaches {
    pub map: DashMap<Url, Arc<JWKSetCache>>,
}

impl JWKSetCaches {
    pub async fn get_cache(&self, url: impl IntoUrl) -> anyhow::Result<Arc<JWKSetCache>> {
        let url = url.into_url()?;
        if let Some(x) = self.map.get(&url) {
            return Ok(x.clone());
        }
        let cache = JWKSetCache::new(url.clone()).await?;
        let cache = Arc::new(cache);
        self.map.insert(url, cache.clone());
        Ok(cache)
    }
}

pub struct JWKSetCache {
    url: Url,
    current: RwLock<biscuit::jwk::JWKSet<()>>,
}

impl JWKSetCache {
    pub async fn new(url: impl IntoUrl) -> anyhow::Result<Self> {
        let url = url.into_url()?;
        let current = reqwest::get(url.clone()).await?.json().await?;
        Ok(Self {
            url,
            current: RwLock::new(current),
        })
    }

    pub async fn get(&self, kid: &str) -> anyhow::Result<JWK<()>> {
        let current = self.current.read().await.clone();
        if let Some(key) = current.find(kid) {
            return Ok(key.clone());
        }
        let new = reqwest::get(self.url.clone()).await?.json().await?;
        if new == current {
            bail!("not found")
        }
        *self.current.write().await = new;
        current.find(kid).cloned().context("not found")
    }

    pub async fn decode<T, H>(
        &self,
        token: &jws::Compact<T, H>,
    ) -> anyhow::Result<jws::Compact<T, H>>
    where
        T: CompactPart,
        H: Serialize + DeserializeOwned,
    {
        let current = self.current.read().await.clone();
        match token.decode_with_jwks(&current, None) {
            Ok(t) => Ok(t),
            Err(biscuit::errors::Error::ValidationError(
                biscuit::errors::ValidationError::KeyNotFound,
            )) => {
                let new: JWKSet<()> = reqwest::get(self.url.clone()).await?.json().await?;
                if new == current {
                    bail!("not found")
                }
                *self.current.write().await = new.clone();
                token.decode_with_jwks(&new, None).context("error")
                // current.find(kid).cloned().context("not found")
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JWKSetCache;
    #[tokio::test]
    async fn jwkset() {
        let cache =
            JWKSetCache::new("https://adapted-gorilla-88.clerk.accounts.dev/.well-known/jwks.json")
                .await
                .unwrap();
        dbg!(cache.get("ins_2YFechxysnwZcZN6TDHEz6u6w6v").await.unwrap());
    }
}
