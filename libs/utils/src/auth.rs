// For details about authentication see docs/authentication.md
//
// TODO: use ed25519 keys
// Relevant issue: https://github.com/Keats/jsonwebtoken/issues/162

use serde;
use std::fs;
use std::path::Path;

use anyhow::Result;
use jsonwebtoken::{
    decode, encode, Algorithm, DecodingKey, EncodingKey, Header, TokenData, Validation,
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

use crate::id::TenantId;

const JWT_ALGORITHM: Algorithm = Algorithm::RS256;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    Tenant,
    PageServerApi,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub tenant_id: Option<TenantId>,
    pub scope: Scope,
}

impl Claims {
    pub fn new(tenant_id: Option<TenantId>, scope: Scope) -> Self {
        Self { tenant_id, scope }
    }
}

pub struct JwtAuth {
    decoding_key: DecodingKey,
    validation: Validation,
}

impl JwtAuth {
    pub fn new(decoding_key: DecodingKey) -> Self {
        let mut validation = Validation::new(JWT_ALGORITHM);
        // The default 'required_spec_claims' is 'exp'. But we don't want to require
        // expiration.
        validation.required_spec_claims = [].into();
        Self {
            decoding_key,
            validation,
        }
    }

    pub fn from_key_path(key_path: &Path) -> Result<Self> {
        let public_key = fs::read(key_path)?;
        Ok(Self::new(DecodingKey::from_rsa_pem(&public_key)?))
    }

    pub fn decode(&self, token: &str) -> Result<TokenData<Claims>> {
        Ok(decode(token, &self.decoding_key, &self.validation)?)
    }
}

impl std::fmt::Debug for JwtAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtAuth")
            .field("validation", &self.validation)
            .finish()
    }
}

// this function is used only for testing purposes in CLI e g generate tokens during init
pub fn encode_from_key_file(claims: &Claims, key_data: &[u8]) -> Result<String> {
    let key = EncodingKey::from_rsa_pem(key_data)?;
    Ok(encode(&Header::new(JWT_ALGORITHM), claims, &key)?)
}
