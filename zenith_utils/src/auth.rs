// For details about authentication see docs/authentication.md
// TODO there are two issues for our use case in jsonwebtoken library which will be resolved in next release
// The first one is that there is no way to disable expiration claim, but it can be excluded from validation, so use this as a workaround for now.
// Relevant issue: https://github.com/Keats/jsonwebtoken/issues/190
// The second one is that we wanted to use ed25519 keys, but they are also not supported until next version. So we go with RSA keys for now.
// Relevant issue: https://github.com/Keats/jsonwebtoken/issues/162

use hex::{self, FromHex};
use serde::de::Error;
use serde::{self, Deserializer, Serializer};
use std::fs;
use std::path::Path;

use anyhow::{bail, Result};
use jsonwebtoken::{
    decode, encode, Algorithm, DecodingKey, EncodingKey, Header, TokenData, Validation,
};
use serde::{Deserialize, Serialize};

use crate::zid::ZTenantId;

const JWT_ALGORITHM: Algorithm = Algorithm::RS256;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    Tenant,
    PageServerApi,
}

pub fn to_hex_option<S>(value: &Option<ZTenantId>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(tid) => hex::serialize(tid, serializer),
        None => Option::serialize(value, serializer),
    }
}

fn from_hex_option<'de, D>(deserializer: D) -> Result<Option<ZTenantId>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    match opt {
        Some(tid) => Ok(Some(ZTenantId::from_hex(tid).map_err(Error::custom)?)),
        None => Ok(None),
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    // this custom serialize/deserialize_with is needed because Option is not transparent to serde
    // so clearest option is serde(with = "hex") but it is not working, for details see https://github.com/serde-rs/serde/issues/1301
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "to_hex_option",
        deserialize_with = "from_hex_option"
    )]
    pub tenant_id: Option<ZTenantId>,
    pub scope: Scope,
}

impl Claims {
    pub fn new(tenant_id: Option<ZTenantId>, scope: Scope) -> Self {
        Self { tenant_id, scope }
    }
}

pub fn check_permission(claims: &Claims, tenantid: Option<ZTenantId>) -> Result<()> {
    match (&claims.scope, tenantid) {
        (Scope::Tenant, None) => {
            bail!("Attempt to access management api with tenant scope. Permission denied")
        }
        (Scope::Tenant, Some(tenantid)) => {
            if claims.tenant_id.unwrap() != tenantid {
                bail!("Tenant id mismatch. Permission denied")
            }
            Ok(())
        }
        (Scope::PageServerApi, None) => Ok(()), // access to management api for PageServerApi scope
        (Scope::PageServerApi, Some(_)) => Ok(()), // access to tenant api using PageServerApi scope
    }
}

#[derive(Debug)]
pub struct JwtAuth {
    decoding_key: DecodingKey<'static>,
    validation: Validation,
}

impl JwtAuth {
    pub fn new(decoding_key: DecodingKey<'_>) -> Self {
        Self {
            decoding_key: decoding_key.into_static(),
            validation: Validation {
                algorithms: vec![JWT_ALGORITHM],
                validate_exp: false,
                ..Default::default()
            },
        }
    }

    pub fn from_key_path(key_path: &Path) -> Result<Self> {
        let public_key = fs::read_to_string(key_path)?;
        Ok(Self::new(DecodingKey::from_rsa_pem(public_key.as_bytes())?))
    }

    pub fn decode(&self, token: &str) -> Result<TokenData<Claims>> {
        Ok(decode(token, &self.decoding_key, &self.validation)?)
    }
}

// this function is used only for testing purposes in CLI e g generate tokens during init
pub fn encode_from_key_path(claims: &Claims, key_path: &Path) -> Result<String> {
    let key_data = fs::read_to_string(key_path)?;
    let key = EncodingKey::from_rsa_pem(key_data.as_bytes())?;
    Ok(encode(&Header::new(JWT_ALGORITHM), claims, &key)?)
}
