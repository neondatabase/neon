// For details about authentication see docs/authentication.md

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

/// Algorithm to use. We require EdDSA.
const STORAGE_TOKEN_ALGORITHM: Algorithm = Algorithm::EdDSA;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    // Provides access to all data for a specific tenant (specified in `struct Claims` below)
    // TODO: join these two?
    Tenant,
    // Provides blanket access to all tenants on the pageserver plus pageserver-wide APIs.
    // Should only be used e.g. for status check/tenant creation/list.
    PageServerApi,
    // Provides blanket access to all data on the safekeeper plus safekeeper-wide APIs.
    // Should only be used e.g. for status check.
    // Currently also used for connection from any pageserver to any safekeeper.
    SafekeeperData,
}

/// JWT payload. See docs/authentication.md for the format
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
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
        let mut validation = Validation::default();
        validation.algorithms = vec![STORAGE_TOKEN_ALGORITHM];
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
        Ok(Self::new(DecodingKey::from_ed_pem(&public_key)?))
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
    let key = EncodingKey::from_ed_pem(key_data)?;
    Ok(encode(&Header::new(STORAGE_TOKEN_ALGORITHM), claims, &key)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    // Generated with:
    //
    // openssl genpkey -algorithm ed25519 -out ed25519-priv.pem
    // openssl pkey -in ed25519-priv.pem -pubout -out ed25519-pub.pem
    const TEST_PUB_KEY_ED25519: &[u8] = br#"
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEARYwaNBayR+eGI0iXB4s3QxE3Nl2g1iWbr6KtLWeVD/w=
-----END PUBLIC KEY-----
"#;

    const TEST_PRIV_KEY_ED25519: &[u8] = br#"
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEID/Drmc1AA6U/znNRWpF3zEGegOATQxfkdWxitcOMsIH
-----END PRIVATE KEY-----
"#;

    #[test]
    fn test_decode() -> Result<(), anyhow::Error> {
        let expected_claims = Claims {
            tenant_id: Some(TenantId::from_str("3d1f7595b468230304e0b73cecbcb081")?),
            scope: Scope::Tenant,
        };

        // A test token containing the following payload, signed using TEST_PRIV_KEY_ED25519:
        //
        // ```
        // {
        //   "scope": "tenant",
        //   "tenant_id": "3d1f7595b468230304e0b73cecbcb081",
        //   "iss": "neon.controlplane",
        //   "exp": 1709200879,
        //   "iat": 1678442479
        // }
        // ```
        //
        let encoded_eddsa = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6InRlbmFudCIsInRlbmFudF9pZCI6IjNkMWY3NTk1YjQ2ODIzMDMwNGUwYjczY2VjYmNiMDgxIiwiaXNzIjoibmVvbi5jb250cm9scGxhbmUiLCJleHAiOjE3MDkyMDA4NzksImlhdCI6MTY3ODQ0MjQ3OX0.U3eA8j-uU-JnhzeO3EDHRuXLwkAUFCPxtGHEgw6p7Ccc3YRbFs2tmCdbD9PZEXP-XsxSeBQi1FY0YPcT3NXADw";

        // Check it can be validated with the public key
        let auth = JwtAuth::new(DecodingKey::from_ed_pem(TEST_PUB_KEY_ED25519)?);
        let claims_from_token = auth.decode(encoded_eddsa)?.claims;
        assert_eq!(claims_from_token, expected_claims);

        Ok(())
    }

    #[test]
    fn test_encode() -> Result<(), anyhow::Error> {
        let claims = Claims {
            tenant_id: Some(TenantId::from_str("3d1f7595b468230304e0b73cecbcb081")?),
            scope: Scope::Tenant,
        };

        let encoded = encode_from_key_file(&claims, TEST_PRIV_KEY_ED25519)?;

        // decode it back
        let auth = JwtAuth::new(DecodingKey::from_ed_pem(TEST_PUB_KEY_ED25519)?);
        let decoded = auth.decode(&encoded)?;

        assert_eq!(decoded.claims, claims);

        Ok(())
    }
}
