use anyhow::Result;
use camino::Utf8Path;
use jsonwebtoken::EncodingKey;
use std::fs;
use utils::{
    auth::{Claims, Scope, encode_hadron_token_with_encoding_key},
    id::TenantId,
};
use uuid::Uuid;

pub struct HadronTokenGenerator {
    encoding_key: EncodingKey,
}

impl HadronTokenGenerator {
    pub fn new(path: &Utf8Path) -> Self {
        let key_data = fs::read(path).unwrap_or_else(|e| {
            panic!("Error reading private key file {:?}. Error: {:?}", path, e)
        });
        let encoding_key = EncodingKey::from_rsa_pem(&key_data).unwrap_or_else(|e| {
            panic!(
                "Error reading private key file {:?} as RSA private key. Error: {:?}",
                path, e
            )
        });
        Self { encoding_key }
    }

    pub fn generate_tenant_scope_token(&self, tenant_id: TenantId) -> Result<String> {
        let claims = Claims::new(Some(tenant_id), Scope::Tenant);
        self.internal_encode_token(&claims)
    }

    pub fn generate_tenant_endpoint_scope_token(&self, endpoint_id: Uuid) -> Result<String> {
        let claims = Claims::new_for_endpoint(endpoint_id);
        self.internal_encode_token(&claims)
    }

    pub fn generate_ps_sk_auth_token(&self) -> Result<String> {
        let claims = Claims {
            tenant_id: None,
            endpoint_id: None,
            scope: Scope::SafekeeperData,
        };
        self.internal_encode_token(&claims)
    }

    fn internal_encode_token(&self, claims: &Claims) -> Result<String> {
        encode_hadron_token_with_encoding_key(claims, &self.encoding_key)
    }
}
