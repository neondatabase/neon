//! A module that provides a KMS implementation that generates and unwraps the keys.
//!

#![allow(dead_code)]

/// A KMS implementation that does static wrapping and unwrapping of the keys.
pub struct NaiveKms {
    account_id: String,
}

pub struct KeyPair {
    pub wrapped: Vec<u8>,
    pub plain: Vec<u8>,
}

impl NaiveKms {
    pub fn new(account_id: String) -> Self {
        Self { account_id }
    }

    /// Generate a new key in the KMS. Returns the plain and wrapped keys.
    pub fn generate_key(&self) -> anyhow::Result<KeyPair> {
        // TODO: use a proper library for generating the key (i.e., ring).
        let plain = rand::random::<[u8; 32]>().to_vec();
        let wrapped = [self.account_id.as_bytes(), "-wrapped-".as_bytes(), &plain].concat();
        Ok(KeyPair { wrapped, plain })
    }

    pub fn unwrap_key(&self, wrapped: &[u8]) -> anyhow::Result<Vec<u8>> {
        let Some(wrapped) = wrapped.strip_prefix(self.account_id.as_bytes()) else {
            return Err(anyhow::anyhow!("invalid key"));
        };
        let Some(plain) = wrapped.strip_prefix(b"-wrapped-") else {
            return Err(anyhow::anyhow!("invalid key"));
        };
        Ok(plain.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_key() {
        let kms = NaiveKms::new("test-tenant".to_string());
        let key_pair = kms.generate_key().unwrap();
        assert_eq!(kms.unwrap_key(&key_pair.wrapped).unwrap(), key_pair.plain);
    }
}
