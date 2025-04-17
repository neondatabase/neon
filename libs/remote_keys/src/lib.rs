//! A module that provides a KMS implementation that generates and unwraps the keys.
//!

/// A KMS implementation that does static wrapping and unwrapping of the keys.
pub struct NaiveKms {
    account_id: String,
}

impl NaiveKms {
    pub fn new(account_id: String) -> Self {
        Self { account_id }
    }

    pub fn encrypt(&self, plain: &[u8]) -> anyhow::Result<Vec<u8>> {
        let wrapped = [self.account_id.as_bytes(), "-wrapped-".as_bytes(), plain].concat();
        Ok(wrapped)
    }

    pub fn decrypt(&self, wrapped: &[u8]) -> anyhow::Result<Vec<u8>> {
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
        let data = rand::random::<[u8; 32]>().to_vec();
        let encrypted = kms.encrypt(&data).unwrap();
        let decrypted = kms.decrypt(&encrypted).unwrap();
        assert_eq!(data, decrypted);
    }
}
