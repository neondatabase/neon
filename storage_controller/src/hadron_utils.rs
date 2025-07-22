use std::collections::BTreeMap;

use rand::Rng;
use utils::shard::TenantShardId;

static CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()";

/// Generate a random string of `length` that can be used as a password. The generated string
/// contains alphanumeric characters and special characters (!@#$%^&*())
pub fn generate_random_password(length: usize) -> String {
    let mut rng = rand::rng();
    (0..length)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

pub(crate) struct TenantShardSizeMap {
    #[expect(dead_code)]
    pub map: BTreeMap<TenantShardId, u64>,
}

impl TenantShardSizeMap {
    pub fn new(map: BTreeMap<TenantShardId, u64>) -> Self {
        Self { map }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_generate_random_password() {
        let pwd1 = generate_random_password(10);
        assert_eq!(pwd1.len(), 10);
        let pwd2 = generate_random_password(10);
        assert_ne!(pwd1, pwd2);
        assert!(pwd1.chars().all(|c| CHARSET.contains(&(c as u8))));
        assert!(pwd2.chars().all(|c| CHARSET.contains(&(c as u8))));
    }
}
