use crate::pqproto::CancelKeyData;

pub mod keyspace {
    pub const CANCEL_PREFIX: &str = "cancel";
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum KeyPrefix {
    Cancel(CancelKeyData),
}

impl KeyPrefix {
    pub(crate) fn build_redis_key(&self) -> String {
        match self {
            KeyPrefix::Cancel(key) => {
                let id = key.0.get();
                let keyspace = keyspace::CANCEL_PREFIX;
                format!("{keyspace}:{id:x}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pqproto::id_to_cancel_key;

    #[test]
    fn test_build_redis_key() {
        let cancel_key: KeyPrefix = KeyPrefix::Cancel(id_to_cancel_key(12345 << 32 | 54321));

        let redis_key = cancel_key.build_redis_key();
        assert_eq!(redis_key, "cancel:30390000d431");
    }
}
