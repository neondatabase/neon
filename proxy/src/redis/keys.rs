use pq_proto::CancelKeyData;

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
                let hi = (key.backend_pid as u64) << 32;
                let lo = (key.cancel_key as u64) & 0xffff_ffff;
                let id = hi | lo;
                let keyspace = keyspace::CANCEL_PREFIX;
                format!("{keyspace}:{id:x}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_redis_key() {
        let cancel_key: KeyPrefix = KeyPrefix::Cancel(CancelKeyData {
            backend_pid: 12345,
            cancel_key: 54321,
        });

        let redis_key = cancel_key.build_redis_key();
        assert_eq!(redis_key, "cancel:30390000d431");
    }
}
