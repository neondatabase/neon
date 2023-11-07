use std::{ops::RangeInclusive, str::FromStr};

use hex::FromHex;
use serde::{Deserialize, Serialize};
use utils::id::TenantId;

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Serialize, Deserialize, Debug)]
pub struct ShardNumber(pub u8);

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Serialize, Deserialize, Debug)]
pub struct ShardCount(pub u8);

impl ShardCount {
    pub const MAX: Self = Self(u8::MAX);
}

impl ShardNumber {
    pub const MAX: Self = Self(u8::MAX);
}

#[derive(Eq, PartialEq, PartialOrd, Ord, Clone, Copy)]
pub struct TenantShardId {
    pub tenant_id: TenantId,
    pub shard_number: ShardNumber,
    pub shard_count: ShardCount,
}

impl TenantShardId {
    pub fn unsharded(tenant_id: TenantId) -> Self {
        Self {
            tenant_id,
            shard_number: ShardNumber(0),
            shard_count: ShardCount(0),
        }
    }

    /// The range of all TenantShardId that belong to a particular TenantId.  This is useful when
    /// you have a BTreeMap of TenantShardId, and are querying by TenantId.
    pub fn tenant_range(tenant_id: TenantId) -> RangeInclusive<Self> {
        RangeInclusive::new(
            Self {
                tenant_id,
                shard_number: ShardNumber(0),
                shard_count: ShardCount(0),
            },
            Self {
                tenant_id,
                shard_number: ShardNumber::MAX,
                shard_count: ShardCount::MAX,
            },
        )
    }

    pub fn shard_slug(&self) -> String {
        format!("{:02x}{:02x}", self.shard_number.0, self.shard_count.0)
    }
}

impl std::fmt::Display for TenantShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.shard_count != ShardCount(0) {
            write!(
                f,
                "{}-{:02x}{:02x}",
                self.tenant_id, self.shard_number.0, self.shard_count.0
            )
        } else {
            // Legacy case (shard_count == 0) -- format as just the tenant id.  Note that this
            // is distinct from the normal single shard case (shard count == 1).
            self.tenant_id.fmt(f)
        }
    }
}

impl std::fmt::Debug for TenantShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Debug is the same as Display: the compact hex representation
        write!(f, "{}", self)
    }
}

impl std::str::FromStr for TenantShardId {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Expect format: 16 byte TenantId, '-', 1 byte shard number, 1 byte shard count
        if s.len() == 32 {
            // Legacy case: no shard specified
            Ok(Self {
                tenant_id: TenantId::from_str(s)?,
                shard_number: ShardNumber(0),
                shard_count: ShardCount(0),
            })
        } else if s.len() == 37 {
            let bytes = s.as_bytes();
            let tenant_id = TenantId::from_hex(&bytes[0..32])?;
            let mut shard_parts: [u8; 2] = [0u8; 2];
            hex::decode_to_slice(&bytes[33..37], &mut shard_parts)?;
            Ok(Self {
                tenant_id,
                shard_number: ShardNumber(shard_parts[0]),
                shard_count: ShardCount(shard_parts[1]),
            })
        } else {
            Err(hex::FromHexError::InvalidStringLength)
        }
    }
}

impl From<[u8; 18]> for TenantShardId {
    fn from(b: [u8; 18]) -> Self {
        let mut tenant_id_bytes: [u8; 16] = [0; 16];
        tenant_id_bytes.copy_from_slice(&b[0..16]);

        Self {
            tenant_id: TenantId::from(tenant_id_bytes),
            shard_number: ShardNumber(b[16]),
            shard_count: ShardCount(b[16]),
        }
    }
}

impl Serialize for TenantShardId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            let mut packed: [u8; 18] = [0; 18];
            packed[0..16].clone_from_slice(&self.tenant_id.as_arr());
            packed[16] = self.shard_number.0;
            packed[17] = self.shard_count.0;

            packed.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for TenantShardId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IdVisitor {
            is_human_readable_deserializer: bool,
        }

        impl<'de> serde::de::Visitor<'de> for IdVisitor {
            type Value = TenantShardId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                if self.is_human_readable_deserializer {
                    formatter.write_str("value in form of hex string")
                } else {
                    formatter.write_str("value in form of integer array([u8; 18])")
                }
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let s = serde::de::value::SeqAccessDeserializer::new(seq);
                let id: [u8; 18] = Deserialize::deserialize(s)?;
                Ok(TenantShardId::from(id))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                TenantShardId::from_str(v).map_err(E::custom)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(IdVisitor {
                is_human_readable_deserializer: true,
            })
        } else {
            deserializer.deserialize_tuple(
                16,
                IdVisitor {
                    is_human_readable_deserializer: false,
                },
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use utils::id::TenantId;

    use super::*;

    #[test]
    fn tenant_shard_id_string() -> Result<(), hex::FromHexError> {
        let example = TenantShardId {
            tenant_id: TenantId::generate(),
            shard_count: ShardCount(10),
            shard_number: ShardNumber(7),
        };

        let encoded = format!("{example}");

        let decoded = TenantShardId::from_str(&encoded)?;

        assert_eq!(example, decoded);

        Ok(())
    }

    #[test]
    fn tenant_shard_id_legacy() -> Result<(), hex::FromHexError> {
        let example = TenantId::generate();
        let encoded = format!("{example}");
        let decoded = TenantShardId::from_str(&encoded)?;

        assert_eq!(example, decoded.tenant_id);
        assert_eq!(decoded.shard_count, ShardCount(0));
        assert_eq!(decoded.shard_number, ShardNumber(0));

        Ok(())
    }
}
