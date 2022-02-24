use std::{fmt, str::FromStr};

use hex::FromHex;
use rand::Rng;
use serde::{
    de::{self, Visitor},
    Deserialize, Serialize,
};

macro_rules! mutual_from {
    ($id1:ident, $id2:ident) => {
        impl From<$id1> for $id2 {
            fn from(id1: $id1) -> Self {
                Self(id1.0.into())
            }
        }

        impl From<$id2> for $id1 {
            fn from(id2: $id2) -> Self {
                Self(id2.0.into())
            }
        }
    };
}

/// Zenith ID is a 128-bit random ID.
/// Used to represent various identifiers. Provides handy utility methods and impls.
///
/// NOTE: It (de)serializes as an array of hex bytes, so the string representation would look
/// like `[173,80,132,115,129,226,72,254,170,201,135,108,199,26,228,24]`.
/// Use [`HexZId`] to serialize it as hex string instead: `ad50847381e248feaac9876cc71ae418`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
struct ZId([u8; 16]);

/// [`ZId`] version that serializes and deserializes as a hex string.
/// Useful for various json serializations, where hex byte array from original id is not convenient.
///
/// Plain `ZId` could be (de)serialized into hex string with `#[serde(with = "hex")]` attribute.
/// This however won't work on nested types like `Option<ZId>` or `Vec<ZId>`, see https://github.com/serde-rs/serde/issues/723 for the details.
/// Every separate type currently needs a new (de)serializing method for every type separately.
///
/// To provide a generic way to serialize the ZId as a hex string where `#[serde(with = "hex")]` is not enough, this wrapper is created.
/// The default wrapper serialization is left unchanged due to
/// * byte array (de)serialization being faster and simpler
/// * byte deserialization being used in Safekeeper already, with those bytes coming from compute (see `ProposerGreeting` in safekeeper)
/// * current `HexZId`'s deserialization impl breaks on compute byte array deserialization, having it by default is dangerous
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct HexZId([u8; 16]);

impl Serialize for HexZId {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        hex::encode(self.0).serialize(ser)
    }
}

impl<'de> Deserialize<'de> for HexZId {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        de.deserialize_bytes(HexVisitor)
    }
}

struct HexVisitor;

impl<'de> Visitor<'de> for HexVisitor {
    type Value = HexZId;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "A hexadecimal representation of a 128-bit random Zenith ID"
        )
    }

    fn visit_bytes<E>(self, hex_bytes: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        ZId::from_hex(hex_bytes)
            .map(HexZId::from)
            .map_err(de::Error::custom)
    }

    fn visit_str<E>(self, hex_bytes_str: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Self::visit_bytes(self, hex_bytes_str.as_bytes())
    }
}

mutual_from!(ZId, HexZId);

impl ZId {
    pub fn get_from_buf(buf: &mut dyn bytes::Buf) -> ZId {
        let mut arr = [0u8; 16];
        buf.copy_to_slice(&mut arr);
        ZId::from(arr)
    }

    pub fn as_arr(&self) -> [u8; 16] {
        self.0
    }

    pub fn generate() -> Self {
        let mut tli_buf = [0u8; 16];
        rand::thread_rng().fill(&mut tli_buf);
        ZId::from(tli_buf)
    }
}

impl FromStr for ZId {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<ZId, Self::Err> {
        Self::from_hex(s)
    }
}

// this is needed for pretty serialization and deserialization of ZId's using serde integration with hex crate
impl FromHex for ZId {
    type Error = hex::FromHexError;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        let mut buf: [u8; 16] = [0u8; 16];
        hex::decode_to_slice(hex, &mut buf)?;
        Ok(ZId(buf))
    }
}

impl AsRef<[u8]> for ZId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<[u8; 16]> for ZId {
    fn from(b: [u8; 16]) -> Self {
        ZId(b)
    }
}

impl fmt::Display for ZId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for ZId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

macro_rules! zid_newtype {
    ($t:ident) => {
        impl $t {
            pub fn get_from_buf(buf: &mut dyn bytes::Buf) -> $t {
                $t(ZId::get_from_buf(buf))
            }

            pub fn as_arr(&self) -> [u8; 16] {
                self.0.as_arr()
            }

            pub fn generate() -> Self {
                $t(ZId::generate())
            }

            pub const fn from_array(b: [u8; 16]) -> Self {
                $t(ZId(b))
            }
        }

        impl FromStr for $t {
            type Err = hex::FromHexError;

            fn from_str(s: &str) -> Result<$t, Self::Err> {
                let value = ZId::from_str(s)?;
                Ok($t(value))
            }
        }

        impl From<[u8; 16]> for $t {
            fn from(b: [u8; 16]) -> Self {
                $t(ZId::from(b))
            }
        }

        impl FromHex for $t {
            type Error = hex::FromHexError;

            fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
                Ok($t(ZId::from_hex(hex)?))
            }
        }

        impl AsRef<[u8]> for $t {
            fn as_ref(&self) -> &[u8] {
                &self.0 .0
            }
        }

        impl fmt::Display for $t {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl fmt::Debug for $t {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

/// Zenith timeline IDs are different from PostgreSQL timeline
/// IDs. They serve a similar purpose though: they differentiate
/// between different "histories" of the same cluster.  However,
/// PostgreSQL timeline IDs are a bit cumbersome, because they are only
/// 32-bits wide, and they must be in ascending order in any given
/// timeline history.  Those limitations mean that we cannot generate a
/// new PostgreSQL timeline ID by just generating a random number. And
/// that in turn is problematic for the "pull/push" workflow, where you
/// have a local copy of a zenith repository, and you periodically sync
/// the local changes with a remote server. When you work "detached"
/// from the remote server, you cannot create a PostgreSQL timeline ID
/// that's guaranteed to be different from all existing timelines in
/// the remote server. For example, if two people are having a clone of
/// the repository on their laptops, and they both create a new branch
/// with different name. What timeline ID would they assign to their
/// branches? If they pick the same one, and later try to push the
/// branches to the same remote server, they will get mixed up.
///
/// To avoid those issues, Zenith has its own concept of timelines that
/// is separate from PostgreSQL timelines, and doesn't have those
/// limitations. A zenith timeline is identified by a 128-bit ID, which
/// is usually printed out as a hex string.
///
/// NOTE: It (de)serializes as an array of hex bytes, so the string representation would look
/// like `[173,80,132,115,129,226,72,254,170,201,135,108,199,26,228,24]`.
/// Use [`HexZTimelineId`] to serialize it as hex string instead: `ad50847381e248feaac9876cc71ae418`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ZTimelineId(ZId);

/// A [`ZTimelineId`] version that gets (de)serialized as a hex string.
/// Use in complex types, where `#[serde(with = "hex")]` does not work.
/// See [`HexZId`] for more details.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct HexZTimelineId(HexZId);

impl std::fmt::Debug for HexZTimelineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        ZTimelineId::from(*self).fmt(f)
    }
}

impl std::fmt::Display for HexZTimelineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        ZTimelineId::from(*self).fmt(f)
    }
}

impl FromStr for HexZTimelineId {
    type Err = <ZTimelineId as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(HexZTimelineId::from(ZTimelineId::from_str(s)?))
    }
}

zid_newtype!(ZTimelineId);
mutual_from!(ZTimelineId, HexZTimelineId);

/// Zenith Tenant Id represents identifiar of a particular tenant.
/// Is used for distinguishing requests and data belonging to different users.
///
/// NOTE: It (de)serializes as an array of hex bytes, so the string representation would look
/// like `[173,80,132,115,129,226,72,254,170,201,135,108,199,26,228,24]`.
/// Use [`HexZTenantId`] to serialize it as hex string instead: `ad50847381e248feaac9876cc71ae418`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct ZTenantId(ZId);

/// A [`ZTenantId`] version that gets (de)serialized as a hex string.
/// Use in complex types, where `#[serde(with = "hex")]` does not work.
/// See [`HexZId`] for more details.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct HexZTenantId(HexZId);

impl std::fmt::Debug for HexZTenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        ZTenantId::from(*self).fmt(f)
    }
}

impl std::fmt::Display for HexZTenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        ZTenantId::from(*self).fmt(f)
    }
}

impl FromStr for HexZTenantId {
    type Err = <ZTenantId as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(HexZTenantId::from(ZTenantId::from_str(s)?))
    }
}

zid_newtype!(ZTenantId);
mutual_from!(ZTenantId, HexZTenantId);

// A pair uniquely identifying Zenith instance.
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ZTenantTimelineId {
    pub tenant_id: ZTenantId,
    pub timeline_id: ZTimelineId,
}

impl ZTenantTimelineId {
    pub fn new(tenant_id: ZTenantId, timeline_id: ZTimelineId) -> Self {
        ZTenantTimelineId {
            tenant_id,
            timeline_id,
        }
    }

    pub fn generate() -> Self {
        Self::new(ZTenantId::generate(), ZTimelineId::generate())
    }

    pub fn empty() -> Self {
        Self::new(ZTenantId::from([0u8; 16]), ZTimelineId::from([0u8; 16]))
    }
}

impl fmt::Display for ZTenantTimelineId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}-{}", self.tenant_id, self.timeline_id)
    }
}

// Unique ID of a storage node (safekeeper or pageserver). Supposed to be issued
// by the console.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ZNodeId(pub u64);

impl fmt::Display for ZNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Display;

    use super::*;
    use hex::FromHexError;
    use hex_literal::hex;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestStruct<E: Display, T: FromStr<Err = E> + Display> {
        field: Option<T>,
    }

    #[test]
    fn test_hex_serializations_tenant_id() {
        let original_struct = TestStruct {
            field: Some(HexZTenantId::from(ZTenantId::from_array(hex!(
                "11223344556677881122334455667788"
            )))),
        };

        let serialized_string = serde_json::to_string(&original_struct).unwrap();
        assert_eq!(
            serialized_string,
            r#"{"field":"11223344556677881122334455667788"}"#
        );

        let deserialized_struct: TestStruct<FromHexError, HexZTenantId> =
            serde_json::from_str(&serialized_string).unwrap();
        assert_eq!(original_struct, deserialized_struct);
    }

    #[test]
    fn test_hex_serializations_timeline_id() {
        let original_struct = TestStruct {
            field: Some(HexZTimelineId::from(ZTimelineId::from_array(hex!(
                "AA223344556677881122334455667788"
            )))),
        };

        let serialized_string = serde_json::to_string(&original_struct).unwrap();
        assert_eq!(
            serialized_string,
            r#"{"field":"aa223344556677881122334455667788"}"#
        );

        let deserialized_struct: TestStruct<FromHexError, HexZTimelineId> =
            serde_json::from_str(&serialized_string).unwrap();
        assert_eq!(original_struct, deserialized_struct);
    }
}
