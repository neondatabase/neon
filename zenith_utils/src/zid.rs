use std::{fmt, str::FromStr};

use hex::FromHex;
use rand::Rng;
use serde::{Deserialize, Serialize};

// Zenith ID is a 128-bit random ID.
// Used to represent various identifiers. Provides handy utility methods and impls.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
struct ZId<const LENGTH: usize>(#[serde(with = "serde_const_length_array")] [u8; LENGTH]);

impl<const LENGTH: usize> ZId<LENGTH> {
    pub fn get_from_buf(buf: &mut dyn bytes::Buf) -> ZId<LENGTH> {
        let mut arr = [0u8; LENGTH];
        buf.copy_to_slice(&mut arr);
        ZId::from(arr)
    }

    pub fn as_arr(&self) -> [u8; LENGTH] {
        self.0
    }

    pub fn generate() -> Self {
        let mut tli_buf = [0u8; LENGTH];
        rand::thread_rng().fill(&mut tli_buf[..]); // in 1.57+ can be tli_buf.as_mut_slice()
        ZId::from(tli_buf)
    }
}

impl<const LENGTH: usize> FromStr for ZId<LENGTH> {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<ZId<LENGTH>, Self::Err> {
        Self::from_hex(s)
    }
}

// this is needed for pretty serialization and deserialization of ZId's using serde integration with hex crate
impl<const LENGTH: usize> FromHex for ZId<LENGTH> {
    type Error = hex::FromHexError;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        let mut buf: [u8; LENGTH] = [0u8; LENGTH];
        hex::decode_to_slice(hex, &mut buf)?;
        Ok(ZId(buf))
    }
}

impl<const LENGTH: usize> AsRef<[u8]> for ZId<LENGTH> {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<const LENGTH: usize> From<[u8; LENGTH]> for ZId<LENGTH> {
    fn from(b: [u8; LENGTH]) -> Self {
        ZId(b)
    }
}

impl<const LENGTH: usize> fmt::Display for ZId<LENGTH> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl<const LENGTH: usize> fmt::Debug for ZId<LENGTH> {
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
#[derive(Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ZTimelineId(ZId<16>);

zid_newtype!(ZTimelineId);

// Zenith Tenant Id represents identifiar of a particular tenant.
// Is used for distinguishing requests and data belonging to different users.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct ZTenantId(ZId<16>);

zid_newtype!(ZTenantId);

/// Serde routines for Option<T> (de)serialization, using `T:Display` representations for inner values.
/// Useful for Option<ZTenantId> and Option<ZTimelineId> to get their hex representations into serialized string and deserialize them back.
pub mod serde_opt_display {
    use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
    use std::{fmt::Display, str::FromStr};

    pub fn serialize<S, Id>(id: &Option<Id>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        Id: Display,
    {
        id.as_ref().map(ToString::to_string).serialize(ser)
    }

    pub fn deserialize<'de, D, Id>(des: D) -> Result<Option<Id>, D::Error>
    where
        D: Deserializer<'de>,
        Id: FromStr,
        <Id as FromStr>::Err: Display,
    {
        Ok(if let Some(s) = Option::<String>::deserialize(des)? {
            Some(Id::from_str(&s).map_err(de::Error::custom)?)
        } else {
            None
        })
    }
}

/// Serde currently does not support serialization of arrays with length expressed as const generic
/// See https://github.com/serde-rs/serde/issues/1937. This code comes from described workaround
pub mod serde_const_length_array {
    use std::{convert::TryInto, marker::PhantomData};

    use serde::{
        de::{SeqAccess, Visitor},
        ser::SerializeTuple,
        Deserialize, Deserializer, Serialize, Serializer,
    };
    pub fn serialize<S: Serializer, T: Serialize, const N: usize>(
        data: &[T; N],
        ser: S,
    ) -> Result<S::Ok, S::Error> {
        let mut s = ser.serialize_tuple(N)?;
        for item in data {
            s.serialize_element(item)?;
        }
        s.end()
    }

    struct ArrayVisitor<T, const N: usize>(PhantomData<T>);

    impl<'de, T, const N: usize> Visitor<'de> for ArrayVisitor<T, N>
    where
        T: Deserialize<'de>,
    {
        type Value = [T; N];

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str(&format!("an array of length {}", N))
        }

        #[inline]
        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            // can be optimized using MaybeUninit
            let mut data = Vec::with_capacity(N);
            for _ in 0..N {
                match (seq.next_element())? {
                    Some(val) => data.push(val),
                    None => return Err(serde::de::Error::invalid_length(N, &self)),
                }
            }
            match data.try_into() {
                Ok(arr) => Ok(arr),
                Err(_) => unreachable!(),
            }
        }
    }
    pub fn deserialize<'de, D, T, const N: usize>(deserializer: D) -> Result<[T; N], D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        deserializer.deserialize_tuple(N, ArrayVisitor::<T, N>(PhantomData))
    }
}

// A pair uniquely identifying Zenith instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
}

impl fmt::Display for ZTenantTimelineId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}-{}", self.tenant_id, self.timeline_id)
    }
}

#[cfg(test)]
mod tests {
    use std::{fmt::Display, io::Cursor};

    use super::*;
    use crate::bin_ser::BeSer;
    use hex::FromHexError;
    use hex_literal::hex;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestStruct<E: Display, T: FromStr<Err = E> + Display> {
        #[serde(with = "serde_opt_display")]
        field: Option<T>,
    }

    #[test]
    fn test_hex_serializations_tenant_id() {
        let original_struct = TestStruct {
            field: Some(ZTenantId::from_array(hex!(
                "11223344556677881122334455667788"
            ))),
        };

        let serialized_string = serde_json::to_string(&original_struct).unwrap();
        assert_eq!(
            serialized_string,
            r#"{"field":"11223344556677881122334455667788"}"#
        );

        let deserialized_struct: TestStruct<FromHexError, ZTenantId> =
            serde_json::from_str(&serialized_string).unwrap();
        assert_eq!(original_struct, deserialized_struct);
    }

    #[test]
    fn test_hex_serializations_timeline_id() {
        let original_struct = TestStruct {
            field: Some(ZTimelineId::from_array(hex!(
                "AA223344556677881122334455667788"
            ))),
        };

        let serialized_string = serde_json::to_string(&original_struct).unwrap();
        assert_eq!(
            serialized_string,
            r#"{"field":"aa223344556677881122334455667788"}"#
        );

        let deserialized_struct: TestStruct<FromHexError, ZTimelineId> =
            serde_json::from_str(&serialized_string).unwrap();
        assert_eq!(original_struct, deserialized_struct);
    }

    #[test]
    fn test_const_generic_workaround() {
        let zid = ZTimelineId::generate();
        let mut buf = Cursor::new(Vec::with_capacity(16));
        zid.ser_into(&mut buf)
            .expect("serialization should succeed");

        buf.set_position(0);
        let new_zid = ZTimelineId::des_from(&mut buf).expect("deserialization should succeed");
        assert_eq!(zid, new_zid)
    }
}
