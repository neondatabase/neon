use std::{fmt, str::FromStr};

use hex::FromHex;
use rand::Rng;
use serde::{Deserialize, Serialize};

// Zenith ID is a 128-bit random ID.
// Used to represent various identifiers. Provides handy utility methods and impls.
macro_rules! zid_newtype {
    // $attr meta argument is for attaching the doc string (which is in fact an
    // attribute) to the type declaration.
    // $typname is name of the generated type
    ($(#[$attr:meta])* => $typname:ident) => {
        $(#[$attr])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
        pub struct $typname([u8; 16]);

        impl $typname {
            pub fn get_from_buf(buf: &mut dyn bytes::Buf) -> $typname {
                let mut arr = [0u8; 16];
                buf.copy_to_slice(&mut arr);
                $typname::from(arr)
            }

            pub fn as_arr(&self) -> [u8; 16] {
                self.0
            }

            pub fn generate() -> Self {
                let mut tli_buf = [0u8; 16];
                rand::thread_rng().fill(&mut tli_buf);
                $typname::from(tli_buf)
            }

            pub const fn from_array(b: [u8; 16]) -> Self {
                $typname(b)
            }

            pub fn empty() -> Self {
                $typname([0u8; 16])
            }
        }

        impl FromStr for $typname {
            type Err = hex::FromHexError;

            fn from_str(s: &str) -> Result<$typname, Self::Err> {
                Self::from_hex(s)
            }
        }

        // this is needed for pretty serialization and deserialization of ZId's using serde integration with hex crate
        impl FromHex for $typname {
            type Error = hex::FromHexError;

            fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
                let mut buf: [u8; 16] = [0u8; 16];
                hex::decode_to_slice(hex, &mut buf)?;
                Ok($typname(buf))
            }
        }

        impl AsRef<[u8]> for $typname {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }

        impl From<[u8; 16]> for $typname {
            fn from(b: [u8; 16]) -> Self {
                $typname(b)
            }
        }

        impl fmt::Display for $typname {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&hex::encode(self.0))
            }
        }

        impl fmt::Debug for $typname {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&hex::encode(self.0))
            }
        }
    };
}

zid_newtype!(
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
    => ZTimelineId
);

zid_newtype!(
    // Zenith Tenant Id represents identifier of a particular tenant.
    // Is used for distinguishing requests and data belonging to different users.
    => ZTenantId
);

/// Serde routines for Option<T> (de)serialization, using `T:Display` representations for inner values.
/// Useful for Option<ZTenantId> and Option<ZTimelineId> to get their hex representations into serialized string and deserialize them back.
pub mod opt_display_serde {
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
    use std::fmt::Display;

    use super::*;
    use hex::FromHexError;
    use hex_literal::hex;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestStruct<E: Display, T: FromStr<Err = E> + Display> {
        #[serde(with = "opt_display_serde")]
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
}
