use std::{fmt, str::FromStr};

use hex::FromHex;
use rand::Rng;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IdError {
    #[error("invalid id length {0}")]
    VecParseError(usize),
}

/// Neon ID is a 128-bit random ID.
/// Used to represent various identifiers. Provides handy utility methods and impls.
///
/// NOTE: It (de)serializes as an array of hex bytes, so the string representation would look
/// like `[173,80,132,115,129,226,72,254,170,201,135,108,199,26,228,24]`.
///
/// Use `#[serde_as(as = "DisplayFromStr")]` to (de)serialize it as hex string instead: `ad50847381e248feaac9876cc71ae418`.
/// Check the `serde_with::serde_as` documentation for options for more complex types.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
struct Id([u8; 16]);

impl Id {
    pub fn get_from_buf(buf: &mut dyn bytes::Buf) -> Id {
        let mut arr = [0u8; 16];
        buf.copy_to_slice(&mut arr);
        Id::from(arr)
    }

    pub fn from_vec(src: &Vec<u8>) -> Result<Id, IdError> {
        if src.len() != 16 {
            return Err(IdError::VecParseError(src.len()));
        }
        let mut zid_slice = [0u8; 16];
        zid_slice.copy_from_slice(&src);
        Ok(zid_slice.into())
    }

    pub fn as_arr(&self) -> [u8; 16] {
        self.0
    }

    pub fn generate() -> Self {
        let mut tli_buf = [0u8; 16];
        rand::thread_rng().fill(&mut tli_buf);
        Id::from(tli_buf)
    }

    fn hex_encode(&self) -> String {
        static HEX: &[u8] = b"0123456789abcdef";

        let mut buf = vec![0u8; self.0.len() * 2];
        for (&b, chunk) in self.0.as_ref().iter().zip(buf.chunks_exact_mut(2)) {
            chunk[0] = HEX[((b >> 4) & 0xf) as usize];
            chunk[1] = HEX[(b & 0xf) as usize];
        }
        unsafe { String::from_utf8_unchecked(buf) }
    }
}

impl FromStr for Id {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<Id, Self::Err> {
        Self::from_hex(s)
    }
}

// this is needed for pretty serialization and deserialization of Id's using serde integration with hex crate
impl FromHex for Id {
    type Error = hex::FromHexError;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        let mut buf: [u8; 16] = [0u8; 16];
        hex::decode_to_slice(hex, &mut buf)?;
        Ok(Id(buf))
    }
}

impl AsRef<[u8]> for Id {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<[u8; 16]> for Id {
    fn from(b: [u8; 16]) -> Self {
        Id(b)
    }
}

impl From<Id> for u128 {
    fn from(id: Id) -> Self {
        u128::from_le_bytes(id.0)
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.hex_encode())
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.hex_encode())
    }
}

macro_rules! id_newtype {
    ($t:ident) => {
        impl $t {
            pub fn get_from_buf(buf: &mut dyn bytes::Buf) -> $t {
                $t(Id::get_from_buf(buf))
            }

            pub fn from_vec(src: &Vec<u8>) -> Result<$t, IdError> {
                Ok($t(Id::from_vec(src)?))
            }

            pub fn as_arr(&self) -> [u8; 16] {
                self.0.as_arr()
            }

            pub fn generate() -> Self {
                $t(Id::generate())
            }

            pub const fn from_array(b: [u8; 16]) -> Self {
                $t(Id(b))
            }
        }

        impl FromStr for $t {
            type Err = hex::FromHexError;

            fn from_str(s: &str) -> Result<$t, Self::Err> {
                let value = Id::from_str(s)?;
                Ok($t(value))
            }
        }

        impl From<[u8; 16]> for $t {
            fn from(b: [u8; 16]) -> Self {
                $t(Id::from(b))
            }
        }

        impl FromHex for $t {
            type Error = hex::FromHexError;

            fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
                Ok($t(Id::from_hex(hex)?))
            }
        }

        impl AsRef<[u8]> for $t {
            fn as_ref(&self) -> &[u8] {
                &self.0 .0
            }
        }

        impl From<$t> for u128 {
            fn from(id: $t) -> Self {
                u128::from(id.0)
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

/// Neon timeline IDs are different from PostgreSQL timeline
/// IDs. They serve a similar purpose though: they differentiate
/// between different "histories" of the same cluster.  However,
/// PostgreSQL timeline IDs are a bit cumbersome, because they are only
/// 32-bits wide, and they must be in ascending order in any given
/// timeline history.  Those limitations mean that we cannot generate a
/// new PostgreSQL timeline ID by just generating a random number. And
/// that in turn is problematic for the "pull/push" workflow, where you
/// have a local copy of a Neon repository, and you periodically sync
/// the local changes with a remote server. When you work "detached"
/// from the remote server, you cannot create a PostgreSQL timeline ID
/// that's guaranteed to be different from all existing timelines in
/// the remote server. For example, if two people are having a clone of
/// the repository on their laptops, and they both create a new branch
/// with different name. What timeline ID would they assign to their
/// branches? If they pick the same one, and later try to push the
/// branches to the same remote server, they will get mixed up.
///
/// To avoid those issues, Neon has its own concept of timelines that
/// is separate from PostgreSQL timelines, and doesn't have those
/// limitations. A Neon timeline is identified by a 128-bit ID, which
/// is usually printed out as a hex string.
///
/// NOTE: It (de)serializes as an array of hex bytes, so the string representation would look
/// like `[173,80,132,115,129,226,72,254,170,201,135,108,199,26,228,24]`.
/// See [`Id`] for alternative ways to serialize it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct TimelineId(Id);

id_newtype!(TimelineId);

/// Neon Tenant Id represents identifiar of a particular tenant.
/// Is used for distinguishing requests and data belonging to different users.
///
/// NOTE: It (de)serializes as an array of hex bytes, so the string representation would look
/// like `[173,80,132,115,129,226,72,254,170,201,135,108,199,26,228,24]`.
/// See [`Id`] for alternative ways to serialize it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct TenantId(Id);

id_newtype!(TenantId);

// A pair uniquely identifying Neon instance.
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantTimelineId {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
}

impl TenantTimelineId {
    pub fn new(tenant_id: TenantId, timeline_id: TimelineId) -> Self {
        TenantTimelineId {
            tenant_id,
            timeline_id,
        }
    }

    pub fn generate() -> Self {
        Self::new(TenantId::generate(), TimelineId::generate())
    }

    pub fn empty() -> Self {
        Self::new(TenantId::from([0u8; 16]), TimelineId::from([0u8; 16]))
    }
}

impl fmt::Display for TenantTimelineId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.tenant_id, self.timeline_id)
    }
}

// Unique ID of a storage node (safekeeper or pageserver). Supposed to be issued
// by the console.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
