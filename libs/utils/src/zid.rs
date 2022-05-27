use std::{fmt, str::FromStr};

use hex::FromHex;
use rand::Rng;
use serde::{Deserialize, Serialize};

/// Zenith ID is a 128-bit random ID.
/// Used to represent various identifiers. Provides handy utility methods and impls.
///
/// NOTE: It (de)serializes as an array of hex bytes, so the string representation would look
/// like `[173,80,132,115,129,226,72,254,170,201,135,108,199,26,228,24]`.
///
/// Use `#[serde_as(as = "DisplayFromStr")]` to (de)serialize it as hex string instead: `ad50847381e248feaac9876cc71ae418`.
/// Check the `serde_with::serde_as` documentation for options for more complex types.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
struct ZId([u8; 16]);

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
        f.write_str(&self.hex_encode())
    }
}

impl fmt::Debug for ZId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.hex_encode())
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
/// See [`ZId`] for alternative ways to serialize it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ZTimelineId(ZId);

zid_newtype!(ZTimelineId);

/// Zenith Tenant Id represents identifiar of a particular tenant.
/// Is used for distinguishing requests and data belonging to different users.
///
/// NOTE: It (de)serializes as an array of hex bytes, so the string representation would look
/// like `[173,80,132,115,129,226,72,254,170,201,135,108,199,26,228,24]`.
/// See [`ZId`] for alternative ways to serialize it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct ZTenantId(ZId);

zid_newtype!(ZTenantId);

// A pair uniquely identifying Zenith instance.
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
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
