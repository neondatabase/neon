use std::{fmt, str::FromStr};

use hex::FromHex;
use rand::Rng;
use serde::{Deserialize, Serialize};

// Zenith ID is a 128-bit random ID.
// Used to represent various identifiers. Provides handy utility methods and impls.
// TODO (LizardWizzard) figure out best way to remove boiler plate with trait impls caused by newtype pattern
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
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

        impl fmt::Display for $t {
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ZTimelineId(ZId);

zid_newtype!(ZTimelineId);

// Zenith Tenant Id represents identifiar of a particular tenant.
// Is used for distinguishing requests and data belonging to different users.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct ZTenantId(ZId);

zid_newtype!(ZTenantId);

// for now the following impls are used only with ZTenantId,
// if this impls become useful in other newtypes they can be moved under zid_newtype macro too
impl FromHex for ZTenantId {
    type Error = hex::FromHexError;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        Ok(ZTenantId(ZId::from_hex(hex)?))
    }
}

impl AsRef<[u8]> for ZTenantId {
    fn as_ref(&self) -> &[u8] {
        &self.0 .0
    }
}
