//! Tools for client/server/stored key management.

use subtle::ConstantTimeEq;

/// Faithfully taken from PostgreSQL.
pub(crate) const SCRAM_KEY_LEN: usize = 32;

/// One of the keys derived from the user's password.
/// We use the same structure for all keys, i.e.
/// `ClientKey`, `StoredKey`, and `ServerKey`.
#[derive(Clone, Default, Eq, Debug)]
#[repr(transparent)]
pub(crate) struct ScramKey {
    bytes: [u8; SCRAM_KEY_LEN],
}

impl PartialEq for ScramKey {
    fn eq(&self, other: &Self) -> bool {
        self.ct_eq(other).into()
    }
}

impl ConstantTimeEq for ScramKey {
    fn ct_eq(&self, other: &Self) -> subtle::Choice {
        self.bytes.ct_eq(&other.bytes)
    }
}

impl ScramKey {
    pub(crate) fn sha256(&self) -> Self {
        super::sha256([self.as_ref()]).into()
    }

    pub(crate) fn as_bytes(&self) -> [u8; SCRAM_KEY_LEN] {
        self.bytes
    }
}

impl From<[u8; SCRAM_KEY_LEN]> for ScramKey {
    #[inline(always)]
    fn from(bytes: [u8; SCRAM_KEY_LEN]) -> Self {
        Self { bytes }
    }
}

impl AsRef<[u8]> for ScramKey {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
