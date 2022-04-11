//! Tools for client/server/stored key management.

/// Faithfully taken from PostgreSQL.
pub const SCRAM_KEY_LEN: usize = 32;

/// One of the keys derived from the [password](super::password::SaltedPassword).
/// We use the same structure for all keys, i.e.
/// `ClientKey`, `StoredKey`, and `ServerKey`.
#[derive(Default, PartialEq, Eq)]
#[repr(transparent)]
pub struct ScramKey {
    bytes: [u8; SCRAM_KEY_LEN],
}

impl ScramKey {
    pub fn sha256(&self) -> Self {
        super::sha256([self.as_ref()]).into()
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
