//! Tools for client/server/stored keys management.

use sha2::{Digest, Sha256};

/// Faithfully taken from PostgreSQL.
pub const SCRAM_KEY_LEN: usize = 32;

/// Thin wrapper for byte array.
#[derive(Debug, PartialEq, Eq)]  // TODO maybe no debug? Avoid accidental logging.
#[repr(transparent)]
pub struct ScramKey {
    pub bytes: [u8; SCRAM_KEY_LEN],  // TODO does it have to be public?
}

impl ScramKey {
    pub fn sha256(&self) -> ScramKey {
        let mut bytes = [0u8; SCRAM_KEY_LEN];
        bytes.copy_from_slice({
            let mut hash = Sha256::new();
            hash.update(&self.bytes);
            hash.finalize().as_slice()
        });

        bytes.into()
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
