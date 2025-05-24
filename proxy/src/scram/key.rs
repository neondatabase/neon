//! Tools for client/server/stored key management.

use hmac::Mac;
use subtle::ConstantTimeEq;
use x509_cert::der::zeroize::Zeroize;

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

impl Drop for ScramKey {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
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

    pub(crate) fn client_key(b: &[u8; 32]) -> Self {
        let mut hmac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b)
            .expect("HMAC is able to accept all key sizes");
        hmac.update(b"Client Key");
        let client_key: [u8; 32] = hmac.finalize().into_bytes().into();
        client_key.into()
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
