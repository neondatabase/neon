//! Password hashing routines.

use super::key::ScramKey;

pub const SALTED_PASSWORD_LEN: usize = 32;

/// Salted hashed password is essential for [key](super::key) derivation.
#[repr(transparent)]
pub struct SaltedPassword {
    bytes: [u8; SALTED_PASSWORD_LEN],
}

impl SaltedPassword {
    /// See `scram-common.c : scram_SaltedPassword` for details.
    /// Further reading: <https://datatracker.ietf.org/doc/html/rfc2898> (see `PBKDF2`).
    pub fn new(password: &[u8], salt: &[u8], iterations: u32) -> SaltedPassword {
        pbkdf2::pbkdf2_hmac_array::<sha2::Sha256, 32>(password, salt, iterations).into()
    }

    /// Derive `ClientKey` from a salted hashed password.
    pub fn client_key(&self) -> ScramKey {
        super::hmac_sha256(&self.bytes, [b"Client Key".as_ref()]).into()
    }

    /// Derive `ServerKey` from a salted hashed password.
    pub fn server_key(&self) -> ScramKey {
        super::hmac_sha256(&self.bytes, [b"Server Key".as_ref()]).into()
    }
}

impl From<[u8; SALTED_PASSWORD_LEN]> for SaltedPassword {
    #[inline(always)]
    fn from(bytes: [u8; SALTED_PASSWORD_LEN]) -> Self {
        Self { bytes }
    }
}
