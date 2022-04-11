#![allow(unused)]

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
        let one = 1_u32.to_be_bytes(); // magic

        let mut current = super::hmac_sha256(password, [salt, &one]);
        let mut result = current;
        for _ in 1..iterations {
            current = super::hmac_sha256(password, [current.as_ref()]);
            // TODO: result = current.zip(result).map(|(x, y)| x ^ y), issue #80094
            for (i, x) in current.iter().enumerate() {
                result[i] ^= x;
            }
        }

        result.into()
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
