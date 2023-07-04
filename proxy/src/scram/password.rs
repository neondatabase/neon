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

#[cfg(test)]
mod tests {
    use super::SaltedPassword;

    fn legacy_pbkdf2_impl(password: &[u8], salt: &[u8], iterations: u32) -> SaltedPassword {
        let one = 1_u32.to_be_bytes(); // magic

        let mut current = super::super::hmac_sha256(password, [salt, &one]);
        let mut result = current;
        for _ in 1..iterations {
            current = super::super::hmac_sha256(password, [current.as_ref()]);
            // TODO: result = current.zip(result).map(|(x, y)| x ^ y), issue #80094
            for (i, x) in current.iter().enumerate() {
                result[i] ^= x;
            }
        }

        result.into()
    }

    #[test]
    fn pbkdf2() {
        let password = "a-very-secure-password";
        let salt = "such-a-random-salt";
        let iterations = 4096;
        let output = [
            203, 18, 206, 81, 4, 154, 193, 100, 147, 41, 211, 217, 177, 203, 69, 210, 194, 211,
            101, 1, 248, 156, 96, 0, 8, 223, 30, 87, 158, 41, 20, 42,
        ];

        let actual = SaltedPassword::new(password.as_bytes(), salt.as_bytes(), iterations);
        let expected = legacy_pbkdf2_impl(password.as_bytes(), salt.as_bytes(), iterations);

        assert_eq!(actual.bytes, output);
        assert_eq!(actual.bytes, expected.bytes);
    }
}
