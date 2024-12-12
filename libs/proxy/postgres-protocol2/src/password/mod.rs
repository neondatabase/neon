//! Functions to encrypt a password in the client.
//!
//! This is intended to be used by client applications that wish to
//! send commands like `ALTER USER joe PASSWORD 'pwd'`. The password
//! need not be sent in cleartext if it is encrypted on the client
//! side. This is good because it ensures the cleartext password won't
//! end up in logs pg_stat displays, etc.

use crate::authentication::sasl;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::digest::FixedOutput;
use sha2::{Digest, Sha256};

#[cfg(test)]
mod test;

const SCRAM_DEFAULT_ITERATIONS: u32 = 4096;
const SCRAM_DEFAULT_SALT_LEN: usize = 16;

/// Hash password using SCRAM-SHA-256 with a randomly-generated
/// salt.
///
/// The client may assume the returned string doesn't contain any
/// special characters that would require escaping in an SQL command.
pub async fn scram_sha_256(password: &[u8]) -> String {
    let mut salt: [u8; SCRAM_DEFAULT_SALT_LEN] = [0; SCRAM_DEFAULT_SALT_LEN];
    let mut rng = rand::thread_rng();
    rng.fill_bytes(&mut salt);
    scram_sha_256_salt(password, salt).await
}

// Internal implementation of scram_sha_256 with a caller-provided
// salt. This is useful for testing.
pub(crate) async fn scram_sha_256_salt(
    password: &[u8],
    salt: [u8; SCRAM_DEFAULT_SALT_LEN],
) -> String {
    // Prepare the password, per [RFC
    // 4013](https://tools.ietf.org/html/rfc4013), if possible.
    //
    // Postgres treats passwords as byte strings (without embedded NUL
    // bytes), but SASL expects passwords to be valid UTF-8.
    //
    // Follow the behavior of libpq's PQencryptPasswordConn(), and
    // also the backend. If the password is not valid UTF-8, or if it
    // contains prohibited characters (such as non-ASCII whitespace),
    // just skip the SASLprep step and use the original byte
    // sequence.
    let prepared: Vec<u8> = match std::str::from_utf8(password) {
        Ok(password_str) => {
            match stringprep::saslprep(password_str) {
                Ok(p) => p.into_owned().into_bytes(),
                // contains invalid characters; skip saslprep
                Err(_) => Vec::from(password),
            }
        }
        // not valid UTF-8; skip saslprep
        Err(_) => Vec::from(password),
    };

    // salt password
    let salted_password = sasl::hi(&prepared, &salt, SCRAM_DEFAULT_ITERATIONS).await;

    // client key
    let mut hmac = Hmac::<Sha256>::new_from_slice(&salted_password)
        .expect("HMAC is able to accept all key sizes");
    hmac.update(b"Client Key");
    let client_key = hmac.finalize().into_bytes();

    // stored key
    let mut hash = Sha256::default();
    hash.update(client_key.as_slice());
    let stored_key = hash.finalize_fixed();

    // server key
    let mut hmac = Hmac::<Sha256>::new_from_slice(&salted_password)
        .expect("HMAC is able to accept all key sizes");
    hmac.update(b"Server Key");
    let server_key = hmac.finalize().into_bytes();

    format!(
        "SCRAM-SHA-256${}:{}${}:{}",
        SCRAM_DEFAULT_ITERATIONS,
        base64::encode(salt),
        base64::encode(stored_key),
        base64::encode(server_key)
    )
}
