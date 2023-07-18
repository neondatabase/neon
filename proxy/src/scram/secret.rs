//! Tools for SCRAM server secret management.

use super::base64_decode_array;
use super::key::ScramKey;

/// Server secret is produced from [password](super::password::SaltedPassword)
/// and is used throughout the authentication process.
pub struct ServerSecret {
    /// Number of iterations for `PBKDF2` function.
    pub iterations: u32,
    /// Salt used to hash user's password.
    pub salt_base64: String,
    /// Hashed `ClientKey`.
    pub stored_key: ScramKey,
    /// Used by client to verify server's signature.
    pub server_key: ScramKey,
    /// Should auth fail no matter what?
    /// This is exactly the case for mocked secrets.
    pub doomed: bool,
}

impl ServerSecret {
    pub fn parse(input: &str) -> Option<Self> {
        // SCRAM-SHA-256$<iterations>:<salt>$<storedkey>:<serverkey>
        let s = input.strip_prefix("SCRAM-SHA-256$")?;
        let (params, keys) = s.split_once('$')?;

        let ((iterations, salt), (stored_key, server_key)) =
            params.split_once(':').zip(keys.split_once(':'))?;

        let secret = ServerSecret {
            iterations: iterations.parse().ok()?,
            salt_base64: salt.to_owned(),
            stored_key: base64_decode_array(stored_key)?.into(),
            server_key: base64_decode_array(server_key)?.into(),
            doomed: false,
        };

        Some(secret)
    }

    /// To avoid revealing information to an attacker, we use a
    /// mocked server secret even if the user doesn't exist.
    /// See `auth-scram.c : mock_scram_secret` for details.
    pub fn mock(user: &str, nonce: [u8; 32]) -> Self {
        // Refer to `auth-scram.c : scram_mock_salt`.
        let mocked_salt = super::sha256([user.as_bytes(), &nonce]);

        Self {
            iterations: 4096,
            salt_base64: base64::encode(mocked_salt),
            stored_key: ScramKey::default(),
            server_key: ScramKey::default(),
            doomed: true,
        }
    }

    /// Build a new server secret from the prerequisites.
    /// XXX: We only use this function in tests.
    #[cfg(test)]
    pub fn build(password: &str) -> Option<Self> {
        Self::parse(&postgres_protocol::password::scram_sha_256(
            password.as_bytes(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_scram_secret() {
        let iterations = 4096;
        let salt = "+/tQQax7twvwTj64mjBsxQ==";
        let stored_key = "D5h6KTMBlUvDJk2Y8ELfC1Sjtc6k9YHjRyuRZyBNJns=";
        let server_key = "Pi3QHbcluX//NDfVkKlFl88GGzlJ5LkyPwcdlN/QBvI=";

        let secret = format!(
            "SCRAM-SHA-256${iterations}:{salt}${stored_key}:{server_key}",
            iterations = iterations,
            salt = salt,
            stored_key = stored_key,
            server_key = server_key,
        );

        let parsed = ServerSecret::parse(&secret).unwrap();
        assert_eq!(parsed.iterations, iterations);
        assert_eq!(parsed.salt_base64, salt);

        assert_eq!(base64::encode(parsed.stored_key), stored_key);
        assert_eq!(base64::encode(parsed.server_key), server_key);
    }
}
