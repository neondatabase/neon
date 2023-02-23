//! Tools for SCRAM server secret management.

use super::{base64_decode_array, key::ScramKey, password::SaltedPassword};

/// Server secret is produced from [password](SaltedPassword)
/// and is used throughout the authentication process.
pub struct ServerSecret {
    /// Number of iterations for `PBKDF2` function.
    pub iterations: u32,
    /// Salt used to hash user's password.
    pub salt: Vec<u8>,
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
            salt: base64::decode(salt).ok()?,
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
            salt: mocked_salt.into(),
            stored_key: ScramKey::default(),
            server_key: ScramKey::default(),
            doomed: true,
        }
    }

    /// Check if this secret was derived from the given password.
    pub fn matches_password(&self, password: &[u8]) -> bool {
        let password = SaltedPassword::new(password, &self.salt, self.iterations);
        self.server_key == password.server_key()
    }

    /// Build a new server secret from the prerequisites.
    #[cfg(test)]
    pub fn build(password: &[u8], salt: &[u8], iterations: u32) -> Self {
        let password = SaltedPassword::new(password, salt, iterations);

        Self {
            iterations,
            salt: salt.into(),
            stored_key: password.client_key().sha256(),
            server_key: password.server_key(),
            doomed: false,
        }
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

        let secret = format!("SCRAM-SHA-256${iterations}:{salt}${stored_key}:{server_key}");

        let parsed = ServerSecret::parse(&secret).unwrap();
        assert_eq!(parsed.iterations, iterations);
        assert_eq!(base64::encode(parsed.salt), salt);

        assert_eq!(base64::encode(parsed.stored_key), stored_key);
        assert_eq!(base64::encode(parsed.server_key), server_key);
    }

    #[test]
    fn build_scram_secret() {
        let salt = b"salt";
        let secret = ServerSecret::build(b"password", salt, 4096);
        assert_eq!(secret.iterations, 4096);
        assert_eq!(secret.salt, salt);
        assert_eq!(
            base64::encode(secret.stored_key.as_ref()),
            "lF4cRm/Jky763CN4HtxdHnjV4Q8AWTNlKvGmEFFU8IQ="
        );
        assert_eq!(
            base64::encode(secret.server_key.as_ref()),
            "ub8OgRsftnk2ccDMOt7ffHXNcikRkQkq1lh4xaAqrSw="
        );
    }

    #[test]
    fn secret_match_password() {
        let password = b"password";
        let secret = ServerSecret::build(password, b"salt", 2);
        assert!(secret.matches_password(password));
        assert!(!secret.matches_password(b"different"));
    }
}
