//! Tools for SCRAM server secret management.

use super::base64_decode_array;
use super::key::ScramKey;

#[derive(Debug)]
pub struct ServerSecret {
    pub iterations: u32,
    pub salt_base64: String,
    pub stored_key: ScramKey,
    pub server_key: ScramKey,
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
        };

        Some(secret)
    }

    pub fn mock() -> Self {
        todo!("see auth-scram.c : mock_scram_secret")
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
        assert_eq!(parsed.salt_base64, salt);

        // TODO: derive from 'password'
        assert_eq!(base64::encode(parsed.stored_key), stored_key);
        assert_eq!(base64::encode(parsed.server_key), server_key);
    }
}
