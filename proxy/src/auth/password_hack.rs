//! Payload for ad hoc authentication method for clients that don't support SNI.
//! See the `impl` for [`super::backend::BackendType<ClientCredentials>`].
//! Read more: <https://github.com/neondatabase/cloud/issues/1620#issuecomment-1165332290>.

use serde::{Deserialize, Deserializer};

#[derive(Deserialize)]
pub struct PasswordHackPayload {
    pub project: String,

    /// Token is base64-encoded because it may contain arbitrary byte sequences.
    #[serde(deserialize_with = "deserialize_base64")]
    pub password: Vec<u8>,
}

fn deserialize_base64<'a, D: Deserializer<'a>>(des: D) -> Result<Vec<u8>, D::Error> {
    let base64: &str = Deserialize::deserialize(des)?;
    base64::decode(base64).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse() -> anyhow::Result<()> {
        let (password, project) = ("password", "pie-in-the-sky");
        let payload = json!({
            "project": project,
            "password": base64::encode(password),
        })
        .to_string();

        let payload: PasswordHackPayload = serde_json::from_str(&payload)?;
        assert_eq!(payload.password, password.as_bytes());
        assert_eq!(payload.project, project);

        Ok(())
    }
}
