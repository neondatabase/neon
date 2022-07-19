use serde::{de, Deserialize, Deserializer};
use std::fmt;

#[derive(Deserialize)]
pub struct PasswordHackPayload {
    pub project: String,

    /// Token is base64-encoded because it may contain arbitrary byte sequences.
    #[serde(deserialize_with = "deserialize_base64")]
    pub token: Vec<u8>,
}

fn deserialize_base64<'a, D: Deserializer<'a>>(des: D) -> Result<Vec<u8>, D::Error> {
    struct Visitor;

    impl<'a> de::Visitor<'a> for Visitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string")
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            base64::decode(v).map_err(E::custom)
        }
    }

    des.deserialize_string(Visitor)
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
            "token": base64::encode(password),
        })
        .to_string();

        let payload: PasswordHackPayload = serde_json::from_str(&payload)?;
        assert_eq!(payload.token, password.as_bytes());
        assert_eq!(payload.project, project);

        Ok(())
    }
}
