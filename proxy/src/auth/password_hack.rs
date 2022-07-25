//! Payload for ad hoc authentication method for clients that don't support SNI.
//! See the `impl` for [`super::backend::BackendType<ClientCredentials>`].
//! Read more: <https://github.com/neondatabase/cloud/issues/1620#issuecomment-1165332290>.

use serde::{de, Deserialize, Deserializer};
use std::fmt;

#[derive(Deserialize)]
#[serde(untagged)]
pub enum Password {
    /// A regular string for utf-8 encoded passwords.
    Simple { password: String },

    /// Password is base64-encoded because it may contain arbitrary byte sequences.
    Encoded {
        #[serde(rename = "password_", deserialize_with = "deserialize_base64")]
        password: Vec<u8>,
    },
}

impl AsRef<[u8]> for Password {
    fn as_ref(&self) -> &[u8] {
        match self {
            Password::Simple { password } => password.as_ref(),
            Password::Encoded { password } => password.as_ref(),
        }
    }
}

#[derive(Deserialize)]
pub struct PasswordHackPayload {
    pub project: String,

    #[serde(flatten)]
    pub password: Password,
}

fn deserialize_base64<'a, D: Deserializer<'a>>(des: D) -> Result<Vec<u8>, D::Error> {
    // It's very tempting to replace this with
    //
    // ```
    // let base64: &str = Deserialize::deserialize(des)?;
    // base64::decode(base64).map_err(serde::de::Error::custom)
    // ```
    //
    // Unfortunately, we can't always deserialize into `&str`, so we'd
    // have to use an allocating `String` instead. Thus, visitor is better.
    struct Visitor;

    impl<'de> de::Visitor<'de> for Visitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string")
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            base64::decode(v).map_err(de::Error::custom)
        }
    }

    des.deserialize_str(Visitor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    #[test]
    fn parse_password() -> anyhow::Result<()> {
        let password: Password = serde_json::from_value(json!({
            "password": "foo",
        }))?;
        assert_eq!(password.as_ref(), "foo".as_bytes());

        let password: Password = serde_json::from_value(json!({
            "password_": base64::encode("foo"),
        }))?;
        assert_eq!(password.as_ref(), "foo".as_bytes());

        Ok(())
    }

    #[rstest]
    #[case("password", str::to_owned)]
    #[case("password_", base64::encode)]
    fn parse(#[case] key: &str, #[case] encode: fn(&'static str) -> String) -> anyhow::Result<()> {
        let (password, project) = ("password", "pie-in-the-sky");
        let payload = json!({
            "project": project,
            key: encode(password),
        });

        let payload: PasswordHackPayload = serde_json::from_value(payload)?;
        assert_eq!(payload.password.as_ref(), password.as_bytes());
        assert_eq!(payload.project, project);

        Ok(())
    }
}
