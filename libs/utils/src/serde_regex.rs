//! A `serde::{Deserialize,Serialize}` type for regexes.

use std::ops::Deref;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Regex(
    #[serde(
        deserialize_with = "deserialize_regex",
        serialize_with = "serialize_regex"
    )]
    regex::Regex,
);

fn deserialize_regex<'de, D>(deserializer: D) -> Result<regex::Regex, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let s: String = serde::de::Deserialize::deserialize(deserializer)?;
    let re = regex::Regex::new(&s).map_err(serde::de::Error::custom)?;
    Ok(re)
}

fn serialize_regex<S>(re: &regex::Regex, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::ser::Serializer,
{
    serializer.collect_str(re.as_str())
}

impl Deref for Regex {
    type Target = regex::Regex;

    fn deref(&self) -> &regex::Regex {
        &self.0
    }
}

impl PartialEq for Regex {
    fn eq(&self, other: &Regex) -> bool {
        // comparing the automatons would be quite complicated
        self.as_str() == other.as_str()
    }
}

impl Eq for Regex {}

#[cfg(test)]
mod tests {

    #[test]
    fn roundtrip() {
        let input = r#""foo.*bar""#;
        let re: super::Regex = serde_json::from_str(input).unwrap();
        assert!(re.is_match("foo123bar"));
        assert!(!re.is_match("foo"));
        let output = serde_json::to_string(&re).unwrap();
        assert_eq!(output, input);
    }
}
