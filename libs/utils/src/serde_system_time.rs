//! A `serde::{Deserialize,Serialize}` type for SystemTime with RFC3339 format and millisecond precision.

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct SystemTime(
    #[serde(
        deserialize_with = "deser_rfc3339_millis",
        serialize_with = "ser_rfc3339_millis"
    )]
    pub std::time::SystemTime,
);

fn ser_rfc3339_millis<S: serde::ser::Serializer>(
    ts: &std::time::SystemTime,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_str(&humantime::format_rfc3339_millis(*ts))
}

fn deser_rfc3339_millis<'de, D>(deserializer: D) -> Result<std::time::SystemTime, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let s: String = serde::de::Deserialize::deserialize(deserializer)?;
    humantime::parse_rfc3339(&s).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper function to make a SystemTime have millisecond precision by truncating additional nanoseconds.
    fn to_millisecond_precision(time: SystemTime) -> SystemTime {
        match time.0.duration_since(std::time::SystemTime::UNIX_EPOCH) {
            Ok(duration) => {
                let total_millis = duration.as_secs() * 1_000 + u64::from(duration.subsec_millis());
                SystemTime(
                    std::time::SystemTime::UNIX_EPOCH
                        + std::time::Duration::from_millis(total_millis),
                )
            }
            Err(_) => time,
        }
    }

    #[test]
    fn test_serialize_deserialize() {
        let input = SystemTime(std::time::SystemTime::now());
        let expected_serialized = format!("\"{}\"", humantime::format_rfc3339_millis(input.0));
        let serialized = serde_json::to_string(&input).unwrap();
        assert_eq!(expected_serialized, serialized);
        let deserialized: SystemTime = serde_json::from_str(&expected_serialized).unwrap();
        assert_eq!(to_millisecond_precision(input), deserialized);
    }
}
