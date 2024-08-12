use utils::serde_system_time::SystemTime;

/// Pageserver current utilization and scoring for how good candidate the pageserver would be for
/// the next tenant.
///
/// See and maintain pageserver openapi spec for `/v1/utilization_score` as the truth.
///
/// `format: int64` fields must use `ser_saturating_u63` because openapi generated clients might
/// not handle full u64 values properly.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct PageserverUtilization {
    /// Used disk space (physical, ground truth from statfs())
    #[serde(serialize_with = "ser_saturating_u63")]
    pub disk_usage_bytes: u64,
    /// Free disk space
    #[serde(serialize_with = "ser_saturating_u63")]
    pub free_space_bytes: u64,

    /// Wanted disk space, based on the tenant shards currently present on this pageserver: this
    /// is like disk_usage_bytes, but it is stable and does not change with the cache state of
    /// tenants, whereas disk_usage_bytes may reach the disk eviction `max_usage_pct` and stay
    /// there, or may be unrealistically low if the pageserver has attached tenants which haven't
    /// downloaded layers yet.
    #[serde(serialize_with = "ser_saturating_u63")]
    pub disk_wanted_bytes: u64,

    /// Lower is better score for how good candidate for a next tenant would this pageserver be.
    #[serde(serialize_with = "ser_saturating_u63")]
    pub utilization_score: u64,
    /// When was this snapshot captured, pageserver local time.
    ///
    /// Use millis to give confidence that the value is regenerated often enough.
    pub captured_at: SystemTime,
}

/// openapi knows only `format: int64`, so avoid outputting a non-parseable value by generated clients.
///
/// Instead of newtype, use this because a newtype would get require handling deserializing values
/// with the highest bit set which is properly parsed by serde formats, but would create a
/// conundrum on how to handle and again serialize such values at type level. It will be a few
/// years until we can use more than `i64::MAX` bytes on a disk.
fn ser_saturating_u63<S: serde::Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    const MAX_FORMAT_INT64: u64 = i64::MAX as u64;

    let value = (*value).min(MAX_FORMAT_INT64);

    serializer.serialize_u64(value)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn u64_max_is_serialized_as_u63_max() {
        let doc = PageserverUtilization {
            disk_usage_bytes: u64::MAX,
            free_space_bytes: 0,
            disk_wanted_bytes: u64::MAX,
            utilization_score: u64::MAX,
            captured_at: SystemTime(
                std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1708509779),
            ),
        };

        let s = serde_json::to_string(&doc).unwrap();

        let expected = r#"{"disk_usage_bytes":9223372036854775807,"free_space_bytes":0,"disk_wanted_bytes":9223372036854775807,"utilization_score":9223372036854775807,"captured_at":"2024-02-21T10:02:59.000Z"}"#;

        assert_eq!(s, expected);
    }
}
