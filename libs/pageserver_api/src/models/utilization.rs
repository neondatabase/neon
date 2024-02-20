use std::time::SystemTime;

/// Pageserver current utilization and scoring for how good candidate the pageserver would be for
/// the next tenant.
///
/// See and maintain pageserver openapi spec for `/v1/utilization_score` as the truth.
#[derive(serde::Serialize, Debug)]
pub struct PageserverUtilization {
    /// Used disk space
    pub disk_usage_bytes: u64,
    /// Free disk space
    pub free_space_bytes: u64,
    /// Lower is better score for how good candidate for a next tenant would this pageserver be.
    pub utilization_score: u64,
    /// When was this snapshot captured, pageserver local time.
    ///
    /// Use millis to give confidence that the value is regenerated often enough.
    #[serde(serialize_with = "ser_rfc3339_millis")]
    pub captured_at: SystemTime,
}

fn ser_rfc3339_millis<S: serde::Serializer>(
    ts: &SystemTime,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_str(&humantime::format_rfc3339_millis(*ts))
}
