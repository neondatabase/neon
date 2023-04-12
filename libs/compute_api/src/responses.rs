//! Structs representing the JSON formats used in the compute_ctl's HTTP API.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, Serializer};

#[derive(Serialize, Debug, Deserialize)]
pub struct GenericAPIError {
    pub error: String,
}

/// Response of the /status API
#[derive(Serialize, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ComputeStatusResponse {
    pub tenant: Option<String>,
    pub timeline: Option<String>,
    pub status: ComputeStatus,
    #[serde(serialize_with = "rfc3339_serialize")]
    pub last_active: DateTime<Utc>,
    pub error: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ComputeState {
    pub status: ComputeStatus,
    /// Timestamp of the last Postgres activity
    #[serde(serialize_with = "rfc3339_serialize")]
    pub last_active: DateTime<Utc>,
    pub error: Option<String>,
}

#[derive(Serialize, Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ComputeStatus {
    // Spec wasn't provided at start, waiting for it to be
    // provided by control-plane.
    Empty,
    // Compute configuration was requested.
    ConfigurationPending,
    // Compute node has spec and initial startup and
    // configuration is in progress.
    Init,
    // Compute is configured and running.
    Running,
    // Either startup or configuration failed,
    // compute will exit soon or is waiting for
    // control-plane to terminate it.
    Failed,
}

fn rfc3339_serialize<S>(x: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    x.to_rfc3339().serialize(s)
}

/// Response of the /metrics.json API
#[derive(Clone, Debug, Default, Serialize)]
pub struct ComputeMetrics {
    pub sync_safekeepers_ms: u64,
    pub basebackup_ms: u64,
    pub config_ms: u64,
    pub total_startup_ms: u64,
}
