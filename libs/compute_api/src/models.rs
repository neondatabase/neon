//! Structs representing the JSON formats used in the compute_ctl's HTTP API.
use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};

/// Response of the /status API
///
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ComputeState {
    pub status: ComputeStatus,
    /// Timestamp of the last Postgres activity
    #[serde(serialize_with = "rfc3339_serialize")]
    pub last_active: DateTime<Utc>,
    pub error: Option<String>,
}

#[derive(Serialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum ComputeStatus {
    // Spec wasn't provided as start, waiting for it to be
    // provided by control-plane.
    WaitingSpec,
    // Compute node has initial spec and is starting up.
    Init,
    // Compute is configured and running.
    Running,
    // Either startup or configuration failed,
    // compute will exit soon or is waiting for
    // control-plane to terminate it.
    Failed,
    // Control-plane requested reconfiguration.
    ConfigurationPending,
    // New spec is being applied.
    Reconfiguration,
}

fn rfc3339_serialize<S>(x: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    x.to_rfc3339().serialize(s)
}

/// Response of the /metrics.json API
#[derive(Clone, Default, Serialize)]
pub struct ComputeMetrics {
    pub sync_safekeepers_ms: u64,
    pub basebackup_ms: u64,
    pub config_ms: u64,
    pub total_startup_ms: u64,
}
