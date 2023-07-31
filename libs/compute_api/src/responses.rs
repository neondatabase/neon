//! Structs representing the JSON formats used in the compute_ctl's HTTP API.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, Serializer};

use crate::spec::ComputeSpec;

#[derive(Serialize, Debug, Deserialize)]
pub struct GenericAPIError {
    pub error: String,
}

/// Response of the /status API
#[derive(Serialize, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ComputeStatusResponse {
    pub start_time: DateTime<Utc>,
    pub tenant: Option<String>,
    pub timeline: Option<String>,
    pub status: ComputeStatus,
    #[serde(serialize_with = "rfc3339_serialize")]
    pub last_active: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ComputeState {
    pub status: ComputeStatus,
    /// Timestamp of the last Postgres activity
    #[serde(serialize_with = "rfc3339_serialize")]
    pub last_active: Option<DateTime<Utc>>,
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
    // New spec is being applied.
    Configuration,
    // Either startup or configuration failed,
    // compute will exit soon or is waiting for
    // control-plane to terminate it.
    Failed,
}

fn rfc3339_serialize<S>(x: &Option<DateTime<Utc>>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if let Some(x) = x {
        x.to_rfc3339().serialize(s)
    } else {
        s.serialize_none()
    }
}

/// Response of the /metrics.json API
#[derive(Clone, Debug, Default, Serialize)]
pub struct ComputeMetrics {
    pub wait_for_spec_ms: u64,
    pub sync_safekeepers_ms: u64,
    pub sync_sk_check_ms: u64,
    pub basebackup_ms: u64,
    pub basebackup_bytes: u64,
    pub start_postgres_ms: u64,
    pub config_ms: u64,
    pub total_startup_ms: u64,
    pub load_ext_ms: u64,
    pub num_ext_downloaded: u64,
    pub largest_ext_size: u64, // these are measured in bytes
    pub total_ext_download_size: u64,
    pub prep_extensions_ms: u64,
}

/// Response of the `/computes/{compute_id}/spec` control-plane API.
/// This is not actually a compute API response, so consider moving
/// to a different place.
#[derive(Deserialize, Debug)]
pub struct ControlPlaneSpecResponse {
    pub spec: Option<ComputeSpec>,
    pub status: ControlPlaneComputeStatus,
}

#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ControlPlaneComputeStatus {
    // Compute is known to control-plane, but it's not
    // yet attached to any timeline / endpoint.
    Empty,
    // Compute is attached to some timeline / endpoint and
    // should be able to start with provided spec.
    Attached,
}
