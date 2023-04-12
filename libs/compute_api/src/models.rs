//! Structs representing the JSON formats used in the compute_ctl's HTTP API.
use crate::rfc3339_serialize;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Response of the /status API
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ComputeState {
    pub status: ComputeStatus,
    /// Timestamp of the last Postgres activity
    #[serde(serialize_with = "rfc3339_serialize")]
    pub last_active: DateTime<Utc>,
    pub error: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ComputeStatus {
    Init,
    Running,
    Failed,
}

/// Response of the /metrics.json API
#[derive(Clone, Default, Serialize)]
pub struct ComputeMetrics {
    pub sync_safekeepers_ms: u64,
    pub basebackup_ms: u64,
    pub config_ms: u64,
    pub total_startup_ms: u64,
}
