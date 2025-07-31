//! Structs representing the JSON formats used in the compute_ctl's HTTP API.

use chrono::{DateTime, Utc};
use jsonwebtoken::jwk::JwkSet;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Display;

use crate::privilege::Privilege;
use crate::spec::{ComputeSpec, Database, ExtVersion, PgIdent, Role};

#[derive(Serialize, Debug, Deserialize)]
pub struct GenericAPIError {
    pub error: String,
}

/// All configuration parameters necessary for a compute. When
/// [`ComputeConfig::spec`] is provided, it means that the compute is attached
/// to a tenant. [`ComputeConfig::compute_ctl_config`] will always be provided
/// and contains parameters necessary for operating `compute_ctl` independently
/// of whether a tenant is attached to the compute or not.
///
/// This also happens to be the body of `compute_ctl`'s /configure request.
#[derive(Debug, Deserialize, Serialize)]
pub struct ComputeConfig {
    /// The compute spec
    pub spec: Option<ComputeSpec>,

    /// The compute_ctl configuration
    #[allow(dead_code)]
    pub compute_ctl_config: ComputeCtlConfig,
}

impl From<ControlPlaneConfigResponse> for ComputeConfig {
    fn from(value: ControlPlaneConfigResponse) -> Self {
        Self {
            spec: value.spec,
            compute_ctl_config: value.compute_ctl_config,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ExtensionInstallResponse {
    pub extension: PgIdent,
    pub version: ExtVersion,
}

/// Status of the LFC prewarm process. The same state machine is reused for
/// both autoprewarm (prewarm after compute/Postgres start using the previously
/// stored LFC state) and explicit prewarming via API.
#[derive(Serialize, Default, Debug, Clone)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum LfcPrewarmState {
    /// Default value when compute boots up.
    #[default]
    NotPrewarmed,
    /// Prewarming thread is active and loading pages into LFC.
    Prewarming,
    /// We found requested LFC state in the endpoint storage and
    /// completed prewarming successfully.
    Completed {
        total: i32,
        prewarmed: i32,
        skipped: i32,
        state_download_time_ms: u32,
        uncompress_time_ms: u32,
        prewarm_time_ms: u32,
    },
    /// Unexpected error happened during prewarming. Note, `Not Found 404`
    /// response from the endpoint storage is explicitly excluded here
    /// because it can normally happen on the first compute start,
    /// since LFC state is not available yet.
    Failed { error: String },
    /// We tried to fetch the corresponding LFC state from the endpoint storage,
    /// but received `Not Found 404`. This should normally happen only during the
    /// first endpoint start after creation with `autoprewarm: true`.
    /// This may also happen if LFC is turned off or not initialized
    ///
    /// During the orchestrated prewarm via API, when a caller explicitly
    /// provides the LFC state key to prewarm from, it's the caller responsibility
    /// to handle this status as an error state in this case.
    Skipped,
    /// LFC prewarm was cancelled. Some pages in LFC cache may be prewarmed if query
    /// has started working before cancellation
    Cancelled,
}

impl Display for LfcPrewarmState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LfcPrewarmState::NotPrewarmed => f.write_str("NotPrewarmed"),
            LfcPrewarmState::Prewarming => f.write_str("Prewarming"),
            LfcPrewarmState::Completed { .. } => f.write_str("Completed"),
            LfcPrewarmState::Skipped => f.write_str("Skipped"),
            LfcPrewarmState::Failed { error } => write!(f, "Error({error})"),
            LfcPrewarmState::Cancelled => f.write_str("Cancelled"),
        }
    }
}

#[derive(Serialize, Default, Debug, Clone)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum LfcOffloadState {
    #[default]
    NotOffloaded,
    Offloading,
    Completed {
        state_query_time_ms: u32,
        compress_time_ms: u32,
        state_upload_time_ms: u32,
    },
    Failed {
        error: String,
    },
    /// LFC state was empty so it wasn't offloaded
    Skipped,
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PromoteState {
    NotPromoted,
    Completed {
        lsn_wait_time_ms: u32,
        pg_promote_time_ms: u32,
        reconfigure_time_ms: u32,
    },
    Failed {
        error: String,
    },
}

#[derive(Deserialize, Default, Debug)]
#[serde(rename_all = "snake_case")]
pub struct PromoteConfig {
    pub spec: ComputeSpec,
    pub wal_flush_lsn: utils::lsn::Lsn,
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

#[derive(Serialize, Clone, Copy, Debug, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TerminateMode {
    #[default]
    /// wait 30s till returning from /terminate to allow control plane to get the error
    Fast,
    /// return from /terminate immediately as soon as all components are terminated
    Immediate,
}

impl From<TerminateMode> for ComputeStatus {
    fn from(mode: TerminateMode) -> Self {
        match mode {
            TerminateMode::Fast => ComputeStatus::TerminationPendingFast,
            TerminateMode::Immediate => ComputeStatus::TerminationPendingImmediate,
        }
    }
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
    // Termination requested
    TerminationPendingFast,
    // Termination requested, without waiting 30s before returning from /terminate
    TerminationPendingImmediate,
    // Terminated Postgres
    Terminated,
    // A spec refresh is being requested
    RefreshConfigurationPending,
    // A spec refresh is being applied. We cannot refresh configuration again until the current
    // refresh is done, i.e., signal_refresh_configuration() will return 500 error.
    RefreshConfiguration,
}

#[derive(Deserialize, Serialize)]
pub struct TerminateResponse {
    pub lsn: Option<utils::lsn::Lsn>,
}

impl Display for ComputeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComputeStatus::Empty => f.write_str("empty"),
            ComputeStatus::ConfigurationPending => f.write_str("configuration-pending"),
            ComputeStatus::RefreshConfiguration => f.write_str("refresh-configuration"),
            ComputeStatus::RefreshConfigurationPending => {
                f.write_str("refresh-configuration-pending")
            }
            ComputeStatus::Init => f.write_str("init"),
            ComputeStatus::Running => f.write_str("running"),
            ComputeStatus::Configuration => f.write_str("configuration"),
            ComputeStatus::Failed => f.write_str("failed"),
            ComputeStatus::TerminationPendingFast => f.write_str("termination-pending-fast"),
            ComputeStatus::TerminationPendingImmediate => {
                f.write_str("termination-pending-immediate")
            }
            ComputeStatus::Terminated => f.write_str("terminated"),
        }
    }
}

pub fn rfc3339_serialize<S>(x: &Option<DateTime<Utc>>, s: S) -> Result<S::Ok, S::Error>
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
    /// Time spent waiting in pool
    pub wait_for_spec_ms: u64,

    /// Time spent checking if safekeepers are synced
    pub sync_sk_check_ms: u64,

    /// Time spent syncing safekeepers (walproposer.c).
    /// In most cases this should be zero.
    pub sync_safekeepers_ms: u64,

    /// Time it took to establish a pg connection to the pageserver.
    /// This is two roundtrips, so it's a good proxy for compute-pageserver
    /// latency. The latency is usually 0.2ms, but it's not safe to assume
    /// that.
    pub pageserver_connect_micros: u64,

    /// Time to get basebackup from pageserver and write it to disk.
    pub basebackup_ms: u64,

    /// Compressed size of basebackup received.
    pub basebackup_bytes: u64,

    /// Time spent starting potgres. This includes initialization of shared
    /// buffers, preloading extensions, and other pg operations.
    pub start_postgres_ms: u64,

    /// Time spent applying pg catalog updates that were made in the console
    /// UI. This should be 0 when startup time matters, since cplane tries
    /// to do these updates eagerly, and passes the skip_pg_catalog_updates
    /// when it's safe to skip this step.
    pub config_ms: u64,

    /// Total time, from when we receive the spec to when we're ready to take
    /// pg connections.
    pub total_startup_ms: u64,
    pub load_ext_ms: u64,
    pub num_ext_downloaded: u64,
    pub largest_ext_size: u64, // these are measured in bytes
    pub total_ext_download_size: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct CatalogObjects {
    pub roles: Vec<Role>,
    pub databases: Vec<Database>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ComputeCtlConfig {
    /// Set of JSON web keys that the compute can use to authenticate
    /// communication from the control plane.
    pub jwks: JwkSet,
    pub tls: Option<TlsConfig>,
}

impl Default for ComputeCtlConfig {
    fn default() -> Self {
        Self {
            jwks: JwkSet {
                keys: Vec::default(),
            },
            tls: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TlsConfig {
    pub key_path: String,
    pub cert_path: String,
}

/// Response of the `/computes/{compute_id}/spec` control-plane API.
#[derive(Deserialize, Debug)]
pub struct ControlPlaneConfigResponse {
    pub spec: Option<ComputeSpec>,
    pub status: ControlPlaneComputeStatus,
    pub compute_ctl_config: ComputeCtlConfig,
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

#[derive(Clone, Debug, Default, Serialize)]
pub struct InstalledExtension {
    pub extname: String,
    pub version: String,
    pub n_databases: u32, // Number of databases using this extension
    pub owned_by_superuser: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct InstalledExtensions {
    pub extensions: Vec<InstalledExtension>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ExtensionInstallResult {
    pub extension: PgIdent,
    pub version: ExtVersion,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct SetRoleGrantsResponse {
    pub database: PgIdent,
    pub schema: PgIdent,
    pub privileges: Vec<Privilege>,
    pub role: PgIdent,
}
