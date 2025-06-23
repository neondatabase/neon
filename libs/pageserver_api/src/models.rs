pub mod detach_ancestor;
pub mod partitioning;
pub mod utilization;

use core::ops::Range;
use std::collections::HashMap;
use std::fmt::Display;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::time::{Duration, SystemTime};

#[cfg(feature = "testing")]
use camino::Utf8PathBuf;
use postgres_versioninfo::PgMajorVersion;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::serde_as;
pub use utilization::PageserverUtilization;
use utils::id::{NodeId, TenantId, TimelineId};
use utils::lsn::Lsn;
use utils::{completion, serde_system_time};

use crate::config::Ratio;
use crate::key::{CompactKey, Key};
use crate::shard::{DEFAULT_STRIPE_SIZE, ShardCount, ShardStripeSize, TenantShardId};

/// The state of a tenant in this pageserver.
///
/// ```mermaid
/// stateDiagram-v2
///
///     [*] --> Attaching: spawn_attach()
///
///     Attaching --> Activating: activate()
///     Activating --> Active: infallible
///
///     Attaching --> Broken: attach() failure
///
///     Active --> Stopping: set_stopping(), part of shutdown & detach
///     Stopping --> Broken: late error in remove_tenant_from_memory
///
///     Broken --> [*]: ignore / detach / shutdown
///     Stopping --> [*]: remove_from_memory complete
///
///     Active --> Broken: cfg(testing)-only tenant break point
/// ```
#[derive(
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    strum_macros::Display,
    strum_macros::VariantNames,
    strum_macros::AsRefStr,
    strum_macros::IntoStaticStr,
)]
#[serde(tag = "slug", content = "data")]
pub enum TenantState {
    /// This tenant is being attached to the pageserver.
    ///
    /// `set_stopping()` and `set_broken()` do not work in this state and wait for it to pass.
    Attaching,
    /// The tenant is transitioning from Loading/Attaching to Active.
    ///
    /// While in this state, the individual timelines are being activated.
    ///
    /// `set_stopping()` and `set_broken()` do not work in this state and wait for it to pass.
    Activating(ActivatingFrom),
    /// The tenant has finished activating and is open for business.
    ///
    /// Transitions out of this state are possible through `set_stopping()` and `set_broken()`.
    Active,
    /// The tenant is recognized by pageserver, but it is being detached or the
    /// system is being shut down.
    ///
    /// Transitions out of this state are possible through `set_broken()`.
    Stopping {
        /// The barrier can be used to wait for shutdown to complete. The first caller to set
        /// Some(Barrier) is responsible for driving shutdown to completion. Subsequent callers
        /// will wait for the first caller's existing barrier.
        ///
        /// None is set when an attach is cancelled, to signal to shutdown that the attach has in
        /// fact cancelled:
        ///
        /// 1. `shutdown` sees `TenantState::Attaching`, and cancels the tenant.
        /// 2. `attach` sets `TenantState::Stopping(None)` and exits.
        /// 3. `set_stopping` waits for `TenantState::Stopping(None)` and sets
        ///    `TenantState::Stopping(Some)` to claim the barrier as the shutdown owner.
        //
        // Because of https://github.com/serde-rs/serde/issues/2105 this has to be a named field,
        // otherwise it will not be skipped during deserialization
        #[serde(skip)]
        progress: Option<completion::Barrier>,
    },
    /// The tenant is recognized by the pageserver, but can no longer be used for
    /// any operations.
    ///
    /// If the tenant fails to load or attach, it will transition to this state
    /// and it is guaranteed that no background tasks are running in its name.
    ///
    /// The other way to transition into this state is from `Stopping` state
    /// through `set_broken()` called from `remove_tenant_from_memory()`. That happens
    /// if the cleanup future executed by `remove_tenant_from_memory()` fails.
    Broken { reason: String, backtrace: String },
}

impl TenantState {
    pub fn attachment_status(&self) -> TenantAttachmentStatus {
        use TenantAttachmentStatus::*;

        // Below TenantState::Activating is used as "transient" or "transparent" state for
        // attachment_status determining.
        match self {
            // The attach procedure writes the marker file before adding the Attaching tenant to the tenants map.
            // So, technically, we can return Attached here.
            // However, as soon as Console observes Attached, it will proceed with the Postgres-level health check.
            // But, our attach task might still be fetching the remote timelines, etc.
            // So, return `Maybe` while Attaching, making Console wait for the attach task to finish.
            Self::Attaching | Self::Activating(ActivatingFrom::Attaching) => Maybe,
            // We only reach Active after successful load / attach.
            // So, call atttachment status Attached.
            Self::Active => Attached,
            // If the (initial or resumed) attach procedure fails, the tenant becomes Broken.
            // However, it also becomes Broken if the regular load fails.
            // From Console's perspective there's no practical difference
            // because attachment_status is polled by console only during attach operation execution.
            Self::Broken { reason, .. } => Failed {
                reason: reason.to_owned(),
            },
            // Why is Stopping a Maybe case? Because, during pageserver shutdown,
            // we set the Stopping state irrespective of whether the tenant
            // has finished attaching or not.
            Self::Stopping { .. } => Maybe,
        }
    }

    pub fn broken_from_reason(reason: String) -> Self {
        let backtrace_str: String = format!("{}", std::backtrace::Backtrace::force_capture());
        Self::Broken {
            reason,
            backtrace: backtrace_str,
        }
    }
}

impl std::fmt::Debug for TenantState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Broken { reason, backtrace } if !reason.is_empty() => {
                write!(f, "Broken due to: {reason}. Backtrace:\n{backtrace}")
            }
            _ => write!(f, "{self}"),
        }
    }
}

/// A temporary lease to a specific lsn inside a timeline.
/// Access to the lsn is guaranteed by the pageserver until the expiration indicated by `valid_until`.
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LsnLease {
    #[serde_as(as = "SystemTimeAsRfc3339Millis")]
    pub valid_until: SystemTime,
}

serde_with::serde_conv!(
    SystemTimeAsRfc3339Millis,
    SystemTime,
    |time: &SystemTime| humantime::format_rfc3339_millis(*time).to_string(),
    |value: String| -> Result<_, humantime::TimestampError> { humantime::parse_rfc3339(&value) }
);

impl LsnLease {
    /// The default length for an explicit LSN lease request (10 minutes).
    pub const DEFAULT_LENGTH: Duration = Duration::from_secs(10 * 60);

    /// The default length for an implicit LSN lease granted during
    /// `get_lsn_by_timestamp` request (1 minutes).
    pub const DEFAULT_LENGTH_FOR_TS: Duration = Duration::from_secs(60);

    /// Checks whether the lease is expired.
    pub fn is_expired(&self, now: &SystemTime) -> bool {
        now > &self.valid_until
    }
}

/// Controls the detach ancestor behavior.
/// - When set to `NoAncestorAndReparent`, we will only detach a branch if its ancestor is a root branch. It will automatically reparent any children of the ancestor before and at the branch point.
/// - When set to `MultiLevelAndNoReparent`, we will detach a branch from multiple levels of ancestors, and no reparenting will happen at all.
#[derive(Debug, Clone, Copy, Default)]
pub enum DetachBehavior {
    #[default]
    NoAncestorAndReparent,
    MultiLevelAndNoReparent,
}

impl std::str::FromStr for DetachBehavior {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "no_ancestor_and_reparent" => Ok(DetachBehavior::NoAncestorAndReparent),
            "multi_level_and_no_reparent" => Ok(DetachBehavior::MultiLevelAndNoReparent),
            "v1" => Ok(DetachBehavior::NoAncestorAndReparent),
            "v2" => Ok(DetachBehavior::MultiLevelAndNoReparent),
            _ => Err("cannot parse detach behavior"),
        }
    }
}

impl std::fmt::Display for DetachBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DetachBehavior::NoAncestorAndReparent => write!(f, "no_ancestor_and_reparent"),
            DetachBehavior::MultiLevelAndNoReparent => write!(f, "multi_level_and_no_reparent"),
        }
    }
}

/// The only [`TenantState`] variants we could be `TenantState::Activating` from.
///
/// XXX: We used to have more variants here, but now it's just one, which makes this rather
/// useless. Remove, once we've checked that there's no client code left that looks at this.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ActivatingFrom {
    /// Arrived to [`TenantState::Activating`] from [`TenantState::Attaching`]
    Attaching,
}

/// A state of a timeline in pageserver's memory.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TimelineState {
    /// The timeline is recognized by the pageserver but is not yet operational.
    /// In particular, the walreceiver connection loop is not running for this timeline.
    /// It will eventually transition to state Active or Broken.
    Loading,
    /// The timeline is fully operational.
    /// It can be queried, and the walreceiver connection loop is running.
    Active,
    /// The timeline was previously Loading or Active but is shutting down.
    /// It cannot transition back into any other state.
    Stopping,
    /// The timeline is broken and not operational (previous states: Loading or Active).
    Broken { reason: String, backtrace: String },
}

#[serde_with::serde_as]
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct CompactLsnRange {
    pub start: Lsn,
    pub end: Lsn,
}

#[serde_with::serde_as]
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct CompactKeyRange {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub start: Key,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub end: Key,
}

impl From<Range<Lsn>> for CompactLsnRange {
    fn from(range: Range<Lsn>) -> Self {
        Self {
            start: range.start,
            end: range.end,
        }
    }
}

impl From<Range<Key>> for CompactKeyRange {
    fn from(range: Range<Key>) -> Self {
        Self {
            start: range.start,
            end: range.end,
        }
    }
}

impl From<CompactLsnRange> for Range<Lsn> {
    fn from(range: CompactLsnRange) -> Self {
        range.start..range.end
    }
}

impl From<CompactKeyRange> for Range<Key> {
    fn from(range: CompactKeyRange) -> Self {
        range.start..range.end
    }
}

impl CompactLsnRange {
    pub fn above(lsn: Lsn) -> Self {
        Self {
            start: lsn,
            end: Lsn::MAX,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CompactInfoResponse {
    pub compact_key_range: Option<CompactKeyRange>,
    pub compact_lsn_range: Option<CompactLsnRange>,
    pub sub_compaction: bool,
    pub running: bool,
    pub job_id: usize,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TimelineCreateRequest {
    pub new_timeline_id: TimelineId,
    #[serde(flatten)]
    pub mode: TimelineCreateRequestMode,
}

impl TimelineCreateRequest {
    pub fn mode_tag(&self) -> &'static str {
        match &self.mode {
            TimelineCreateRequestMode::Branch { .. } => "branch",
            TimelineCreateRequestMode::ImportPgdata { .. } => "import",
            TimelineCreateRequestMode::Bootstrap { .. } => "bootstrap",
        }
    }

    pub fn is_import(&self) -> bool {
        matches!(self.mode, TimelineCreateRequestMode::ImportPgdata { .. })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ShardImportStatus {
    InProgress(Option<ShardImportProgress>),
    Done,
    Error(String),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ShardImportProgress {
    V1(ShardImportProgressV1),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardImportProgressV1 {
    /// Total number of jobs in the import plan
    pub jobs: usize,
    /// Number of jobs completed
    pub completed: usize,
    /// Hash of the plan
    pub import_plan_hash: u64,
    /// Soft limit for the job size
    /// This needs to remain constant throughout the import
    pub job_soft_size_limit: usize,
}

impl ShardImportStatus {
    pub fn is_terminal(&self) -> bool {
        match self {
            ShardImportStatus::InProgress(_) => false,
            ShardImportStatus::Done | ShardImportStatus::Error(_) => true,
        }
    }
}

/// Storage controller specific extensions to [`TimelineInfo`].
#[derive(Serialize, Deserialize, Clone)]
pub struct TimelineCreateResponseStorcon {
    #[serde(flatten)]
    pub timeline_info: TimelineInfo,

    pub safekeepers: Option<SafekeepersInfo>,
}

/// Safekeepers as returned in timeline creation request to storcon or pushed to
/// cplane in the post migration hook.
#[derive(Serialize, Deserialize, Clone)]
pub struct SafekeepersInfo {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub generation: u32,
    pub safekeepers: Vec<SafekeeperInfo>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SafekeeperInfo {
    pub id: NodeId,
    pub hostname: String,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum TimelineCreateRequestMode {
    Branch {
        ancestor_timeline_id: TimelineId,
        #[serde(default)]
        ancestor_start_lsn: Option<Lsn>,
        // TODO: cplane sets this, but, the branching code always
        // inherits the ancestor's pg_version. Earlier code wasn't
        // using a flattened enum, so, it was an accepted field, and
        // we continue to accept it by having it here.
        pg_version: Option<PgMajorVersion>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        read_only: bool,
    },
    ImportPgdata {
        import_pgdata: TimelineCreateRequestModeImportPgdata,
    },
    // NB: Bootstrap is all-optional, and thus the serde(untagged) will cause serde to stop at Bootstrap.
    // (serde picks the first matching enum variant, in declaration order).
    Bootstrap {
        #[serde(default)]
        existing_initdb_timeline_id: Option<TimelineId>,
        pg_version: Option<PgMajorVersion>,
    },
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TimelineCreateRequestModeImportPgdata {
    pub location: ImportPgdataLocation,
    pub idempotency_key: ImportPgdataIdempotencyKey,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ImportPgdataLocation {
    #[cfg(feature = "testing")]
    LocalFs { path: Utf8PathBuf },
    AwsS3 {
        region: String,
        bucket: String,
        /// A better name for this would be `prefix`; changing requires coordination with cplane.
        /// See <https://github.com/neondatabase/cloud/issues/20646>.
        key: String,
    },
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(transparent)]
pub struct ImportPgdataIdempotencyKey(pub String);

impl ImportPgdataIdempotencyKey {
    pub fn random() -> Self {
        use rand::Rng;
        use rand::distributions::Alphanumeric;
        Self(
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(20)
                .map(char::from)
                .collect(),
        )
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LsnLeaseRequest {
    pub lsn: Lsn,
}

#[derive(Serialize, Deserialize)]
pub struct TenantShardSplitRequest {
    pub new_shard_count: u8,

    // A tenant's stripe size is only meaningful the first time their shard count goes
    // above 1: therefore during a split from 1->N shards, we may modify the stripe size.
    //
    // If this is set while the stripe count is being increased from an already >1 value,
    // then the request will fail with 400.
    pub new_stripe_size: Option<ShardStripeSize>,
}

#[derive(Serialize, Deserialize)]
pub struct TenantShardSplitResponse {
    pub new_shards: Vec<TenantShardId>,
}

/// Parameters that apply to all shards in a tenant.  Used during tenant creation.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ShardParameters {
    pub count: ShardCount,
    pub stripe_size: ShardStripeSize,
}

impl ShardParameters {
    pub fn is_unsharded(&self) -> bool {
        self.count.is_unsharded()
    }
}

impl Default for ShardParameters {
    fn default() -> Self {
        Self {
            count: ShardCount::new(0),
            stripe_size: DEFAULT_STRIPE_SIZE,
        }
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub enum FieldPatch<T> {
    Upsert(T),
    Remove,
    #[default]
    Noop,
}

impl<T> FieldPatch<T> {
    fn is_noop(&self) -> bool {
        matches!(self, FieldPatch::Noop)
    }

    pub fn apply(self, target: &mut Option<T>) {
        match self {
            Self::Upsert(v) => *target = Some(v),
            Self::Remove => *target = None,
            Self::Noop => {}
        }
    }

    pub fn map<U, E, F: FnOnce(T) -> Result<U, E>>(self, map: F) -> Result<FieldPatch<U>, E> {
        match self {
            Self::Upsert(v) => Ok(FieldPatch::<U>::Upsert(map(v)?)),
            Self::Remove => Ok(FieldPatch::<U>::Remove),
            Self::Noop => Ok(FieldPatch::<U>::Noop),
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for FieldPatch<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::deserialize(deserializer).map(|opt| match opt {
            None => FieldPatch::Remove,
            Some(val) => FieldPatch::Upsert(val),
        })
    }
}

impl<T: Serialize> Serialize for FieldPatch<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            FieldPatch::Upsert(val) => serializer.serialize_some(val),
            FieldPatch::Remove => serializer.serialize_none(),
            FieldPatch::Noop => unreachable!(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
#[serde(default)]
pub struct TenantConfigPatch {
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub checkpoint_distance: FieldPatch<u64>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub checkpoint_timeout: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub compaction_target_size: FieldPatch<u64>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub compaction_period: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub compaction_threshold: FieldPatch<usize>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub compaction_upper_limit: FieldPatch<usize>,
    // defer parsing compaction_algorithm, like eviction_policy
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub compaction_algorithm: FieldPatch<CompactionAlgorithmSettings>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub compaction_shard_ancestor: FieldPatch<bool>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub compaction_l0_first: FieldPatch<bool>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub compaction_l0_semaphore: FieldPatch<bool>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub l0_flush_delay_threshold: FieldPatch<usize>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub l0_flush_stall_threshold: FieldPatch<usize>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub gc_horizon: FieldPatch<u64>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub gc_period: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub image_creation_threshold: FieldPatch<usize>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub pitr_interval: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub walreceiver_connect_timeout: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub lagging_wal_timeout: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub max_lsn_wal_lag: FieldPatch<NonZeroU64>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub eviction_policy: FieldPatch<EvictionPolicy>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub min_resident_size_override: FieldPatch<u64>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub evictions_low_residence_duration_metric_threshold: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub heatmap_period: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub lazy_slru_download: FieldPatch<bool>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub timeline_get_throttle: FieldPatch<ThrottleConfig>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub image_layer_creation_check_threshold: FieldPatch<u8>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub image_creation_preempt_threshold: FieldPatch<usize>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub lsn_lease_length: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub lsn_lease_length_for_ts: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub timeline_offloading: FieldPatch<bool>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub rel_size_v2_enabled: FieldPatch<bool>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub gc_compaction_enabled: FieldPatch<bool>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub gc_compaction_verification: FieldPatch<bool>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub gc_compaction_initial_threshold_kb: FieldPatch<u64>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub gc_compaction_ratio_percent: FieldPatch<u64>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub sampling_ratio: FieldPatch<Option<Ratio>>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub relsize_snapshot_cache_capacity: FieldPatch<usize>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub basebackup_cache_enabled: FieldPatch<bool>,
}

/// Like [`crate::config::TenantConfigToml`], but preserves the information
/// about which parameters are set and which are not.
///
/// Used in many places, including durably stored ones.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)] // this maps omitted fields in deserialization to None
pub struct TenantConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_distance: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    pub checkpoint_timeout: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_target_size: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    pub compaction_period: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_threshold: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_upper_limit: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_algorithm: Option<CompactionAlgorithmSettings>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_shard_ancestor: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_l0_first: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_l0_semaphore: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub l0_flush_delay_threshold: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub l0_flush_stall_threshold: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub gc_horizon: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    pub gc_period: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_creation_threshold: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    pub pitr_interval: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    pub walreceiver_connect_timeout: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    pub lagging_wal_timeout: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_lsn_wal_lag: Option<NonZeroU64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub eviction_policy: Option<EvictionPolicy>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_resident_size_override: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    pub evictions_low_residence_duration_metric_threshold: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    pub heatmap_period: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub lazy_slru_download: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeline_get_throttle: Option<ThrottleConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_layer_creation_check_threshold: Option<u8>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_creation_preempt_threshold: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    pub lsn_lease_length: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    pub lsn_lease_length_for_ts: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeline_offloading: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub rel_size_v2_enabled: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub gc_compaction_enabled: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub gc_compaction_verification: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub gc_compaction_initial_threshold_kb: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub gc_compaction_ratio_percent: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampling_ratio: Option<Option<Ratio>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub relsize_snapshot_cache_capacity: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub basebackup_cache_enabled: Option<bool>,
}

impl TenantConfig {
    pub fn apply_patch(
        self,
        patch: TenantConfigPatch,
    ) -> Result<TenantConfig, humantime::DurationError> {
        let Self {
            mut checkpoint_distance,
            mut checkpoint_timeout,
            mut compaction_target_size,
            mut compaction_period,
            mut compaction_threshold,
            mut compaction_upper_limit,
            mut compaction_algorithm,
            mut compaction_shard_ancestor,
            mut compaction_l0_first,
            mut compaction_l0_semaphore,
            mut l0_flush_delay_threshold,
            mut l0_flush_stall_threshold,
            mut gc_horizon,
            mut gc_period,
            mut image_creation_threshold,
            mut pitr_interval,
            mut walreceiver_connect_timeout,
            mut lagging_wal_timeout,
            mut max_lsn_wal_lag,
            mut eviction_policy,
            mut min_resident_size_override,
            mut evictions_low_residence_duration_metric_threshold,
            mut heatmap_period,
            mut lazy_slru_download,
            mut timeline_get_throttle,
            mut image_layer_creation_check_threshold,
            mut image_creation_preempt_threshold,
            mut lsn_lease_length,
            mut lsn_lease_length_for_ts,
            mut timeline_offloading,
            mut rel_size_v2_enabled,
            mut gc_compaction_enabled,
            mut gc_compaction_verification,
            mut gc_compaction_initial_threshold_kb,
            mut gc_compaction_ratio_percent,
            mut sampling_ratio,
            mut relsize_snapshot_cache_capacity,
            mut basebackup_cache_enabled,
        } = self;

        patch.checkpoint_distance.apply(&mut checkpoint_distance);
        patch
            .checkpoint_timeout
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut checkpoint_timeout);
        patch
            .compaction_target_size
            .apply(&mut compaction_target_size);
        patch
            .compaction_period
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut compaction_period);
        patch.compaction_threshold.apply(&mut compaction_threshold);
        patch
            .compaction_upper_limit
            .apply(&mut compaction_upper_limit);
        patch.compaction_algorithm.apply(&mut compaction_algorithm);
        patch
            .compaction_shard_ancestor
            .apply(&mut compaction_shard_ancestor);
        patch.compaction_l0_first.apply(&mut compaction_l0_first);
        patch
            .compaction_l0_semaphore
            .apply(&mut compaction_l0_semaphore);
        patch
            .l0_flush_delay_threshold
            .apply(&mut l0_flush_delay_threshold);
        patch
            .l0_flush_stall_threshold
            .apply(&mut l0_flush_stall_threshold);
        patch.gc_horizon.apply(&mut gc_horizon);
        patch
            .gc_period
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut gc_period);
        patch
            .image_creation_threshold
            .apply(&mut image_creation_threshold);
        patch
            .pitr_interval
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut pitr_interval);
        patch
            .walreceiver_connect_timeout
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut walreceiver_connect_timeout);
        patch
            .lagging_wal_timeout
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut lagging_wal_timeout);
        patch.max_lsn_wal_lag.apply(&mut max_lsn_wal_lag);
        patch.eviction_policy.apply(&mut eviction_policy);
        patch
            .min_resident_size_override
            .apply(&mut min_resident_size_override);
        patch
            .evictions_low_residence_duration_metric_threshold
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut evictions_low_residence_duration_metric_threshold);
        patch
            .heatmap_period
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut heatmap_period);
        patch.lazy_slru_download.apply(&mut lazy_slru_download);
        patch
            .timeline_get_throttle
            .apply(&mut timeline_get_throttle);
        patch
            .image_layer_creation_check_threshold
            .apply(&mut image_layer_creation_check_threshold);
        patch
            .image_creation_preempt_threshold
            .apply(&mut image_creation_preempt_threshold);
        patch
            .lsn_lease_length
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut lsn_lease_length);
        patch
            .lsn_lease_length_for_ts
            .map(|v| humantime::parse_duration(&v))?
            .apply(&mut lsn_lease_length_for_ts);
        patch.timeline_offloading.apply(&mut timeline_offloading);
        patch.rel_size_v2_enabled.apply(&mut rel_size_v2_enabled);
        patch
            .gc_compaction_enabled
            .apply(&mut gc_compaction_enabled);
        patch
            .gc_compaction_verification
            .apply(&mut gc_compaction_verification);
        patch
            .gc_compaction_initial_threshold_kb
            .apply(&mut gc_compaction_initial_threshold_kb);
        patch
            .gc_compaction_ratio_percent
            .apply(&mut gc_compaction_ratio_percent);
        patch.sampling_ratio.apply(&mut sampling_ratio);
        patch
            .relsize_snapshot_cache_capacity
            .apply(&mut relsize_snapshot_cache_capacity);
        patch
            .basebackup_cache_enabled
            .apply(&mut basebackup_cache_enabled);

        Ok(Self {
            checkpoint_distance,
            checkpoint_timeout,
            compaction_target_size,
            compaction_period,
            compaction_threshold,
            compaction_upper_limit,
            compaction_algorithm,
            compaction_shard_ancestor,
            compaction_l0_first,
            compaction_l0_semaphore,
            l0_flush_delay_threshold,
            l0_flush_stall_threshold,
            gc_horizon,
            gc_period,
            image_creation_threshold,
            pitr_interval,
            walreceiver_connect_timeout,
            lagging_wal_timeout,
            max_lsn_wal_lag,
            eviction_policy,
            min_resident_size_override,
            evictions_low_residence_duration_metric_threshold,
            heatmap_period,
            lazy_slru_download,
            timeline_get_throttle,
            image_layer_creation_check_threshold,
            image_creation_preempt_threshold,
            lsn_lease_length,
            lsn_lease_length_for_ts,
            timeline_offloading,
            rel_size_v2_enabled,
            gc_compaction_enabled,
            gc_compaction_verification,
            gc_compaction_initial_threshold_kb,
            gc_compaction_ratio_percent,
            sampling_ratio,
            relsize_snapshot_cache_capacity,
            basebackup_cache_enabled,
        })
    }

    pub fn merge(
        &self,
        global_conf: crate::config::TenantConfigToml,
    ) -> crate::config::TenantConfigToml {
        crate::config::TenantConfigToml {
            checkpoint_distance: self
                .checkpoint_distance
                .unwrap_or(global_conf.checkpoint_distance),
            checkpoint_timeout: self
                .checkpoint_timeout
                .unwrap_or(global_conf.checkpoint_timeout),
            compaction_target_size: self
                .compaction_target_size
                .unwrap_or(global_conf.compaction_target_size),
            compaction_period: self
                .compaction_period
                .unwrap_or(global_conf.compaction_period),
            compaction_threshold: self
                .compaction_threshold
                .unwrap_or(global_conf.compaction_threshold),
            compaction_upper_limit: self
                .compaction_upper_limit
                .unwrap_or(global_conf.compaction_upper_limit),
            compaction_algorithm: self
                .compaction_algorithm
                .as_ref()
                .unwrap_or(&global_conf.compaction_algorithm)
                .clone(),
            compaction_shard_ancestor: self
                .compaction_shard_ancestor
                .unwrap_or(global_conf.compaction_shard_ancestor),
            compaction_l0_first: self
                .compaction_l0_first
                .unwrap_or(global_conf.compaction_l0_first),
            compaction_l0_semaphore: self
                .compaction_l0_semaphore
                .unwrap_or(global_conf.compaction_l0_semaphore),
            l0_flush_delay_threshold: self
                .l0_flush_delay_threshold
                .or(global_conf.l0_flush_delay_threshold),
            l0_flush_stall_threshold: self
                .l0_flush_stall_threshold
                .or(global_conf.l0_flush_stall_threshold),
            gc_horizon: self.gc_horizon.unwrap_or(global_conf.gc_horizon),
            gc_period: self.gc_period.unwrap_or(global_conf.gc_period),
            image_creation_threshold: self
                .image_creation_threshold
                .unwrap_or(global_conf.image_creation_threshold),
            pitr_interval: self.pitr_interval.unwrap_or(global_conf.pitr_interval),
            walreceiver_connect_timeout: self
                .walreceiver_connect_timeout
                .unwrap_or(global_conf.walreceiver_connect_timeout),
            lagging_wal_timeout: self
                .lagging_wal_timeout
                .unwrap_or(global_conf.lagging_wal_timeout),
            max_lsn_wal_lag: self.max_lsn_wal_lag.unwrap_or(global_conf.max_lsn_wal_lag),
            eviction_policy: self.eviction_policy.unwrap_or(global_conf.eviction_policy),
            min_resident_size_override: self
                .min_resident_size_override
                .or(global_conf.min_resident_size_override),
            evictions_low_residence_duration_metric_threshold: self
                .evictions_low_residence_duration_metric_threshold
                .unwrap_or(global_conf.evictions_low_residence_duration_metric_threshold),
            heatmap_period: self.heatmap_period.unwrap_or(global_conf.heatmap_period),
            lazy_slru_download: self
                .lazy_slru_download
                .unwrap_or(global_conf.lazy_slru_download),
            timeline_get_throttle: self
                .timeline_get_throttle
                .clone()
                .unwrap_or(global_conf.timeline_get_throttle),
            image_layer_creation_check_threshold: self
                .image_layer_creation_check_threshold
                .unwrap_or(global_conf.image_layer_creation_check_threshold),
            image_creation_preempt_threshold: self
                .image_creation_preempt_threshold
                .unwrap_or(global_conf.image_creation_preempt_threshold),
            lsn_lease_length: self
                .lsn_lease_length
                .unwrap_or(global_conf.lsn_lease_length),
            lsn_lease_length_for_ts: self
                .lsn_lease_length_for_ts
                .unwrap_or(global_conf.lsn_lease_length_for_ts),
            timeline_offloading: self
                .timeline_offloading
                .unwrap_or(global_conf.timeline_offloading),
            rel_size_v2_enabled: self
                .rel_size_v2_enabled
                .unwrap_or(global_conf.rel_size_v2_enabled),
            gc_compaction_enabled: self
                .gc_compaction_enabled
                .unwrap_or(global_conf.gc_compaction_enabled),
            gc_compaction_verification: self
                .gc_compaction_verification
                .unwrap_or(global_conf.gc_compaction_verification),
            gc_compaction_initial_threshold_kb: self
                .gc_compaction_initial_threshold_kb
                .unwrap_or(global_conf.gc_compaction_initial_threshold_kb),
            gc_compaction_ratio_percent: self
                .gc_compaction_ratio_percent
                .unwrap_or(global_conf.gc_compaction_ratio_percent),
            sampling_ratio: self.sampling_ratio.unwrap_or(global_conf.sampling_ratio),
            relsize_snapshot_cache_capacity: self
                .relsize_snapshot_cache_capacity
                .unwrap_or(global_conf.relsize_snapshot_cache_capacity),
            basebackup_cache_enabled: self
                .basebackup_cache_enabled
                .unwrap_or(global_conf.basebackup_cache_enabled),
        }
    }
}

/// The policy for the aux file storage.
///
/// It can be switched through `switch_aux_file_policy` tenant config.
/// When the first aux file written, the policy will be persisted in the
/// `index_part.json` file and has a limited migration path.
///
/// Currently, we only allow the following migration path:
///
/// Unset -> V1
///       -> V2
///       -> CrossValidation -> V2
#[derive(
    Eq,
    PartialEq,
    Debug,
    Copy,
    Clone,
    strum_macros::EnumString,
    strum_macros::Display,
    serde_with::DeserializeFromStr,
    serde_with::SerializeDisplay,
)]
#[strum(serialize_all = "kebab-case")]
pub enum AuxFilePolicy {
    /// V1 aux file policy: store everything in AUX_FILE_KEY
    #[strum(ascii_case_insensitive)]
    V1,
    /// V2 aux file policy: store in the AUX_FILE keyspace
    #[strum(ascii_case_insensitive)]
    V2,
    /// Cross validation runs both formats on the write path and does validation
    /// on the read path.
    #[strum(ascii_case_insensitive)]
    CrossValidation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum EvictionPolicy {
    NoEviction,
    LayerAccessThreshold(EvictionPolicyLayerAccessThreshold),
    OnlyImitiate(EvictionPolicyLayerAccessThreshold),
}

impl EvictionPolicy {
    pub fn discriminant_str(&self) -> &'static str {
        match self {
            EvictionPolicy::NoEviction => "NoEviction",
            EvictionPolicy::LayerAccessThreshold(_) => "LayerAccessThreshold",
            EvictionPolicy::OnlyImitiate(_) => "OnlyImitiate",
        }
    }
}

#[derive(
    Eq,
    PartialEq,
    Debug,
    Copy,
    Clone,
    strum_macros::EnumString,
    strum_macros::Display,
    serde_with::DeserializeFromStr,
    serde_with::SerializeDisplay,
)]
#[strum(serialize_all = "kebab-case")]
pub enum CompactionAlgorithm {
    Legacy,
    Tiered,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde_with::DeserializeFromStr, serde_with::SerializeDisplay,
)]
pub enum ImageCompressionAlgorithm {
    // Disabled for writes, support decompressing during read path
    Disabled,
    /// Zstandard compression. Level 0 means and None mean the same (default level). Levels can be negative as well.
    /// For details, see the [manual](http://facebook.github.io/zstd/zstd_manual.html).
    Zstd {
        level: Option<i8>,
    },
}

impl FromStr for ImageCompressionAlgorithm {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut components = s.split(['(', ')']);
        let first = components
            .next()
            .ok_or_else(|| anyhow::anyhow!("empty string"))?;
        match first {
            "disabled" => Ok(ImageCompressionAlgorithm::Disabled),
            "zstd" => {
                let level = if let Some(v) = components.next() {
                    let v: i8 = v.parse()?;
                    Some(v)
                } else {
                    None
                };

                Ok(ImageCompressionAlgorithm::Zstd { level })
            }
            _ => anyhow::bail!("invalid specifier '{first}'"),
        }
    }
}

impl Display for ImageCompressionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImageCompressionAlgorithm::Disabled => write!(f, "disabled"),
            ImageCompressionAlgorithm::Zstd { level } => {
                if let Some(level) = level {
                    write!(f, "zstd({level})")
                } else {
                    write!(f, "zstd")
                }
            }
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct CompactionAlgorithmSettings {
    pub kind: CompactionAlgorithm,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
pub enum L0FlushConfig {
    #[serde(rename_all = "snake_case")]
    Direct { max_concurrency: NonZeroUsize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvictionPolicyLayerAccessThreshold {
    #[serde(with = "humantime_serde")]
    pub period: Duration,
    #[serde(with = "humantime_serde")]
    pub threshold: Duration,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ThrottleConfig {
    /// See [`ThrottleConfigTaskKinds`] for why we do the serde `rename`.
    #[serde(rename = "task_kinds")]
    pub enabled: ThrottleConfigTaskKinds,
    pub initial: u32,
    #[serde(with = "humantime_serde")]
    pub refill_interval: Duration,
    pub refill_amount: NonZeroU32,
    pub max: u32,
}

/// Before <https://github.com/neondatabase/neon/pull/9962>
/// the throttle was a per `Timeline::get`/`Timeline::get_vectored` call.
/// The `task_kinds` field controlled which Pageserver "Task Kind"s
/// were subject to the throttle.
///
/// After that PR, the throttle is applied at pagestream request level
/// and the `task_kinds` field does not apply since the only task kind
/// that us subject to the throttle is that of the page service.
///
/// However, we don't want to make a breaking config change right now
/// because it means we have to migrate all the tenant configs.
/// This will be done in a future PR.
///
/// In the meantime, we use emptiness / non-emptsiness of the `task_kinds`
/// field to determine if the throttle is enabled or not.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(transparent)]
pub struct ThrottleConfigTaskKinds(Vec<String>);

impl ThrottleConfigTaskKinds {
    pub fn disabled() -> Self {
        Self(vec![])
    }
    pub fn is_enabled(&self) -> bool {
        !self.0.is_empty()
    }
}

impl ThrottleConfig {
    pub fn disabled() -> Self {
        Self {
            enabled: ThrottleConfigTaskKinds::disabled(),
            // other values don't matter with emtpy `task_kinds`.
            initial: 0,
            refill_interval: Duration::from_millis(1),
            refill_amount: NonZeroU32::new(1).unwrap(),
            max: 1,
        }
    }
    /// The requests per second allowed  by the given config.
    pub fn steady_rps(&self) -> f64 {
        (self.refill_amount.get() as f64) / (self.refill_interval.as_secs_f64())
    }
}

#[cfg(test)]
mod throttle_config_tests {
    use super::*;

    #[test]
    fn test_disabled_is_disabled() {
        let config = ThrottleConfig::disabled();
        assert!(!config.enabled.is_enabled());
    }
    #[test]
    fn test_enabled_backwards_compat() {
        let input = serde_json::json!({
            "task_kinds": ["PageRequestHandler"],
            "initial": 40000,
            "refill_interval": "50ms",
            "refill_amount": 1000,
            "max": 40000,
            "fair": true
        });
        let config: ThrottleConfig = serde_json::from_value(input).unwrap();
        assert!(config.enabled.is_enabled());
    }
}

/// A flattened analog of a `pagesever::tenant::LocationMode`, which
/// lists out all possible states (and the virtual "Detached" state)
/// in a flat form rather than using rust-style enums.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq)]
pub enum LocationConfigMode {
    AttachedSingle,
    AttachedMulti,
    AttachedStale,
    Secondary,
    Detached,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct LocationConfigSecondary {
    pub warm: bool,
}

/// An alternative representation of `pageserver::tenant::LocationConf`,
/// for use in external-facing APIs.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct LocationConfig {
    pub mode: LocationConfigMode,
    /// If attaching, in what generation?
    #[serde(default)]
    pub generation: Option<u32>,

    // If requesting mode `Secondary`, configuration for that.
    #[serde(default)]
    pub secondary_conf: Option<LocationConfigSecondary>,

    // Shard parameters: if shard_count is nonzero, then other shard_* fields
    // must be set accurately.
    #[serde(default)]
    pub shard_number: u8,
    #[serde(default)]
    pub shard_count: u8,
    #[serde(default)]
    pub shard_stripe_size: u32,

    // This configuration only affects attached mode, but should be provided irrespective
    // of the mode, as a secondary location might transition on startup if the response
    // to the `/re-attach` control plane API requests it.
    pub tenant_conf: TenantConfig,
}

#[derive(Serialize, Deserialize)]
pub struct LocationConfigListResponse {
    pub tenant_shards: Vec<(TenantShardId, Option<LocationConfig>)>,
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub id: NodeId,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantLocationConfigRequest {
    #[serde(flatten)]
    pub config: LocationConfig, // as we have a flattened field, we should reject all unknown fields in it
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantTimeTravelRequest {
    pub shard_counts: Vec<ShardCount>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantShardLocation {
    pub shard_id: TenantShardId,
    pub node_id: NodeId,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantLocationConfigResponse {
    pub shards: Vec<TenantShardLocation>,
    // If the shards' ShardCount count is >1, stripe_size will be set.
    pub stripe_size: Option<ShardStripeSize>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantConfigRequest {
    pub tenant_id: TenantId,
    #[serde(flatten)]
    pub config: TenantConfig, // as we have a flattened field, we should reject all unknown fields in it
}

impl std::ops::Deref for TenantConfigRequest {
    type Target = TenantConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl TenantConfigRequest {
    pub fn new(tenant_id: TenantId) -> TenantConfigRequest {
        let config = TenantConfig::default();
        TenantConfigRequest { tenant_id, config }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantConfigPatchRequest {
    pub tenant_id: TenantId,
    #[serde(flatten)]
    pub config: TenantConfigPatch, // as we have a flattened field, we should reject all unknown fields in it
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TenantWaitLsnRequest {
    #[serde(flatten)]
    pub timelines: HashMap<TimelineId, Lsn>,
    pub timeout: Duration,
}

/// See [`TenantState::attachment_status`] and the OpenAPI docs for context.
#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "slug", content = "data", rename_all = "snake_case")]
pub enum TenantAttachmentStatus {
    Maybe,
    Attached,
    Failed { reason: String },
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TenantInfo {
    pub id: TenantShardId,
    // NB: intentionally not part of OpenAPI, we don't want to commit to a specific set of TenantState's
    pub state: TenantState,
    /// Sum of the size of all layer files.
    /// If a layer is present in both local FS and S3, it counts only once.
    pub current_physical_size: Option<u64>, // physical size is only included in `tenant_status` endpoint
    pub attachment_status: TenantAttachmentStatus,
    pub generation: u32,

    /// Opaque explanation if gc is being blocked.
    ///
    /// Only looked up for the individual tenant detail, not the listing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gc_blocking: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TenantDetails {
    #[serde(flatten)]
    pub tenant_info: TenantInfo,

    pub walredo: Option<WalRedoManagerStatus>,

    pub timelines: Vec<TimelineId>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy, Debug)]
pub enum TimelineArchivalState {
    Archived,
    Unarchived,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum TimelineVisibilityState {
    Visible,
    Invisible,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct TimelineArchivalConfigRequest {
    pub state: TimelineArchivalState,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct TimelinePatchIndexPartRequest {
    pub rel_size_migration: Option<RelSizeMigration>,
    pub gc_compaction_last_completed_lsn: Option<Lsn>,
    pub applied_gc_cutoff_lsn: Option<Lsn>,
    #[serde(default)]
    pub force_index_update: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimelinesInfoAndOffloaded {
    pub timelines: Vec<TimelineInfo>,
    pub offloaded: Vec<OffloadedTimelineInfo>,
}

/// Analog of [`TimelineInfo`] for offloaded timelines.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OffloadedTimelineInfo {
    pub tenant_id: TenantShardId,
    pub timeline_id: TimelineId,
    /// Whether the timeline has a parent it has been branched off from or not
    pub ancestor_timeline_id: Option<TimelineId>,
    /// Whether to retain the branch lsn at the ancestor or not
    pub ancestor_retain_lsn: Option<Lsn>,
    /// The time point when the timeline was archived
    pub archived_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RelSizeMigration {
    /// The tenant is using the old rel_size format.
    /// Note that this enum is persisted as `Option<RelSizeMigration>` in the index part, so
    /// `None` is the same as `Some(RelSizeMigration::Legacy)`.
    Legacy,
    /// The tenant is migrating to the new rel_size format. Both old and new rel_size format are
    /// persisted in the index part. The read path will read both formats and merge them.
    Migrating,
    /// The tenant has migrated to the new rel_size format. Only the new rel_size format is persisted
    /// in the index part, and the read path will not read the old format.
    Migrated,
}

/// This represents the output of the "timeline_detail" and "timeline_list" API calls.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimelineInfo {
    pub tenant_id: TenantShardId,
    pub timeline_id: TimelineId,

    pub ancestor_timeline_id: Option<TimelineId>,
    pub ancestor_lsn: Option<Lsn>,
    pub last_record_lsn: Lsn,
    pub prev_record_lsn: Option<Lsn>,

    /// The LSN up to which GC has advanced: older data may still exist but it is not available for clients.
    /// This LSN is not suitable for deciding where to create branches etc: use [`TimelineInfo::min_readable_lsn`] instead,
    /// as it is easier to reason about.
    #[serde(default)]
    pub applied_gc_cutoff_lsn: Lsn,

    /// The upper bound of data which is either already GC'ed, or elegible to be GC'ed at any time based on PITR interval.
    /// This LSN represents the "end of history" for this timeline, and callers should use it to figure out the oldest
    /// LSN at which it is legal to create a branch or ephemeral endpoint.
    ///
    /// Note that holders of valid LSN leases may be able to create branches and read pages earlier
    /// than this LSN, but new leases may not be taken out earlier than this LSN.
    #[serde(default)]
    pub min_readable_lsn: Lsn,

    pub disk_consistent_lsn: Lsn,

    /// The LSN that we have succesfully uploaded to remote storage
    pub remote_consistent_lsn: Lsn,

    /// The LSN that we are advertizing to safekeepers
    pub remote_consistent_lsn_visible: Lsn,

    /// The LSN from the start of the root timeline (never changes)
    pub initdb_lsn: Lsn,

    pub current_logical_size: u64,
    pub current_logical_size_is_accurate: bool,

    pub directory_entries_counts: Vec<u64>,

    /// Sum of the size of all layer files.
    /// If a layer is present in both local FS and S3, it counts only once.
    pub current_physical_size: Option<u64>, // is None when timeline is Unloaded
    pub current_logical_size_non_incremental: Option<u64>,

    /// How many bytes of WAL are within this branch's pitr_interval.  If the pitr_interval goes
    /// beyond the branch's branch point, we only count up to the branch point.
    pub pitr_history_size: u64,

    /// Whether this branch's branch point is within its ancestor's PITR interval (i.e. any
    /// ancestor data used by this branch would have been retained anyway).  If this is false, then
    /// this branch may be imposing a cost on the ancestor by causing it to retain layers that it would
    /// otherwise be able to GC.
    pub within_ancestor_pitr: bool,

    pub timeline_dir_layer_file_size_sum: Option<u64>,

    pub wal_source_connstr: Option<String>,
    pub last_received_msg_lsn: Option<Lsn>,
    /// the timestamp (in microseconds) of the last received message
    pub last_received_msg_ts: Option<u128>,
    pub pg_version: PgMajorVersion,

    pub state: TimelineState,

    pub walreceiver_status: String,

    // ALWAYS add new fields at the end of the struct with `Option` to ensure forward/backward compatibility.
    // Backward compatibility: you will get a JSON not containing the newly-added field.
    // Forward compatibility: a previous version of the pageserver will receive a JSON. serde::Deserialize does
    // not deny unknown fields by default so it's safe to set the field to some value, though it won't be
    // read.
    /// Whether the timeline is archived.
    pub is_archived: Option<bool>,

    /// The status of the rel_size migration.
    pub rel_size_migration: Option<RelSizeMigration>,

    /// Whether the timeline is invisible in synthetic size calculations.
    pub is_invisible: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerMapInfo {
    pub in_memory_layers: Vec<InMemoryLayerInfo>,
    pub historic_layers: Vec<HistoricLayerInfo>,
}

/// The residence status of a layer
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum LayerResidenceStatus {
    /// Residence status for a layer file that exists locally.
    /// It may also exist on the remote, we don't care here.
    Resident,
    /// Residence status for a layer file that only exists on the remote.
    Evicted,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerAccessStats {
    #[serde_as(as = "serde_with::TimestampMilliSeconds")]
    pub access_time: SystemTime,

    #[serde_as(as = "serde_with::TimestampMilliSeconds")]
    pub residence_time: SystemTime,

    pub visible: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum InMemoryLayerInfo {
    Open { lsn_start: Lsn },
    Frozen { lsn_start: Lsn, lsn_end: Lsn },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum HistoricLayerInfo {
    Delta {
        layer_file_name: String,
        layer_file_size: u64,

        lsn_start: Lsn,
        lsn_end: Lsn,
        remote: bool,
        access_stats: LayerAccessStats,

        l0: bool,
    },
    Image {
        layer_file_name: String,
        layer_file_size: u64,

        lsn_start: Lsn,
        remote: bool,
        access_stats: LayerAccessStats,
    },
}

impl HistoricLayerInfo {
    pub fn layer_file_name(&self) -> &str {
        match self {
            HistoricLayerInfo::Delta {
                layer_file_name, ..
            } => layer_file_name,
            HistoricLayerInfo::Image {
                layer_file_name, ..
            } => layer_file_name,
        }
    }
    pub fn is_remote(&self) -> bool {
        match self {
            HistoricLayerInfo::Delta { remote, .. } => *remote,
            HistoricLayerInfo::Image { remote, .. } => *remote,
        }
    }
    pub fn set_remote(&mut self, value: bool) {
        let field = match self {
            HistoricLayerInfo::Delta { remote, .. } => remote,
            HistoricLayerInfo::Image { remote, .. } => remote,
        };
        *field = value;
    }
    pub fn layer_file_size(&self) -> u64 {
        match self {
            HistoricLayerInfo::Delta {
                layer_file_size, ..
            } => *layer_file_size,
            HistoricLayerInfo::Image {
                layer_file_size, ..
            } => *layer_file_size,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadRemoteLayersTaskSpawnRequest {
    pub max_concurrent_downloads: NonZeroUsize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestAuxFilesRequest {
    pub aux_files: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListAuxFilesRequest {
    pub lsn: Lsn,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DownloadRemoteLayersTaskInfo {
    pub task_id: String,
    pub state: DownloadRemoteLayersTaskState,
    pub total_layer_count: u64,         // stable once `completed`
    pub successful_download_count: u64, // stable once `completed`
    pub failed_download_count: u64,     // stable once `completed`
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DownloadRemoteLayersTaskState {
    Running,
    Completed,
    ShutDown,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TimelineGcRequest {
    pub gc_horizon: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRedoManagerProcessStatus {
    pub pid: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRedoManagerStatus {
    pub last_redo_at: Option<chrono::DateTime<chrono::Utc>>,
    pub process: Option<WalRedoManagerProcessStatus>,
}

/// The progress of a secondary tenant.
///
/// It is mostly useful when doing a long running download: e.g. initiating
/// a download job, timing out while waiting for it to run, and then inspecting this status to understand
/// what's happening.
#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct SecondaryProgress {
    /// The remote storage LastModified time of the heatmap object we last downloaded.
    pub heatmap_mtime: Option<serde_system_time::SystemTime>,

    /// The number of layers currently on-disk
    pub layers_downloaded: usize,
    /// The number of layers in the most recently seen heatmap
    pub layers_total: usize,

    /// The number of layer bytes currently on-disk
    pub bytes_downloaded: u64,
    /// The number of layer bytes in the most recently seen heatmap
    pub bytes_total: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TenantScanRemoteStorageShard {
    pub tenant_shard_id: TenantShardId,
    pub generation: Option<u32>,
    pub stripe_size: Option<ShardStripeSize>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct TenantScanRemoteStorageResponse {
    pub shards: Vec<TenantScanRemoteStorageShard>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum TenantSorting {
    /// Total size of layers on local disk for all timelines in a shard.
    ResidentSize,
    /// The logical size of the largest timeline within a _tenant_ (not shard). Only tracked on
    /// shard 0, contains the sum across all shards.
    MaxLogicalSize,
    /// The logical size of the largest timeline within a _tenant_ (not shard), divided by number of
    /// shards. Only tracked on shard 0, and estimates the per-shard logical size.
    MaxLogicalSizePerShard,
}

impl Default for TenantSorting {
    fn default() -> Self {
        Self::ResidentSize
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopTenantShardsRequest {
    // How would you like to sort the tenants?
    pub order_by: TenantSorting,

    // How many results?
    pub limit: usize,

    // Omit tenants with more than this many shards (e.g. if this is the max number of shards
    // that the caller would ever split to)
    pub where_shards_lt: Option<ShardCount>,

    // Omit tenants where the ordering metric is less than this (this is an optimization to
    // let us quickly exclude numerous tiny shards)
    pub where_gt: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct TopTenantShardItem {
    pub id: TenantShardId,

    /// Total size of layers on local disk for all timelines in this shard.
    pub resident_size: u64,

    /// Total size of layers in remote storage for all timelines in this shard.
    pub physical_size: u64,

    /// The largest logical size of a timeline within this _tenant_ (not shard). This is only
    /// tracked on shard 0, and contains the sum of the logical size across all shards.
    pub max_logical_size: u64,

    /// The largest logical size of a timeline within this _tenant_ (not shard) divided by number of
    /// shards. This is only tracked on shard 0, and is only an estimate as we divide it evenly by
    /// shard count, rounded up.
    pub max_logical_size_per_shard: u64,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct TopTenantShardsResponse {
    pub shards: Vec<TopTenantShardItem>,
}

pub mod virtual_file {

    #[derive(
        Copy,
        Clone,
        PartialEq,
        Eq,
        Hash,
        strum_macros::EnumString,
        strum_macros::Display,
        serde_with::DeserializeFromStr,
        serde_with::SerializeDisplay,
        Debug,
    )]
    #[strum(serialize_all = "kebab-case")]
    pub enum IoEngineKind {
        StdFs,
        #[cfg(target_os = "linux")]
        TokioEpollUring,
    }

    /// Direct IO modes for a pageserver.
    #[derive(
        Copy,
        Clone,
        PartialEq,
        Eq,
        Hash,
        strum_macros::EnumString,
        strum_macros::EnumIter,
        strum_macros::Display,
        serde_with::DeserializeFromStr,
        serde_with::SerializeDisplay,
        Debug,
    )]
    #[strum(serialize_all = "kebab-case")]
    #[repr(u8)]
    pub enum IoMode {
        /// Uses buffered IO.
        Buffered,
        /// Uses direct IO for reads only.
        Direct,
        /// Use direct IO for reads and writes.
        DirectRw,
    }

    impl IoMode {
        pub fn preferred() -> Self {
            IoMode::DirectRw
        }
    }

    impl TryFrom<u8> for IoMode {
        type Error = u8;

        fn try_from(value: u8) -> Result<Self, Self::Error> {
            Ok(match value {
                v if v == (IoMode::Buffered as u8) => IoMode::Buffered,
                v if v == (IoMode::Direct as u8) => IoMode::Direct,
                v if v == (IoMode::DirectRw as u8) => IoMode::DirectRw,
                x => return Err(x),
            })
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanDisposableKeysResponse {
    pub disposable_count: usize,
    pub not_disposable_count: usize,
}

// This is a cut-down version of TenantHistorySize from the pageserver crate, omitting fields
// that require pageserver-internal types.  It is sufficient to get the total size.
#[derive(Serialize, Deserialize, Debug)]
pub struct TenantHistorySize {
    pub id: TenantId,
    /// Size is a mixture of WAL and logical size, so the unit is bytes.
    ///
    /// Will be none if `?inputs_only=true` was given.
    pub size: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PageTraceEvent {
    pub key: CompactKey,
    pub effective_lsn: Lsn,
    pub time: SystemTime,
}

impl Default for PageTraceEvent {
    fn default() -> Self {
        Self {
            key: Default::default(),
            effective_lsn: Default::default(),
            time: std::time::UNIX_EPOCH,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;

    use super::*;

    #[test]
    fn test_tenantinfo_serde() {
        // Test serialization/deserialization of TenantInfo
        let original_active = TenantInfo {
            id: TenantShardId::unsharded(TenantId::generate()),
            state: TenantState::Active,
            current_physical_size: Some(42),
            attachment_status: TenantAttachmentStatus::Attached,
            generation: 1,
            gc_blocking: None,
        };
        let expected_active = json!({
            "id": original_active.id.to_string(),
            "state": {
                "slug": "Active",
            },
            "current_physical_size": 42,
            "attachment_status": {
                "slug":"attached",
            },
            "generation" : 1
        });

        let original_broken = TenantInfo {
            id: TenantShardId::unsharded(TenantId::generate()),
            state: TenantState::Broken {
                reason: "reason".into(),
                backtrace: "backtrace info".into(),
            },
            current_physical_size: Some(42),
            attachment_status: TenantAttachmentStatus::Attached,
            generation: 1,
            gc_blocking: None,
        };
        let expected_broken = json!({
            "id": original_broken.id.to_string(),
            "state": {
                "slug": "Broken",
                "data": {
                    "backtrace": "backtrace info",
                    "reason": "reason",
                }
            },
            "current_physical_size": 42,
            "attachment_status": {
                "slug":"attached",
            },
            "generation" : 1
        });

        assert_eq!(
            serde_json::to_value(&original_active).unwrap(),
            expected_active
        );

        assert_eq!(
            serde_json::to_value(&original_broken).unwrap(),
            expected_broken
        );
        assert!(format!("{:?}", &original_broken.state).contains("reason"));
        assert!(format!("{:?}", &original_broken.state).contains("backtrace info"));
    }

    #[test]
    fn test_reject_unknown_field() {
        let id = TenantId::generate();
        let config_request = json!({
            "tenant_id": id.to_string(),
            "unknown_field": "unknown_value".to_string(),
        });
        let err = serde_json::from_value::<TenantConfigRequest>(config_request).unwrap_err();
        assert!(
            err.to_string().contains("unknown field `unknown_field`"),
            "expect unknown field `unknown_field` error, got: {err}"
        );
    }

    #[test]
    fn tenantstatus_activating_serde() {
        let states = [TenantState::Activating(ActivatingFrom::Attaching)];
        let expected = "[{\"slug\":\"Activating\",\"data\":\"Attaching\"}]";

        let actual = serde_json::to_string(&states).unwrap();

        assert_eq!(actual, expected);

        let parsed = serde_json::from_str::<Vec<TenantState>>(&actual).unwrap();

        assert_eq!(states.as_slice(), &parsed);
    }

    #[test]
    fn tenantstatus_activating_strum() {
        // tests added, because we use these for metrics
        let examples = [
            (line!(), TenantState::Attaching, "Attaching"),
            (
                line!(),
                TenantState::Activating(ActivatingFrom::Attaching),
                "Activating",
            ),
            (line!(), TenantState::Active, "Active"),
            (
                line!(),
                TenantState::Stopping { progress: None },
                "Stopping",
            ),
            (
                line!(),
                TenantState::Stopping {
                    progress: Some(completion::Barrier::default()),
                },
                "Stopping",
            ),
            (
                line!(),
                TenantState::Broken {
                    reason: "Example".into(),
                    backtrace: "Looooong backtrace".into(),
                },
                "Broken",
            ),
        ];

        for (line, rendered, expected) in examples {
            let actual: &'static str = rendered.into();
            assert_eq!(actual, expected, "example on {line}");
        }
    }

    #[test]
    fn test_image_compression_algorithm_parsing() {
        use ImageCompressionAlgorithm::*;
        let cases = [
            ("disabled", Disabled),
            ("zstd", Zstd { level: None }),
            ("zstd(18)", Zstd { level: Some(18) }),
            ("zstd(-3)", Zstd { level: Some(-3) }),
        ];

        for (display, expected) in cases {
            assert_eq!(
                ImageCompressionAlgorithm::from_str(display).unwrap(),
                expected,
                "parsing works"
            );
            assert_eq!(format!("{expected}"), display, "Display FromStr roundtrip");

            let ser = serde_json::to_string(&expected).expect("serialization");
            assert_eq!(
                serde_json::from_str::<ImageCompressionAlgorithm>(&ser).unwrap(),
                expected,
                "serde roundtrip"
            );

            assert_eq!(
                serde_json::Value::String(display.to_string()),
                serde_json::to_value(expected).unwrap(),
                "Display is the serde serialization"
            );
        }
    }

    #[test]
    fn test_tenant_config_patch_request_serde() {
        let patch_request = TenantConfigPatchRequest {
            tenant_id: TenantId::from_str("17c6d121946a61e5ab0fe5a2fd4d8215").unwrap(),
            config: TenantConfigPatch {
                checkpoint_distance: FieldPatch::Upsert(42),
                gc_horizon: FieldPatch::Remove,
                compaction_threshold: FieldPatch::Noop,
                ..TenantConfigPatch::default()
            },
        };

        let json = serde_json::to_string(&patch_request).unwrap();

        let expected = r#"{"tenant_id":"17c6d121946a61e5ab0fe5a2fd4d8215","checkpoint_distance":42,"gc_horizon":null}"#;
        assert_eq!(json, expected);

        let decoded: TenantConfigPatchRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.tenant_id, patch_request.tenant_id);
        assert_eq!(decoded.config, patch_request.config);

        // Now apply the patch to a config to demonstrate semantics

        let base = TenantConfig {
            checkpoint_distance: Some(28),
            gc_horizon: Some(100),
            compaction_target_size: Some(1024),
            ..Default::default()
        };

        let expected = TenantConfig {
            checkpoint_distance: Some(42),
            gc_horizon: None,
            ..base.clone()
        };

        let patched = base.apply_patch(decoded.config).unwrap();

        assert_eq!(patched, expected);
    }
}
