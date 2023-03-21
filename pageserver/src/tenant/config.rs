//! Functions for handling per-tenant configuration options
//!
//! If tenant is created with --config option,
//! the tenant-specific config will be stored in tenant's directory.
//! Otherwise, global pageserver's config is used.
//!
//! If the tenant config file is corrupted, the tenant will be disabled.
//! We cannot use global or default config instead, because wrong settings
//! may lead to a data loss.
//!
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;
use std::time::Duration;

pub mod defaults {
    // FIXME: This current value is very low. I would imagine something like 1 GB or 10 GB
    // would be more appropriate. But a low value forces the code to be exercised more,
    // which is good for now to trigger bugs.
    // This parameter actually determines L0 layer file size.
    pub const DEFAULT_CHECKPOINT_DISTANCE: u64 = 256 * 1024 * 1024;
    pub const DEFAULT_CHECKPOINT_TIMEOUT: &str = "10 m";

    // Target file size, when creating image and delta layers.
    // This parameter determines L1 layer file size.
    pub const DEFAULT_COMPACTION_TARGET_SIZE: u64 = 128 * 1024 * 1024;

    pub const DEFAULT_COMPACTION_PERIOD: &str = "20 s";
    pub const DEFAULT_COMPACTION_THRESHOLD: usize = 10;

    pub const DEFAULT_GC_HORIZON: u64 = 64 * 1024 * 1024;

    // Large DEFAULT_GC_PERIOD is fine as long as PITR_INTERVAL is larger.
    // If there's a need to decrease this value, first make sure that GC
    // doesn't hold a layer map write lock for non-trivial operations.
    // Relevant: https://github.com/neondatabase/neon/issues/3394
    pub const DEFAULT_GC_PERIOD: &str = "1 hr";
    pub const DEFAULT_IMAGE_CREATION_THRESHOLD: usize = 3;
    pub const DEFAULT_PITR_INTERVAL: &str = "7 days";
    pub const DEFAULT_WALRECEIVER_CONNECT_TIMEOUT: &str = "2 seconds";
    pub const DEFAULT_WALRECEIVER_LAGGING_WAL_TIMEOUT: &str = "3 seconds";
    pub const DEFAULT_MAX_WALRECEIVER_LSN_WAL_LAG: u64 = 10 * 1024 * 1024;
}

/// Per-tenant configuration options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantConf {
    // Flush out an inmemory layer, if it's holding WAL older than this
    // This puts a backstop on how much WAL needs to be re-digested if the
    // page server crashes.
    // This parameter actually determines L0 layer file size.
    pub checkpoint_distance: u64,
    // Inmemory layer is also flushed at least once in checkpoint_timeout to
    // eventually upload WAL after activity is stopped.
    #[serde(with = "humantime_serde")]
    pub checkpoint_timeout: Duration,
    // Target file size, when creating image and delta layers.
    // This parameter determines L1 layer file size.
    pub compaction_target_size: u64,
    // How often to check if there's compaction work to be done.
    // Duration::ZERO means automatic compaction is disabled.
    #[serde(with = "humantime_serde")]
    pub compaction_period: Duration,
    // Level0 delta layer threshold for compaction.
    pub compaction_threshold: usize,
    // Determines how much history is retained, to allow
    // branching and read replicas at an older point in time.
    // The unit is #of bytes of WAL.
    // Page versions older than this are garbage collected away.
    pub gc_horizon: u64,
    // Interval at which garbage collection is triggered.
    // Duration::ZERO means automatic GC is disabled
    #[serde(with = "humantime_serde")]
    pub gc_period: Duration,
    // Delta layer churn threshold to create L1 image layers.
    pub image_creation_threshold: usize,
    // Determines how much history is retained, to allow
    // branching and read replicas at an older point in time.
    // The unit is time.
    // Page versions older than this are garbage collected away.
    #[serde(with = "humantime_serde")]
    pub pitr_interval: Duration,
    /// Maximum amount of time to wait while opening a connection to receive wal, before erroring.
    #[serde(with = "humantime_serde")]
    pub walreceiver_connect_timeout: Duration,
    /// Considers safekeepers stalled after no WAL updates were received longer than this threshold.
    /// A stalled safekeeper will be changed to a newer one when it appears.
    #[serde(with = "humantime_serde")]
    pub lagging_wal_timeout: Duration,
    /// Considers safekeepers lagging when their WAL is behind another safekeeper for more than this threshold.
    /// A lagging safekeeper will be changed after `lagging_wal_timeout` time elapses since the last WAL update,
    /// to avoid eager reconnects.
    pub max_lsn_wal_lag: NonZeroU64,
    pub trace_read_requests: bool,
    pub eviction_policy: EvictionPolicy,
}

/// Same as TenantConf, but this struct preserves the information about
/// which parameters are set and which are not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TenantConfOpt {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub checkpoint_distance: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub checkpoint_timeout: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub compaction_target_size: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub compaction_period: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub compaction_threshold: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub gc_horizon: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub gc_period: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub image_creation_threshold: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub pitr_interval: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub walreceiver_connect_timeout: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub lagging_wal_timeout: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub max_lsn_wal_lag: Option<NonZeroU64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub trace_read_requests: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub eviction_policy: Option<EvictionPolicy>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum EvictionPolicy {
    NoEviction,
    LayerAccessThreshold(EvictionPolicyLayerAccessThreshold),
}

impl EvictionPolicy {
    pub fn discriminant_str(&self) -> &'static str {
        match self {
            EvictionPolicy::NoEviction => "NoEviction",
            EvictionPolicy::LayerAccessThreshold(_) => "LayerAccessThreshold",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvictionPolicyLayerAccessThreshold {
    /// The period at which the policy is evaluated for the timeline's layers.
    #[serde(with = "humantime_serde")]
    pub period: Duration,
    /// Layers for which `now - latest_activity() > threshold` become eviction candidates.
    #[serde(with = "humantime_serde")]
    pub threshold: Duration,
    /// Spare the given size worth of eviction candidates from eviction.
    ///
    /// Eviction cancidates as determined through threshold.
    ///
    /// The reason why this setting exists is the following:
    /// In the Neon production deployment, the control plane does periodic health checks
    /// that write to the database. Their purpose is to ensure that the data path works.
    /// Also, there is the check_availability operation. TODO actually these health checks ARE check_availability.
    /// So, even if a tenant is unused or read-only from the Neon customer's point of view,
    /// there are small amounts of writes from time to time.
    /// The pageserver flushes the in-memory layer that holds the WAL for these writes
    /// periodically (default 10min), thereby creating a very small L0 layer every time
    /// the health check runs (health check period >> 10min).
    /// Once `compaction_threshold` L0 layers have accumulated compaction processes all those
    /// small L0 layers into an L1 layer, and deletes the small L0 layers after.
    /// For an inactive tenant, that happens every `compaction_threshold x health check period` days.
    ///
    /// Enter eviction & on-demand downloads.
    ///
    /// If the control plane's periodic health checks happen less frequently than the eviction
    /// policy's `threshold` value, the small L0 layers are evicted, because nothing is accessing them.
    /// But, a pageserver restart will lead to a huge spike of on-demand downloads.
    /// For example, on a system with 1500 tenants, unknown portion inactive, it's 5k layer downloads,
    /// very small size for most of them, split 50:50 between initial size calculation and compaction.
    /// We have reason to assume that, if the initial size calculation were not happenening,
    /// compaction would download all of the 5k, and vice versa.
    /// Why is that?
    /// 1. Compaction: even if there are < `compaction_threshold` layers, the
    ///    repartitioning / collect_keyspace causes on-demand downloads.
    ///    The first place that needs the download is the read of DBDIR_KEY.
    ///    The small L0's from above don't update that key, but since L0s cover the full key range,
    ///    they get downloaded regardless. ALL L0s!
    /// 2. Initital size calcuation: even if we disable compaction entirely, initial size calculation
    ///    does on-demand downloads. In the neon prod deployment, the initial size calculation spike
    ///    after pageserver restart is caused by
    ///     https://neonprod.grafana.net/explore?orgId=1&left=%7B%22datasource%22:%22grafanacloud-logs%22,%22queries%22:%5B%7B%22refId%22:%22A%22,%22datasource%22:%7B%22type%22:%22loki%22,%22uid%22:%22grafanacloud-logs%22%7D,%22editorMode%22:%22builder%22,%22expr%22:%22%7Bneon_service%3D%5C%22pageserver%5C%22,%20hostname%3D%5C%22pageserver-0.eu-central-1.aws.neon.tech%5C%22%7D%20%7C%3D%20%60spawning%20logical%20size%20computation%20from%20context%20of%20task%20kind%60%20%7C%20regexp%20%60spawning%20logical%20size%20computation%20from%20context%20of%20task%20kind%20%28%3FP%3Ctask_kind%3E%5C%5CS%2B%29%60%20%7C%20line_format%20%60%7B%7B.task_kind%7D%7D%60%22,%22queryType%22:%22range%22,%22maxLines%22:5000%7D%5D,%22range%22:%7B%22from%22:%221679046960000%22,%22to%22:%221679047560000%22%7D%7D
    ///         265 MetricsCollection
    ///         1472 MgmtRequest
    ///         89 WalReceiverConnectionHandler
    ///    It is unclear whether MgmtRequest is from console or from layer map scraper.
    ///    Anyways, the cause for on-demand download of all L0's overwhelmingly is the
    ///    get_current_logical_size_non_incremental accessing DBDIR_KEY.
    ///
    /// Now back to the question why pageserver restart causes the spike.
    /// Why do we have more on-demand downloads after a PS restart compared to regular operation?
    /// The answer must be that something caches the keys that, without a cache, would cause the small L0s to be donwloaded.
    /// My first suspect was the page cache, but, I did an experiment where I manually dropped all page cache pages via a to-be-PR'ed HTTP handler, then re-reran compaction.
    /// In that experiment, compaction didn't do any re-downloads.
    /// So, digging further, I found that the Timeline::repartition early-exits with "no repartitioning needed"
    /// if there has been a repartitioning in the past, and the WAL has grown less than `repartition_threshold`.
    /// That's it.
    /// The restart blows away the Timeline::partitioning, and the initial size calculation result, both of which act as caches.
    /// For otherwise unused tenants, they together prevent on-demand downloads for compaction or initial size calculation.
    /// So, otherwise unused tenants will see evictions of their small L0s.
    /// So, on pageserver restart, we see the spike in re-downloads.
    ///
    /// (The GC doesn't keep the layers resident because TODO (the L0's key range is everything, so, image_layer_exists returns false?))
    ///
    /// TODO: figure out if our strategy to spare an amount of candidates is still the right move.
    /// Since nothing accesses these L0s, it's somewhat dubious whether they will be the most-recently-used eviction candidates.
    /// Would it maybe better to not evict L0s at all, and add some kind of timeout to compaction that forces compact_level0?
    #[serde(default)]
    pub do_not_evict_most_recent_candidates_bytes: u64,
}

impl TenantConfOpt {
    pub fn merge(&self, global_conf: TenantConf) -> TenantConf {
        TenantConf {
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
            trace_read_requests: self
                .trace_read_requests
                .unwrap_or(global_conf.trace_read_requests),
            eviction_policy: self.eviction_policy.unwrap_or(global_conf.eviction_policy),
        }
    }
}

impl Default for TenantConf {
    fn default() -> Self {
        use defaults::*;
        Self {
            checkpoint_distance: DEFAULT_CHECKPOINT_DISTANCE,
            checkpoint_timeout: humantime::parse_duration(DEFAULT_CHECKPOINT_TIMEOUT)
                .expect("cannot parse default checkpoint timeout"),
            compaction_target_size: DEFAULT_COMPACTION_TARGET_SIZE,
            compaction_period: humantime::parse_duration(DEFAULT_COMPACTION_PERIOD)
                .expect("cannot parse default compaction period"),
            compaction_threshold: DEFAULT_COMPACTION_THRESHOLD,
            gc_horizon: DEFAULT_GC_HORIZON,
            gc_period: humantime::parse_duration(DEFAULT_GC_PERIOD)
                .expect("cannot parse default gc period"),
            image_creation_threshold: DEFAULT_IMAGE_CREATION_THRESHOLD,
            pitr_interval: humantime::parse_duration(DEFAULT_PITR_INTERVAL)
                .expect("cannot parse default PITR interval"),
            walreceiver_connect_timeout: humantime::parse_duration(
                DEFAULT_WALRECEIVER_CONNECT_TIMEOUT,
            )
            .expect("cannot parse default walreceiver connect timeout"),
            lagging_wal_timeout: humantime::parse_duration(DEFAULT_WALRECEIVER_LAGGING_WAL_TIMEOUT)
                .expect("cannot parse default walreceiver lagging wal timeout"),
            max_lsn_wal_lag: NonZeroU64::new(DEFAULT_MAX_WALRECEIVER_LSN_WAL_LAG)
                .expect("cannot parse default max walreceiver Lsn wal lag"),
            trace_read_requests: false,
            eviction_policy: EvictionPolicy::NoEviction,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn de_serializing_pageserver_config_omits_empty_values() {
        let small_conf = TenantConfOpt {
            gc_horizon: Some(42),
            ..TenantConfOpt::default()
        };

        let toml_form = toml_edit::easy::to_string(&small_conf).unwrap();
        assert_eq!(toml_form, "gc_horizon = 42\n");
        assert_eq!(small_conf, toml_edit::easy::from_str(&toml_form).unwrap());

        let json_form = serde_json::to_string(&small_conf).unwrap();
        assert_eq!(json_form, "{\"gc_horizon\":42}");
        assert_eq!(small_conf, serde_json::from_str(&json_form).unwrap());
    }
}
