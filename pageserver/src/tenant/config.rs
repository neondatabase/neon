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
    #[serde(with = "humantime_serde")]
    pub period: Duration,
    #[serde(with = "humantime_serde")]
    pub threshold: Duration,
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

    pub fn update(&mut self, other: &TenantConfOpt) {
        if let Some(checkpoint_distance) = other.checkpoint_distance {
            self.checkpoint_distance = Some(checkpoint_distance);
        }
        if let Some(checkpoint_timeout) = other.checkpoint_timeout {
            self.checkpoint_timeout = Some(checkpoint_timeout);
        }
        if let Some(compaction_target_size) = other.compaction_target_size {
            self.compaction_target_size = Some(compaction_target_size);
        }
        if let Some(compaction_period) = other.compaction_period {
            self.compaction_period = Some(compaction_period);
        }
        if let Some(compaction_threshold) = other.compaction_threshold {
            self.compaction_threshold = Some(compaction_threshold);
        }
        if let Some(gc_horizon) = other.gc_horizon {
            self.gc_horizon = Some(gc_horizon);
        }
        if let Some(gc_period) = other.gc_period {
            self.gc_period = Some(gc_period);
        }
        if let Some(image_creation_threshold) = other.image_creation_threshold {
            self.image_creation_threshold = Some(image_creation_threshold);
        }
        if let Some(pitr_interval) = other.pitr_interval {
            self.pitr_interval = Some(pitr_interval);
        }
        if let Some(walreceiver_connect_timeout) = other.walreceiver_connect_timeout {
            self.walreceiver_connect_timeout = Some(walreceiver_connect_timeout);
        }
        if let Some(lagging_wal_timeout) = other.lagging_wal_timeout {
            self.lagging_wal_timeout = Some(lagging_wal_timeout);
        }
        if let Some(max_lsn_wal_lag) = other.max_lsn_wal_lag {
            self.max_lsn_wal_lag = Some(max_lsn_wal_lag);
        }
        if let Some(trace_read_requests) = other.trace_read_requests {
            self.trace_read_requests = Some(trace_read_requests);
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
