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
use crate::config::PageServerConf;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use utils::zid::ZTenantId;

pub const TENANT_CONFIG_NAME: &str = "config";

pub mod defaults {
    // FIXME: This current value is very low. I would imagine something like 1 GB or 10 GB
    // would be more appropriate. But a low value forces the code to be exercised more,
    // which is good for now to trigger bugs.
    // This parameter actually determines L0 layer file size.
    pub const DEFAULT_CHECKPOINT_DISTANCE: u64 = 256 * 1024 * 1024;

    // Target file size, when creating image and delta layers.
    // This parameter determines L1 layer file size.
    pub const DEFAULT_COMPACTION_TARGET_SIZE: u64 = 128 * 1024 * 1024;

    pub const DEFAULT_COMPACTION_PERIOD: &str = "1 s";
    pub const DEFAULT_COMPACTION_THRESHOLD: usize = 10;

    pub const DEFAULT_GC_HORIZON: u64 = 64 * 1024 * 1024;
    pub const DEFAULT_GC_PERIOD: &str = "100 s";
    pub const DEFAULT_PITR_INTERVAL: &str = "30 days";
}

/// Per-tenant configuration options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantConf {
    // Flush out an inmemory layer, if it's holding WAL older than this
    // This puts a backstop on how much WAL needs to be re-digested if the
    // page server crashes.
    // This parameter actually determines L0 layer file size.
    pub checkpoint_distance: u64,
    // Target file size, when creating image and delta layers.
    // This parameter determines L1 layer file size.
    pub compaction_target_size: u64,
    // How often to check if there's compaction work to be done.
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
    #[serde(with = "humantime_serde")]
    pub gc_period: Duration,
    // Determines how much history is retained, to allow
    // branching and read replicas at an older point in time.
    // The unit is time.
    // Page versions older than this are garbage collected away.
    #[serde(with = "humantime_serde")]
    pub pitr_interval: Duration,
}

/// Same as TenantConf, but this struct preserves the information about
/// which parameters are set and which are not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TenantConfOpt {
    pub checkpoint_distance: Option<u64>,
    pub compaction_target_size: Option<u64>,
    #[serde(with = "humantime_serde")]
    pub compaction_period: Option<Duration>,
    pub compaction_threshold: Option<usize>,
    pub gc_horizon: Option<u64>,
    #[serde(with = "humantime_serde")]
    pub gc_period: Option<Duration>,
    #[serde(with = "humantime_serde")]
    pub pitr_interval: Option<Duration>,
}

impl TenantConfOpt {
    pub fn merge(&self, global_conf: TenantConf) -> TenantConf {
        TenantConf {
            checkpoint_distance: self
                .checkpoint_distance
                .unwrap_or(global_conf.checkpoint_distance),
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
            pitr_interval: self.pitr_interval.unwrap_or(global_conf.pitr_interval),
        }
    }

    pub fn update(&mut self, other: &TenantConfOpt) {
        if let Some(checkpoint_distance) = other.checkpoint_distance {
            self.checkpoint_distance = Some(checkpoint_distance);
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
        if let Some(pitr_interval) = other.pitr_interval {
            self.pitr_interval = Some(pitr_interval);
        }
    }
}

impl TenantConf {
    pub fn default() -> TenantConf {
        use defaults::*;

        TenantConf {
            checkpoint_distance: DEFAULT_CHECKPOINT_DISTANCE,
            compaction_target_size: DEFAULT_COMPACTION_TARGET_SIZE,
            compaction_period: humantime::parse_duration(DEFAULT_COMPACTION_PERIOD)
                .expect("cannot parse default compaction period"),
            compaction_threshold: DEFAULT_COMPACTION_THRESHOLD,
            gc_horizon: DEFAULT_GC_HORIZON,
            gc_period: humantime::parse_duration(DEFAULT_GC_PERIOD)
                .expect("cannot parse default gc period"),
            pitr_interval: humantime::parse_duration(DEFAULT_PITR_INTERVAL)
                .expect("cannot parse default PITR interval"),
        }
    }

    /// Points to a place in pageserver's local directory,
    /// where certain tenant's tenantconf file should be located.
    pub fn path(conf: &'static PageServerConf, tenantid: ZTenantId) -> PathBuf {
        conf.tenant_path(&tenantid).join(TENANT_CONFIG_NAME)
    }

    #[cfg(test)]
    pub fn dummy_conf() -> Self {
        TenantConf {
            checkpoint_distance: defaults::DEFAULT_CHECKPOINT_DISTANCE,
            compaction_target_size: 4 * 1024 * 1024,
            compaction_period: Duration::from_secs(10),
            compaction_threshold: defaults::DEFAULT_COMPACTION_THRESHOLD,
            gc_horizon: defaults::DEFAULT_GC_HORIZON,
            gc_period: Duration::from_secs(10),
            pitr_interval: Duration::from_secs(60 * 60),
        }
    }
}
