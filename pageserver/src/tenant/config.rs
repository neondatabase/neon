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
use anyhow::Context;
use pageserver_api::models;
use pageserver_api::shard::{ShardCount, ShardIdentity, ShardNumber, ShardStripeSize};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;
use std::time::Duration;
use utils::generation::Generation;

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
    pub const DEFAULT_WALRECEIVER_CONNECT_TIMEOUT: &str = "10 seconds";
    pub const DEFAULT_WALRECEIVER_LAGGING_WAL_TIMEOUT: &str = "10 seconds";
    pub const DEFAULT_MAX_WALRECEIVER_LSN_WAL_LAG: u64 = 10 * 1024 * 1024;
    pub const DEFAULT_EVICTIONS_LOW_RESIDENCE_DURATION_METRIC_THRESHOLD: &str = "24 hour";
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum AttachmentMode {
    /// Our generation is current as far as we know, and as far as we know we are the only attached
    /// pageserver.  This is the "normal" attachment mode.
    Single,
    /// Our generation number is current as far as we know, but we are advised that another
    /// pageserver is still attached, and therefore to avoid executing deletions.   This is
    /// the attachment mode of a pagesever that is the destination of a migration.
    Multi,
    /// Our generation number is superseded, or about to be superseded.  We are advised
    /// to avoid remote storage writes if possible, and to avoid sending billing data.  This
    /// is the attachment mode of a pageserver that is the origin of a migration.
    Stale,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AttachedLocationConfig {
    pub(crate) generation: Generation,
    pub(crate) attach_mode: AttachmentMode,
    // TODO: add a flag to override AttachmentMode's policies under
    // disk pressure (i.e. unblock uploads under disk pressure in Stale
    // state, unblock deletions after timeout in Multi state)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SecondaryLocationConfig {
    /// If true, keep the local cache warm by polling remote storage
    pub(crate) warm: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum LocationMode {
    Attached(AttachedLocationConfig),
    Secondary(SecondaryLocationConfig),
}

/// Per-tenant, per-pageserver configuration.  All pageservers use the same TenantConf,
/// but have distinct LocationConf.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LocationConf {
    /// Detailed identity of this TenantShard.  The shard number and count usually
    /// appear in the keys of maps containing tenants, but it is convenient to also
    /// store it here.
    pub(crate) shard: ShardIdentity,

    /// The location-specific part of the configuration, describes the operating
    /// mode of this pageserver for this tenant.
    pub(crate) mode: LocationMode,
    /// The pan-cluster tenant configuration, the same on all locations
    pub(crate) tenant_conf: TenantConfOpt,
}

impl std::fmt::Debug for LocationConf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.mode {
            LocationMode::Attached(conf) => {
                write!(
                    f,
                    "Attached {:?}, gen={:?}",
                    conf.attach_mode, conf.generation
                )
            }
            LocationMode::Secondary(conf) => {
                write!(f, "Secondary, warm={}", conf.warm)
            }
        }
    }
}

impl AttachedLocationConfig {
    /// Consult attachment mode to determine whether we are currently permitted
    /// to delete layers.  This is only advisory, not required for data safety.
    /// See [`AttachmentMode`] for more context.
    pub(crate) fn may_delete_layers_hint(&self) -> bool {
        // TODO: add an override for disk pressure in AttachedLocationConfig,
        // and respect it here.
        match &self.attach_mode {
            AttachmentMode::Single => true,
            AttachmentMode::Multi | AttachmentMode::Stale => {
                // In Multi mode we avoid doing deletions because some other
                // attached pageserver might get 404 while trying to read
                // a layer we delete which is still referenced in their metadata.
                //
                // In Stale mode, we avoid doing deletions because we expect
                // that they would ultimately fail validation in the deletion
                // queue due to our stale generation.
                false
            }
        }
    }

    /// Whether we are currently hinted that it is worthwhile to upload layers.
    /// This is only advisory, not required for data safety.
    /// See [`AttachmentMode`] for more context.
    pub(crate) fn may_upload_layers_hint(&self) -> bool {
        // TODO: add an override for disk pressure in AttachedLocationConfig,
        // and respect it here.
        match &self.attach_mode {
            AttachmentMode::Single | AttachmentMode::Multi => true,
            AttachmentMode::Stale => {
                // In Stale mode, we avoid doing uploads because we expect that
                // our replacement pageserver will already have started its own
                // IndexPart that will never reference layers we upload: it is
                // wasteful.
                false
            }
        }
    }
}

impl LocationConf {
    /// For use when loading from a legacy configuration: presence of a tenant
    /// implies it is in AttachmentMode::Single, which used to be the only
    /// possible state.  This function should eventually be removed.
    pub(crate) fn attached_single(tenant_conf: TenantConfOpt, generation: Generation) -> Self {
        Self {
            shard: ShardIdentity::none(),
            mode: LocationMode::Attached(AttachedLocationConfig {
                generation,
                attach_mode: AttachmentMode::Single,
            }),
            tenant_conf,
        }
    }

    /// For use when attaching/re-attaching: update the generation stored in this
    /// structure.  If we were in a secondary state, promote to attached (posession
    /// of a fresh generation implies this).
    pub(crate) fn attach_in_generation(&mut self, generation: Generation) {
        match &mut self.mode {
            LocationMode::Attached(attach_conf) => {
                attach_conf.generation = generation;
            }
            LocationMode::Secondary(_) => {
                // We are promoted to attached by the control plane's re-attach response
                self.mode = LocationMode::Attached(AttachedLocationConfig {
                    generation,
                    attach_mode: AttachmentMode::Single,
                })
            }
        }
    }

    pub(crate) fn try_from(conf: &'_ models::LocationConfig) -> anyhow::Result<Self> {
        let tenant_conf = TenantConfOpt::try_from(&conf.tenant_conf)?;

        fn get_generation(conf: &'_ models::LocationConfig) -> Result<Generation, anyhow::Error> {
            conf.generation
                .ok_or_else(|| anyhow::anyhow!("Generation must be set when attaching"))
        }

        let mode = match &conf.mode {
            models::LocationConfigMode::AttachedMulti => {
                LocationMode::Attached(AttachedLocationConfig {
                    generation: get_generation(conf)?,
                    attach_mode: AttachmentMode::Multi,
                })
            }
            models::LocationConfigMode::AttachedSingle => {
                LocationMode::Attached(AttachedLocationConfig {
                    generation: get_generation(conf)?,
                    attach_mode: AttachmentMode::Single,
                })
            }
            models::LocationConfigMode::AttachedStale => {
                LocationMode::Attached(AttachedLocationConfig {
                    generation: get_generation(conf)?,
                    attach_mode: AttachmentMode::Stale,
                })
            }
            models::LocationConfigMode::Secondary => {
                anyhow::ensure!(conf.generation.is_none());

                let warm = conf
                    .secondary_conf
                    .as_ref()
                    .map(|c| c.warm)
                    .unwrap_or(false);
                LocationMode::Secondary(SecondaryLocationConfig { warm })
            }
            models::LocationConfigMode::Detached => {
                // Should not have been called: API code should translate this mode
                // into a detach rather than trying to decode it as a LocationConf
                return Err(anyhow::anyhow!("Cannot decode a Detached configuration"));
            }
        };

        let shard = if conf.shard_count == 0 {
            ShardIdentity::none()
        } else {
            ShardIdentity::new(
                ShardNumber(conf.shard_number),
                ShardCount(conf.shard_count),
                ShardStripeSize(conf.shard_stripe_size),
            )
        };

        Ok(Self {
            shard,
            mode,
            tenant_conf,
        })
    }
}

impl Default for LocationConf {
    // TODO: this should be removed once tenant loading can guarantee that we are never
    // loading from a directory without a configuration.
    // => tech debt since https://github.com/neondatabase/neon/issues/1555
    fn default() -> Self {
        Self {
            shard: ShardIdentity::none(),
            mode: LocationMode::Attached(AttachedLocationConfig {
                generation: Generation::none(),
                attach_mode: AttachmentMode::Single,
            }),
            tenant_conf: TenantConfOpt::default(),
        }
    }
}

/// A tenant's calcuated configuration, which is the result of merging a
/// tenant's TenantConfOpt with the global TenantConf from PageServerConf.
///
/// For storing and transmitting individual tenant's configuration, see
/// TenantConfOpt.
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
    pub min_resident_size_override: Option<u64>,
    // See the corresponding metric's help string.
    #[serde(with = "humantime_serde")]
    pub evictions_low_residence_duration_metric_threshold: Duration,
    pub gc_feedback: bool,
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

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub min_resident_size_override: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[serde(default)]
    pub evictions_low_residence_duration_metric_threshold: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub gc_feedback: Option<bool>,
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
            min_resident_size_override: self
                .min_resident_size_override
                .or(global_conf.min_resident_size_override),
            evictions_low_residence_duration_metric_threshold: self
                .evictions_low_residence_duration_metric_threshold
                .unwrap_or(global_conf.evictions_low_residence_duration_metric_threshold),
            gc_feedback: self.gc_feedback.unwrap_or(global_conf.gc_feedback),
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
            min_resident_size_override: None,
            evictions_low_residence_duration_metric_threshold: humantime::parse_duration(
                DEFAULT_EVICTIONS_LOW_RESIDENCE_DURATION_METRIC_THRESHOLD,
            )
            .expect("cannot parse default evictions_low_residence_duration_metric_threshold"),
            gc_feedback: false,
        }
    }
}

// Helper function to standardize the error messages we produce on bad durations
//
// Intended to be used with anyhow's `with_context`, e.g.:
//
//   let value = result.with_context(bad_duration("name", &value))?;
//
fn bad_duration<'a>(field_name: &'static str, value: &'a str) -> impl 'a + Fn() -> String {
    move || format!("Cannot parse `{field_name}` duration {value:?}")
}

impl TryFrom<&'_ models::TenantConfig> for TenantConfOpt {
    type Error = anyhow::Error;

    fn try_from(request_data: &'_ models::TenantConfig) -> Result<Self, Self::Error> {
        let mut tenant_conf = TenantConfOpt::default();

        if let Some(gc_period) = &request_data.gc_period {
            tenant_conf.gc_period = Some(
                humantime::parse_duration(gc_period)
                    .with_context(bad_duration("gc_period", gc_period))?,
            );
        }
        tenant_conf.gc_horizon = request_data.gc_horizon;
        tenant_conf.image_creation_threshold = request_data.image_creation_threshold;

        if let Some(pitr_interval) = &request_data.pitr_interval {
            tenant_conf.pitr_interval = Some(
                humantime::parse_duration(pitr_interval)
                    .with_context(bad_duration("pitr_interval", pitr_interval))?,
            );
        }

        if let Some(walreceiver_connect_timeout) = &request_data.walreceiver_connect_timeout {
            tenant_conf.walreceiver_connect_timeout = Some(
                humantime::parse_duration(walreceiver_connect_timeout).with_context(
                    bad_duration("walreceiver_connect_timeout", walreceiver_connect_timeout),
                )?,
            );
        }
        if let Some(lagging_wal_timeout) = &request_data.lagging_wal_timeout {
            tenant_conf.lagging_wal_timeout = Some(
                humantime::parse_duration(lagging_wal_timeout)
                    .with_context(bad_duration("lagging_wal_timeout", lagging_wal_timeout))?,
            );
        }
        if let Some(max_lsn_wal_lag) = request_data.max_lsn_wal_lag {
            tenant_conf.max_lsn_wal_lag = Some(max_lsn_wal_lag);
        }
        if let Some(trace_read_requests) = request_data.trace_read_requests {
            tenant_conf.trace_read_requests = Some(trace_read_requests);
        }

        tenant_conf.checkpoint_distance = request_data.checkpoint_distance;
        if let Some(checkpoint_timeout) = &request_data.checkpoint_timeout {
            tenant_conf.checkpoint_timeout = Some(
                humantime::parse_duration(checkpoint_timeout)
                    .with_context(bad_duration("checkpoint_timeout", checkpoint_timeout))?,
            );
        }

        tenant_conf.compaction_target_size = request_data.compaction_target_size;
        tenant_conf.compaction_threshold = request_data.compaction_threshold;

        if let Some(compaction_period) = &request_data.compaction_period {
            tenant_conf.compaction_period = Some(
                humantime::parse_duration(compaction_period)
                    .with_context(bad_duration("compaction_period", compaction_period))?,
            );
        }

        if let Some(eviction_policy) = &request_data.eviction_policy {
            tenant_conf.eviction_policy = Some(
                serde::Deserialize::deserialize(eviction_policy)
                    .context("parse field `eviction_policy`")?,
            );
        }

        tenant_conf.min_resident_size_override = request_data.min_resident_size_override;

        if let Some(evictions_low_residence_duration_metric_threshold) =
            &request_data.evictions_low_residence_duration_metric_threshold
        {
            tenant_conf.evictions_low_residence_duration_metric_threshold = Some(
                humantime::parse_duration(evictions_low_residence_duration_metric_threshold)
                    .with_context(bad_duration(
                        "evictions_low_residence_duration_metric_threshold",
                        evictions_low_residence_duration_metric_threshold,
                    ))?,
            );
        }
        tenant_conf.gc_feedback = request_data.gc_feedback;

        Ok(tenant_conf)
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

        let toml_form = toml_edit::ser::to_string(&small_conf).unwrap();
        assert_eq!(toml_form, "gc_horizon = 42\n");
        assert_eq!(small_conf, toml_edit::de::from_str(&toml_form).unwrap());

        let json_form = serde_json::to_string(&small_conf).unwrap();
        assert_eq!(json_form, "{\"gc_horizon\":42}");
        assert_eq!(small_conf, serde_json::from_str(&json_form).unwrap());
    }
}
