use camino::Utf8PathBuf;

#[cfg(test)]
mod tests;

use const_format::formatcp;
use posthog_client_lite::PostHogClientConfig;
use utils::serde_percent::Percent;
pub const DEFAULT_PG_LISTEN_PORT: u16 = 64000;
pub const DEFAULT_PG_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_PG_LISTEN_PORT}");
pub const DEFAULT_HTTP_LISTEN_PORT: u16 = 9898;
pub const DEFAULT_HTTP_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_HTTP_LISTEN_PORT}");
// TODO: gRPC is disabled by default for now, but the port is used in neon_local.
pub const DEFAULT_GRPC_LISTEN_PORT: u16 = 51051; // storage-broker already uses 50051

use std::collections::HashMap;
use std::fmt::Display;
use std::num::{NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::time::Duration;

use postgres_backend::AuthType;
use remote_storage::RemoteStorageConfig;
use serde_with::serde_as;
use utils::logging::LogFormat;

use crate::models::{ImageCompressionAlgorithm, LsnLease};

// Certain metadata (e.g. externally-addressable name, AZ) is delivered
// as a separate structure.  This information is not needed by the pageserver
// itself, it is only used for registering the pageserver with the control
// plane and/or storage controller.
#[derive(PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub struct NodeMetadata {
    #[serde(rename = "host")]
    pub postgres_host: String,
    #[serde(rename = "port")]
    pub postgres_port: u16,
    pub grpc_host: Option<String>,
    pub grpc_port: Option<u16>,
    pub http_host: String,
    pub http_port: u16,
    pub https_port: Option<u16>,

    // Deployment tools may write fields to the metadata file beyond what we
    // use in this type: this type intentionally only names fields that require.
    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}

impl Display for NodeMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "postgresql://{}:{} ",
            self.postgres_host, self.postgres_port
        )?;
        if let Some(grpc_host) = &self.grpc_host {
            let grpc_port = self.grpc_port.unwrap_or_default();
            write!(f, "grpc://{grpc_host}:{grpc_port} ")?;
        }
        write!(f, "http://{}:{} ", self.http_host, self.http_port)?;
        write!(f, "other:{:?}", self.other)?;
        Ok(())
    }
}

/// PostHog integration config. This is used in pageserver, storcon, and neon_local.
/// Ensure backward compatibility when adding new fields.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PostHogConfig {
    /// PostHog project ID
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Server-side (private) API key
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_api_key: Option<String>,
    /// Client-side (public) API key
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_api_key: Option<String>,
    /// Private API URL
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_api_url: Option<String>,
    /// Public API URL
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_api_url: Option<String>,
    /// Refresh interval for the feature flag spec.
    /// The storcon will push the feature flag spec to the pageserver. If the pageserver does not receive
    /// the spec for `refresh_interval`, it will fetch the spec from the PostHog API.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    pub refresh_interval: Option<Duration>,
}

impl PostHogConfig {
    pub fn try_into_posthog_config(self) -> Result<PostHogClientConfig, &'static str> {
        let Some(project_id) = self.project_id else {
            return Err("project_id is required");
        };
        let Some(server_api_key) = self.server_api_key else {
            return Err("server_api_key is required");
        };
        let Some(client_api_key) = self.client_api_key else {
            return Err("client_api_key is required");
        };
        let Some(private_api_url) = self.private_api_url else {
            return Err("private_api_url is required");
        };
        let Some(public_api_url) = self.public_api_url else {
            return Err("public_api_url is required");
        };
        Ok(PostHogClientConfig {
            project_id,
            server_api_key,
            client_api_key,
            private_api_url,
            public_api_url,
        })
    }
}

/// `pageserver.toml`
///
/// We use serde derive with `#[serde(default)]` to generate a deserializer
/// that fills in the default values for each config field.
///
/// If there cannot be a static default value because we need to make runtime
/// checks to determine the default, make it an `Option` (which defaults to None).
/// The runtime check should be done in the consuming crate, i.e., `pageserver`.
///
/// Unknown fields are silently ignored during deserialization.
/// The alternative, which we used in the past, was to set `deny_unknown_fields`,
/// which fails deserialization, and hence pageserver startup, if there is an unknown field.
/// The reason we don't do that anymore is that it complicates
/// usage of config fields for feature flagging, which we commonly do for
/// region-by-region rollouts.
/// The complications mainly arise because the `pageserver.toml` contents on a
/// prod server have a separate lifecycle from the pageserver binary.
/// For instance, `pageserver.toml` contents today are defined in the internal
/// infra repo, and thus introducing a new config field to pageserver and
/// rolling it out to prod servers are separate commits in separate repos
/// that can't be made or rolled back atomically.
/// Rollbacks in particular pose a risk with deny_unknown_fields because
/// the old pageserver binary may reject a new config field, resulting in
/// an outage unless the person doing the pageserver rollback remembers
/// to also revert the commit that added the config field in to the
/// `pageserver.toml` templates in the internal infra repo.
/// (A pre-deploy config check would eliminate this risk during rollbacks,
///  cf [here](https://github.com/neondatabase/cloud/issues/24349).)
/// In addition to this compatibility problem during emergency rollbacks,
/// deny_unknown_fields adds further complications when decomissioning a feature
/// flag: with deny_unknown_fields, we can't remove a flag from the [`ConfigToml`]
/// until all prod servers' `pageserver.toml` files have been updated to a version
/// that doesn't specify the flag. Otherwise new software would fail to start up.
/// This adds the requirement for an intermediate step where the new config field
/// is accepted but ignored, prolonging the decomissioning process by an entire
/// release cycle.
/// By contrast  with unknown fields silently ignored, decomissioning a feature
/// flag is a one-step process: we can skip the intermediate step and straight
/// remove the field from the [`ConfigToml`]. We leave the field in the
/// `pageserver.toml` files on prod servers until we reach certainty that we
/// will not roll back to old software whose behavior was dependent on config.
/// Then we can remove the field from the templates in the internal infra repo.
/// This process is [documented internally](
/// https://docs.neon.build/storage/pageserver_configuration.html).
///
/// Note that above relaxed compatbility for the config format does NOT APPLY
/// TO THE STORAGE FORMAT. As general guidance, when introducing storage format
/// changes, ensure that the potential rollback target version will be compatible
/// with the new format. This must hold regardless of what flags are set in in the `pageserver.toml`:
/// any format version that exists in an environment must be compatible with the software that runs there.
/// Use a pageserver.toml flag only to gate whether software _writes_ the new format.
/// For more compatibility considerations, refer to [internal docs](
/// https://docs.neon.build/storage/compat.html?highlight=compat#format-versions--compatibility)
#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct ConfigToml {
    // types mapped 1:1 into the runtime PageServerConfig type
    pub listen_pg_addr: String,
    pub listen_http_addr: String,
    pub listen_https_addr: Option<String>,
    pub listen_grpc_addr: Option<String>,
    pub ssl_key_file: Utf8PathBuf,
    pub ssl_cert_file: Utf8PathBuf,
    #[serde(with = "humantime_serde")]
    pub ssl_cert_reload_period: Duration,
    pub ssl_ca_file: Option<Utf8PathBuf>,
    pub availability_zone: Option<String>,
    #[serde(with = "humantime_serde")]
    pub wait_lsn_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub wal_redo_timeout: Duration,
    pub superuser: String,
    pub locale: String,
    pub page_cache_size: usize,
    pub max_file_descriptors: usize,
    pub pg_distrib_dir: Option<Utf8PathBuf>,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub http_auth_type: AuthType,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub pg_auth_type: AuthType,
    pub grpc_auth_type: AuthType,
    pub auth_validation_public_key_path: Option<Utf8PathBuf>,
    pub remote_storage: Option<RemoteStorageConfig>,
    pub tenant_config: TenantConfigToml,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub broker_endpoint: storage_broker::Uri,
    #[serde(with = "humantime_serde")]
    pub broker_keepalive_interval: Duration,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub log_format: LogFormat,
    pub concurrent_tenant_warmup: NonZeroUsize,
    pub concurrent_tenant_size_logical_size_queries: NonZeroUsize,
    #[serde(with = "humantime_serde")]
    pub metric_collection_interval: Duration,
    pub metric_collection_endpoint: Option<reqwest::Url>,
    pub metric_collection_bucket: Option<RemoteStorageConfig>,
    #[serde(with = "humantime_serde")]
    pub synthetic_size_calculation_interval: Duration,
    pub disk_usage_based_eviction: DiskUsageEvictionTaskConfig,
    pub test_remote_failures: u64,
    pub test_remote_failures_probability: u64,
    pub ondemand_download_behavior_treat_error_as_warn: bool,
    #[serde(with = "humantime_serde")]
    pub background_task_maximum_delay: Duration,
    pub control_plane_api: Option<reqwest::Url>,
    pub control_plane_api_token: Option<String>,
    pub control_plane_emergency_mode: bool,
    /// Unstable feature: subject to change or removal without notice.
    /// See <https://github.com/neondatabase/neon/pull/9218>.
    pub import_pgdata_upcall_api: Option<reqwest::Url>,
    /// Unstable feature: subject to change or removal without notice.
    /// See <https://github.com/neondatabase/neon/pull/9218>.
    pub import_pgdata_upcall_api_token: Option<String>,
    /// Unstable feature: subject to change or removal without notice.
    /// See <https://github.com/neondatabase/neon/pull/9218>.
    pub import_pgdata_aws_endpoint_url: Option<reqwest::Url>,
    pub heatmap_upload_concurrency: usize,
    pub secondary_download_concurrency: usize,
    pub virtual_file_io_engine: Option<crate::models::virtual_file::IoEngineKind>,
    pub ingest_batch_size: u64,
    pub max_vectored_read_bytes: MaxVectoredReadBytes,
    pub max_get_vectored_keys: MaxGetVectoredKeys,
    pub image_compression: ImageCompressionAlgorithm,
    pub timeline_offloading: bool,
    pub ephemeral_bytes_per_memory_kb: usize,
    pub l0_flush: Option<crate::models::L0FlushConfig>,
    pub virtual_file_io_mode: Option<crate::models::virtual_file::IoMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no_sync: Option<bool>,
    pub page_service_pipelining: PageServicePipeliningConfig,
    pub get_vectored_concurrent_io: GetVectoredConcurrentIo,
    pub enable_read_path_debugging: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validate_wal_contiguity: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_previous_heatmap: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generate_unarchival_heatmap: Option<bool>,
    pub tracing: Option<Tracing>,
    pub enable_tls_page_service_api: bool,
    pub dev_mode: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub posthog_config: Option<PostHogConfig>,
    pub timeline_import_config: TimelineImportConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub basebackup_cache_config: Option<BasebackupCacheConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_layer_generation_large_timeline_threshold: Option<u64>,
    pub force_metric_collection_on_scrape: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct DiskUsageEvictionTaskConfig {
    pub max_usage_pct: utils::serde_percent::Percent,
    pub min_avail_bytes: u64,
    #[serde(with = "humantime_serde")]
    pub period: Duration,
    #[cfg(feature = "testing")]
    pub mock_statvfs: Option<statvfs::mock::Behavior>,
    /// Select sorting for evicted layers
    #[serde(default)]
    pub eviction_order: EvictionOrder,
    pub enabled: bool,
}

impl Default for DiskUsageEvictionTaskConfig {
    fn default() -> Self {
        Self {
            max_usage_pct: Percent::new(80).unwrap(),
            min_avail_bytes: 2_000_000_000,
            period: Duration::from_secs(60),
            #[cfg(feature = "testing")]
            mock_statvfs: None,
            eviction_order: EvictionOrder::default(),
            enabled: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
pub enum PageServicePipeliningConfig {
    Serial,
    Pipelined(PageServicePipeliningConfigPipelined),
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PageServicePipeliningConfigPipelined {
    /// Failed config parsing and validation if larger than `max_get_vectored_keys`.
    pub max_batch_size: NonZeroUsize,
    pub execution: PageServiceProtocolPipelinedExecutionStrategy,
    // The default below is such that new versions of the software can start
    // with the old configuration.
    #[serde(default)]
    pub batching: PageServiceProtocolPipelinedBatchingStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PageServiceProtocolPipelinedExecutionStrategy {
    ConcurrentFutures,
    Tasks,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PageServiceProtocolPipelinedBatchingStrategy {
    /// All get page requests in a batch will be at the same LSN
    #[default]
    UniformLsn,
    /// Get page requests in a batch may be at different LSN
    ///
    /// One key cannot be present more than once at different LSNs in
    /// the same batch.
    ScatteredLsn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
pub enum GetVectoredConcurrentIo {
    /// The read path is fully sequential: layers are visited
    /// one after the other and IOs are issued and waited upon
    /// from the same task that traverses the layers.
    Sequential,
    /// The read path still traverses layers sequentially, and
    /// index blocks will be read into the PS PageCache from
    /// that task, with waiting.
    /// But data IOs are dispatched and waited upon from a sidecar
    /// task so that the traversing task can continue to traverse
    /// layers while the IOs are in flight.
    /// If the PS PageCache miss rate is low, this improves
    /// throughput dramatically.
    SidecarTask,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Ratio {
    pub numerator: usize,
    pub denominator: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OtelExporterConfig {
    pub endpoint: String,
    pub protocol: OtelExporterProtocol,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OtelExporterProtocol {
    Grpc,
    HttpBinary,
    HttpJson,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Tracing {
    pub sampling_ratio: Ratio,
    pub export_config: OtelExporterConfig,
}

impl From<&OtelExporterConfig> for tracing_utils::ExportConfig {
    fn from(val: &OtelExporterConfig) -> Self {
        tracing_utils::ExportConfig {
            endpoint: Some(val.endpoint.clone()),
            protocol: val.protocol.into(),
            timeout: val.timeout,
        }
    }
}

impl From<OtelExporterProtocol> for tracing_utils::Protocol {
    fn from(val: OtelExporterProtocol) -> Self {
        match val {
            OtelExporterProtocol::Grpc => tracing_utils::Protocol::Grpc,
            OtelExporterProtocol::HttpJson => tracing_utils::Protocol::HttpJson,
            OtelExporterProtocol::HttpBinary => tracing_utils::Protocol::HttpBinary,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TimelineImportConfig {
    pub import_job_concurrency: NonZeroUsize,
    pub import_job_soft_size_limit: NonZeroUsize,
    pub import_job_checkpoint_threshold: NonZeroUsize,
    /// Max size of the remote storage partial read done by any job
    pub import_job_max_byte_range_size: NonZeroUsize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct BasebackupCacheConfig {
    #[serde(with = "humantime_serde")]
    pub cleanup_period: Duration,
    /// Maximum total size of basebackup cache entries on disk in bytes.
    /// The cache may slightly exceed this limit because we do not know
    /// the exact size of the cache entry untill it's written to disk.
    pub max_total_size_bytes: u64,
    // TODO(diko): support max_entry_size_bytes.
    // pub max_entry_size_bytes: u64,
    pub max_size_entries: usize,
    /// Size of the channel used to send prepare requests to the basebackup cache worker.
    /// If exceeded, new prepare requests will be dropped.
    pub prepare_channel_size: usize,
}

impl Default for BasebackupCacheConfig {
    fn default() -> Self {
        Self {
            cleanup_period: Duration::from_secs(60),
            max_total_size_bytes: 1024 * 1024 * 1024, // 1 GiB
            // max_entry_size_bytes: 16 * 1024 * 1024,   // 16 MiB
            max_size_entries: 10000,
            prepare_channel_size: 100,
        }
    }
}

pub mod statvfs {
    pub mod mock {
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        #[serde(tag = "type")]
        pub enum Behavior {
            Success {
                blocksize: u64,
                total_blocks: u64,
                name_filter: Option<utils::serde_regex::Regex>,
            },
            #[cfg(feature = "testing")]
            Failure { mocked_error: MockedError },
        }

        #[cfg(feature = "testing")]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        #[allow(clippy::upper_case_acronyms)]
        pub enum MockedError {
            EIO,
        }

        #[cfg(feature = "testing")]
        impl From<MockedError> for nix::Error {
            fn from(e: MockedError) -> Self {
                match e {
                    MockedError::EIO => nix::Error::EIO,
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "args")]
pub enum EvictionOrder {
    RelativeAccessed {
        highest_layer_count_loses_first: bool,
    },
}

impl Default for EvictionOrder {
    fn default() -> Self {
        Self::RelativeAccessed {
            highest_layer_count_loses_first: true,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct MaxVectoredReadBytes(pub NonZeroUsize);

#[derive(Copy, Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct MaxGetVectoredKeys(NonZeroUsize);

impl MaxGetVectoredKeys {
    pub fn get(&self) -> usize {
        self.0.get()
    }
}

/// Tenant-level configuration values, used for various purposes.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct TenantConfigToml {
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
    /// Level0 delta layer threshold for compaction.
    pub compaction_threshold: usize,
    /// Controls the amount of L0 included in a single compaction iteration.
    /// The unit is `checkpoint_distance`, i.e., a size.
    /// We add L0s to the set of layers to compact until their cumulative
    /// size exceeds `compaction_upper_limit * checkpoint_distance`.
    pub compaction_upper_limit: usize,
    pub compaction_algorithm: crate::models::CompactionAlgorithmSettings,
    /// If true, enable shard ancestor compaction (enabled by default).
    pub compaction_shard_ancestor: bool,
    /// If true, compact down L0 across all tenant timelines before doing regular compaction. L0
    /// compaction must be responsive to avoid read amp during heavy ingestion. Defaults to true.
    pub compaction_l0_first: bool,
    /// If true, use a separate semaphore (i.e. concurrency limit) for the L0 compaction pass. Only
    /// has an effect if `compaction_l0_first` is true. Defaults to true.
    pub compaction_l0_semaphore: bool,
    /// Level0 delta layer threshold at which to delay layer flushes such that they take 2x as long,
    /// and block on layer flushes during ephemeral layer rolls, for compaction backpressure. This
    /// helps compaction keep up with WAL ingestion, and avoids read amplification blowing up.
    /// Should be >compaction_threshold. 0 to disable. Defaults to 3x compaction_threshold.
    pub l0_flush_delay_threshold: Option<usize>,
    /// Level0 delta layer threshold at which to stall layer flushes. Must be >compaction_threshold
    /// to avoid deadlock. 0 to disable. Disabled by default.
    pub l0_flush_stall_threshold: Option<usize>,
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
    // HADRON
    // When the timeout is reached, PageServer will (1) force compact any remaining L0 deltas and
    // (2) create image layers if there are any L1 deltas.
    #[serde(with = "humantime_serde")]
    pub image_layer_force_creation_period: Option<Duration>,
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
    pub eviction_policy: crate::models::EvictionPolicy,
    pub min_resident_size_override: Option<u64>,
    // See the corresponding metric's help string.
    #[serde(with = "humantime_serde")]
    pub evictions_low_residence_duration_metric_threshold: Duration,

    /// If non-zero, the period between uploads of a heatmap from attached tenants.  This
    /// may be disabled if a Tenant will not have secondary locations: only secondary
    /// locations will use the heatmap uploaded by attached locations.
    #[serde(with = "humantime_serde")]
    pub heatmap_period: Duration,

    /// If true then SLRU segments are dowloaded on demand, if false SLRU segments are included in basebackup
    pub lazy_slru_download: bool,

    pub timeline_get_throttle: crate::models::ThrottleConfig,

    // How much WAL must be ingested before checking again whether a new image layer is required.
    // Expresed in multiples of checkpoint distance.
    pub image_layer_creation_check_threshold: u8,

    // How many multiples of L0 `compaction_threshold` will preempt image layer creation and do L0 compaction.
    // Set to 0 to disable preemption.
    pub image_creation_preempt_threshold: usize,

    /// The length for an explicit LSN lease request.
    /// Layers needed to reconstruct pages at LSN will not be GC-ed during this interval.
    #[serde(with = "humantime_serde")]
    pub lsn_lease_length: Duration,

    /// The length for an implicit LSN lease granted as part of `get_lsn_by_timestamp` request.
    /// Layers needed to reconstruct pages at LSN will not be GC-ed during this interval.
    #[serde(with = "humantime_serde")]
    pub lsn_lease_length_for_ts: Duration,

    /// Enable auto-offloading of timelines.
    /// (either this flag or the pageserver-global one need to be set)
    pub timeline_offloading: bool,

    /// Enable rel_size_v2 for this tenant. Once enabled, the tenant will persist this information into
    /// `index_part.json`, and it cannot be reversed.
    pub rel_size_v2_enabled: bool,

    // gc-compaction related configs
    /// Enable automatic gc-compaction trigger on this tenant.
    pub gc_compaction_enabled: bool,
    /// Enable verification of gc-compaction results.
    pub gc_compaction_verification: bool,
    /// The initial threshold for gc-compaction in KB. Once the total size of layers below the gc-horizon is above this threshold,
    /// gc-compaction will be triggered.
    pub gc_compaction_initial_threshold_kb: u64,
    /// The ratio that triggers the auto gc-compaction. If (the total size of layers between L2 LSN and gc-horizon) / (size below the L2 LSN)
    /// is above this ratio, gc-compaction will be triggered.
    pub gc_compaction_ratio_percent: u64,
    /// Tenant level performance sampling ratio override. Controls the ratio of get page requests
    /// that will get perf sampling for the tenant.
    pub sampling_ratio: Option<Ratio>,

    /// Capacity of relsize snapshot cache (used by replicas).
    pub relsize_snapshot_cache_capacity: usize,

    /// Enable preparing basebackup on XLOG_CHECKPOINT_SHUTDOWN and using it in basebackup requests.
    // FIXME: Remove skip_serializing_if when the feature is stable.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub basebackup_cache_enabled: bool,
}

pub mod defaults {
    pub use storage_broker::DEFAULT_ENDPOINT as BROKER_DEFAULT_ENDPOINT;

    use crate::models::ImageCompressionAlgorithm;

    pub const DEFAULT_WAIT_LSN_TIMEOUT: &str = "300 s";
    pub const DEFAULT_WAL_REDO_TIMEOUT: &str = "60 s";

    pub const DEFAULT_SUPERUSER: &str = "cloud_admin";
    pub const DEFAULT_LOCALE: &str = if cfg!(target_os = "macos") {
        "C"
    } else {
        "C.UTF-8"
    };

    pub const DEFAULT_PAGE_CACHE_SIZE: usize = 8192;
    pub const DEFAULT_MAX_FILE_DESCRIPTORS: usize = 100;

    pub const DEFAULT_LOG_FORMAT: &str = "plain";

    pub const DEFAULT_CONCURRENT_TENANT_WARMUP: usize = 8;

    pub const DEFAULT_CONCURRENT_TENANT_SIZE_LOGICAL_SIZE_QUERIES: usize = 1;

    pub const DEFAULT_METRIC_COLLECTION_INTERVAL: &str = "10 min";
    pub const DEFAULT_METRIC_COLLECTION_ENDPOINT: Option<reqwest::Url> = None;
    pub const DEFAULT_SYNTHETIC_SIZE_CALCULATION_INTERVAL: &str = "10 min";
    pub const DEFAULT_BACKGROUND_TASK_MAXIMUM_DELAY: &str = "10s";

    pub const DEFAULT_HEATMAP_UPLOAD_CONCURRENCY: usize = 8;
    pub const DEFAULT_SECONDARY_DOWNLOAD_CONCURRENCY: usize = 1;

    pub const DEFAULT_INGEST_BATCH_SIZE: u64 = 100;

    /// Soft limit for the maximum size of a vectored read.
    ///
    /// This is determined by the largest NeonWalRecord that can exist (minus dbdir and reldir keys
    /// which are bounded by the blob io limits only). As of this writing, that is a `NeonWalRecord::ClogSetCommitted` record,
    /// with 32k xids. That's the max number of XIDS on a single CLOG page. The size of such a record
    /// is `sizeof(Transactionid) * 32768 + (some fixed overhead from 'timestamp`, the Vec length and whatever extra serde serialization adds)`.
    /// That is, slightly above 128 kB.
    pub const DEFAULT_MAX_VECTORED_READ_BYTES: usize = 130 * 1024; // 130 KiB

    pub const DEFAULT_MAX_GET_VECTORED_KEYS: usize = 32;

    pub const DEFAULT_IMAGE_COMPRESSION: ImageCompressionAlgorithm =
        ImageCompressionAlgorithm::Zstd { level: Some(1) };

    pub const DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB: usize = 0;

    pub const DEFAULT_IO_BUFFER_ALIGNMENT: usize = 512;

    pub const DEFAULT_SSL_KEY_FILE: &str = "server.key";
    pub const DEFAULT_SSL_CERT_FILE: &str = "server.crt";
}

impl Default for ConfigToml {
    fn default() -> Self {
        use defaults::*;

        Self {
            listen_pg_addr: (DEFAULT_PG_LISTEN_ADDR.to_string()),
            listen_http_addr: (DEFAULT_HTTP_LISTEN_ADDR.to_string()),
            listen_https_addr: (None),
            listen_grpc_addr: None, // TODO: default to 127.0.0.1:51051
            ssl_key_file: Utf8PathBuf::from(DEFAULT_SSL_KEY_FILE),
            ssl_cert_file: Utf8PathBuf::from(DEFAULT_SSL_CERT_FILE),
            ssl_cert_reload_period: Duration::from_secs(60),
            ssl_ca_file: None,
            availability_zone: (None),
            wait_lsn_timeout: (humantime::parse_duration(DEFAULT_WAIT_LSN_TIMEOUT)
                .expect("cannot parse default wait lsn timeout")),
            wal_redo_timeout: (humantime::parse_duration(DEFAULT_WAL_REDO_TIMEOUT)
                .expect("cannot parse default wal redo timeout")),
            superuser: (DEFAULT_SUPERUSER.to_string()),
            locale: DEFAULT_LOCALE.to_string(),
            page_cache_size: (DEFAULT_PAGE_CACHE_SIZE),
            max_file_descriptors: (DEFAULT_MAX_FILE_DESCRIPTORS),
            pg_distrib_dir: None, // Utf8PathBuf::from("./pg_install"), // TODO: formely, this was std::env::current_dir()
            http_auth_type: (AuthType::Trust),
            pg_auth_type: (AuthType::Trust),
            grpc_auth_type: (AuthType::Trust),
            auth_validation_public_key_path: (None),
            remote_storage: None,
            broker_endpoint: (storage_broker::DEFAULT_ENDPOINT
                .parse()
                .expect("failed to parse default broker endpoint")),
            broker_keepalive_interval: (humantime::parse_duration(
                storage_broker::DEFAULT_KEEPALIVE_INTERVAL,
            )
            .expect("cannot parse default keepalive interval")),
            log_format: (LogFormat::from_str(DEFAULT_LOG_FORMAT).unwrap()),

            concurrent_tenant_warmup: (NonZeroUsize::new(DEFAULT_CONCURRENT_TENANT_WARMUP)
                .expect("Invalid default constant")),
            concurrent_tenant_size_logical_size_queries: NonZeroUsize::new(
                DEFAULT_CONCURRENT_TENANT_SIZE_LOGICAL_SIZE_QUERIES,
            )
            .unwrap(),
            metric_collection_interval: (humantime::parse_duration(
                DEFAULT_METRIC_COLLECTION_INTERVAL,
            )
            .expect("cannot parse default metric collection interval")),
            synthetic_size_calculation_interval: (humantime::parse_duration(
                DEFAULT_SYNTHETIC_SIZE_CALCULATION_INTERVAL,
            )
            .expect("cannot parse default synthetic size calculation interval")),
            metric_collection_endpoint: (DEFAULT_METRIC_COLLECTION_ENDPOINT),

            metric_collection_bucket: (None),

            disk_usage_based_eviction: DiskUsageEvictionTaskConfig::default(),

            test_remote_failures: (0),
            test_remote_failures_probability: (100),

            ondemand_download_behavior_treat_error_as_warn: (false),

            background_task_maximum_delay: (humantime::parse_duration(
                DEFAULT_BACKGROUND_TASK_MAXIMUM_DELAY,
            )
            .unwrap()),

            control_plane_api: (None),
            control_plane_api_token: (None),
            control_plane_emergency_mode: (false),

            import_pgdata_upcall_api: (None),
            import_pgdata_upcall_api_token: (None),
            import_pgdata_aws_endpoint_url: (None),

            heatmap_upload_concurrency: (DEFAULT_HEATMAP_UPLOAD_CONCURRENCY),
            secondary_download_concurrency: (DEFAULT_SECONDARY_DOWNLOAD_CONCURRENCY),

            ingest_batch_size: (DEFAULT_INGEST_BATCH_SIZE),

            virtual_file_io_engine: None,

            max_vectored_read_bytes: (MaxVectoredReadBytes(
                NonZeroUsize::new(DEFAULT_MAX_VECTORED_READ_BYTES).unwrap(),
            )),
            max_get_vectored_keys: (MaxGetVectoredKeys(
                NonZeroUsize::new(DEFAULT_MAX_GET_VECTORED_KEYS).unwrap(),
            )),
            image_compression: (DEFAULT_IMAGE_COMPRESSION),
            timeline_offloading: true,
            ephemeral_bytes_per_memory_kb: (DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB),
            l0_flush: None,
            virtual_file_io_mode: None,
            tenant_config: TenantConfigToml::default(),
            no_sync: None,
            page_service_pipelining: PageServicePipeliningConfig::Pipelined(
                PageServicePipeliningConfigPipelined {
                    max_batch_size: NonZeroUsize::new(32).unwrap(),
                    execution: PageServiceProtocolPipelinedExecutionStrategy::ConcurrentFutures,
                    batching: PageServiceProtocolPipelinedBatchingStrategy::ScatteredLsn,
                },
            ),
            get_vectored_concurrent_io: GetVectoredConcurrentIo::SidecarTask,
            enable_read_path_debugging: if cfg!(feature = "testing") {
                Some(true)
            } else {
                None
            },
            validate_wal_contiguity: None,
            load_previous_heatmap: None,
            generate_unarchival_heatmap: None,
            tracing: None,
            enable_tls_page_service_api: false,
            dev_mode: false,
            timeline_import_config: TimelineImportConfig {
                import_job_concurrency: NonZeroUsize::new(32).unwrap(),
                import_job_soft_size_limit: NonZeroUsize::new(256 * 1024 * 1024).unwrap(),
                import_job_checkpoint_threshold: NonZeroUsize::new(32).unwrap(),
                import_job_max_byte_range_size: NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
            },
            basebackup_cache_config: None,
            posthog_config: None,
            image_layer_generation_large_timeline_threshold: Some(2 * 1024 * 1024 * 1024),
            force_metric_collection_on_scrape: true,
        }
    }
}

pub mod tenant_conf_defaults {

    // FIXME: This current value is very low. I would imagine something like 1 GB or 10 GB
    // would be more appropriate. But a low value forces the code to be exercised more,
    // which is good for now to trigger bugs.
    // This parameter actually determines L0 layer file size.
    pub const DEFAULT_CHECKPOINT_DISTANCE: u64 = 256 * 1024 * 1024;
    pub const DEFAULT_CHECKPOINT_TIMEOUT: &str = "10 m";

    // FIXME the below configs are only used by legacy algorithm. The new algorithm
    // has different parameters.

    // Target file size, when creating image and delta layers.
    // This parameter determines L1 layer file size.
    pub const DEFAULT_COMPACTION_TARGET_SIZE: u64 = 128 * 1024 * 1024;

    pub const DEFAULT_COMPACTION_PERIOD: &str = "20 s";
    pub const DEFAULT_COMPACTION_THRESHOLD: usize = 10;
    pub const DEFAULT_COMPACTION_SHARD_ANCESTOR: bool = true;

    // This value needs to be tuned to avoid OOM. We have 3/4*CPUs threads for L0 compaction, that's
    // 3/4*8=6 on most of our pageservers. Compacting 10 layers requires a maximum of
    // DEFAULT_CHECKPOINT_DISTANCE*10 memory, that's 2560MB. So with this config, we can get a maximum peak
    // compaction usage of 15360MB.
    pub const DEFAULT_COMPACTION_UPPER_LIMIT: usize = 10;
    // Enable L0 compaction pass and semaphore by default. L0 compaction must be responsive to avoid
    // read amp.
    pub const DEFAULT_COMPACTION_L0_FIRST: bool = true;
    pub const DEFAULT_COMPACTION_L0_SEMAPHORE: bool = true;

    pub const DEFAULT_COMPACTION_ALGORITHM: crate::models::CompactionAlgorithm =
        crate::models::CompactionAlgorithm::Legacy;

    pub const DEFAULT_GC_HORIZON: u64 = 64 * 1024 * 1024;

    // Large DEFAULT_GC_PERIOD is fine as long as PITR_INTERVAL is larger.
    // If there's a need to decrease this value, first make sure that GC
    // doesn't hold a layer map write lock for non-trivial operations.
    // Relevant: https://github.com/neondatabase/neon/issues/3394
    pub const DEFAULT_GC_PERIOD: &str = "1 hr";
    pub const DEFAULT_IMAGE_CREATION_THRESHOLD: usize = 3;
    // Currently, any value other than 0 will trigger image layer creation preemption immediately with L0 backpressure
    // without looking at the exact number of L0 layers.
    // It was expected to have the following behavior:
    // > If there are more than threshold * compaction_threshold (that is 3 * 10 in the default config) L0 layers, image
    // > layer creation will end immediately. Set to 0 to disable.
    pub const DEFAULT_IMAGE_CREATION_PREEMPT_THRESHOLD: usize = 3;
    pub const DEFAULT_PITR_INTERVAL: &str = "7 days";
    pub const DEFAULT_WALRECEIVER_CONNECT_TIMEOUT: &str = "10 seconds";
    pub const DEFAULT_WALRECEIVER_LAGGING_WAL_TIMEOUT: &str = "10 seconds";
    // The default limit on WAL lag should be set to avoid causing disconnects under high throughput
    // scenarios: since the broker stats are updated ~1/s, a value of 1GiB should be sufficient for
    // throughputs up to 1GiB/s per timeline.
    pub const DEFAULT_MAX_WALRECEIVER_LSN_WAL_LAG: u64 = 1024 * 1024 * 1024;
    pub const DEFAULT_EVICTIONS_LOW_RESIDENCE_DURATION_METRIC_THRESHOLD: &str = "24 hour";
    // By default ingest enough WAL for two new L0 layers before checking if new image
    // image layers should be created.
    pub const DEFAULT_IMAGE_LAYER_CREATION_CHECK_THRESHOLD: u8 = 2;
    pub const DEFAULT_GC_COMPACTION_ENABLED: bool = true;
    pub const DEFAULT_GC_COMPACTION_VERIFICATION: bool = true;
    pub const DEFAULT_GC_COMPACTION_INITIAL_THRESHOLD_KB: u64 = 5 * 1024 * 1024; // 5GB
    pub const DEFAULT_GC_COMPACTION_RATIO_PERCENT: u64 = 100;
    pub const DEFAULT_RELSIZE_SNAPSHOT_CACHE_CAPACITY: usize = 1000;
}

impl Default for TenantConfigToml {
    fn default() -> Self {
        use tenant_conf_defaults::*;
        Self {
            checkpoint_distance: DEFAULT_CHECKPOINT_DISTANCE,
            checkpoint_timeout: humantime::parse_duration(DEFAULT_CHECKPOINT_TIMEOUT)
                .expect("cannot parse default checkpoint timeout"),
            compaction_target_size: DEFAULT_COMPACTION_TARGET_SIZE,
            compaction_period: humantime::parse_duration(DEFAULT_COMPACTION_PERIOD)
                .expect("cannot parse default compaction period"),
            compaction_threshold: DEFAULT_COMPACTION_THRESHOLD,
            compaction_upper_limit: DEFAULT_COMPACTION_UPPER_LIMIT,
            compaction_algorithm: crate::models::CompactionAlgorithmSettings {
                kind: DEFAULT_COMPACTION_ALGORITHM,
            },
            compaction_shard_ancestor: DEFAULT_COMPACTION_SHARD_ANCESTOR,
            compaction_l0_first: DEFAULT_COMPACTION_L0_FIRST,
            compaction_l0_semaphore: DEFAULT_COMPACTION_L0_SEMAPHORE,
            l0_flush_delay_threshold: None,
            l0_flush_stall_threshold: None,
            gc_horizon: DEFAULT_GC_HORIZON,
            gc_period: humantime::parse_duration(DEFAULT_GC_PERIOD)
                .expect("cannot parse default gc period"),
            image_creation_threshold: DEFAULT_IMAGE_CREATION_THRESHOLD,
            image_layer_force_creation_period: None,
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
            eviction_policy: crate::models::EvictionPolicy::NoEviction,
            min_resident_size_override: None,
            evictions_low_residence_duration_metric_threshold: humantime::parse_duration(
                DEFAULT_EVICTIONS_LOW_RESIDENCE_DURATION_METRIC_THRESHOLD,
            )
            .expect("cannot parse default evictions_low_residence_duration_metric_threshold"),
            heatmap_period: Duration::ZERO,
            lazy_slru_download: false,
            timeline_get_throttle: crate::models::ThrottleConfig::disabled(),
            image_layer_creation_check_threshold: DEFAULT_IMAGE_LAYER_CREATION_CHECK_THRESHOLD,
            image_creation_preempt_threshold: DEFAULT_IMAGE_CREATION_PREEMPT_THRESHOLD,
            lsn_lease_length: LsnLease::DEFAULT_LENGTH,
            lsn_lease_length_for_ts: LsnLease::DEFAULT_LENGTH_FOR_TS,
            timeline_offloading: true,
            rel_size_v2_enabled: false,
            gc_compaction_enabled: DEFAULT_GC_COMPACTION_ENABLED,
            gc_compaction_verification: DEFAULT_GC_COMPACTION_VERIFICATION,
            gc_compaction_initial_threshold_kb: DEFAULT_GC_COMPACTION_INITIAL_THRESHOLD_KB,
            gc_compaction_ratio_percent: DEFAULT_GC_COMPACTION_RATIO_PERCENT,
            sampling_ratio: None,
            relsize_snapshot_cache_capacity: DEFAULT_RELSIZE_SNAPSHOT_CACHE_CAPACITY,
            basebackup_cache_enabled: false,
        }
    }
}
