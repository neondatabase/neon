use camino::Utf8PathBuf;

#[cfg(test)]
mod tests;

use const_format::formatcp;
pub const DEFAULT_PG_LISTEN_PORT: u16 = 64000;
pub const DEFAULT_PG_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_PG_LISTEN_PORT}");
pub const DEFAULT_HTTP_LISTEN_PORT: u16 = 9898;
pub const DEFAULT_HTTP_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_HTTP_LISTEN_PORT}");

use postgres_backend::AuthType;
use remote_storage::RemoteStorageConfig;
use serde_with::serde_as;
use std::{
    collections::HashMap,
    num::{NonZeroU64, NonZeroUsize},
    str::FromStr,
    time::Duration,
};
use utils::{logging::LogFormat, postgres_client::PostgresClientProtocol};

use crate::models::ImageCompressionAlgorithm;
use crate::models::LsnLease;

// Certain metadata (e.g. externally-addressable name, AZ) is delivered
// as a separate structure.  This information is not neeed by the pageserver
// itself, it is only used for registering the pageserver with the control
// plane and/or storage controller.
//
#[derive(PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub struct NodeMetadata {
    #[serde(rename = "host")]
    pub postgres_host: String,
    #[serde(rename = "port")]
    pub postgres_port: u16,
    pub http_host: String,
    pub http_port: u16,

    // Deployment tools may write fields to the metadata file beyond what we
    // use in this type: this type intentionally only names fields that require.
    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}

/// `pageserver.toml`
///
/// We use serde derive with `#[serde(default)]` to generate a deserializer
/// that fills in the default values for each config field.
///
/// If there cannot be a static default value because we need to make runtime
/// checks to determine the default, make it an `Option` (which defaults to None).
/// The runtime check should be done in the consuming crate, i.e., `pageserver`.
#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigToml {
    // types mapped 1:1 into the runtime PageServerConfig type
    pub listen_pg_addr: String,
    pub listen_http_addr: String,
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
    pub disk_usage_based_eviction: Option<DiskUsageEvictionTaskConfig>,
    pub test_remote_failures: u64,
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
    pub image_compression: ImageCompressionAlgorithm,
    pub timeline_offloading: bool,
    pub ephemeral_bytes_per_memory_kb: usize,
    pub l0_flush: Option<crate::models::L0FlushConfig>,
    pub virtual_file_io_mode: Option<crate::models::virtual_file::IoMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no_sync: Option<bool>,
    pub wal_receiver_protocol: PostgresClientProtocol,
    pub page_service_pipelining: PageServicePipeliningConfig,
    pub get_vectored_concurrent_io: GetVectoredConcurrentIo,
    pub enable_read_path_debugging: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
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
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub enum PageServicePipeliningConfig {
    Serial,
    Pipelined(PageServicePipeliningConfigPipelined),
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageServicePipeliningConfigPipelined {
    /// Causes runtime errors if larger than max get_vectored batch size.
    pub max_batch_size: NonZeroUsize,
    pub execution: PageServiceProtocolPipelinedExecutionStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PageServiceProtocolPipelinedExecutionStrategy {
    ConcurrentFutures,
    Tasks,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
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

/// A tenant's calcuated configuration, which is the result of merging a
/// tenant's TenantConfOpt with the global TenantConf from PageServerConf.
///
/// For storing and transmitting individual tenant's configuration, see
/// TenantConfOpt.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields, default)]
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
    /// If true, compact down L0 across all tenant timelines before doing regular compaction.
    pub compaction_l0_first: bool,
    /// If true, use a separate semaphore (i.e. concurrency limit) for the L0 compaction pass. Only
    /// has an effect if `compaction_l0_first` is `true`.
    pub compaction_l0_semaphore: bool,
    /// Level0 delta layer threshold at which to delay layer flushes for compaction backpressure,
    /// such that they take 2x as long, and start waiting for layer flushes during ephemeral layer
    /// rolls. This helps compaction keep up with WAL ingestion, and avoids read amplification
    /// blowing up. Should be >compaction_threshold. 0 to disable. Disabled by default.
    pub l0_flush_delay_threshold: Option<usize>,
    /// Level0 delta layer threshold at which to stall layer flushes. Must be >compaction_threshold
    /// to avoid deadlock. 0 to disable. Disabled by default.
    pub l0_flush_stall_threshold: Option<usize>,
    /// If true, Level0 delta layer flushes will wait for S3 upload before flushing the next
    /// layer. This is a temporary backpressure mechanism which should be removed once
    /// l0_flush_{delay,stall}_threshold is fully enabled.
    pub l0_flush_wait_upload: bool,
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

    pub wal_receiver_protocol_override: Option<PostgresClientProtocol>,

    /// Enable rel_size_v2 for this tenant. Once enabled, the tenant will persist this information into
    /// `index_part.json`, and it cannot be reversed.
    pub rel_size_v2_enabled: bool,

    // gc-compaction related configs
    /// Enable automatic gc-compaction trigger on this tenant.
    pub gc_compaction_enabled: bool,
    /// The initial threshold for gc-compaction in KB. Once the total size of layers below the gc-horizon is above this threshold,
    /// gc-compaction will be triggered.
    pub gc_compaction_initial_threshold_kb: u64,
    /// The ratio that triggers the auto gc-compaction. If (the total size of layers between L2 LSN and gc-horizon) / (size below the L2 LSN)
    /// is above this ratio, gc-compaction will be triggered.
    pub gc_compaction_ratio_percent: u64,
}

pub mod defaults {
    use crate::models::ImageCompressionAlgorithm;

    pub use storage_broker::DEFAULT_ENDPOINT as BROKER_DEFAULT_ENDPOINT;

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

    pub const DEFAULT_IMAGE_COMPRESSION: ImageCompressionAlgorithm =
        ImageCompressionAlgorithm::Zstd { level: Some(1) };

    pub const DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB: usize = 0;

    pub const DEFAULT_IO_BUFFER_ALIGNMENT: usize = 512;

    pub const DEFAULT_WAL_RECEIVER_PROTOCOL: utils::postgres_client::PostgresClientProtocol =
        utils::postgres_client::PostgresClientProtocol::Vanilla;
}

impl Default for ConfigToml {
    fn default() -> Self {
        use defaults::*;

        Self {
            listen_pg_addr: (DEFAULT_PG_LISTEN_ADDR.to_string()),
            listen_http_addr: (DEFAULT_HTTP_LISTEN_ADDR.to_string()),
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

            disk_usage_based_eviction: (None),

            test_remote_failures: (0),

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
            image_compression: (DEFAULT_IMAGE_COMPRESSION),
            timeline_offloading: true,
            ephemeral_bytes_per_memory_kb: (DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB),
            l0_flush: None,
            virtual_file_io_mode: None,
            tenant_config: TenantConfigToml::default(),
            no_sync: None,
            wal_receiver_protocol: DEFAULT_WAL_RECEIVER_PROTOCOL,
            page_service_pipelining: if !cfg!(test) {
                PageServicePipeliningConfig::Serial
            } else {
                PageServicePipeliningConfig::Pipelined(PageServicePipeliningConfigPipelined {
                    max_batch_size: NonZeroUsize::new(32).unwrap(),
                    execution: PageServiceProtocolPipelinedExecutionStrategy::ConcurrentFutures,
                })
            },
            get_vectored_concurrent_io: if !cfg!(test) {
                GetVectoredConcurrentIo::Sequential
            } else {
                GetVectoredConcurrentIo::SidecarTask
            },
            enable_read_path_debugging: if cfg!(test) || cfg!(feature = "testing") {
                Some(true)
            } else {
                None
            },
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

    // This value needs to be tuned to avoid OOM. We have 3/4 of the total CPU threads to do background works, that's 16*3/4=9 on
    // most of our pageservers. Compaction ~50 layers requires about 2GB memory (could be reduced later by optimizing L0 hole
    // calculation to avoid loading all keys into the memory). So with this config, we can get a maximum peak compaction usage of 18GB.
    pub const DEFAULT_COMPACTION_UPPER_LIMIT: usize = 50;
    pub const DEFAULT_COMPACTION_L0_FIRST: bool = false;
    pub const DEFAULT_COMPACTION_L0_SEMAPHORE: bool = true;

    pub const DEFAULT_COMPACTION_ALGORITHM: crate::models::CompactionAlgorithm =
        crate::models::CompactionAlgorithm::Legacy;

    pub const DEFAULT_L0_FLUSH_WAIT_UPLOAD: bool = true;

    pub const DEFAULT_GC_HORIZON: u64 = 64 * 1024 * 1024;

    // Large DEFAULT_GC_PERIOD is fine as long as PITR_INTERVAL is larger.
    // If there's a need to decrease this value, first make sure that GC
    // doesn't hold a layer map write lock for non-trivial operations.
    // Relevant: https://github.com/neondatabase/neon/issues/3394
    pub const DEFAULT_GC_PERIOD: &str = "1 hr";
    pub const DEFAULT_IMAGE_CREATION_THRESHOLD: usize = 3;
    // If there are more than threshold * compaction_threshold (that is 3 * 10 in the default config) L0 layers, image
    // layer creation will end immediately. Set to 0 to disable. The target default will be 3 once we
    // want to enable this feature.
    pub const DEFAULT_IMAGE_CREATION_PREEMPT_THRESHOLD: usize = 0;
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
    pub const DEFAULT_GC_COMPACTION_ENABLED: bool = false;
    pub const DEFAULT_GC_COMPACTION_INITIAL_THRESHOLD_KB: u64 = 10240000;
    pub const DEFAULT_GC_COMPACTION_RATIO_PERCENT: u64 = 100;
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
            compaction_l0_first: DEFAULT_COMPACTION_L0_FIRST,
            compaction_l0_semaphore: DEFAULT_COMPACTION_L0_SEMAPHORE,
            l0_flush_delay_threshold: None,
            l0_flush_stall_threshold: None,
            l0_flush_wait_upload: DEFAULT_L0_FLUSH_WAIT_UPLOAD,
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
            wal_receiver_protocol_override: None,
            rel_size_v2_enabled: true, // TODO: before merge the pull request set it to false
            gc_compaction_enabled: DEFAULT_GC_COMPACTION_ENABLED,
            gc_compaction_initial_threshold_kb: DEFAULT_GC_COMPACTION_INITIAL_THRESHOLD_KB,
            gc_compaction_ratio_percent: DEFAULT_GC_COMPACTION_RATIO_PERCENT,
        }
    }
}
