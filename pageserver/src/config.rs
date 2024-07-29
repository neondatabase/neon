//! Functions for handling page server configuration options
//!
//! Configuration options can be set in the pageserver.toml configuration
//! file, or on the command line.
//! See also `settings.md` for better description on every parameter.

use anyhow::{anyhow, bail, ensure, Context, Result};
use pageserver_api::{models::ImageCompressionAlgorithm, shard::TenantShardId};
use remote_storage::{RemotePath, RemoteStorageConfig};
use serde::de::IntoDeserializer;
use serde::{self, Deserialize};
use std::env;
use storage_broker::Uri;
use utils::crashsafe::path_with_suffix_extension;
use utils::logging::SecretString;

use once_cell::sync::OnceCell;
use reqwest::Url;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use toml_edit::{Document, Item};

use camino::{Utf8Path, Utf8PathBuf};
use postgres_backend::AuthType;
use utils::{
    id::{NodeId, TimelineId},
    logging::LogFormat,
};

use crate::tenant::vectored_blob_io::MaxVectoredReadBytes;
use crate::tenant::{config::TenantConfOpt, timeline::GetImpl};
use crate::tenant::{TENANTS_SEGMENT_NAME, TIMELINES_SEGMENT_NAME};
use crate::{disk_usage_eviction_task::DiskUsageEvictionTaskConfig, virtual_file::io_engine};
use crate::{l0_flush::L0FlushConfig, tenant::timeline::GetVectoredImpl};
use crate::{tenant::config::TenantConf, virtual_file};
use crate::{TENANT_HEATMAP_BASENAME, TENANT_LOCATION_CONFIG_NAME, TIMELINE_DELETE_MARK_SUFFIX};

use self::defaults::DEFAULT_CONCURRENT_TENANT_WARMUP;

use self::defaults::DEFAULT_VIRTUAL_FILE_IO_ENGINE;

pub mod defaults {
    use crate::tenant::config::defaults::*;
    use const_format::formatcp;

    pub use pageserver_api::config::{
        DEFAULT_HTTP_LISTEN_ADDR, DEFAULT_HTTP_LISTEN_PORT, DEFAULT_PG_LISTEN_ADDR,
        DEFAULT_PG_LISTEN_PORT,
    };
    use pageserver_api::models::ImageCompressionAlgorithm;
    pub use storage_broker::DEFAULT_ENDPOINT as BROKER_DEFAULT_ENDPOINT;

    pub const DEFAULT_WAIT_LSN_TIMEOUT: &str = "300 s";
    pub const DEFAULT_WAL_REDO_TIMEOUT: &str = "60 s";

    pub const DEFAULT_SUPERUSER: &str = "cloud_admin";

    pub const DEFAULT_PAGE_CACHE_SIZE: usize = 8192;
    pub const DEFAULT_MAX_FILE_DESCRIPTORS: usize = 100;

    pub const DEFAULT_LOG_FORMAT: &str = "plain";

    pub const DEFAULT_CONCURRENT_TENANT_WARMUP: usize = 8;

    pub const DEFAULT_CONCURRENT_TENANT_SIZE_LOGICAL_SIZE_QUERIES: usize =
        super::ConfigurableSemaphore::DEFAULT_INITIAL.get();

    pub const DEFAULT_METRIC_COLLECTION_INTERVAL: &str = "10 min";
    pub const DEFAULT_METRIC_COLLECTION_ENDPOINT: Option<reqwest::Url> = None;
    pub const DEFAULT_SYNTHETIC_SIZE_CALCULATION_INTERVAL: &str = "10 min";
    pub const DEFAULT_BACKGROUND_TASK_MAXIMUM_DELAY: &str = "10s";

    pub const DEFAULT_HEATMAP_UPLOAD_CONCURRENCY: usize = 8;
    pub const DEFAULT_SECONDARY_DOWNLOAD_CONCURRENCY: usize = 1;

    pub const DEFAULT_INGEST_BATCH_SIZE: u64 = 100;

    #[cfg(target_os = "linux")]
    pub const DEFAULT_VIRTUAL_FILE_IO_ENGINE: &str = "tokio-epoll-uring";

    #[cfg(not(target_os = "linux"))]
    pub const DEFAULT_VIRTUAL_FILE_IO_ENGINE: &str = "std-fs";

    pub const DEFAULT_GET_VECTORED_IMPL: &str = "vectored";

    pub const DEFAULT_GET_IMPL: &str = "vectored";

    pub const DEFAULT_MAX_VECTORED_READ_BYTES: usize = 128 * 1024; // 128 KiB

    pub const DEFAULT_IMAGE_COMPRESSION: ImageCompressionAlgorithm =
        ImageCompressionAlgorithm::Disabled;

    pub const DEFAULT_VALIDATE_VECTORED_GET: bool = false;

    pub const DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB: usize = 0;

    ///
    /// Default built-in configuration file.
    ///
    pub const DEFAULT_CONFIG_FILE: &str = formatcp!(
        r#"
# Initial configuration file created by 'pageserver --init'
#listen_pg_addr = '{DEFAULT_PG_LISTEN_ADDR}'
#listen_http_addr = '{DEFAULT_HTTP_LISTEN_ADDR}'

#wait_lsn_timeout = '{DEFAULT_WAIT_LSN_TIMEOUT}'
#wal_redo_timeout = '{DEFAULT_WAL_REDO_TIMEOUT}'

#page_cache_size = {DEFAULT_PAGE_CACHE_SIZE}
#max_file_descriptors = {DEFAULT_MAX_FILE_DESCRIPTORS}

# initial superuser role name to use when creating a new tenant
#initial_superuser_name = '{DEFAULT_SUPERUSER}'

#broker_endpoint = '{BROKER_DEFAULT_ENDPOINT}'

#log_format = '{DEFAULT_LOG_FORMAT}'

#concurrent_tenant_size_logical_size_queries = '{DEFAULT_CONCURRENT_TENANT_SIZE_LOGICAL_SIZE_QUERIES}'
#concurrent_tenant_warmup = '{DEFAULT_CONCURRENT_TENANT_WARMUP}'

#metric_collection_interval = '{DEFAULT_METRIC_COLLECTION_INTERVAL}'
#synthetic_size_calculation_interval = '{DEFAULT_SYNTHETIC_SIZE_CALCULATION_INTERVAL}'

#disk_usage_based_eviction = {{ max_usage_pct = .., min_avail_bytes = .., period = "10s"}}

#background_task_maximum_delay = '{DEFAULT_BACKGROUND_TASK_MAXIMUM_DELAY}'

#ingest_batch_size = {DEFAULT_INGEST_BATCH_SIZE}

#virtual_file_io_engine = '{DEFAULT_VIRTUAL_FILE_IO_ENGINE}'

#get_vectored_impl = '{DEFAULT_GET_VECTORED_IMPL}'

#get_impl = '{DEFAULT_GET_IMPL}'

#max_vectored_read_bytes = '{DEFAULT_MAX_VECTORED_READ_BYTES}'

#validate_vectored_get = '{DEFAULT_VALIDATE_VECTORED_GET}'

[tenant_config]
#checkpoint_distance = {DEFAULT_CHECKPOINT_DISTANCE} # in bytes
#checkpoint_timeout = {DEFAULT_CHECKPOINT_TIMEOUT}
#compaction_target_size = {DEFAULT_COMPACTION_TARGET_SIZE} # in bytes
#compaction_period = '{DEFAULT_COMPACTION_PERIOD}'
#compaction_threshold = {DEFAULT_COMPACTION_THRESHOLD}

#gc_period = '{DEFAULT_GC_PERIOD}'
#gc_horizon = {DEFAULT_GC_HORIZON}
#image_creation_threshold = {DEFAULT_IMAGE_CREATION_THRESHOLD}
#pitr_interval = '{DEFAULT_PITR_INTERVAL}'

#min_resident_size_override = .. # in bytes
#evictions_low_residence_duration_metric_threshold = '{DEFAULT_EVICTIONS_LOW_RESIDENCE_DURATION_METRIC_THRESHOLD}'

#heatmap_upload_concurrency = {DEFAULT_HEATMAP_UPLOAD_CONCURRENCY}
#secondary_download_concurrency = {DEFAULT_SECONDARY_DOWNLOAD_CONCURRENCY}

#ephemeral_bytes_per_memory_kb = {DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB}

#[remote_storage]

"#
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageServerConf {
    // Identifier of that particular pageserver so e g safekeepers
    // can safely distinguish different pageservers
    pub id: NodeId,

    /// Example (default): 127.0.0.1:64000
    pub listen_pg_addr: String,
    /// Example (default): 127.0.0.1:9898
    pub listen_http_addr: String,

    /// Current availability zone. Used for traffic metrics.
    pub availability_zone: Option<String>,

    // Timeout when waiting for WAL receiver to catch up to an LSN given in a GetPage@LSN call.
    pub wait_lsn_timeout: Duration,
    // How long to wait for WAL redo to complete.
    pub wal_redo_timeout: Duration,

    pub superuser: String,

    pub page_cache_size: usize,
    pub max_file_descriptors: usize,

    // Repository directory, relative to current working directory.
    // Normally, the page server changes the current working directory
    // to the repository, and 'workdir' is always '.'. But we don't do
    // that during unit testing, because the current directory is global
    // to the process but different unit tests work on different
    // repositories.
    pub workdir: Utf8PathBuf,

    pub pg_distrib_dir: Utf8PathBuf,

    // Authentication
    /// authentication method for the HTTP mgmt API
    pub http_auth_type: AuthType,
    /// authentication method for libpq connections from compute
    pub pg_auth_type: AuthType,
    /// Path to a file or directory containing public key(s) for verifying JWT tokens.
    /// Used for both mgmt and compute auth, if enabled.
    pub auth_validation_public_key_path: Option<Utf8PathBuf>,

    pub remote_storage_config: Option<RemoteStorageConfig>,

    pub default_tenant_conf: TenantConf,

    /// Storage broker endpoints to connect to.
    pub broker_endpoint: Uri,
    pub broker_keepalive_interval: Duration,

    pub log_format: LogFormat,

    /// Number of tenants which will be concurrently loaded from remote storage proactively on startup or attach.
    ///
    /// A lower value implicitly deprioritizes loading such tenants, vs. other work in the system.
    pub concurrent_tenant_warmup: ConfigurableSemaphore,

    /// Number of concurrent [`Tenant::gather_size_inputs`](crate::tenant::Tenant::gather_size_inputs) allowed.
    pub concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore,
    /// Limit of concurrent [`Tenant::gather_size_inputs`] issued by module `eviction_task`.
    /// The number of permits is the same as `concurrent_tenant_size_logical_size_queries`.
    /// See the comment in `eviction_task` for details.
    ///
    /// [`Tenant::gather_size_inputs`]: crate::tenant::Tenant::gather_size_inputs
    pub eviction_task_immitated_concurrent_logical_size_queries: ConfigurableSemaphore,

    // How often to collect metrics and send them to the metrics endpoint.
    pub metric_collection_interval: Duration,
    // How often to send unchanged cached metrics to the metrics endpoint.
    pub metric_collection_endpoint: Option<Url>,
    pub metric_collection_bucket: Option<RemoteStorageConfig>,
    pub synthetic_size_calculation_interval: Duration,

    pub disk_usage_based_eviction: Option<DiskUsageEvictionTaskConfig>,

    pub test_remote_failures: u64,

    pub ondemand_download_behavior_treat_error_as_warn: bool,

    /// How long will background tasks be delayed at most after initial load of tenants.
    ///
    /// Our largest initialization completions are in the range of 100-200s, so perhaps 10s works
    /// as we now isolate initial loading, initial logical size calculation and background tasks.
    /// Smaller nodes will have background tasks "not running" for this long unless every timeline
    /// has it's initial logical size calculated. Not running background tasks for some seconds is
    /// not terrible.
    pub background_task_maximum_delay: Duration,

    pub control_plane_api: Option<Url>,

    /// JWT token for use with the control plane API.
    pub control_plane_api_token: Option<SecretString>,

    /// If true, pageserver will make best-effort to operate without a control plane: only
    /// for use in major incidents.
    pub control_plane_emergency_mode: bool,

    /// How many heatmap uploads may be done concurrency: lower values implicitly deprioritize
    /// heatmap uploads vs. other remote storage operations.
    pub heatmap_upload_concurrency: usize,

    /// How many remote storage downloads may be done for secondary tenants concurrently.  Implicitly
    /// deprioritises secondary downloads vs. remote storage operations for attached tenants.
    pub secondary_download_concurrency: usize,

    /// Maximum number of WAL records to be ingested and committed at the same time
    pub ingest_batch_size: u64,

    pub virtual_file_io_engine: virtual_file::IoEngineKind,

    pub get_vectored_impl: GetVectoredImpl,

    pub get_impl: GetImpl,

    pub max_vectored_read_bytes: MaxVectoredReadBytes,

    pub validate_vectored_get: bool,

    pub image_compression: ImageCompressionAlgorithm,

    /// How many bytes of ephemeral layer content will we allow per kilobyte of RAM.  When this
    /// is exceeded, we start proactively closing ephemeral layers to limit the total amount
    /// of ephemeral data.
    ///
    /// Setting this to zero disables limits on total ephemeral layer size.
    pub ephemeral_bytes_per_memory_kb: usize,

    pub l0_flush: L0FlushConfig,
}

/// We do not want to store this in a PageServerConf because the latter may be logged
/// and/or serialized at a whim, while the token is secret. Currently this token is the
/// same for accessing all tenants/timelines, but may become per-tenant/per-timeline in
/// the future, more tokens and auth may arrive for storage broker, completely changing the logic.
/// Hence, we resort to a global variable for now instead of passing the token from the
/// startup code to the connection code through a dozen layers.
pub static SAFEKEEPER_AUTH_TOKEN: OnceCell<Arc<String>> = OnceCell::new();

// use dedicated enum for builder to better indicate the intention
// and avoid possible confusion with nested options
#[derive(Clone, Default)]
pub enum BuilderValue<T> {
    Set(T),
    #[default]
    NotSet,
}

impl<T: Clone> BuilderValue<T> {
    pub fn ok_or(&self, field_name: &'static str, default: BuilderValue<T>) -> anyhow::Result<T> {
        match self {
            Self::Set(v) => Ok(v.clone()),
            Self::NotSet => match default {
                BuilderValue::Set(v) => Ok(v.clone()),
                BuilderValue::NotSet => {
                    anyhow::bail!("missing config value {field_name:?}")
                }
            },
        }
    }
}

// needed to simplify config construction
#[derive(Default)]
struct PageServerConfigBuilder {
    listen_pg_addr: BuilderValue<String>,

    listen_http_addr: BuilderValue<String>,

    availability_zone: BuilderValue<Option<String>>,

    wait_lsn_timeout: BuilderValue<Duration>,
    wal_redo_timeout: BuilderValue<Duration>,

    superuser: BuilderValue<String>,

    page_cache_size: BuilderValue<usize>,
    max_file_descriptors: BuilderValue<usize>,

    workdir: BuilderValue<Utf8PathBuf>,

    pg_distrib_dir: BuilderValue<Utf8PathBuf>,

    http_auth_type: BuilderValue<AuthType>,
    pg_auth_type: BuilderValue<AuthType>,

    //
    auth_validation_public_key_path: BuilderValue<Option<Utf8PathBuf>>,
    remote_storage_config: BuilderValue<Option<RemoteStorageConfig>>,

    broker_endpoint: BuilderValue<Uri>,
    broker_keepalive_interval: BuilderValue<Duration>,

    log_format: BuilderValue<LogFormat>,

    concurrent_tenant_warmup: BuilderValue<NonZeroUsize>,
    concurrent_tenant_size_logical_size_queries: BuilderValue<NonZeroUsize>,

    metric_collection_interval: BuilderValue<Duration>,
    metric_collection_endpoint: BuilderValue<Option<Url>>,
    synthetic_size_calculation_interval: BuilderValue<Duration>,
    metric_collection_bucket: BuilderValue<Option<RemoteStorageConfig>>,

    disk_usage_based_eviction: BuilderValue<Option<DiskUsageEvictionTaskConfig>>,

    test_remote_failures: BuilderValue<u64>,

    ondemand_download_behavior_treat_error_as_warn: BuilderValue<bool>,

    background_task_maximum_delay: BuilderValue<Duration>,

    control_plane_api: BuilderValue<Option<Url>>,
    control_plane_api_token: BuilderValue<Option<SecretString>>,
    control_plane_emergency_mode: BuilderValue<bool>,

    heatmap_upload_concurrency: BuilderValue<usize>,
    secondary_download_concurrency: BuilderValue<usize>,

    ingest_batch_size: BuilderValue<u64>,

    virtual_file_io_engine: BuilderValue<virtual_file::IoEngineKind>,

    get_vectored_impl: BuilderValue<GetVectoredImpl>,

    get_impl: BuilderValue<GetImpl>,

    max_vectored_read_bytes: BuilderValue<MaxVectoredReadBytes>,

    validate_vectored_get: BuilderValue<bool>,

    image_compression: BuilderValue<ImageCompressionAlgorithm>,

    ephemeral_bytes_per_memory_kb: BuilderValue<usize>,

    l0_flush: BuilderValue<L0FlushConfig>,
}

impl PageServerConfigBuilder {
    fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    fn default_values() -> Self {
        use self::BuilderValue::*;
        use defaults::*;
        Self {
            listen_pg_addr: Set(DEFAULT_PG_LISTEN_ADDR.to_string()),
            listen_http_addr: Set(DEFAULT_HTTP_LISTEN_ADDR.to_string()),
            availability_zone: Set(None),
            wait_lsn_timeout: Set(humantime::parse_duration(DEFAULT_WAIT_LSN_TIMEOUT)
                .expect("cannot parse default wait lsn timeout")),
            wal_redo_timeout: Set(humantime::parse_duration(DEFAULT_WAL_REDO_TIMEOUT)
                .expect("cannot parse default wal redo timeout")),
            superuser: Set(DEFAULT_SUPERUSER.to_string()),
            page_cache_size: Set(DEFAULT_PAGE_CACHE_SIZE),
            max_file_descriptors: Set(DEFAULT_MAX_FILE_DESCRIPTORS),
            workdir: Set(Utf8PathBuf::new()),
            pg_distrib_dir: Set(Utf8PathBuf::from_path_buf(
                env::current_dir().expect("cannot access current directory"),
            )
            .expect("non-Unicode path")
            .join("pg_install")),
            http_auth_type: Set(AuthType::Trust),
            pg_auth_type: Set(AuthType::Trust),
            auth_validation_public_key_path: Set(None),
            remote_storage_config: Set(None),
            broker_endpoint: Set(storage_broker::DEFAULT_ENDPOINT
                .parse()
                .expect("failed to parse default broker endpoint")),
            broker_keepalive_interval: Set(humantime::parse_duration(
                storage_broker::DEFAULT_KEEPALIVE_INTERVAL,
            )
            .expect("cannot parse default keepalive interval")),
            log_format: Set(LogFormat::from_str(DEFAULT_LOG_FORMAT).unwrap()),

            concurrent_tenant_warmup: Set(NonZeroUsize::new(DEFAULT_CONCURRENT_TENANT_WARMUP)
                .expect("Invalid default constant")),
            concurrent_tenant_size_logical_size_queries: Set(
                ConfigurableSemaphore::DEFAULT_INITIAL,
            ),
            metric_collection_interval: Set(humantime::parse_duration(
                DEFAULT_METRIC_COLLECTION_INTERVAL,
            )
            .expect("cannot parse default metric collection interval")),
            synthetic_size_calculation_interval: Set(humantime::parse_duration(
                DEFAULT_SYNTHETIC_SIZE_CALCULATION_INTERVAL,
            )
            .expect("cannot parse default synthetic size calculation interval")),
            metric_collection_endpoint: Set(DEFAULT_METRIC_COLLECTION_ENDPOINT),

            metric_collection_bucket: Set(None),

            disk_usage_based_eviction: Set(None),

            test_remote_failures: Set(0),

            ondemand_download_behavior_treat_error_as_warn: Set(false),

            background_task_maximum_delay: Set(humantime::parse_duration(
                DEFAULT_BACKGROUND_TASK_MAXIMUM_DELAY,
            )
            .unwrap()),

            control_plane_api: Set(None),
            control_plane_api_token: Set(None),
            control_plane_emergency_mode: Set(false),

            heatmap_upload_concurrency: Set(DEFAULT_HEATMAP_UPLOAD_CONCURRENCY),
            secondary_download_concurrency: Set(DEFAULT_SECONDARY_DOWNLOAD_CONCURRENCY),

            ingest_batch_size: Set(DEFAULT_INGEST_BATCH_SIZE),

            virtual_file_io_engine: Set(DEFAULT_VIRTUAL_FILE_IO_ENGINE.parse().unwrap()),

            get_vectored_impl: Set(DEFAULT_GET_VECTORED_IMPL.parse().unwrap()),
            get_impl: Set(DEFAULT_GET_IMPL.parse().unwrap()),
            max_vectored_read_bytes: Set(MaxVectoredReadBytes(
                NonZeroUsize::new(DEFAULT_MAX_VECTORED_READ_BYTES).unwrap(),
            )),
            image_compression: Set(DEFAULT_IMAGE_COMPRESSION),
            validate_vectored_get: Set(DEFAULT_VALIDATE_VECTORED_GET),
            ephemeral_bytes_per_memory_kb: Set(DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB),
            l0_flush: Set(L0FlushConfig::default()),
        }
    }
}

impl PageServerConfigBuilder {
    pub fn listen_pg_addr(&mut self, listen_pg_addr: String) {
        self.listen_pg_addr = BuilderValue::Set(listen_pg_addr)
    }

    pub fn listen_http_addr(&mut self, listen_http_addr: String) {
        self.listen_http_addr = BuilderValue::Set(listen_http_addr)
    }

    pub fn availability_zone(&mut self, availability_zone: Option<String>) {
        self.availability_zone = BuilderValue::Set(availability_zone)
    }

    pub fn wait_lsn_timeout(&mut self, wait_lsn_timeout: Duration) {
        self.wait_lsn_timeout = BuilderValue::Set(wait_lsn_timeout)
    }

    pub fn wal_redo_timeout(&mut self, wal_redo_timeout: Duration) {
        self.wal_redo_timeout = BuilderValue::Set(wal_redo_timeout)
    }

    pub fn superuser(&mut self, superuser: String) {
        self.superuser = BuilderValue::Set(superuser)
    }

    pub fn page_cache_size(&mut self, page_cache_size: usize) {
        self.page_cache_size = BuilderValue::Set(page_cache_size)
    }

    pub fn max_file_descriptors(&mut self, max_file_descriptors: usize) {
        self.max_file_descriptors = BuilderValue::Set(max_file_descriptors)
    }

    pub fn workdir(&mut self, workdir: Utf8PathBuf) {
        self.workdir = BuilderValue::Set(workdir)
    }

    pub fn pg_distrib_dir(&mut self, pg_distrib_dir: Utf8PathBuf) {
        self.pg_distrib_dir = BuilderValue::Set(pg_distrib_dir)
    }

    pub fn http_auth_type(&mut self, auth_type: AuthType) {
        self.http_auth_type = BuilderValue::Set(auth_type)
    }

    pub fn pg_auth_type(&mut self, auth_type: AuthType) {
        self.pg_auth_type = BuilderValue::Set(auth_type)
    }

    pub fn auth_validation_public_key_path(
        &mut self,
        auth_validation_public_key_path: Option<Utf8PathBuf>,
    ) {
        self.auth_validation_public_key_path = BuilderValue::Set(auth_validation_public_key_path)
    }

    pub fn remote_storage_config(&mut self, remote_storage_config: Option<RemoteStorageConfig>) {
        self.remote_storage_config = BuilderValue::Set(remote_storage_config)
    }

    pub fn broker_endpoint(&mut self, broker_endpoint: Uri) {
        self.broker_endpoint = BuilderValue::Set(broker_endpoint)
    }

    pub fn broker_keepalive_interval(&mut self, broker_keepalive_interval: Duration) {
        self.broker_keepalive_interval = BuilderValue::Set(broker_keepalive_interval)
    }

    pub fn log_format(&mut self, log_format: LogFormat) {
        self.log_format = BuilderValue::Set(log_format)
    }

    pub fn concurrent_tenant_warmup(&mut self, u: NonZeroUsize) {
        self.concurrent_tenant_warmup = BuilderValue::Set(u);
    }

    pub fn concurrent_tenant_size_logical_size_queries(&mut self, u: NonZeroUsize) {
        self.concurrent_tenant_size_logical_size_queries = BuilderValue::Set(u);
    }

    pub fn metric_collection_interval(&mut self, metric_collection_interval: Duration) {
        self.metric_collection_interval = BuilderValue::Set(metric_collection_interval)
    }

    pub fn metric_collection_endpoint(&mut self, metric_collection_endpoint: Option<Url>) {
        self.metric_collection_endpoint = BuilderValue::Set(metric_collection_endpoint)
    }

    pub fn metric_collection_bucket(
        &mut self,
        metric_collection_bucket: Option<RemoteStorageConfig>,
    ) {
        self.metric_collection_bucket = BuilderValue::Set(metric_collection_bucket)
    }

    pub fn synthetic_size_calculation_interval(
        &mut self,
        synthetic_size_calculation_interval: Duration,
    ) {
        self.synthetic_size_calculation_interval =
            BuilderValue::Set(synthetic_size_calculation_interval)
    }

    pub fn test_remote_failures(&mut self, fail_first: u64) {
        self.test_remote_failures = BuilderValue::Set(fail_first);
    }

    pub fn disk_usage_based_eviction(&mut self, value: Option<DiskUsageEvictionTaskConfig>) {
        self.disk_usage_based_eviction = BuilderValue::Set(value);
    }

    pub fn ondemand_download_behavior_treat_error_as_warn(
        &mut self,
        ondemand_download_behavior_treat_error_as_warn: bool,
    ) {
        self.ondemand_download_behavior_treat_error_as_warn =
            BuilderValue::Set(ondemand_download_behavior_treat_error_as_warn);
    }

    pub fn background_task_maximum_delay(&mut self, delay: Duration) {
        self.background_task_maximum_delay = BuilderValue::Set(delay);
    }

    pub fn control_plane_api(&mut self, api: Option<Url>) {
        self.control_plane_api = BuilderValue::Set(api)
    }

    pub fn control_plane_api_token(&mut self, token: Option<SecretString>) {
        self.control_plane_api_token = BuilderValue::Set(token)
    }

    pub fn control_plane_emergency_mode(&mut self, enabled: bool) {
        self.control_plane_emergency_mode = BuilderValue::Set(enabled)
    }

    pub fn heatmap_upload_concurrency(&mut self, value: usize) {
        self.heatmap_upload_concurrency = BuilderValue::Set(value)
    }

    pub fn secondary_download_concurrency(&mut self, value: usize) {
        self.secondary_download_concurrency = BuilderValue::Set(value)
    }

    pub fn ingest_batch_size(&mut self, ingest_batch_size: u64) {
        self.ingest_batch_size = BuilderValue::Set(ingest_batch_size)
    }

    pub fn virtual_file_io_engine(&mut self, value: virtual_file::IoEngineKind) {
        self.virtual_file_io_engine = BuilderValue::Set(value);
    }

    pub fn get_vectored_impl(&mut self, value: GetVectoredImpl) {
        self.get_vectored_impl = BuilderValue::Set(value);
    }

    pub fn get_impl(&mut self, value: GetImpl) {
        self.get_impl = BuilderValue::Set(value);
    }

    pub fn get_max_vectored_read_bytes(&mut self, value: MaxVectoredReadBytes) {
        self.max_vectored_read_bytes = BuilderValue::Set(value);
    }

    pub fn get_validate_vectored_get(&mut self, value: bool) {
        self.validate_vectored_get = BuilderValue::Set(value);
    }

    pub fn get_image_compression(&mut self, value: ImageCompressionAlgorithm) {
        self.image_compression = BuilderValue::Set(value);
    }

    pub fn get_ephemeral_bytes_per_memory_kb(&mut self, value: usize) {
        self.ephemeral_bytes_per_memory_kb = BuilderValue::Set(value);
    }

    pub fn l0_flush(&mut self, value: L0FlushConfig) {
        self.l0_flush = BuilderValue::Set(value);
    }

    pub fn build(self, id: NodeId) -> anyhow::Result<PageServerConf> {
        let default = Self::default_values();

        macro_rules! conf {
            (USING DEFAULT { $($field:ident,)* } CUSTOM LOGIC { $($custom_field:ident : $custom_value:expr,)* } ) => {
                PageServerConf {
                    $(
                        $field: self.$field.ok_or(stringify!($field), default.$field)?,
                    )*
                    $(
                        $custom_field: $custom_value,
                    )*
                }
            };
        }

        Ok(conf!(
            USING DEFAULT
            {
                listen_pg_addr,
                listen_http_addr,
                availability_zone,
                wait_lsn_timeout,
                wal_redo_timeout,
                superuser,
                page_cache_size,
                max_file_descriptors,
                workdir,
                pg_distrib_dir,
                http_auth_type,
                pg_auth_type,
                auth_validation_public_key_path,
                remote_storage_config,
                broker_endpoint,
                broker_keepalive_interval,
                log_format,
                metric_collection_interval,
                metric_collection_endpoint,
                metric_collection_bucket,
                synthetic_size_calculation_interval,
                disk_usage_based_eviction,
                test_remote_failures,
                ondemand_download_behavior_treat_error_as_warn,
                background_task_maximum_delay,
                control_plane_api,
                control_plane_api_token,
                control_plane_emergency_mode,
                heatmap_upload_concurrency,
                secondary_download_concurrency,
                ingest_batch_size,
                get_vectored_impl,
                get_impl,
                max_vectored_read_bytes,
                validate_vectored_get,
                image_compression,
                ephemeral_bytes_per_memory_kb,
                l0_flush,
            }
            CUSTOM LOGIC
            {
                id: id,
                // TenantConf is handled separately
                default_tenant_conf: TenantConf::default(),
                concurrent_tenant_warmup: ConfigurableSemaphore::new({
                    self
                        .concurrent_tenant_warmup
                        .ok_or("concurrent_tenant_warmpup",
                               default.concurrent_tenant_warmup)?
                }),
                concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore::new(
                    self
                        .concurrent_tenant_size_logical_size_queries
                        .ok_or("concurrent_tenant_size_logical_size_queries",
                               default.concurrent_tenant_size_logical_size_queries.clone())?
                ),
                eviction_task_immitated_concurrent_logical_size_queries: ConfigurableSemaphore::new(
                    // re-use `concurrent_tenant_size_logical_size_queries`
                    self
                        .concurrent_tenant_size_logical_size_queries
                        .ok_or("eviction_task_immitated_concurrent_logical_size_queries",
                               default.concurrent_tenant_size_logical_size_queries.clone())?,
                ),
                virtual_file_io_engine: match self.virtual_file_io_engine {
                    BuilderValue::Set(v) => v,
                    BuilderValue::NotSet => match crate::virtual_file::io_engine_feature_test().context("auto-detect virtual_file_io_engine")? {
                        io_engine::FeatureTestResult::PlatformPreferred(v) => v, // make no noise
                        io_engine::FeatureTestResult::Worse { engine, remark } => {
                            // TODO: bubble this up to the caller so we can tracing::warn! it.
                            eprintln!("auto-detected IO engine is not platform-preferred: engine={engine:?} remark={remark:?}");
                            engine
                        }
                    },
                },
            }
        ))
    }
}

impl PageServerConf {
    //
    // Repository paths, relative to workdir.
    //

    pub fn tenants_path(&self) -> Utf8PathBuf {
        self.workdir.join(TENANTS_SEGMENT_NAME)
    }

    pub fn deletion_prefix(&self) -> Utf8PathBuf {
        self.workdir.join("deletion")
    }

    pub fn metadata_path(&self) -> Utf8PathBuf {
        self.workdir.join("metadata.json")
    }

    pub fn deletion_list_path(&self, sequence: u64) -> Utf8PathBuf {
        // Encode a version in the filename, so that if we ever switch away from JSON we can
        // increment this.
        const VERSION: u8 = 1;

        self.deletion_prefix()
            .join(format!("{sequence:016x}-{VERSION:02x}.list"))
    }

    pub fn deletion_header_path(&self) -> Utf8PathBuf {
        // Encode a version in the filename, so that if we ever switch away from JSON we can
        // increment this.
        const VERSION: u8 = 1;

        self.deletion_prefix().join(format!("header-{VERSION:02x}"))
    }

    pub fn tenant_path(&self, tenant_shard_id: &TenantShardId) -> Utf8PathBuf {
        self.tenants_path().join(tenant_shard_id.to_string())
    }

    /// Points to a place in pageserver's local directory,
    /// where certain tenant's LocationConf be stored.
    pub(crate) fn tenant_location_config_path(
        &self,
        tenant_shard_id: &TenantShardId,
    ) -> Utf8PathBuf {
        self.tenant_path(tenant_shard_id)
            .join(TENANT_LOCATION_CONFIG_NAME)
    }

    pub(crate) fn tenant_heatmap_path(&self, tenant_shard_id: &TenantShardId) -> Utf8PathBuf {
        self.tenant_path(tenant_shard_id)
            .join(TENANT_HEATMAP_BASENAME)
    }

    pub fn timelines_path(&self, tenant_shard_id: &TenantShardId) -> Utf8PathBuf {
        self.tenant_path(tenant_shard_id)
            .join(TIMELINES_SEGMENT_NAME)
    }

    pub fn timeline_path(
        &self,
        tenant_shard_id: &TenantShardId,
        timeline_id: &TimelineId,
    ) -> Utf8PathBuf {
        self.timelines_path(tenant_shard_id)
            .join(timeline_id.to_string())
    }

    pub(crate) fn timeline_delete_mark_file_path(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
    ) -> Utf8PathBuf {
        path_with_suffix_extension(
            self.timeline_path(&tenant_shard_id, &timeline_id),
            TIMELINE_DELETE_MARK_SUFFIX,
        )
    }

    /// Turns storage remote path of a file into its local path.
    pub fn local_path(&self, remote_path: &RemotePath) -> Utf8PathBuf {
        remote_path.with_base(&self.workdir)
    }

    //
    // Postgres distribution paths
    //
    pub fn pg_distrib_dir(&self, pg_version: u32) -> anyhow::Result<Utf8PathBuf> {
        let path = self.pg_distrib_dir.clone();

        #[allow(clippy::manual_range_patterns)]
        match pg_version {
            14 | 15 | 16 => Ok(path.join(format!("v{pg_version}"))),
            _ => bail!("Unsupported postgres version: {}", pg_version),
        }
    }

    pub fn pg_bin_dir(&self, pg_version: u32) -> anyhow::Result<Utf8PathBuf> {
        Ok(self.pg_distrib_dir(pg_version)?.join("bin"))
    }
    pub fn pg_lib_dir(&self, pg_version: u32) -> anyhow::Result<Utf8PathBuf> {
        Ok(self.pg_distrib_dir(pg_version)?.join("lib"))
    }

    /// Parse a configuration file (pageserver.toml) into a PageServerConf struct,
    /// validating the input and failing on errors.
    ///
    /// This leaves any options not present in the file in the built-in defaults.
    pub fn parse_and_validate(
        node_id: NodeId,
        toml: &Document,
        workdir: &Utf8Path,
    ) -> anyhow::Result<Self> {
        let mut builder = PageServerConfigBuilder::new();
        builder.workdir(workdir.to_owned());

        let mut t_conf = TenantConfOpt::default();

        for (key, item) in toml.iter() {
            match key {
                "listen_pg_addr" => builder.listen_pg_addr(parse_toml_string(key, item)?),
                "listen_http_addr" => builder.listen_http_addr(parse_toml_string(key, item)?),
                "availability_zone" => builder.availability_zone(Some(parse_toml_string(key, item)?)),
                "wait_lsn_timeout" => builder.wait_lsn_timeout(parse_toml_duration(key, item)?),
                "wal_redo_timeout" => builder.wal_redo_timeout(parse_toml_duration(key, item)?),
                "initial_superuser_name" => builder.superuser(parse_toml_string(key, item)?),
                "page_cache_size" => builder.page_cache_size(parse_toml_u64(key, item)? as usize),
                "max_file_descriptors" => {
                    builder.max_file_descriptors(parse_toml_u64(key, item)? as usize)
                }
                "pg_distrib_dir" => {
                    builder.pg_distrib_dir(Utf8PathBuf::from(parse_toml_string(key, item)?))
                }
                "auth_validation_public_key_path" => builder.auth_validation_public_key_path(Some(
                    Utf8PathBuf::from(parse_toml_string(key, item)?),
                )),
                "http_auth_type" => builder.http_auth_type(parse_toml_from_str(key, item)?),
                "pg_auth_type" => builder.pg_auth_type(parse_toml_from_str(key, item)?),
                "remote_storage" => {
                    builder.remote_storage_config(Some(RemoteStorageConfig::from_toml(item).context("remote_storage")?))
                }
                "tenant_config" => {
                    t_conf = TenantConfOpt::try_from(item.to_owned()).context(format!("failed to parse: '{key}'"))?;
                }
                "broker_endpoint" => builder.broker_endpoint(parse_toml_string(key, item)?.parse().context("failed to parse broker endpoint")?),
                "broker_keepalive_interval" => builder.broker_keepalive_interval(parse_toml_duration(key, item)?),
                "log_format" => builder.log_format(
                    LogFormat::from_config(&parse_toml_string(key, item)?)?
                ),
                "concurrent_tenant_warmup" => builder.concurrent_tenant_warmup({
                    let input = parse_toml_string(key, item)?;
                    let permits = input.parse::<usize>().context("expected a number of initial permits, not {s:?}")?;
                    NonZeroUsize::new(permits).context("initial semaphore permits out of range: 0, use other configuration to disable a feature")?
                }),
                "concurrent_tenant_size_logical_size_queries" => builder.concurrent_tenant_size_logical_size_queries({
                    let input = parse_toml_string(key, item)?;
                    let permits = input.parse::<usize>().context("expected a number of initial permits, not {s:?}")?;
                    NonZeroUsize::new(permits).context("initial semaphore permits out of range: 0, use other configuration to disable a feature")?
                }),
                "metric_collection_interval" => builder.metric_collection_interval(parse_toml_duration(key, item)?),
                "metric_collection_endpoint" => {
                    let endpoint = parse_toml_string(key, item)?.parse().context("failed to parse metric_collection_endpoint")?;
                    builder.metric_collection_endpoint(Some(endpoint));
                },
                "metric_collection_bucket" => {
                    builder.metric_collection_bucket(Some(RemoteStorageConfig::from_toml(item)?))
                }
                "synthetic_size_calculation_interval" =>
                    builder.synthetic_size_calculation_interval(parse_toml_duration(key, item)?),
                "test_remote_failures" => builder.test_remote_failures(parse_toml_u64(key, item)?),
                "disk_usage_based_eviction" => {
                    tracing::info!("disk_usage_based_eviction: {:#?}", &item);
                    builder.disk_usage_based_eviction(
                        deserialize_from_item("disk_usage_based_eviction", item)
                            .context("parse disk_usage_based_eviction")?
                    )
                },
                "ondemand_download_behavior_treat_error_as_warn" => builder.ondemand_download_behavior_treat_error_as_warn(parse_toml_bool(key, item)?),
                "background_task_maximum_delay" => builder.background_task_maximum_delay(parse_toml_duration(key, item)?),
                "control_plane_api" => {
                    let parsed = parse_toml_string(key, item)?;
                    if parsed.is_empty() {
                        builder.control_plane_api(None)
                    } else {
                        builder.control_plane_api(Some(parsed.parse().context("failed to parse control plane URL")?))
                    }
                },
                "control_plane_api_token" => {
                    let parsed = parse_toml_string(key, item)?;
                    if parsed.is_empty() {
                        builder.control_plane_api_token(None)
                    } else {
                        builder.control_plane_api_token(Some(parsed.into()))
                    }
                },
                "control_plane_emergency_mode" => {
                    builder.control_plane_emergency_mode(parse_toml_bool(key, item)?)
                },
                "heatmap_upload_concurrency" => {
                    builder.heatmap_upload_concurrency(parse_toml_u64(key, item)? as usize)
                },
                "secondary_download_concurrency" => {
                    builder.secondary_download_concurrency(parse_toml_u64(key, item)? as usize)
                },
                "ingest_batch_size" => builder.ingest_batch_size(parse_toml_u64(key, item)?),
                "virtual_file_io_engine" => {
                    builder.virtual_file_io_engine(parse_toml_from_str("virtual_file_io_engine", item)?)
                }
                "get_vectored_impl" => {
                    builder.get_vectored_impl(parse_toml_from_str("get_vectored_impl", item)?)
                }
                "get_impl" => {
                    builder.get_impl(parse_toml_from_str("get_impl", item)?)
                }
                "max_vectored_read_bytes" => {
                    let bytes = parse_toml_u64("max_vectored_read_bytes", item)? as usize;
                    builder.get_max_vectored_read_bytes(
                        MaxVectoredReadBytes(
                            NonZeroUsize::new(bytes).expect("Max byte size of vectored read must be greater than 0")))
                }
                "validate_vectored_get" => {
                    builder.get_validate_vectored_get(parse_toml_bool("validate_vectored_get", item)?)
                }
                "image_compression" => {
                    builder.get_image_compression(parse_toml_from_str("image_compression", item)?)
                }
                "ephemeral_bytes_per_memory_kb" => {
                    builder.get_ephemeral_bytes_per_memory_kb(parse_toml_u64("ephemeral_bytes_per_memory_kb", item)? as usize)
                }
                "l0_flush" => {
                    builder.l0_flush(utils::toml_edit_ext::deserialize_item(item).context("l0_flush")?)
                }
                _ => bail!("unrecognized pageserver option '{key}'"),
            }
        }

        let mut conf = builder.build(node_id).context("invalid config")?;

        if conf.http_auth_type == AuthType::NeonJWT || conf.pg_auth_type == AuthType::NeonJWT {
            let auth_validation_public_key_path = conf
                .auth_validation_public_key_path
                .get_or_insert_with(|| workdir.join("auth_public_key.pem"));
            ensure!(
                auth_validation_public_key_path.exists(),
                format!(
                    "Can't find auth_validation_public_key at '{auth_validation_public_key_path}'",
                )
            );
        }

        conf.default_tenant_conf = t_conf.merge(TenantConf::default());

        Ok(conf)
    }

    #[cfg(test)]
    pub fn test_repo_dir(test_name: &str) -> Utf8PathBuf {
        let test_output_dir = std::env::var("TEST_OUTPUT").unwrap_or("../tmp_check".into());
        Utf8PathBuf::from(format!("{test_output_dir}/test_{test_name}"))
    }

    pub fn dummy_conf(repo_dir: Utf8PathBuf) -> Self {
        let pg_distrib_dir = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../pg_install");

        PageServerConf {
            id: NodeId(0),
            wait_lsn_timeout: Duration::from_secs(60),
            wal_redo_timeout: Duration::from_secs(60),
            page_cache_size: defaults::DEFAULT_PAGE_CACHE_SIZE,
            max_file_descriptors: defaults::DEFAULT_MAX_FILE_DESCRIPTORS,
            listen_pg_addr: defaults::DEFAULT_PG_LISTEN_ADDR.to_string(),
            listen_http_addr: defaults::DEFAULT_HTTP_LISTEN_ADDR.to_string(),
            availability_zone: None,
            superuser: "cloud_admin".to_string(),
            workdir: repo_dir,
            pg_distrib_dir,
            http_auth_type: AuthType::Trust,
            pg_auth_type: AuthType::Trust,
            auth_validation_public_key_path: None,
            remote_storage_config: None,
            default_tenant_conf: TenantConf::default(),
            broker_endpoint: storage_broker::DEFAULT_ENDPOINT.parse().unwrap(),
            broker_keepalive_interval: Duration::from_secs(5000),
            log_format: LogFormat::from_str(defaults::DEFAULT_LOG_FORMAT).unwrap(),
            concurrent_tenant_warmup: ConfigurableSemaphore::new(
                NonZeroUsize::new(DEFAULT_CONCURRENT_TENANT_WARMUP)
                    .expect("Invalid default constant"),
            ),
            concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore::default(),
            eviction_task_immitated_concurrent_logical_size_queries: ConfigurableSemaphore::default(
            ),
            metric_collection_interval: Duration::from_secs(60),
            metric_collection_endpoint: defaults::DEFAULT_METRIC_COLLECTION_ENDPOINT,
            metric_collection_bucket: None,
            synthetic_size_calculation_interval: Duration::from_secs(60),
            disk_usage_based_eviction: None,
            test_remote_failures: 0,
            ondemand_download_behavior_treat_error_as_warn: false,
            background_task_maximum_delay: Duration::ZERO,
            control_plane_api: None,
            control_plane_api_token: None,
            control_plane_emergency_mode: false,
            heatmap_upload_concurrency: defaults::DEFAULT_HEATMAP_UPLOAD_CONCURRENCY,
            secondary_download_concurrency: defaults::DEFAULT_SECONDARY_DOWNLOAD_CONCURRENCY,
            ingest_batch_size: defaults::DEFAULT_INGEST_BATCH_SIZE,
            virtual_file_io_engine: DEFAULT_VIRTUAL_FILE_IO_ENGINE.parse().unwrap(),
            get_vectored_impl: defaults::DEFAULT_GET_VECTORED_IMPL.parse().unwrap(),
            get_impl: defaults::DEFAULT_GET_IMPL.parse().unwrap(),
            max_vectored_read_bytes: MaxVectoredReadBytes(
                NonZeroUsize::new(defaults::DEFAULT_MAX_VECTORED_READ_BYTES)
                    .expect("Invalid default constant"),
            ),
            image_compression: defaults::DEFAULT_IMAGE_COMPRESSION,
            validate_vectored_get: defaults::DEFAULT_VALIDATE_VECTORED_GET,
            ephemeral_bytes_per_memory_kb: defaults::DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB,
            l0_flush: L0FlushConfig::default(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageserverIdentity {
    pub id: NodeId,
}

// Helper functions to parse a toml Item

fn parse_toml_string(name: &str, item: &Item) -> Result<String> {
    let s = item
        .as_str()
        .with_context(|| format!("configure option {name} is not a string"))?;
    Ok(s.to_string())
}

fn parse_toml_u64(name: &str, item: &Item) -> Result<u64> {
    // A toml integer is signed, so it cannot represent the full range of an u64. That's OK
    // for our use, though.
    let i: i64 = item
        .as_integer()
        .with_context(|| format!("configure option {name} is not an integer"))?;
    if i < 0 {
        bail!("configure option {name} cannot be negative");
    }
    Ok(i as u64)
}

fn parse_toml_bool(name: &str, item: &Item) -> Result<bool> {
    item.as_bool()
        .with_context(|| format!("configure option {name} is not a bool"))
}

fn parse_toml_duration(name: &str, item: &Item) -> Result<Duration> {
    let s = item
        .as_str()
        .with_context(|| format!("configure option {name} is not a string"))?;

    Ok(humantime::parse_duration(s)?)
}

fn parse_toml_from_str<T>(name: &str, item: &Item) -> anyhow::Result<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    let v = item
        .as_str()
        .with_context(|| format!("configure option {name} is not a string"))?;
    T::from_str(v).map_err(|e| {
        anyhow!(
            "Failed to parse string as {parse_type} for configure option {name}: {e}",
            parse_type = stringify!(T)
        )
    })
}

fn deserialize_from_item<T>(name: &str, item: &Item) -> anyhow::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    // ValueDeserializer::new is not public, so use the ValueDeserializer's documented way
    let deserializer = match item.clone().into_value() {
        Ok(value) => value.into_deserializer(),
        Err(item) => anyhow::bail!("toml_edit::Item '{item}' is not a toml_edit::Value"),
    };
    T::deserialize(deserializer).with_context(|| format!("deserializing item for node {name}"))
}

/// Configurable semaphore permits setting.
///
/// Does not allow semaphore permits to be zero, because at runtime initially zero permits and empty
/// semaphore cannot be distinguished, leading any feature using these to await forever (or until
/// new permits are added).
#[derive(Debug, Clone)]
pub struct ConfigurableSemaphore {
    initial_permits: NonZeroUsize,
    inner: std::sync::Arc<tokio::sync::Semaphore>,
}

impl ConfigurableSemaphore {
    pub const DEFAULT_INITIAL: NonZeroUsize = match NonZeroUsize::new(1) {
        Some(x) => x,
        None => panic!("const unwrap is not yet stable"),
    };

    /// Initializse using a non-zero amount of permits.
    ///
    /// Require a non-zero initial permits, because using permits == 0 is a crude way to disable a
    /// feature such as [`Tenant::gather_size_inputs`]. Otherwise any semaphore using future will
    /// behave like [`futures::future::pending`], just waiting until new permits are added.
    ///
    /// [`Tenant::gather_size_inputs`]: crate::tenant::Tenant::gather_size_inputs
    pub fn new(initial_permits: NonZeroUsize) -> Self {
        ConfigurableSemaphore {
            initial_permits,
            inner: std::sync::Arc::new(tokio::sync::Semaphore::new(initial_permits.get())),
        }
    }

    /// Returns the configured amount of permits.
    pub fn initial_permits(&self) -> NonZeroUsize {
        self.initial_permits
    }
}

impl Default for ConfigurableSemaphore {
    fn default() -> Self {
        Self::new(Self::DEFAULT_INITIAL)
    }
}

impl PartialEq for ConfigurableSemaphore {
    fn eq(&self, other: &Self) -> bool {
        // the number of permits can be increased at runtime, so we cannot really fulfill the
        // PartialEq value equality otherwise
        self.initial_permits == other.initial_permits
    }
}

impl Eq for ConfigurableSemaphore {}

impl ConfigurableSemaphore {
    pub fn inner(&self) -> &std::sync::Arc<tokio::sync::Semaphore> {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, num::NonZeroU32};

    use camino_tempfile::{tempdir, Utf8TempDir};
    use pageserver_api::models::EvictionPolicy;
    use remote_storage::{RemoteStorageKind, S3Config};
    use utils::serde_percent::Percent;

    use super::*;
    use crate::DEFAULT_PG_VERSION;

    const ALL_BASE_VALUES_TOML: &str = r#"
# Initial configuration file created by 'pageserver --init'

listen_pg_addr = '127.0.0.1:64000'
listen_http_addr = '127.0.0.1:9898'

wait_lsn_timeout = '111 s'
wal_redo_timeout = '111 s'

page_cache_size = 444
max_file_descriptors = 333

# initial superuser role name to use when creating a new tenant
initial_superuser_name = 'zzzz'
id = 10

metric_collection_interval = '222 s'
metric_collection_endpoint = 'http://localhost:80/metrics'
synthetic_size_calculation_interval = '333 s'

log_format = 'json'
background_task_maximum_delay = '334 s'

"#;

    #[test]
    fn parse_defaults() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;
        let broker_endpoint = storage_broker::DEFAULT_ENDPOINT;
        // we have to create dummy values to overcome the validation errors
        let config_string = format!(
            "pg_distrib_dir='{pg_distrib_dir}'\nid=10\nbroker_endpoint = '{broker_endpoint}'",
        );
        let toml = config_string.parse()?;

        let parsed_config = PageServerConf::parse_and_validate(NodeId(10), &toml, &workdir)
            .unwrap_or_else(|e| panic!("Failed to parse config '{config_string}', reason: {e:?}"));

        assert_eq!(
            parsed_config,
            PageServerConf {
                id: NodeId(10),
                listen_pg_addr: defaults::DEFAULT_PG_LISTEN_ADDR.to_string(),
                listen_http_addr: defaults::DEFAULT_HTTP_LISTEN_ADDR.to_string(),
                availability_zone: None,
                wait_lsn_timeout: humantime::parse_duration(defaults::DEFAULT_WAIT_LSN_TIMEOUT)?,
                wal_redo_timeout: humantime::parse_duration(defaults::DEFAULT_WAL_REDO_TIMEOUT)?,
                superuser: defaults::DEFAULT_SUPERUSER.to_string(),
                page_cache_size: defaults::DEFAULT_PAGE_CACHE_SIZE,
                max_file_descriptors: defaults::DEFAULT_MAX_FILE_DESCRIPTORS,
                workdir,
                pg_distrib_dir,
                http_auth_type: AuthType::Trust,
                pg_auth_type: AuthType::Trust,
                auth_validation_public_key_path: None,
                remote_storage_config: None,
                default_tenant_conf: TenantConf::default(),
                broker_endpoint: storage_broker::DEFAULT_ENDPOINT.parse().unwrap(),
                broker_keepalive_interval: humantime::parse_duration(
                    storage_broker::DEFAULT_KEEPALIVE_INTERVAL
                )?,
                log_format: LogFormat::from_str(defaults::DEFAULT_LOG_FORMAT).unwrap(),
                concurrent_tenant_warmup: ConfigurableSemaphore::new(
                    NonZeroUsize::new(DEFAULT_CONCURRENT_TENANT_WARMUP).unwrap()
                ),
                concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore::default(),
                eviction_task_immitated_concurrent_logical_size_queries:
                    ConfigurableSemaphore::default(),
                metric_collection_interval: humantime::parse_duration(
                    defaults::DEFAULT_METRIC_COLLECTION_INTERVAL
                )?,
                metric_collection_endpoint: defaults::DEFAULT_METRIC_COLLECTION_ENDPOINT,
                metric_collection_bucket: None,
                synthetic_size_calculation_interval: humantime::parse_duration(
                    defaults::DEFAULT_SYNTHETIC_SIZE_CALCULATION_INTERVAL
                )?,
                disk_usage_based_eviction: None,
                test_remote_failures: 0,
                ondemand_download_behavior_treat_error_as_warn: false,
                background_task_maximum_delay: humantime::parse_duration(
                    defaults::DEFAULT_BACKGROUND_TASK_MAXIMUM_DELAY
                )?,
                control_plane_api: None,
                control_plane_api_token: None,
                control_plane_emergency_mode: false,
                heatmap_upload_concurrency: defaults::DEFAULT_HEATMAP_UPLOAD_CONCURRENCY,
                secondary_download_concurrency: defaults::DEFAULT_SECONDARY_DOWNLOAD_CONCURRENCY,
                ingest_batch_size: defaults::DEFAULT_INGEST_BATCH_SIZE,
                virtual_file_io_engine: DEFAULT_VIRTUAL_FILE_IO_ENGINE.parse().unwrap(),
                get_vectored_impl: defaults::DEFAULT_GET_VECTORED_IMPL.parse().unwrap(),
                get_impl: defaults::DEFAULT_GET_IMPL.parse().unwrap(),
                max_vectored_read_bytes: MaxVectoredReadBytes(
                    NonZeroUsize::new(defaults::DEFAULT_MAX_VECTORED_READ_BYTES)
                        .expect("Invalid default constant")
                ),
                validate_vectored_get: defaults::DEFAULT_VALIDATE_VECTORED_GET,
                image_compression: defaults::DEFAULT_IMAGE_COMPRESSION,
                ephemeral_bytes_per_memory_kb: defaults::DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB,
                l0_flush: L0FlushConfig::default(),
            },
            "Correct defaults should be used when no config values are provided"
        );

        Ok(())
    }

    #[test]
    fn parse_basic_config() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;
        let broker_endpoint = storage_broker::DEFAULT_ENDPOINT;

        let config_string = format!(
            "{ALL_BASE_VALUES_TOML}pg_distrib_dir='{pg_distrib_dir}'\nbroker_endpoint = '{broker_endpoint}'",
        );
        let toml = config_string.parse()?;

        let parsed_config = PageServerConf::parse_and_validate(NodeId(10), &toml, &workdir)
            .unwrap_or_else(|e| panic!("Failed to parse config '{config_string}', reason: {e:?}"));

        assert_eq!(
            parsed_config,
            PageServerConf {
                id: NodeId(10),
                listen_pg_addr: "127.0.0.1:64000".to_string(),
                listen_http_addr: "127.0.0.1:9898".to_string(),
                availability_zone: None,
                wait_lsn_timeout: Duration::from_secs(111),
                wal_redo_timeout: Duration::from_secs(111),
                superuser: "zzzz".to_string(),
                page_cache_size: 444,
                max_file_descriptors: 333,
                workdir,
                pg_distrib_dir,
                http_auth_type: AuthType::Trust,
                pg_auth_type: AuthType::Trust,
                auth_validation_public_key_path: None,
                remote_storage_config: None,
                default_tenant_conf: TenantConf::default(),
                broker_endpoint: storage_broker::DEFAULT_ENDPOINT.parse().unwrap(),
                broker_keepalive_interval: Duration::from_secs(5),
                log_format: LogFormat::Json,
                concurrent_tenant_warmup: ConfigurableSemaphore::new(
                    NonZeroUsize::new(DEFAULT_CONCURRENT_TENANT_WARMUP).unwrap()
                ),
                concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore::default(),
                eviction_task_immitated_concurrent_logical_size_queries:
                    ConfigurableSemaphore::default(),
                metric_collection_interval: Duration::from_secs(222),
                metric_collection_endpoint: Some(Url::parse("http://localhost:80/metrics")?),
                metric_collection_bucket: None,
                synthetic_size_calculation_interval: Duration::from_secs(333),
                disk_usage_based_eviction: None,
                test_remote_failures: 0,
                ondemand_download_behavior_treat_error_as_warn: false,
                background_task_maximum_delay: Duration::from_secs(334),
                control_plane_api: None,
                control_plane_api_token: None,
                control_plane_emergency_mode: false,
                heatmap_upload_concurrency: defaults::DEFAULT_HEATMAP_UPLOAD_CONCURRENCY,
                secondary_download_concurrency: defaults::DEFAULT_SECONDARY_DOWNLOAD_CONCURRENCY,
                ingest_batch_size: 100,
                virtual_file_io_engine: DEFAULT_VIRTUAL_FILE_IO_ENGINE.parse().unwrap(),
                get_vectored_impl: defaults::DEFAULT_GET_VECTORED_IMPL.parse().unwrap(),
                get_impl: defaults::DEFAULT_GET_IMPL.parse().unwrap(),
                max_vectored_read_bytes: MaxVectoredReadBytes(
                    NonZeroUsize::new(defaults::DEFAULT_MAX_VECTORED_READ_BYTES)
                        .expect("Invalid default constant")
                ),
                validate_vectored_get: defaults::DEFAULT_VALIDATE_VECTORED_GET,
                image_compression: defaults::DEFAULT_IMAGE_COMPRESSION,
                ephemeral_bytes_per_memory_kb: defaults::DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB,
                l0_flush: L0FlushConfig::default(),
            },
            "Should be able to parse all basic config values correctly"
        );

        Ok(())
    }

    #[test]
    fn parse_remote_fs_storage_config() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;
        let broker_endpoint = "http://127.0.0.1:7777";

        let local_storage_path = tempdir.path().join("local_remote_storage");

        let identical_toml_declarations = &[
            format!(
                r#"[remote_storage]
local_path = '{local_storage_path}'"#,
            ),
            format!("remote_storage={{local_path='{local_storage_path}'}}"),
        ];

        for remote_storage_config_str in identical_toml_declarations {
            let config_string = format!(
                r#"{ALL_BASE_VALUES_TOML}
pg_distrib_dir='{pg_distrib_dir}'
broker_endpoint = '{broker_endpoint}'

{remote_storage_config_str}"#,
            );

            let toml = config_string.parse()?;

            let parsed_remote_storage_config =
                PageServerConf::parse_and_validate(NodeId(10), &toml, &workdir)
                    .unwrap_or_else(|e| {
                        panic!("Failed to parse config '{config_string}', reason: {e:?}")
                    })
                    .remote_storage_config
                    .expect("Should have remote storage config for the local FS");

            assert_eq!(
                parsed_remote_storage_config,
                RemoteStorageConfig {
                    storage: RemoteStorageKind::LocalFs { local_path: local_storage_path.clone() },
                    timeout: RemoteStorageConfig::DEFAULT_TIMEOUT,
                },
                "Remote storage config should correctly parse the local FS config and fill other storage defaults"
            );
        }
        Ok(())
    }

    #[test]
    fn parse_remote_s3_storage_config() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;

        let bucket_name = "some-sample-bucket".to_string();
        let bucket_region = "eu-north-1".to_string();
        let prefix_in_bucket = "test_prefix".to_string();
        let endpoint = "http://localhost:5000".to_string();
        let max_concurrent_syncs = NonZeroUsize::new(111).unwrap();
        let max_sync_errors = NonZeroU32::new(222).unwrap();
        let s3_concurrency_limit = NonZeroUsize::new(333).unwrap();
        let broker_endpoint = "http://127.0.0.1:7777";

        let identical_toml_declarations = &[
            format!(
                r#"[remote_storage]
max_concurrent_syncs = {max_concurrent_syncs}
max_sync_errors = {max_sync_errors}
bucket_name = '{bucket_name}'
bucket_region = '{bucket_region}'
prefix_in_bucket = '{prefix_in_bucket}'
endpoint = '{endpoint}'
concurrency_limit = {s3_concurrency_limit}"#
            ),
            format!(
                "remote_storage={{max_concurrent_syncs={max_concurrent_syncs}, max_sync_errors={max_sync_errors}, bucket_name='{bucket_name}',\
                bucket_region='{bucket_region}', prefix_in_bucket='{prefix_in_bucket}', endpoint='{endpoint}', concurrency_limit={s3_concurrency_limit}}}",
            ),
        ];

        for remote_storage_config_str in identical_toml_declarations {
            let config_string = format!(
                r#"{ALL_BASE_VALUES_TOML}
pg_distrib_dir='{pg_distrib_dir}'
broker_endpoint = '{broker_endpoint}'

{remote_storage_config_str}"#,
            );

            let toml = config_string.parse()?;

            let parsed_remote_storage_config =
                PageServerConf::parse_and_validate(NodeId(10), &toml, &workdir)
                    .unwrap_or_else(|e| {
                        panic!("Failed to parse config '{config_string}', reason: {e:?}")
                    })
                    .remote_storage_config
                    .expect("Should have remote storage config for S3");

            assert_eq!(
                parsed_remote_storage_config,
                RemoteStorageConfig {
                    storage: RemoteStorageKind::AwsS3(S3Config {
                        bucket_name: bucket_name.clone(),
                        bucket_region: bucket_region.clone(),
                        prefix_in_bucket: Some(prefix_in_bucket.clone()),
                        endpoint: Some(endpoint.clone()),
                        concurrency_limit: s3_concurrency_limit,
                        max_keys_per_list_response: None,
                        upload_storage_class: None,
                    }),
                    timeout: RemoteStorageConfig::DEFAULT_TIMEOUT,
                },
                "Remote storage config should correctly parse the S3 config"
            );
        }
        Ok(())
    }

    #[test]
    fn parse_incorrect_tenant_config() -> anyhow::Result<()> {
        let config_string = r#"
            [tenant_config]
            checkpoint_distance = -1 # supposed to be an u64
        "#
        .to_string();

        let toml: Document = config_string.parse()?;
        let item = toml.get("tenant_config").unwrap();
        let error = TenantConfOpt::try_from(item.to_owned()).unwrap_err();

        let expected_error_str = "checkpoint_distance: invalid value: integer `-1`, expected u64";
        assert_eq!(error.to_string(), expected_error_str);

        Ok(())
    }

    #[test]
    fn parse_override_tenant_config() -> anyhow::Result<()> {
        let config_string = r#"tenant_config={ min_resident_size_override =  400 }"#.to_string();

        let toml: Document = config_string.parse()?;
        let item = toml.get("tenant_config").unwrap();
        let conf = TenantConfOpt::try_from(item.to_owned()).unwrap();

        assert_eq!(conf.min_resident_size_override, Some(400));

        Ok(())
    }

    #[test]
    fn eviction_pageserver_config_parse() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;

        let pageserver_conf_toml = format!(
            r#"pg_distrib_dir = "{pg_distrib_dir}"
metric_collection_endpoint = "http://sample.url"
metric_collection_interval = "10min"

[disk_usage_based_eviction]
max_usage_pct = 80
min_avail_bytes = 0
period = "10s"

[tenant_config]
evictions_low_residence_duration_metric_threshold = "20m"

[tenant_config.eviction_policy]
kind = "LayerAccessThreshold"
period = "20m"
threshold = "20m"
"#,
        );
        let toml: Document = pageserver_conf_toml.parse()?;
        let conf = PageServerConf::parse_and_validate(NodeId(333), &toml, &workdir)?;

        assert_eq!(conf.pg_distrib_dir, pg_distrib_dir);
        assert_eq!(
            conf.metric_collection_endpoint,
            Some("http://sample.url".parse().unwrap())
        );
        assert_eq!(
            conf.metric_collection_interval,
            Duration::from_secs(10 * 60)
        );
        assert_eq!(
            conf.default_tenant_conf
                .evictions_low_residence_duration_metric_threshold,
            Duration::from_secs(20 * 60)
        );

        // Assert that the node id provided by the indentity file (threaded
        // through the call to [`PageServerConf::parse_and_validate`] is
        // used.
        assert_eq!(conf.id, NodeId(333));
        assert_eq!(
            conf.disk_usage_based_eviction,
            Some(DiskUsageEvictionTaskConfig {
                max_usage_pct: Percent::new(80).unwrap(),
                min_avail_bytes: 0,
                period: Duration::from_secs(10),
                #[cfg(feature = "testing")]
                mock_statvfs: None,
                eviction_order: Default::default(),
            })
        );

        match &conf.default_tenant_conf.eviction_policy {
            EvictionPolicy::LayerAccessThreshold(eviction_threshold) => {
                assert_eq!(eviction_threshold.period, Duration::from_secs(20 * 60));
                assert_eq!(eviction_threshold.threshold, Duration::from_secs(20 * 60));
            }
            other => unreachable!("Unexpected eviction policy tenant settings: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn parse_imitation_only_pageserver_config() {
        let tempdir = tempdir().unwrap();
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir).unwrap();

        let pageserver_conf_toml = format!(
            r#"pg_distrib_dir = "{pg_distrib_dir}"
metric_collection_endpoint = "http://sample.url"
metric_collection_interval = "10min"

[tenant_config]
evictions_low_residence_duration_metric_threshold = "20m"

[tenant_config.eviction_policy]
kind = "OnlyImitiate"
period = "20m"
threshold = "20m"
"#,
        );
        let toml: Document = pageserver_conf_toml.parse().unwrap();
        let conf = PageServerConf::parse_and_validate(NodeId(222), &toml, &workdir).unwrap();

        match &conf.default_tenant_conf.eviction_policy {
            EvictionPolicy::OnlyImitiate(t) => {
                assert_eq!(t.period, Duration::from_secs(20 * 60));
                assert_eq!(t.threshold, Duration::from_secs(20 * 60));
            }
            other => unreachable!("Unexpected eviction policy tenant settings: {other:?}"),
        }
    }

    #[test]
    fn empty_remote_storage_is_error() {
        let tempdir = tempdir().unwrap();
        let (workdir, _) = prepare_fs(&tempdir).unwrap();
        let input = r#"
remote_storage = {}
        "#;
        let doc = toml_edit::Document::from_str(input).unwrap();
        let err = PageServerConf::parse_and_validate(NodeId(222), &doc, &workdir)
            .expect_err("empty remote_storage field should fail, don't specify it if you want no remote_storage");
        assert!(format!("{err}").contains("remote_storage"), "{err}");
    }

    fn prepare_fs(tempdir: &Utf8TempDir) -> anyhow::Result<(Utf8PathBuf, Utf8PathBuf)> {
        let tempdir_path = tempdir.path();

        let workdir = tempdir_path.join("workdir");
        fs::create_dir_all(&workdir)?;

        let pg_distrib_dir = tempdir_path.join("pg_distrib");
        let pg_distrib_dir_versioned = pg_distrib_dir.join(format!("v{DEFAULT_PG_VERSION}"));
        fs::create_dir_all(&pg_distrib_dir_versioned)?;
        let postgres_bin_dir = pg_distrib_dir_versioned.join("bin");
        fs::create_dir_all(&postgres_bin_dir)?;
        fs::write(postgres_bin_dir.join("postgres"), "I'm postgres, trust me")?;

        Ok((workdir, pg_distrib_dir))
    }
}
