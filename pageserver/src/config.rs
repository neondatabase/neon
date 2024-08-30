//! Functions for handling page server configuration options
//!
//! Configuration options can be set in the pageserver.toml configuration
//! file, or on the command line.
//! See also `settings.md` for better description on every parameter.

use anyhow::{bail, ensure, Context};
use pageserver_api::models::ImageCompressionAlgorithm;
use pageserver_api::{
    config::{DiskUsageEvictionTaskConfig, MaxVectoredReadBytes},
    shard::TenantShardId,
};
use remote_storage::{RemotePath, RemoteStorageConfig};
use std::env;
use storage_broker::Uri;
use utils::crashsafe::path_with_suffix_extension;
use utils::logging::SecretString;

use once_cell::sync::OnceCell;
use reqwest::Url;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use camino::{Utf8Path, Utf8PathBuf};
use postgres_backend::AuthType;
use utils::{
    id::{NodeId, TimelineId},
    logging::LogFormat,
};

use crate::tenant::{TENANTS_SEGMENT_NAME, TIMELINES_SEGMENT_NAME};
use crate::virtual_file;
use crate::virtual_file::io_engine;
use crate::{TENANT_HEATMAP_BASENAME, TENANT_LOCATION_CONFIG_NAME, TIMELINE_DELETE_MARK_SUFFIX};

pub mod defaults {
    // TODO(christian): this can be removed after https://github.com/neondatabase/aws/pull/1322
    pub const DEFAULT_CONFIG_FILE: &str = "";

    #[cfg(test)]
    fn test_default_config_file_var_desers_to_default_impl() {
        use super::PageServerConf;

        let doc = toml_edit::de::from_str(DEFAULT_CONFIG_FILE).unwrap();
        let conf = PageServerConf::parse_and_validate(doc, std::env::current_dir().unwrap());
        assert_eq!(conf, pageserver_api::config::ConfigToml::default());
    }
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

    pub default_tenant_conf: crate::tenant::config::TenantConf,

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

    pub max_vectored_read_bytes: MaxVectoredReadBytes,

    pub image_compression: ImageCompressionAlgorithm,

    /// How many bytes of ephemeral layer content will we allow per kilobyte of RAM.  When this
    /// is exceeded, we start proactively closing ephemeral layers to limit the total amount
    /// of ephemeral data.
    ///
    /// Setting this to zero disables limits on total ephemeral layer size.
    pub ephemeral_bytes_per_memory_kb: usize,

    pub l0_flush: crate::l0_flush::L0FlushConfig,

    /// This flag is temporary and will be removed after gradual rollout.
    /// See <https://github.com/neondatabase/neon/issues/8184>.
    pub compact_level0_phase1_value_access: pageserver_api::config::CompactL0Phase1ValueAccess,

    /// Direct IO settings
    pub virtual_file_direct_io: virtual_file::DirectIoMode,
}

/// We do not want to store this in a PageServerConf because the latter may be logged
/// and/or serialized at a whim, while the token is secret. Currently this token is the
/// same for accessing all tenants/timelines, but may become per-tenant/per-timeline in
/// the future, more tokens and auth may arrive for storage broker, completely changing the logic.
/// Hence, we resort to a global variable for now instead of passing the token from the
/// startup code to the connection code through a dozen layers.
pub static SAFEKEEPER_AUTH_TOKEN: OnceCell<Arc<String>> = OnceCell::new();

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
        id: NodeId,
        config_toml: pageserver_api::config::ConfigToml,
        workdir: &Utf8Path,
    ) -> anyhow::Result<Self> {
        let pageserver_api::config::ConfigToml {
            listen_pg_addr,
            listen_http_addr,
            availability_zone,
            wait_lsn_timeout,
            wal_redo_timeout,
            superuser,
            page_cache_size,
            max_file_descriptors,
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
            max_vectored_read_bytes,
            image_compression,
            ephemeral_bytes_per_memory_kb,
            compact_level0_phase1_value_access,
            l0_flush,
            virtual_file_direct_io,
            concurrent_tenant_warmup,
            concurrent_tenant_size_logical_size_queries,
            virtual_file_io_engine,
            tenant_config,
        } = config_toml;

        let mut conf = PageServerConf {
            // ------------------------------------------------------------
            // fields that are already fully validated by the ConfigToml Deserialize impl
            // ------------------------------------------------------------
            listen_pg_addr,
            listen_http_addr,
            availability_zone,
            wait_lsn_timeout,
            wal_redo_timeout,
            superuser,
            page_cache_size,
            max_file_descriptors,
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
            control_plane_emergency_mode,
            heatmap_upload_concurrency,
            secondary_download_concurrency,
            ingest_batch_size,
            max_vectored_read_bytes,
            image_compression,
            ephemeral_bytes_per_memory_kb,
            compact_level0_phase1_value_access,
            virtual_file_direct_io,

            // ------------------------------------------------------------
            // fields that require additional validation or custom handling
            // ------------------------------------------------------------
            workdir: workdir.to_owned(),
            pg_distrib_dir: pg_distrib_dir.unwrap_or_else(|| {
                std::env::current_dir()
                    .expect("current_dir() failed")
                    .try_into()
                    .expect("current_dir() is not a valid Utf8Path")
            }),
            control_plane_api_token: control_plane_api_token.map(SecretString::from),
            id,
            default_tenant_conf: tenant_config,
            concurrent_tenant_warmup: ConfigurableSemaphore::new(concurrent_tenant_warmup),
            concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore::new(
                concurrent_tenant_size_logical_size_queries,
            ),
            eviction_task_immitated_concurrent_logical_size_queries: ConfigurableSemaphore::new(
                // re-use `concurrent_tenant_size_logical_size_queries`
                concurrent_tenant_size_logical_size_queries,
            ),
            virtual_file_io_engine: match virtual_file_io_engine {
                Some(v) => v,
                None => match crate::virtual_file::io_engine_feature_test()
                    .context("auto-detect virtual_file_io_engine")?
                {
                    io_engine::FeatureTestResult::PlatformPreferred(v) => v, // make no noise
                    io_engine::FeatureTestResult::Worse { engine, remark } => {
                        // TODO: bubble this up to the caller so we can tracing::warn! it.
                        eprintln!("auto-detected IO engine is not platform-preferred: engine={engine:?} remark={remark:?}");
                        engine
                    }
                },
            },
            l0_flush: l0_flush
                .map(crate::l0_flush::L0FlushConfig::from)
                .unwrap_or_default(),
        };

        // ------------------------------------------------------------
        // custom validation code that covers more than one field in isolation
        // ------------------------------------------------------------

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

        Ok(conf)
    }

    #[cfg(test)]
    pub fn test_repo_dir(test_name: &str) -> Utf8PathBuf {
        let test_output_dir = std::env::var("TEST_OUTPUT").unwrap_or("../tmp_check".into());
        Utf8PathBuf::from(format!("{test_output_dir}/test_{test_name}"))
    }

    pub fn dummy_conf(repo_dir: Utf8PathBuf) -> Self {
        let pg_distrib_dir = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../pg_install");

        let config_toml = pageserver_api::config::ConfigToml {
            wait_lsn_timeout: Duration::from_secs(60),
            wal_redo_timeout: Duration::from_secs(60),
            pg_distrib_dir: Some(pg_distrib_dir),
            metric_collection_interval: Duration::from_secs(60),
            synthetic_size_calculation_interval: Duration::from_secs(60),
            background_task_maximum_delay: Duration::ZERO,
            ..Default::default()
        };
        PageServerConf::parse_and_validate(NodeId(0), config_toml, &repo_dir).unwrap()
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PageserverIdentity {
    pub id: NodeId,
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
        let config_string =
            format!("pg_distrib_dir='{pg_distrib_dir}'\nbroker_endpoint = '{broker_endpoint}'",);
        let toml: pageserver_api::config::ConfigToml = toml_edit::de::from_str(&config_string)?;

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
                    NonZeroUsize::new(defaults::DEFAULT_CONCURRENT_TENANT_WARMUP).unwrap()
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
                virtual_file_io_engine: None,
                max_vectored_read_bytes: MaxVectoredReadBytes(
                    NonZeroUsize::new(defaults::DEFAULT_MAX_VECTORED_READ_BYTES)
                        .expect("Invalid default constant")
                ),
                image_compression: defaults::DEFAULT_IMAGE_COMPRESSION,
                ephemeral_bytes_per_memory_kb: defaults::DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB,
                l0_flush: L0FlushConfig::default(),
                compact_level0_phase1_value_access: CompactL0Phase1ValueAccess::default(),
                virtual_file_direct_io: virtual_file::DirectIoMode::default(),
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
                    NonZeroUsize::new(defaults::DEFAULT_CONCURRENT_TENANT_WARMUP).unwrap()
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
                virtual_file_io_engine: None,
                max_vectored_read_bytes: MaxVectoredReadBytes(
                    NonZeroUsize::new(defaults::DEFAULT_MAX_VECTORED_READ_BYTES)
                        .expect("Invalid default constant")
                ),
                image_compression: defaults::DEFAULT_IMAGE_COMPRESSION,
                ephemeral_bytes_per_memory_kb: defaults::DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB,
                l0_flush: L0FlushConfig::default(),
                compact_level0_phase1_value_access: CompactL0Phase1ValueAccess::default(),
                virtual_file_direct_io: virtual_file::DirectIoMode::default(),
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
