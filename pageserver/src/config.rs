//! Functions for handling page server configuration options
//!
//! Configuration options can be set in the pageserver.toml configuration
//! file, or on the command line.
//! See also `settings.md` for better description on every parameter.

pub mod ignored_fields;

use std::env;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, ensure};
use camino::{Utf8Path, Utf8PathBuf};
use once_cell::sync::OnceCell;
use pageserver_api::config::{
    DiskUsageEvictionTaskConfig, MaxGetVectoredKeys, MaxVectoredReadBytes,
    PageServicePipeliningConfig, PageServicePipeliningConfigPipelined, PostHogConfig,
};
use pageserver_api::models::ImageCompressionAlgorithm;
use pageserver_api::shard::TenantShardId;
use pem::Pem;
use postgres_backend::AuthType;
use postgres_ffi::PgMajorVersion;
use remote_storage::{RemotePath, RemoteStorageConfig};
use reqwest::Url;
use storage_broker::Uri;
use utils::id::{NodeId, TimelineId};
use utils::logging::{LogFormat, SecretString};

use crate::tenant::storage_layer::inmemory_layer::IndexEntry;
use crate::tenant::{TENANTS_SEGMENT_NAME, TIMELINES_SEGMENT_NAME};
use crate::virtual_file::io_engine;
use crate::{TENANT_HEATMAP_BASENAME, TENANT_LOCATION_CONFIG_NAME, virtual_file};

/// Global state of pageserver.
///
/// It's mostly immutable configuration, but some semaphores and the
/// like crept in over time and the name stuck.
///
/// Instantiated by deserializing `pageserver.toml` into  [`pageserver_api::config::ConfigToml`]
/// and passing that to [`PageServerConf::parse_and_validate`].
///
/// # Adding a New Field
///
/// 1. Add the field to `pageserver_api::config::ConfigToml`.
/// 2. Fix compiler errors (exhaustive destructuring will guide you).
///
/// For fields that require additional validation or filling in of defaults at runtime,
/// check for examples in the [`PageServerConf::parse_and_validate`] method.
#[derive(Debug, Clone)]
pub struct PageServerConf {
    // Identifier of that particular pageserver so e g safekeepers
    // can safely distinguish different pageservers
    pub id: NodeId,

    /// Example (default): 127.0.0.1:64000
    pub listen_pg_addr: String,
    /// Example (default): 127.0.0.1:9898
    pub listen_http_addr: String,
    /// Example: 127.0.0.1:9899
    pub listen_https_addr: Option<String>,
    /// If set, expose a gRPC API on this address.
    /// Example: 127.0.0.1:51051
    ///
    /// EXPERIMENTAL: this protocol is unstable and under active development.
    pub listen_grpc_addr: Option<String>,

    /// Path to a file with certificate's private key for https and gRPC API.
    /// Default: server.key
    pub ssl_key_file: Utf8PathBuf,
    /// Path to a file with a X509 certificate for https and gRPC API.
    /// Default: server.crt
    pub ssl_cert_file: Utf8PathBuf,
    /// Period to reload certificate and private key from files.
    /// Default: 60s.
    pub ssl_cert_reload_period: Duration,
    /// Trusted root CA certificates to use in https APIs in PEM format.
    pub ssl_ca_certs: Vec<Pem>,

    /// Current availability zone. Used for traffic metrics.
    pub availability_zone: Option<String>,

    // Timeout when waiting for WAL receiver to catch up to an LSN given in a GetPage@LSN call.
    pub wait_lsn_timeout: Duration,
    // How long to wait for WAL redo to complete.
    pub wal_redo_timeout: Duration,

    pub superuser: String,
    pub locale: String,

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
    /// authentication method for gRPC connections from compute
    pub grpc_auth_type: AuthType,
    /// Path to a file or directory containing public key(s) for verifying JWT tokens.
    /// Used for both mgmt and compute auth, if enabled.
    pub auth_validation_public_key_path: Option<Utf8PathBuf>,

    pub remote_storage_config: Option<RemoteStorageConfig>,

    pub default_tenant_conf: pageserver_api::config::TenantConfigToml,

    /// Storage broker endpoints to connect to.
    pub broker_endpoint: Uri,
    pub broker_keepalive_interval: Duration,

    pub log_format: LogFormat,

    /// Number of tenants which will be concurrently loaded from remote storage proactively on startup or attach.
    ///
    /// A lower value implicitly deprioritizes loading such tenants, vs. other work in the system.
    pub concurrent_tenant_warmup: ConfigurableSemaphore,

    /// Number of concurrent [`TenantShard::gather_size_inputs`](crate::tenant::TenantShard::gather_size_inputs) allowed.
    pub concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore,
    /// Limit of concurrent [`TenantShard::gather_size_inputs`] issued by module `eviction_task`.
    /// The number of permits is the same as `concurrent_tenant_size_logical_size_queries`.
    /// See the comment in `eviction_task` for details.
    ///
    /// [`TenantShard::gather_size_inputs`]: crate::tenant::TenantShard::gather_size_inputs
    pub eviction_task_immitated_concurrent_logical_size_queries: ConfigurableSemaphore,

    // How often to collect metrics and send them to the metrics endpoint.
    pub metric_collection_interval: Duration,
    // How often to send unchanged cached metrics to the metrics endpoint.
    pub metric_collection_endpoint: Option<Url>,
    pub metric_collection_bucket: Option<RemoteStorageConfig>,
    pub synthetic_size_calculation_interval: Duration,

    pub disk_usage_based_eviction: DiskUsageEvictionTaskConfig,

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

    pub control_plane_api: Url,

    /// JWT token for use with the control plane API.
    pub control_plane_api_token: Option<SecretString>,

    pub import_pgdata_upcall_api: Option<Url>,
    pub import_pgdata_upcall_api_token: Option<SecretString>,
    pub import_pgdata_aws_endpoint_url: Option<Url>,

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

    /// Maximum number of keys to be read in a single get_vectored call.
    pub max_get_vectored_keys: MaxGetVectoredKeys,

    pub image_compression: ImageCompressionAlgorithm,

    /// Whether to offload archived timelines automatically
    pub timeline_offloading: bool,

    /// How many bytes of ephemeral layer content will we allow per kilobyte of RAM.  When this
    /// is exceeded, we start proactively closing ephemeral layers to limit the total amount
    /// of ephemeral data.
    ///
    /// Setting this to zero disables limits on total ephemeral layer size.
    pub ephemeral_bytes_per_memory_kb: usize,

    pub l0_flush: crate::l0_flush::L0FlushConfig,

    /// Direct IO settings
    pub virtual_file_io_mode: virtual_file::IoMode,

    /// Optionally disable disk syncs (unsafe!)
    pub no_sync: bool,

    pub page_service_pipelining: pageserver_api::config::PageServicePipeliningConfig,

    pub get_vectored_concurrent_io: pageserver_api::config::GetVectoredConcurrentIo,

    /// Enable read path debugging. If enabled, read key errors will print a backtrace of the layer
    /// files read.
    pub enable_read_path_debugging: bool,

    /// Interpreted protocol feature: if enabled, validate that the logical WAL received from
    /// safekeepers does not have gaps.
    pub validate_wal_contiguity: bool,

    /// When set, the previously written to disk heatmap is loaded on tenant attach and used
    /// to avoid clobbering the heatmap from new, cold, attached locations.
    pub load_previous_heatmap: bool,

    /// When set, include visible layers in the next uploaded heatmaps of an unarchived timeline.
    pub generate_unarchival_heatmap: bool,

    pub tracing: Option<pageserver_api::config::Tracing>,

    /// Enable TLS in page service API.
    /// Does not force TLS: the client negotiates TLS usage during the handshake.
    /// Uses key and certificate from ssl_key_file/ssl_cert_file.
    pub enable_tls_page_service_api: bool,

    /// Run in development mode, which disables certain safety checks
    /// such as authentication requirements for HTTP and PostgreSQL APIs.
    /// This is insecure and should only be used in development environments.
    pub dev_mode: bool,

    /// PostHog integration config.
    pub posthog_config: Option<PostHogConfig>,

    pub timeline_import_config: pageserver_api::config::TimelineImportConfig,

    pub basebackup_cache_config: Option<pageserver_api::config::BasebackupCacheConfig>,
}

/// Token for authentication to safekeepers
///
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

    pub fn basebackup_cache_dir(&self) -> Utf8PathBuf {
        self.workdir.join("basebackup_cache")
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

    /// Turns storage remote path of a file into its local path.
    pub fn local_path(&self, remote_path: &RemotePath) -> Utf8PathBuf {
        remote_path.with_base(&self.workdir)
    }

    //
    // Postgres distribution paths
    //
    pub fn pg_distrib_dir(&self, pg_version: PgMajorVersion) -> anyhow::Result<Utf8PathBuf> {
        let path = self.pg_distrib_dir.clone();

        Ok(path.join(pg_version.v_str()))
    }

    pub fn pg_bin_dir(&self, pg_version: PgMajorVersion) -> anyhow::Result<Utf8PathBuf> {
        Ok(self.pg_distrib_dir(pg_version)?.join("bin"))
    }
    pub fn pg_lib_dir(&self, pg_version: PgMajorVersion) -> anyhow::Result<Utf8PathBuf> {
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
            listen_https_addr,
            listen_grpc_addr,
            ssl_key_file,
            ssl_cert_file,
            ssl_cert_reload_period,
            ssl_ca_file,
            availability_zone,
            wait_lsn_timeout,
            wal_redo_timeout,
            superuser,
            locale,
            page_cache_size,
            max_file_descriptors,
            pg_distrib_dir,
            http_auth_type,
            pg_auth_type,
            grpc_auth_type,
            auth_validation_public_key_path,
            remote_storage,
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
            import_pgdata_upcall_api,
            import_pgdata_upcall_api_token,
            import_pgdata_aws_endpoint_url,
            heatmap_upload_concurrency,
            secondary_download_concurrency,
            ingest_batch_size,
            max_vectored_read_bytes,
            max_get_vectored_keys,
            image_compression,
            timeline_offloading,
            ephemeral_bytes_per_memory_kb,
            l0_flush,
            virtual_file_io_mode,
            concurrent_tenant_warmup,
            concurrent_tenant_size_logical_size_queries,
            virtual_file_io_engine,
            tenant_config,
            no_sync,
            page_service_pipelining,
            get_vectored_concurrent_io,
            enable_read_path_debugging,
            validate_wal_contiguity,
            load_previous_heatmap,
            generate_unarchival_heatmap,
            tracing,
            enable_tls_page_service_api,
            dev_mode,
            posthog_config,
            timeline_import_config,
            basebackup_cache_config,
        } = config_toml;

        let mut conf = PageServerConf {
            // ------------------------------------------------------------
            // fields that are already fully validated by the ConfigToml Deserialize impl
            // ------------------------------------------------------------
            listen_pg_addr,
            listen_http_addr,
            listen_https_addr,
            listen_grpc_addr,
            ssl_key_file,
            ssl_cert_file,
            ssl_cert_reload_period,
            availability_zone,
            wait_lsn_timeout,
            wal_redo_timeout,
            superuser,
            locale,
            page_cache_size,
            max_file_descriptors,
            http_auth_type,
            pg_auth_type,
            grpc_auth_type,
            auth_validation_public_key_path,
            remote_storage_config: remote_storage,
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
            control_plane_api: control_plane_api
                .ok_or_else(|| anyhow::anyhow!("`control_plane_api` must be set"))?,
            control_plane_emergency_mode,
            heatmap_upload_concurrency,
            secondary_download_concurrency,
            ingest_batch_size,
            max_vectored_read_bytes,
            max_get_vectored_keys,
            image_compression,
            timeline_offloading,
            ephemeral_bytes_per_memory_kb,
            import_pgdata_upcall_api,
            import_pgdata_upcall_api_token: import_pgdata_upcall_api_token.map(SecretString::from),
            import_pgdata_aws_endpoint_url,
            page_service_pipelining,
            get_vectored_concurrent_io,
            tracing,
            enable_tls_page_service_api,
            dev_mode,
            timeline_import_config,
            basebackup_cache_config,

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
                        eprintln!(
                            "auto-detected IO engine is not platform-preferred: engine={engine:?} remark={remark:?}"
                        );
                        engine
                    }
                },
            },
            l0_flush: l0_flush
                .map(crate::l0_flush::L0FlushConfig::from)
                .unwrap_or_default(),
            virtual_file_io_mode: virtual_file_io_mode.unwrap_or(virtual_file::IoMode::preferred()),
            no_sync: no_sync.unwrap_or(false),
            enable_read_path_debugging: enable_read_path_debugging.unwrap_or(false),
            validate_wal_contiguity: validate_wal_contiguity.unwrap_or(false),
            load_previous_heatmap: load_previous_heatmap.unwrap_or(true),
            generate_unarchival_heatmap: generate_unarchival_heatmap.unwrap_or(true),
            ssl_ca_certs: match ssl_ca_file {
                Some(ssl_ca_file) => {
                    let buf = std::fs::read(ssl_ca_file)?;
                    pem::parse_many(&buf)?
                        .into_iter()
                        .filter(|pem| pem.tag() == "CERTIFICATE")
                        .collect()
                }
                None => Vec::new(),
            },
            posthog_config,
        };

        // ------------------------------------------------------------
        // custom validation code that covers more than one field in isolation
        // ------------------------------------------------------------

        if [conf.http_auth_type, conf.pg_auth_type, conf.grpc_auth_type]
            .contains(&AuthType::NeonJWT)
        {
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

        if let Some(tracing_config) = conf.tracing.as_ref() {
            let ratio = &tracing_config.sampling_ratio;
            ensure!(
                ratio.denominator != 0 && ratio.denominator >= ratio.numerator,
                format!(
                    "Invalid sampling ratio: {}/{}",
                    ratio.numerator, ratio.denominator
                )
            );

            let url = Url::parse(&tracing_config.export_config.endpoint)
                .map_err(anyhow::Error::msg)
                .with_context(|| {
                    format!(
                        "tracing endpoint URL is invalid : {}",
                        tracing_config.export_config.endpoint
                    )
                })?;

            ensure!(
                url.scheme() == "http" || url.scheme() == "https",
                format!(
                    "tracing endpoint URL must start with http:// or https://: {}",
                    tracing_config.export_config.endpoint
                )
            );
        }

        IndexEntry::validate_checkpoint_distance(conf.default_tenant_conf.checkpoint_distance)
            .map_err(anyhow::Error::msg)
            .with_context(|| {
                format!(
                    "effective checkpoint distance is unsupported: {}",
                    conf.default_tenant_conf.checkpoint_distance
                )
            })?;

        if let PageServicePipeliningConfig::Pipelined(PageServicePipeliningConfigPipelined {
            max_batch_size,
            ..
        }) = conf.page_service_pipelining
        {
            if max_batch_size.get() > conf.max_get_vectored_keys.get() {
                return Err(anyhow::anyhow!(
                    "`max_batch_size` ({max_batch_size}) must be less than or equal to `max_get_vectored_keys` ({})",
                    conf.max_get_vectored_keys.get()
                ));
            }
        };

        Ok(conf)
    }

    #[cfg(test)]
    pub fn test_repo_dir(test_name: &str) -> Utf8PathBuf {
        let test_output_dir = std::env::var("TEST_OUTPUT").unwrap_or("../tmp_check".into());

        let test_id = uuid::Uuid::new_v4();
        Utf8PathBuf::from(format!("{test_output_dir}/test_{test_name}_{test_id}"))
    }

    pub fn dummy_conf(repo_dir: Utf8PathBuf) -> Self {
        let pg_distrib_dir = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../pg_install");

        let mut config_toml = pageserver_api::config::ConfigToml {
            wait_lsn_timeout: Duration::from_secs(60),
            wal_redo_timeout: Duration::from_secs(60),
            pg_distrib_dir: Some(pg_distrib_dir),
            metric_collection_interval: Duration::from_secs(60),
            synthetic_size_calculation_interval: Duration::from_secs(60),
            background_task_maximum_delay: Duration::ZERO,
            load_previous_heatmap: Some(true),
            generate_unarchival_heatmap: Some(true),
            control_plane_api: Some(Url::parse("http://localhost:6666").unwrap()),
            ..Default::default()
        };

        // Test authors tend to forget about the default 10min initial lease deadline
        // when writing tests, which turns their immediate gc requests via mgmt API
        // into no-ops. Override the binary default here, such that there is no initial
        // lease deadline by default in tests. Tests that care can always override it
        // themselves.
        // Cf https://databricks.atlassian.net/browse/LKB-92?focusedCommentId=6722329
        config_toml.tenant_config.lsn_lease_length = Duration::from_secs(0);

        PageServerConf::parse_and_validate(NodeId(0), config_toml, &repo_dir).unwrap()
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
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
    /// Initializse using a non-zero amount of permits.
    ///
    /// Require a non-zero initial permits, because using permits == 0 is a crude way to disable a
    /// feature such as [`TenantShard::gather_size_inputs`]. Otherwise any semaphore using future will
    /// behave like [`futures::future::pending`], just waiting until new permits are added.
    ///
    /// [`TenantShard::gather_size_inputs`]: crate::tenant::TenantShard::gather_size_inputs
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

    use std::time::Duration;

    use camino::Utf8PathBuf;
    use pageserver_api::config::{DiskUsageEvictionTaskConfig, EvictionOrder};
    use rstest::rstest;
    use utils::{id::NodeId, serde_percent::Percent};

    use super::PageServerConf;

    #[test]
    fn test_minimal_config_toml_is_valid() {
        // The minimal valid config for running a pageserver:
        // - control_plane_api is mandatory, as pageservers cannot run in isolation
        // - we use Default impl of everything else in this situation
        let input = r#"
            control_plane_api = "http://localhost:6666"
        "#;
        let config_toml = toml_edit::de::from_str::<pageserver_api::config::ConfigToml>(input)
            .expect("empty config is valid");
        let workdir = Utf8PathBuf::from("/nonexistent");
        PageServerConf::parse_and_validate(NodeId(0), config_toml, &workdir)
            .expect("parse_and_validate");
    }

    #[test]
    fn test_config_tracing_endpoint_is_invalid() {
        let input = r#"
            control_plane_api = "http://localhost:6666"

            [tracing]

            sampling_ratio = { numerator = 1, denominator = 0 }

            [tracing.export_config]
            endpoint = "localhost:4317"
            protocol = "http-binary"
            timeout = "1ms"
        "#;
        let config_toml = toml_edit::de::from_str::<pageserver_api::config::ConfigToml>(input)
            .expect("config has valid fields");
        let workdir = Utf8PathBuf::from("/nonexistent");
        PageServerConf::parse_and_validate(NodeId(0), config_toml, &workdir)
            .expect_err("parse_and_validate should fail for endpoint without scheme");
    }

    #[rstest]
    #[case(32, 32, true)]
    #[case(64, 32, false)]
    #[case(64, 64, true)]
    #[case(128, 128, true)]
    fn test_config_max_batch_size_is_valid(
        #[case] max_batch_size: usize,
        #[case] max_get_vectored_keys: usize,
        #[case] is_valid: bool,
    ) {
        let input = format!(
            r#"
            control_plane_api = "http://localhost:6666"
            max_get_vectored_keys = {max_get_vectored_keys}
            page_service_pipelining = {{ mode="pipelined", execution="concurrent-futures", max_batch_size={max_batch_size}, batching="uniform-lsn" }}
        "#,
        );
        let config_toml = toml_edit::de::from_str::<pageserver_api::config::ConfigToml>(&input)
            .expect("config has valid fields");
        let workdir = Utf8PathBuf::from("/nonexistent");
        let result = PageServerConf::parse_and_validate(NodeId(0), config_toml, &workdir);
        assert_eq!(result.is_ok(), is_valid);
    }

    #[test]
    fn test_config_posthog_config_is_valid() {
        let input = r#"
            control_plane_api = "http://localhost:6666"

            [posthog_config]
            server_api_key = "phs_AAA"
            client_api_key = "phc_BBB"
            project_id = "000"
            private_api_url = "https://us.posthog.com"
            public_api_url = "https://us.i.posthog.com"
        "#;
        let config_toml = toml_edit::de::from_str::<pageserver_api::config::ConfigToml>(input)
            .expect("posthogconfig is valid");
        let workdir = Utf8PathBuf::from("/nonexistent");
        PageServerConf::parse_and_validate(NodeId(0), config_toml, &workdir)
            .expect("parse_and_validate");
    }

    #[test]
    fn test_config_posthog_incomplete_config_is_valid() {
        let input = r#"
            control_plane_api = "http://localhost:6666"

            [posthog_config]
            server_api_key = "phs_AAA"
            private_api_url = "https://us.posthog.com"
            public_api_url = "https://us.i.posthog.com"
        "#;
        let config_toml = toml_edit::de::from_str::<pageserver_api::config::ConfigToml>(input)
            .expect("posthogconfig is valid");
        let workdir = Utf8PathBuf::from("/nonexistent");
        PageServerConf::parse_and_validate(NodeId(0), config_toml, &workdir)
            .expect("parse_and_validate");
    }

    #[rstest]
    #[
        case::omit_the_whole_config(
            DiskUsageEvictionTaskConfig {
                max_usage_pct: Percent::new(80).unwrap(),
                min_avail_bytes: 2_000_000_000,
                period: Duration::from_secs(60),
                eviction_order: Default::default(),
                #[cfg(feature = "testing")]
                mock_statvfs: None,
                enabled: true,
            },
        r#"
            control_plane_api = "http://localhost:6666"
        "#,
    )]
    #[
        case::omit_enabled_field(
            DiskUsageEvictionTaskConfig {
                max_usage_pct: Percent::new(80).unwrap(),
                min_avail_bytes: 1_000_000_000,
                period: Duration::from_secs(60),
                eviction_order: EvictionOrder::RelativeAccessed {
                    highest_layer_count_loses_first: true,
                },
                #[cfg(feature = "testing")]
                mock_statvfs: None,
                enabled: true,
            },
        r#"
            control_plane_api = "http://localhost:6666"
            disk_usage_based_eviction = { max_usage_pct = 80, min_avail_bytes = 1000000000, period = "60s" }
        "#,
    )]
    #[case::disabled(
        DiskUsageEvictionTaskConfig {
            max_usage_pct: Percent::new(80).unwrap(),
            min_avail_bytes: 2_000_000_000,
            period: Duration::from_secs(60),
            eviction_order: EvictionOrder::RelativeAccessed {
                highest_layer_count_loses_first: true,
            },
            #[cfg(feature = "testing")]
            mock_statvfs: None,
            enabled: false,
        },
        r#"
            control_plane_api = "http://localhost:6666"
            disk_usage_based_eviction = { enabled = false }
        "#
    )]
    fn test_config_disk_usage_based_eviction_is_valid(
        #[case] expected_disk_usage_based_eviction: DiskUsageEvictionTaskConfig,
        #[case] input: &str,
    ) {
        let config_toml = toml_edit::de::from_str::<pageserver_api::config::ConfigToml>(input)
            .expect("disk_usage_based_eviction is valid");
        let workdir = Utf8PathBuf::from("/nonexistent");
        let config = PageServerConf::parse_and_validate(NodeId(0), config_toml, &workdir).unwrap();
        let disk_usage_based_eviction = config.disk_usage_based_eviction;
        assert_eq!(
            expected_disk_usage_based_eviction,
            disk_usage_based_eviction
        );
    }
}
