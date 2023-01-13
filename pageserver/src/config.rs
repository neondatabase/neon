//! Functions for handling page server configuration options
//!
//! Configuration options can be set in the pageserver.toml configuration
//! file, or on the command line.
//! See also `settings.md` for better description on every parameter.

use anyhow::{anyhow, bail, ensure, Context, Result};
use remote_storage::{RemotePath, RemoteStorageConfig};
use std::env;
use storage_broker::Uri;
use utils::crashsafe::path_with_suffix_extension;
use utils::id::ConnectionId;

use once_cell::sync::OnceCell;
use reqwest::Url;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use toml_edit;
use toml_edit::{Document, Item};

use utils::{
    id::{NodeId, TenantId, TimelineId},
    logging::LogFormat,
    postgres_backend::AuthType,
};

use crate::tenant::config::TenantConf;
use crate::tenant::config::TenantConfOpt;
use crate::tenant::{TENANT_ATTACHING_MARKER_FILENAME, TIMELINES_SEGMENT_NAME};
use crate::{
    IGNORED_TENANT_FILE_NAME, METADATA_FILE_NAME, TENANT_CONFIG_NAME, TIMELINE_UNINIT_MARK_SUFFIX,
};

pub mod defaults {
    use crate::tenant::config::defaults::*;
    use const_format::formatcp;

    pub use pageserver_api::{
        DEFAULT_HTTP_LISTEN_ADDR, DEFAULT_HTTP_LISTEN_PORT, DEFAULT_PG_LISTEN_ADDR,
        DEFAULT_PG_LISTEN_PORT,
    };
    pub use storage_broker::DEFAULT_ENDPOINT as BROKER_DEFAULT_ENDPOINT;

    pub const DEFAULT_WAIT_LSN_TIMEOUT: &str = "60 s";
    pub const DEFAULT_WAL_REDO_TIMEOUT: &str = "60 s";

    pub const DEFAULT_SUPERUSER: &str = "cloud_admin";

    pub const DEFAULT_PAGE_CACHE_SIZE: usize = 8192;
    pub const DEFAULT_MAX_FILE_DESCRIPTORS: usize = 100;

    pub const DEFAULT_LOG_FORMAT: &str = "plain";

    pub const DEFAULT_CONCURRENT_TENANT_SIZE_LOGICAL_SIZE_QUERIES: usize =
        super::ConfigurableSemaphore::DEFAULT_INITIAL.get();

    pub const DEFAULT_METRIC_COLLECTION_INTERVAL: &str = "10 min";
    pub const DEFAULT_METRIC_COLLECTION_ENDPOINT: Option<reqwest::Url> = None;
    pub const DEFAULT_SYNTHETIC_SIZE_CALCULATION_INTERVAL: &str = "10 min";

    ///
    /// Default built-in configuration file.
    ///
    pub const DEFAULT_CONFIG_FILE: &str = formatcp!(
        r###"
# Initial configuration file created by 'pageserver --init'
#listen_pg_addr = '{DEFAULT_PG_LISTEN_ADDR}'
#listen_http_addr = '{DEFAULT_HTTP_LISTEN_ADDR}'

#wait_lsn_timeout = '{DEFAULT_WAIT_LSN_TIMEOUT}'
#wal_redo_timeout = '{DEFAULT_WAL_REDO_TIMEOUT}'

#max_file_descriptors = {DEFAULT_MAX_FILE_DESCRIPTORS}

# initial superuser role name to use when creating a new tenant
#initial_superuser_name = '{DEFAULT_SUPERUSER}'

#broker_endpoint = '{BROKER_DEFAULT_ENDPOINT}'

#log_format = '{DEFAULT_LOG_FORMAT}'

#concurrent_tenant_size_logical_size_queries = '{DEFAULT_CONCURRENT_TENANT_SIZE_LOGICAL_SIZE_QUERIES}'

#metric_collection_interval = '{DEFAULT_METRIC_COLLECTION_INTERVAL}'
#synthetic_size_calculation_interval = '{DEFAULT_SYNTHETIC_SIZE_CALCULATION_INTERVAL}'

# [tenant_config]
#checkpoint_distance = {DEFAULT_CHECKPOINT_DISTANCE} # in bytes
#checkpoint_timeout = {DEFAULT_CHECKPOINT_TIMEOUT}
#compaction_target_size = {DEFAULT_COMPACTION_TARGET_SIZE} # in bytes
#compaction_period = '{DEFAULT_COMPACTION_PERIOD}'
#compaction_threshold = '{DEFAULT_COMPACTION_THRESHOLD}'

#gc_period = '{DEFAULT_GC_PERIOD}'
#gc_horizon = {DEFAULT_GC_HORIZON}
#image_creation_threshold = {DEFAULT_IMAGE_CREATION_THRESHOLD}
#pitr_interval = '{DEFAULT_PITR_INTERVAL}'

# [remote_storage]

"###
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
    pub workdir: PathBuf,

    pub pg_distrib_dir: PathBuf,

    pub auth_type: AuthType,

    pub auth_validation_public_key_path: Option<PathBuf>,
    pub remote_storage_config: Option<RemoteStorageConfig>,

    pub default_tenant_conf: TenantConf,

    /// Storage broker endpoints to connect to.
    pub broker_endpoint: Uri,
    pub broker_keepalive_interval: Duration,

    pub log_format: LogFormat,

    /// Number of concurrent [`Tenant::gather_size_inputs`] allowed.
    pub concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore,

    // How often to collect metrics and send them to the metrics endpoint.
    pub metric_collection_interval: Duration,
    pub metric_collection_endpoint: Option<Url>,
    pub synthetic_size_calculation_interval: Duration,

    pub test_remote_failures: u64,

    pub ondemand_download_behavior_treat_error_as_warn: bool,
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
pub enum BuilderValue<T> {
    Set(T),
    NotSet,
}

impl<T> BuilderValue<T> {
    pub fn ok_or<E>(self, err: E) -> Result<T, E> {
        match self {
            Self::Set(v) => Ok(v),
            Self::NotSet => Err(err),
        }
    }
}

// needed to simplify config construction
struct PageServerConfigBuilder {
    listen_pg_addr: BuilderValue<String>,

    listen_http_addr: BuilderValue<String>,

    wait_lsn_timeout: BuilderValue<Duration>,
    wal_redo_timeout: BuilderValue<Duration>,

    superuser: BuilderValue<String>,

    page_cache_size: BuilderValue<usize>,
    max_file_descriptors: BuilderValue<usize>,

    workdir: BuilderValue<PathBuf>,

    pg_distrib_dir: BuilderValue<PathBuf>,

    auth_type: BuilderValue<AuthType>,

    //
    auth_validation_public_key_path: BuilderValue<Option<PathBuf>>,
    remote_storage_config: BuilderValue<Option<RemoteStorageConfig>>,

    id: BuilderValue<NodeId>,

    broker_endpoint: BuilderValue<Uri>,
    broker_keepalive_interval: BuilderValue<Duration>,

    log_format: BuilderValue<LogFormat>,

    concurrent_tenant_size_logical_size_queries: BuilderValue<ConfigurableSemaphore>,

    metric_collection_interval: BuilderValue<Duration>,
    metric_collection_endpoint: BuilderValue<Option<Url>>,
    synthetic_size_calculation_interval: BuilderValue<Duration>,

    test_remote_failures: BuilderValue<u64>,

    ondemand_download_behavior_treat_error_as_warn: BuilderValue<bool>,
}

impl Default for PageServerConfigBuilder {
    fn default() -> Self {
        use self::BuilderValue::*;
        use defaults::*;
        Self {
            listen_pg_addr: Set(DEFAULT_PG_LISTEN_ADDR.to_string()),
            listen_http_addr: Set(DEFAULT_HTTP_LISTEN_ADDR.to_string()),
            wait_lsn_timeout: Set(humantime::parse_duration(DEFAULT_WAIT_LSN_TIMEOUT)
                .expect("cannot parse default wait lsn timeout")),
            wal_redo_timeout: Set(humantime::parse_duration(DEFAULT_WAL_REDO_TIMEOUT)
                .expect("cannot parse default wal redo timeout")),
            superuser: Set(DEFAULT_SUPERUSER.to_string()),
            page_cache_size: Set(DEFAULT_PAGE_CACHE_SIZE),
            max_file_descriptors: Set(DEFAULT_MAX_FILE_DESCRIPTORS),
            workdir: Set(PathBuf::new()),
            pg_distrib_dir: Set(env::current_dir()
                .expect("cannot access current directory")
                .join("pg_install")),
            auth_type: Set(AuthType::Trust),
            auth_validation_public_key_path: Set(None),
            remote_storage_config: Set(None),
            id: NotSet,
            broker_endpoint: Set(storage_broker::DEFAULT_ENDPOINT
                .parse()
                .expect("failed to parse default broker endpoint")),
            broker_keepalive_interval: Set(humantime::parse_duration(
                storage_broker::DEFAULT_KEEPALIVE_INTERVAL,
            )
            .expect("cannot parse default keepalive interval")),
            log_format: Set(LogFormat::from_str(DEFAULT_LOG_FORMAT).unwrap()),

            concurrent_tenant_size_logical_size_queries: Set(ConfigurableSemaphore::default()),
            metric_collection_interval: Set(humantime::parse_duration(
                DEFAULT_METRIC_COLLECTION_INTERVAL,
            )
            .expect("cannot parse default metric collection interval")),
            synthetic_size_calculation_interval: Set(humantime::parse_duration(
                DEFAULT_SYNTHETIC_SIZE_CALCULATION_INTERVAL,
            )
            .expect("cannot parse default synthetic size calculation interval")),
            metric_collection_endpoint: Set(DEFAULT_METRIC_COLLECTION_ENDPOINT),

            test_remote_failures: Set(0),

            ondemand_download_behavior_treat_error_as_warn: Set(false),
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

    pub fn workdir(&mut self, workdir: PathBuf) {
        self.workdir = BuilderValue::Set(workdir)
    }

    pub fn pg_distrib_dir(&mut self, pg_distrib_dir: PathBuf) {
        self.pg_distrib_dir = BuilderValue::Set(pg_distrib_dir)
    }

    pub fn auth_type(&mut self, auth_type: AuthType) {
        self.auth_type = BuilderValue::Set(auth_type)
    }

    pub fn auth_validation_public_key_path(
        &mut self,
        auth_validation_public_key_path: Option<PathBuf>,
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

    pub fn id(&mut self, node_id: NodeId) {
        self.id = BuilderValue::Set(node_id)
    }

    pub fn log_format(&mut self, log_format: LogFormat) {
        self.log_format = BuilderValue::Set(log_format)
    }

    pub fn concurrent_tenant_size_logical_size_queries(&mut self, u: ConfigurableSemaphore) {
        self.concurrent_tenant_size_logical_size_queries = BuilderValue::Set(u);
    }

    pub fn metric_collection_interval(&mut self, metric_collection_interval: Duration) {
        self.metric_collection_interval = BuilderValue::Set(metric_collection_interval)
    }

    pub fn metric_collection_endpoint(&mut self, metric_collection_endpoint: Option<Url>) {
        self.metric_collection_endpoint = BuilderValue::Set(metric_collection_endpoint)
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

    pub fn ondemand_download_behavior_treat_error_as_warn(
        &mut self,
        ondemand_download_behavior_treat_error_as_warn: bool,
    ) {
        self.ondemand_download_behavior_treat_error_as_warn =
            BuilderValue::Set(ondemand_download_behavior_treat_error_as_warn);
    }

    pub fn build(self) -> anyhow::Result<PageServerConf> {
        Ok(PageServerConf {
            listen_pg_addr: self
                .listen_pg_addr
                .ok_or(anyhow!("missing listen_pg_addr"))?,
            listen_http_addr: self
                .listen_http_addr
                .ok_or(anyhow!("missing listen_http_addr"))?,
            wait_lsn_timeout: self
                .wait_lsn_timeout
                .ok_or(anyhow!("missing wait_lsn_timeout"))?,
            wal_redo_timeout: self
                .wal_redo_timeout
                .ok_or(anyhow!("missing wal_redo_timeout"))?,
            superuser: self.superuser.ok_or(anyhow!("missing superuser"))?,
            page_cache_size: self
                .page_cache_size
                .ok_or(anyhow!("missing page_cache_size"))?,
            max_file_descriptors: self
                .max_file_descriptors
                .ok_or(anyhow!("missing max_file_descriptors"))?,
            workdir: self.workdir.ok_or(anyhow!("missing workdir"))?,
            pg_distrib_dir: self
                .pg_distrib_dir
                .ok_or(anyhow!("missing pg_distrib_dir"))?,
            auth_type: self.auth_type.ok_or(anyhow!("missing auth_type"))?,
            auth_validation_public_key_path: self
                .auth_validation_public_key_path
                .ok_or(anyhow!("missing auth_validation_public_key_path"))?,
            remote_storage_config: self
                .remote_storage_config
                .ok_or(anyhow!("missing remote_storage_config"))?,
            id: self.id.ok_or(anyhow!("missing id"))?,
            // TenantConf is handled separately
            default_tenant_conf: TenantConf::default(),
            broker_endpoint: self
                .broker_endpoint
                .ok_or(anyhow!("No broker endpoints provided"))?,
            broker_keepalive_interval: self
                .broker_keepalive_interval
                .ok_or(anyhow!("No broker keepalive interval provided"))?,
            log_format: self.log_format.ok_or(anyhow!("missing log_format"))?,
            concurrent_tenant_size_logical_size_queries: self
                .concurrent_tenant_size_logical_size_queries
                .ok_or(anyhow!(
                    "missing concurrent_tenant_size_logical_size_queries"
                ))?,
            metric_collection_interval: self
                .metric_collection_interval
                .ok_or(anyhow!("missing metric_collection_interval"))?,
            metric_collection_endpoint: self
                .metric_collection_endpoint
                .ok_or(anyhow!("missing metric_collection_endpoint"))?,
            synthetic_size_calculation_interval: self
                .synthetic_size_calculation_interval
                .ok_or(anyhow!("missing synthetic_size_calculation_interval"))?,
            test_remote_failures: self
                .test_remote_failures
                .ok_or(anyhow!("missing test_remote_failuers"))?,
            ondemand_download_behavior_treat_error_as_warn: self
                .ondemand_download_behavior_treat_error_as_warn
                .ok_or(anyhow!(
                    "missing ondemand_download_behavior_treat_error_as_warn"
                ))?,
        })
    }
}

impl PageServerConf {
    //
    // Repository paths, relative to workdir.
    //

    pub fn tenants_path(&self) -> PathBuf {
        self.workdir.join("tenants")
    }

    pub fn tenant_path(&self, tenant_id: &TenantId) -> PathBuf {
        self.tenants_path().join(tenant_id.to_string())
    }

    pub fn tenant_attaching_mark_file_path(&self, tenant_id: &TenantId) -> PathBuf {
        self.tenant_path(tenant_id)
            .join(TENANT_ATTACHING_MARKER_FILENAME)
    }

    pub fn tenant_ignore_mark_file_path(&self, tenant_id: TenantId) -> PathBuf {
        self.tenant_path(&tenant_id).join(IGNORED_TENANT_FILE_NAME)
    }

    /// Points to a place in pageserver's local directory,
    /// where certain tenant's tenantconf file should be located.
    pub fn tenant_config_path(&self, tenant_id: TenantId) -> PathBuf {
        self.tenant_path(&tenant_id).join(TENANT_CONFIG_NAME)
    }

    pub fn timelines_path(&self, tenant_id: &TenantId) -> PathBuf {
        self.tenant_path(tenant_id).join(TIMELINES_SEGMENT_NAME)
    }

    pub fn timeline_path(&self, timeline_id: &TimelineId, tenant_id: &TenantId) -> PathBuf {
        self.timelines_path(tenant_id).join(timeline_id.to_string())
    }

    pub fn timeline_uninit_mark_file_path(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> PathBuf {
        path_with_suffix_extension(
            self.timeline_path(&timeline_id, &tenant_id),
            TIMELINE_UNINIT_MARK_SUFFIX,
        )
    }

    pub fn traces_path(&self) -> PathBuf {
        self.workdir.join("traces")
    }

    pub fn trace_path(
        &self,
        tenant_id: &TenantId,
        timeline_id: &TimelineId,
        connection_id: &ConnectionId,
    ) -> PathBuf {
        self.traces_path()
            .join(tenant_id.to_string())
            .join(timeline_id.to_string())
            .join(connection_id.to_string())
    }

    /// Points to a place in pageserver's local directory,
    /// where certain timeline's metadata file should be located.
    pub fn metadata_path(&self, timeline_id: TimelineId, tenant_id: TenantId) -> PathBuf {
        self.timeline_path(&timeline_id, &tenant_id)
            .join(METADATA_FILE_NAME)
    }

    /// Files on the remote storage are stored with paths, relative to the workdir.
    /// That path includes in itself both tenant and timeline ids, allowing to have a unique remote storage path.
    ///
    /// Errors if the path provided does not start from pageserver's workdir.
    pub fn remote_path(&self, local_path: &Path) -> anyhow::Result<RemotePath> {
        local_path
            .strip_prefix(&self.workdir)
            .context("Failed to strip workdir prefix")
            .and_then(RemotePath::new)
            .with_context(|| {
                format!(
                    "Failed to resolve remote part of path {:?} for base {:?}",
                    local_path, self.workdir
                )
            })
    }

    /// Turns storage remote path of a file into its local path.
    pub fn local_path(&self, remote_path: &RemotePath) -> PathBuf {
        remote_path.with_base(&self.workdir)
    }

    //
    // Postgres distribution paths
    //
    pub fn pg_distrib_dir(&self, pg_version: u32) -> anyhow::Result<PathBuf> {
        let path = self.pg_distrib_dir.clone();

        match pg_version {
            14 => Ok(path.join(format!("v{pg_version}"))),
            15 => Ok(path.join(format!("v{pg_version}"))),
            _ => bail!("Unsupported postgres version: {}", pg_version),
        }
    }

    pub fn pg_bin_dir(&self, pg_version: u32) -> anyhow::Result<PathBuf> {
        match pg_version {
            14 => Ok(self.pg_distrib_dir(pg_version)?.join("bin")),
            15 => Ok(self.pg_distrib_dir(pg_version)?.join("bin")),
            _ => bail!("Unsupported postgres version: {}", pg_version),
        }
    }
    pub fn pg_lib_dir(&self, pg_version: u32) -> anyhow::Result<PathBuf> {
        match pg_version {
            14 => Ok(self.pg_distrib_dir(pg_version)?.join("lib")),
            15 => Ok(self.pg_distrib_dir(pg_version)?.join("lib")),
            _ => bail!("Unsupported postgres version: {}", pg_version),
        }
    }

    /// Parse a configuration file (pageserver.toml) into a PageServerConf struct,
    /// validating the input and failing on errors.
    ///
    /// This leaves any options not present in the file in the built-in defaults.
    pub fn parse_and_validate(toml: &Document, workdir: &Path) -> anyhow::Result<Self> {
        let mut builder = PageServerConfigBuilder::default();
        builder.workdir(workdir.to_owned());

        let mut t_conf = TenantConfOpt::default();

        for (key, item) in toml.iter() {
            match key {
                "listen_pg_addr" => builder.listen_pg_addr(parse_toml_string(key, item)?),
                "listen_http_addr" => builder.listen_http_addr(parse_toml_string(key, item)?),
                "wait_lsn_timeout" => builder.wait_lsn_timeout(parse_toml_duration(key, item)?),
                "wal_redo_timeout" => builder.wal_redo_timeout(parse_toml_duration(key, item)?),
                "initial_superuser_name" => builder.superuser(parse_toml_string(key, item)?),
                "page_cache_size" => builder.page_cache_size(parse_toml_u64(key, item)? as usize),
                "max_file_descriptors" => {
                    builder.max_file_descriptors(parse_toml_u64(key, item)? as usize)
                }
                "pg_distrib_dir" => {
                    builder.pg_distrib_dir(PathBuf::from(parse_toml_string(key, item)?))
                }
                "auth_validation_public_key_path" => builder.auth_validation_public_key_path(Some(
                    PathBuf::from(parse_toml_string(key, item)?),
                )),
                "auth_type" => builder.auth_type(parse_toml_from_str(key, item)?),
                "remote_storage" => {
                    builder.remote_storage_config(RemoteStorageConfig::from_toml(item)?)
                }
                "tenant_config" => {
                    t_conf = Self::parse_toml_tenant_conf(item)?;
                }
                "id" => builder.id(NodeId(parse_toml_u64(key, item)?)),
                "broker_endpoint" => builder.broker_endpoint(parse_toml_string(key, item)?.parse().context("failed to parse broker endpoint")?),
                "broker_keepalive_interval" => builder.broker_keepalive_interval(parse_toml_duration(key, item)?),
                "log_format" => builder.log_format(
                    LogFormat::from_config(&parse_toml_string(key, item)?)?
                ),
                "concurrent_tenant_size_logical_size_queries" => builder.concurrent_tenant_size_logical_size_queries({
                    let input = parse_toml_string(key, item)?;
                    let permits = input.parse::<usize>().context("expected a number of initial permits, not {s:?}")?;
                    let permits = NonZeroUsize::new(permits).context("initial semaphore permits out of range: 0, use other configuration to disable a feature")?;
                    ConfigurableSemaphore::new(permits)
                }),
                "metric_collection_interval" => builder.metric_collection_interval(parse_toml_duration(key, item)?),
                "metric_collection_endpoint" => {
                    let endpoint = parse_toml_string(key, item)?.parse().context("failed to parse metric_collection_endpoint")?;
                    builder.metric_collection_endpoint(Some(endpoint));
                },
                "synthetic_size_calculation_interval" =>
                    builder.synthetic_size_calculation_interval(parse_toml_duration(key, item)?),
                "test_remote_failures" => builder.test_remote_failures(parse_toml_u64(key, item)?),
                "ondemand_download_behavior_treat_error_as_warn" => builder.ondemand_download_behavior_treat_error_as_warn(parse_toml_bool(key, item)?),
                _ => bail!("unrecognized pageserver option '{key}'"),
            }
        }

        let mut conf = builder.build().context("invalid config")?;

        if conf.auth_type == AuthType::NeonJWT {
            let auth_validation_public_key_path = conf
                .auth_validation_public_key_path
                .get_or_insert_with(|| workdir.join("auth_public_key.pem"));
            ensure!(
                auth_validation_public_key_path.exists(),
                format!(
                    "Can't find auth_validation_public_key at '{}'",
                    auth_validation_public_key_path.display()
                )
            );
        }

        conf.default_tenant_conf = t_conf.merge(TenantConf::default());

        Ok(conf)
    }

    // subroutine of parse_and_validate to parse `[tenant_conf]` section

    pub fn parse_toml_tenant_conf(item: &toml_edit::Item) -> Result<TenantConfOpt> {
        let mut t_conf: TenantConfOpt = Default::default();
        if let Some(checkpoint_distance) = item.get("checkpoint_distance") {
            t_conf.checkpoint_distance =
                Some(parse_toml_u64("checkpoint_distance", checkpoint_distance)?);
        }

        if let Some(checkpoint_timeout) = item.get("checkpoint_timeout") {
            t_conf.checkpoint_timeout = Some(parse_toml_duration(
                "checkpoint_timeout",
                checkpoint_timeout,
            )?);
        }

        if let Some(compaction_target_size) = item.get("compaction_target_size") {
            t_conf.compaction_target_size = Some(parse_toml_u64(
                "compaction_target_size",
                compaction_target_size,
            )?);
        }

        if let Some(compaction_period) = item.get("compaction_period") {
            t_conf.compaction_period =
                Some(parse_toml_duration("compaction_period", compaction_period)?);
        }

        if let Some(compaction_threshold) = item.get("compaction_threshold") {
            t_conf.compaction_threshold =
                Some(parse_toml_u64("compaction_threshold", compaction_threshold)?.try_into()?);
        }

        if let Some(gc_horizon) = item.get("gc_horizon") {
            t_conf.gc_horizon = Some(parse_toml_u64("gc_horizon", gc_horizon)?);
        }

        if let Some(gc_period) = item.get("gc_period") {
            t_conf.gc_period = Some(parse_toml_duration("gc_period", gc_period)?);
        }

        if let Some(pitr_interval) = item.get("pitr_interval") {
            t_conf.pitr_interval = Some(parse_toml_duration("pitr_interval", pitr_interval)?);
        }
        if let Some(walreceiver_connect_timeout) = item.get("walreceiver_connect_timeout") {
            t_conf.walreceiver_connect_timeout = Some(parse_toml_duration(
                "walreceiver_connect_timeout",
                walreceiver_connect_timeout,
            )?);
        }
        if let Some(lagging_wal_timeout) = item.get("lagging_wal_timeout") {
            t_conf.lagging_wal_timeout = Some(parse_toml_duration(
                "lagging_wal_timeout",
                lagging_wal_timeout,
            )?);
        }
        if let Some(max_lsn_wal_lag) = item.get("max_lsn_wal_lag") {
            t_conf.max_lsn_wal_lag = Some(parse_toml_from_str("max_lsn_wal_lag", max_lsn_wal_lag)?);
        }
        if let Some(trace_read_requests) = item.get("trace_read_requests") {
            t_conf.trace_read_requests =
                Some(trace_read_requests.as_bool().with_context(|| {
                    "configure option trace_read_requests is not a bool".to_string()
                })?);
        }

        Ok(t_conf)
    }

    #[cfg(test)]
    pub fn test_repo_dir(test_name: &str) -> PathBuf {
        PathBuf::from(format!("../tmp_check/test_{test_name}"))
    }

    pub fn dummy_conf(repo_dir: PathBuf) -> Self {
        let pg_distrib_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../pg_install");

        PageServerConf {
            id: NodeId(0),
            wait_lsn_timeout: Duration::from_secs(60),
            wal_redo_timeout: Duration::from_secs(60),
            page_cache_size: defaults::DEFAULT_PAGE_CACHE_SIZE,
            max_file_descriptors: defaults::DEFAULT_MAX_FILE_DESCRIPTORS,
            listen_pg_addr: defaults::DEFAULT_PG_LISTEN_ADDR.to_string(),
            listen_http_addr: defaults::DEFAULT_HTTP_LISTEN_ADDR.to_string(),
            superuser: "cloud_admin".to_string(),
            workdir: repo_dir,
            pg_distrib_dir,
            auth_type: AuthType::Trust,
            auth_validation_public_key_path: None,
            remote_storage_config: None,
            default_tenant_conf: TenantConf::default(),
            broker_endpoint: storage_broker::DEFAULT_ENDPOINT.parse().unwrap(),
            broker_keepalive_interval: Duration::from_secs(5000),
            log_format: LogFormat::from_str(defaults::DEFAULT_LOG_FORMAT).unwrap(),
            concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore::default(),
            metric_collection_interval: Duration::from_secs(60),
            metric_collection_endpoint: defaults::DEFAULT_METRIC_COLLECTION_ENDPOINT,
            synthetic_size_calculation_interval: Duration::from_secs(60),
            test_remote_failures: 0,
            ondemand_download_behavior_treat_error_as_warn: false,
        }
    }
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
    pub fn new(initial_permits: NonZeroUsize) -> Self {
        ConfigurableSemaphore {
            initial_permits,
            inner: std::sync::Arc::new(tokio::sync::Semaphore::new(initial_permits.get())),
        }
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
    use std::{
        fs,
        num::{NonZeroU32, NonZeroUsize},
    };

    use remote_storage::{RemoteStorageKind, S3Config};
    use tempfile::{tempdir, TempDir};

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

"#;

    #[test]
    fn parse_defaults() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;
        let broker_endpoint = storage_broker::DEFAULT_ENDPOINT;
        // we have to create dummy values to overcome the validation errors
        let config_string = format!(
            "pg_distrib_dir='{}'\nid=10\nbroker_endpoint = '{broker_endpoint}'",
            pg_distrib_dir.display()
        );
        let toml = config_string.parse()?;

        let parsed_config = PageServerConf::parse_and_validate(&toml, &workdir)
            .unwrap_or_else(|e| panic!("Failed to parse config '{config_string}', reason: {e:?}"));

        assert_eq!(
            parsed_config,
            PageServerConf {
                id: NodeId(10),
                listen_pg_addr: defaults::DEFAULT_PG_LISTEN_ADDR.to_string(),
                listen_http_addr: defaults::DEFAULT_HTTP_LISTEN_ADDR.to_string(),
                wait_lsn_timeout: humantime::parse_duration(defaults::DEFAULT_WAIT_LSN_TIMEOUT)?,
                wal_redo_timeout: humantime::parse_duration(defaults::DEFAULT_WAL_REDO_TIMEOUT)?,
                superuser: defaults::DEFAULT_SUPERUSER.to_string(),
                page_cache_size: defaults::DEFAULT_PAGE_CACHE_SIZE,
                max_file_descriptors: defaults::DEFAULT_MAX_FILE_DESCRIPTORS,
                workdir,
                pg_distrib_dir,
                auth_type: AuthType::Trust,
                auth_validation_public_key_path: None,
                remote_storage_config: None,
                default_tenant_conf: TenantConf::default(),
                broker_endpoint: storage_broker::DEFAULT_ENDPOINT.parse().unwrap(),
                broker_keepalive_interval: humantime::parse_duration(
                    storage_broker::DEFAULT_KEEPALIVE_INTERVAL
                )?,
                log_format: LogFormat::from_str(defaults::DEFAULT_LOG_FORMAT).unwrap(),
                concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore::default(),
                metric_collection_interval: humantime::parse_duration(
                    defaults::DEFAULT_METRIC_COLLECTION_INTERVAL
                )?,
                metric_collection_endpoint: defaults::DEFAULT_METRIC_COLLECTION_ENDPOINT,
                synthetic_size_calculation_interval: humantime::parse_duration(
                    defaults::DEFAULT_SYNTHETIC_SIZE_CALCULATION_INTERVAL
                )?,
                test_remote_failures: 0,
                ondemand_download_behavior_treat_error_as_warn: false,
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
            "{ALL_BASE_VALUES_TOML}pg_distrib_dir='{}'\nbroker_endpoint = '{broker_endpoint}'",
            pg_distrib_dir.display()
        );
        let toml = config_string.parse()?;

        let parsed_config = PageServerConf::parse_and_validate(&toml, &workdir)
            .unwrap_or_else(|e| panic!("Failed to parse config '{config_string}', reason: {e:?}"));

        assert_eq!(
            parsed_config,
            PageServerConf {
                id: NodeId(10),
                listen_pg_addr: "127.0.0.1:64000".to_string(),
                listen_http_addr: "127.0.0.1:9898".to_string(),
                wait_lsn_timeout: Duration::from_secs(111),
                wal_redo_timeout: Duration::from_secs(111),
                superuser: "zzzz".to_string(),
                page_cache_size: 444,
                max_file_descriptors: 333,
                workdir,
                pg_distrib_dir,
                auth_type: AuthType::Trust,
                auth_validation_public_key_path: None,
                remote_storage_config: None,
                default_tenant_conf: TenantConf::default(),
                broker_endpoint: storage_broker::DEFAULT_ENDPOINT.parse().unwrap(),
                broker_keepalive_interval: Duration::from_secs(5),
                log_format: LogFormat::Json,
                concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore::default(),
                metric_collection_interval: Duration::from_secs(222),
                metric_collection_endpoint: Some(Url::parse("http://localhost:80/metrics")?),
                synthetic_size_calculation_interval: Duration::from_secs(333),
                test_remote_failures: 0,
                ondemand_download_behavior_treat_error_as_warn: false,
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
local_path = '{}'"#,
                local_storage_path.display()
            ),
            format!(
                "remote_storage={{local_path='{}'}}",
                local_storage_path.display()
            ),
        ];

        for remote_storage_config_str in identical_toml_declarations {
            let config_string = format!(
                r#"{ALL_BASE_VALUES_TOML}
pg_distrib_dir='{}'
broker_endpoint = '{broker_endpoint}'

{remote_storage_config_str}"#,
                pg_distrib_dir.display(),
            );

            let toml = config_string.parse()?;

            let parsed_remote_storage_config = PageServerConf::parse_and_validate(&toml, &workdir)
                .unwrap_or_else(|e| {
                    panic!("Failed to parse config '{config_string}', reason: {e:?}")
                })
                .remote_storage_config
                .expect("Should have remote storage config for the local FS");

            assert_eq!(
                parsed_remote_storage_config,
                RemoteStorageConfig {
                    max_concurrent_syncs: NonZeroUsize::new(
                        remote_storage::DEFAULT_REMOTE_STORAGE_MAX_CONCURRENT_SYNCS
                    )
                        .unwrap(),
                    max_sync_errors: NonZeroU32::new(remote_storage::DEFAULT_REMOTE_STORAGE_MAX_SYNC_ERRORS)
                        .unwrap(),
                    storage: RemoteStorageKind::LocalFs(local_storage_path.clone()),
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
pg_distrib_dir='{}'
broker_endpoint = '{broker_endpoint}'

{remote_storage_config_str}"#,
                pg_distrib_dir.display(),
            );

            let toml = config_string.parse()?;

            let parsed_remote_storage_config = PageServerConf::parse_and_validate(&toml, &workdir)
                .unwrap_or_else(|e| {
                    panic!("Failed to parse config '{config_string}', reason: {e:?}")
                })
                .remote_storage_config
                .expect("Should have remote storage config for S3");

            assert_eq!(
                parsed_remote_storage_config,
                RemoteStorageConfig {
                    max_concurrent_syncs,
                    max_sync_errors,
                    storage: RemoteStorageKind::AwsS3(S3Config {
                        bucket_name: bucket_name.clone(),
                        bucket_region: bucket_region.clone(),
                        prefix_in_bucket: Some(prefix_in_bucket.clone()),
                        endpoint: Some(endpoint.clone()),
                        concurrency_limit: s3_concurrency_limit,
                    }),
                },
                "Remote storage config should correctly parse the S3 config"
            );
        }
        Ok(())
    }

    #[test]
    fn parse_tenant_config() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;

        let broker_endpoint = "http://127.0.0.1:7777";
        let trace_read_requests = true;

        let config_string = format!(
            r#"{ALL_BASE_VALUES_TOML}
pg_distrib_dir='{}'
broker_endpoint = '{broker_endpoint}'

[tenant_config]
trace_read_requests = {trace_read_requests}"#,
            pg_distrib_dir.display(),
        );

        let toml = config_string.parse()?;

        let conf = PageServerConf::parse_and_validate(&toml, &workdir)?;
        assert_eq!(
            conf.default_tenant_conf.trace_read_requests, trace_read_requests,
            "Tenant config from pageserver config file should be parsed and udpated values used as defaults for all tenants",
        );

        Ok(())
    }

    fn prepare_fs(tempdir: &TempDir) -> anyhow::Result<(PathBuf, PathBuf)> {
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
