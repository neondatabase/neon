//! Functions for handling page server configuration options
//!
//! Configuration options can be set in the pageserver.toml configuration
//! file, or on the command line.
//! See also `settings.md` for better description on every parameter.

use anyhow::{anyhow, bail, ensure, Context, Result};
use remote_storage::{RemoteStorageConfig, RemoteStorageKind, S3Config};
use std::env;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use toml_edit;
use toml_edit::{Document, Item};
use url::Url;
use utils::{
    postgres_backend::AuthType,
    zid::{NodeId, TenantId, ZTimelineId},
};

use crate::layered_repository::TIMELINES_SEGMENT_NAME;
use crate::tenant_config::{TenantConf, TenantConfOpt};

pub mod defaults {
    use crate::tenant_config::defaults::*;
    use const_format::formatcp;

    pub const DEFAULT_PG_LISTEN_PORT: u16 = 64000;
    pub const DEFAULT_PG_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_PG_LISTEN_PORT}");
    pub const DEFAULT_HTTP_LISTEN_PORT: u16 = 9898;
    pub const DEFAULT_HTTP_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_HTTP_LISTEN_PORT}");

    pub const DEFAULT_WAIT_LSN_TIMEOUT: &str = "60 s";
    pub const DEFAULT_WAL_REDO_TIMEOUT: &str = "60 s";

    pub const DEFAULT_SUPERUSER: &str = "zenith_admin";

    pub const DEFAULT_PAGE_CACHE_SIZE: usize = 8192;
    pub const DEFAULT_MAX_FILE_DESCRIPTORS: usize = 100;

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

# [tenant_config]
#checkpoint_distance = {DEFAULT_CHECKPOINT_DISTANCE} # in bytes
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

    pub profiling: ProfilingConfig,
    pub default_tenant_conf: TenantConf,

    /// A prefix to add in etcd brokers before every key.
    /// Can be used for isolating different pageserver groups withing the same etcd cluster.
    pub broker_etcd_prefix: String,

    /// Etcd broker endpoints to connect to.
    pub broker_endpoints: Vec<Url>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfilingConfig {
    Disabled,
    PageRequests,
}

impl FromStr for ProfilingConfig {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<ProfilingConfig, Self::Err> {
        let result = match s {
            "disabled"  => ProfilingConfig::Disabled,
            "page_requests"  => ProfilingConfig::PageRequests,
            _ => bail!("invalid value \"{s}\" for profiling option, valid values are \"disabled\" and \"page_requests\""),
        };
        Ok(result)
    }
}

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

    profiling: BuilderValue<ProfilingConfig>,
    broker_etcd_prefix: BuilderValue<String>,
    broker_endpoints: BuilderValue<Vec<Url>>,
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
                .join("tmp_install")),
            auth_type: Set(AuthType::Trust),
            auth_validation_public_key_path: Set(None),
            remote_storage_config: Set(None),
            id: NotSet,
            profiling: Set(ProfilingConfig::Disabled),
            broker_etcd_prefix: Set(etcd_broker::DEFAULT_NEON_BROKER_ETCD_PREFIX.to_string()),
            broker_endpoints: Set(Vec::new()),
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

    pub fn broker_endpoints(&mut self, broker_endpoints: Vec<Url>) {
        self.broker_endpoints = BuilderValue::Set(broker_endpoints)
    }

    pub fn broker_etcd_prefix(&mut self, broker_etcd_prefix: String) {
        self.broker_etcd_prefix = BuilderValue::Set(broker_etcd_prefix)
    }

    pub fn id(&mut self, node_id: NodeId) {
        self.id = BuilderValue::Set(node_id)
    }

    pub fn profiling(&mut self, profiling: ProfilingConfig) {
        self.profiling = BuilderValue::Set(profiling)
    }

    pub fn build(self) -> anyhow::Result<PageServerConf> {
        let broker_endpoints = self
            .broker_endpoints
            .ok_or(anyhow!("No broker endpoints provided"))?;

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
            profiling: self.profiling.ok_or(anyhow!("missing profiling"))?,
            // TenantConf is handled separately
            default_tenant_conf: TenantConf::default(),
            broker_endpoints,
            broker_etcd_prefix: self
                .broker_etcd_prefix
                .ok_or(anyhow!("missing broker_etcd_prefix"))?,
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

    pub fn tenant_path(&self, tenantid: &TenantId) -> PathBuf {
        self.tenants_path().join(tenantid.to_string())
    }

    pub fn timelines_path(&self, tenantid: &TenantId) -> PathBuf {
        self.tenant_path(tenantid).join(TIMELINES_SEGMENT_NAME)
    }

    pub fn timeline_path(&self, timelineid: &ZTimelineId, tenantid: &TenantId) -> PathBuf {
        self.timelines_path(tenantid).join(timelineid.to_string())
    }

    //
    // Postgres distribution paths
    //

    pub fn pg_bin_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("bin")
    }

    pub fn pg_lib_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("lib")
    }

    /// Parse a configuration file (pageserver.toml) into a PageServerConf struct,
    /// validating the input and failing on errors.
    ///
    /// This leaves any options not present in the file in the built-in defaults.
    pub fn parse_and_validate(toml: &Document, workdir: &Path) -> anyhow::Result<Self> {
        let mut builder = PageServerConfigBuilder::default();
        builder.workdir(workdir.to_owned());

        let mut t_conf: TenantConfOpt = Default::default();

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
                    builder.remote_storage_config(Some(Self::parse_remote_storage_config(item)?))
                }
                "tenant_config" => {
                    t_conf = Self::parse_toml_tenant_conf(item)?;
                }
                "id" => builder.id(NodeId(parse_toml_u64(key, item)?)),
                "profiling" => builder.profiling(parse_toml_from_str(key, item)?),
                "broker_etcd_prefix" => builder.broker_etcd_prefix(parse_toml_string(key, item)?),
                "broker_endpoints" => builder.broker_endpoints(
                    parse_toml_array(key, item)?
                        .into_iter()
                        .map(|endpoint_str| {
                            endpoint_str.parse::<Url>().with_context(|| {
                                format!("Array item {endpoint_str} for key {key} is not a valid url endpoint")
                            })
                        })
                        .collect::<anyhow::Result<_>>()?,
                ),
                _ => bail!("unrecognized pageserver option '{key}'"),
            }
        }

        let mut conf = builder.build().context("invalid config")?;

        if conf.auth_type == AuthType::ZenithJWT {
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

        if !conf.pg_distrib_dir.join("bin/postgres").exists() {
            bail!(
                "Can't find postgres binary at {}",
                conf.pg_distrib_dir.display()
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

        Ok(t_conf)
    }

    /// subroutine of parse_config(), to parse the `[remote_storage]` table.
    fn parse_remote_storage_config(toml: &toml_edit::Item) -> anyhow::Result<RemoteStorageConfig> {
        let local_path = toml.get("local_path");
        let bucket_name = toml.get("bucket_name");
        let bucket_region = toml.get("bucket_region");

        let max_concurrent_syncs = NonZeroUsize::new(
            parse_optional_integer("max_concurrent_syncs", toml)?
                .unwrap_or(remote_storage::DEFAULT_REMOTE_STORAGE_MAX_CONCURRENT_SYNCS),
        )
        .context("Failed to parse 'max_concurrent_syncs' as a positive integer")?;

        let max_sync_errors = NonZeroU32::new(
            parse_optional_integer("max_sync_errors", toml)?
                .unwrap_or(remote_storage::DEFAULT_REMOTE_STORAGE_MAX_SYNC_ERRORS),
        )
        .context("Failed to parse 'max_sync_errors' as a positive integer")?;

        let concurrency_limit = NonZeroUsize::new(
            parse_optional_integer("concurrency_limit", toml)?
                .unwrap_or(remote_storage::DEFAULT_REMOTE_STORAGE_S3_CONCURRENCY_LIMIT),
        )
        .context("Failed to parse 'concurrency_limit' as a positive integer")?;

        let storage = match (local_path, bucket_name, bucket_region) {
            (None, None, None) => bail!("no 'local_path' nor 'bucket_name' option"),
            (_, Some(_), None) => {
                bail!("'bucket_region' option is mandatory if 'bucket_name' is given ")
            }
            (_, None, Some(_)) => {
                bail!("'bucket_name' option is mandatory if 'bucket_region' is given ")
            }
            (None, Some(bucket_name), Some(bucket_region)) => RemoteStorageKind::AwsS3(S3Config {
                bucket_name: parse_toml_string("bucket_name", bucket_name)?,
                bucket_region: parse_toml_string("bucket_region", bucket_region)?,
                prefix_in_bucket: toml
                    .get("prefix_in_bucket")
                    .map(|prefix_in_bucket| parse_toml_string("prefix_in_bucket", prefix_in_bucket))
                    .transpose()?,
                endpoint: toml
                    .get("endpoint")
                    .map(|endpoint| parse_toml_string("endpoint", endpoint))
                    .transpose()?,
                concurrency_limit,
            }),
            (Some(local_path), None, None) => RemoteStorageKind::LocalFs(PathBuf::from(
                parse_toml_string("local_path", local_path)?,
            )),
            (Some(_), Some(_), _) => bail!("local_path and bucket_name are mutually exclusive"),
        };

        Ok(RemoteStorageConfig {
            max_concurrent_syncs,
            max_sync_errors,
            storage,
        })
    }

    #[cfg(test)]
    pub fn test_repo_dir(test_name: &str) -> PathBuf {
        PathBuf::from(format!("../tmp_check/test_{test_name}"))
    }

    #[cfg(test)]
    pub fn dummy_conf(repo_dir: PathBuf) -> Self {
        PageServerConf {
            id: NodeId(0),
            wait_lsn_timeout: Duration::from_secs(60),
            wal_redo_timeout: Duration::from_secs(60),
            page_cache_size: defaults::DEFAULT_PAGE_CACHE_SIZE,
            max_file_descriptors: defaults::DEFAULT_MAX_FILE_DESCRIPTORS,
            listen_pg_addr: defaults::DEFAULT_PG_LISTEN_ADDR.to_string(),
            listen_http_addr: defaults::DEFAULT_HTTP_LISTEN_ADDR.to_string(),
            superuser: "zenith_admin".to_string(),
            workdir: repo_dir,
            pg_distrib_dir: PathBuf::new(),
            auth_type: AuthType::Trust,
            auth_validation_public_key_path: None,
            remote_storage_config: None,
            profiling: ProfilingConfig::Disabled,
            default_tenant_conf: TenantConf::dummy_conf(),
            broker_endpoints: Vec::new(),
            broker_etcd_prefix: etcd_broker::DEFAULT_NEON_BROKER_ETCD_PREFIX.to_string(),
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

fn parse_optional_integer<I, E>(name: &str, item: &toml_edit::Item) -> anyhow::Result<Option<I>>
where
    I: TryFrom<i64, Error = E>,
    E: std::error::Error + Send + Sync + 'static,
{
    let toml_integer = match item.get(name) {
        Some(item) => item
            .as_integer()
            .with_context(|| format!("configure option {name} is not an integer"))?,
        None => return Ok(None),
    };

    I::try_from(toml_integer)
        .map(Some)
        .with_context(|| format!("configure option {name} is too large"))
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

fn parse_toml_array(name: &str, item: &Item) -> anyhow::Result<Vec<String>> {
    let array = item
        .as_array()
        .with_context(|| format!("configure option {name} is not an array"))?;

    array
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .with_context(|| format!("Array item {value:?} for key {name} is not a string"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::{tempdir, TempDir};

    use super::*;

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

"#;

    #[test]
    fn parse_defaults() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;
        let broker_endpoint = "http://127.0.0.1:7777";
        // we have to create dummy values to overcome the validation errors
        let config_string = format!(
            "pg_distrib_dir='{}'\nid=10\nbroker_endpoints = ['{broker_endpoint}']",
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
                profiling: ProfilingConfig::Disabled,
                default_tenant_conf: TenantConf::default(),
                broker_endpoints: vec![broker_endpoint
                    .parse()
                    .expect("Failed to parse a valid broker endpoint URL")],
                broker_etcd_prefix: etcd_broker::DEFAULT_NEON_BROKER_ETCD_PREFIX.to_string(),
            },
            "Correct defaults should be used when no config values are provided"
        );

        Ok(())
    }

    #[test]
    fn parse_basic_config() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;
        let broker_endpoint = "http://127.0.0.1:7777";

        let config_string = format!(
            "{ALL_BASE_VALUES_TOML}pg_distrib_dir='{}'\nbroker_endpoints = ['{broker_endpoint}']",
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
                profiling: ProfilingConfig::Disabled,
                default_tenant_conf: TenantConf::default(),
                broker_endpoints: vec![broker_endpoint
                    .parse()
                    .expect("Failed to parse a valid broker endpoint URL")],
                broker_etcd_prefix: etcd_broker::DEFAULT_NEON_BROKER_ETCD_PREFIX.to_string(),
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
broker_endpoints = ['{broker_endpoint}']

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
broker_endpoints = ['{broker_endpoint}']

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

    fn prepare_fs(tempdir: &TempDir) -> anyhow::Result<(PathBuf, PathBuf)> {
        let tempdir_path = tempdir.path();

        let workdir = tempdir_path.join("workdir");
        fs::create_dir_all(&workdir)?;

        let pg_distrib_dir = tempdir_path.join("pg_distrib");
        fs::create_dir_all(&pg_distrib_dir)?;
        let postgres_bin_dir = pg_distrib_dir.join("bin");
        fs::create_dir_all(&postgres_bin_dir)?;
        fs::write(postgres_bin_dir.join("postgres"), "I'm postgres, trust me")?;

        Ok((workdir, pg_distrib_dir))
    }
}
