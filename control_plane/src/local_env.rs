//! This module is responsible for locating and loading paths in a local setup.
//!
//! Now it also provides init method which acts like a stub for proper installation
//! script which will use local paths.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;
use std::{env, fs};

use anyhow::{Context, bail};
use clap::ValueEnum;
use pem::Pem;
use postgres_backend::AuthType;
use reqwest::{Certificate, Url};
use serde::{Deserialize, Serialize};
use utils::auth::encode_from_key_file;
use utils::id::{NodeId, TenantId, TenantTimelineId, TimelineId};

use crate::broker::StorageBroker;
use crate::endpoint_storage::{
    ENDPOINT_STORAGE_DEFAULT_ADDR, ENDPOINT_STORAGE_REMOTE_STORAGE_DIR, EndpointStorage,
};
use crate::pageserver::{PAGESERVER_REMOTE_STORAGE_DIR, PageServerNode};
use crate::safekeeper::SafekeeperNode;

pub const DEFAULT_PG_VERSION: u32 = 17;

//
// This data structures represents neon_local CLI config
//
// It is deserialized from the .neon/config file, or the config file passed
// to 'neon_local init --config=<path>' option. See control_plane/simple.conf for
// an example.
//
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct LocalEnv {
    // Base directory for all the nodes (the pageserver, safekeepers and
    // compute endpoints).
    //
    // This is not stored in the config file. Rather, this is the path where the
    // config file itself is. It is read from the NEON_REPO_DIR env variable which
    // must be an absolute path. If the env var is not set, $PWD/.neon is used.
    pub base_data_dir: PathBuf,

    // Path to postgres distribution. It's expected that "bin", "include",
    // "lib", "share" from postgres distribution are there. If at some point
    // in time we will be able to run against vanilla postgres we may split that
    // to four separate paths and match OS-specific installation layout.
    pub pg_distrib_dir: PathBuf,

    // Path to pageserver binary.
    pub neon_distrib_dir: PathBuf,

    // Default tenant ID to use with the 'neon_local' command line utility, when
    // --tenant_id is not explicitly specified.
    pub default_tenant_id: Option<TenantId>,

    // used to issue tokens during e.g pg start
    pub private_key_path: PathBuf,
    /// Path to environment's public key
    pub public_key_path: PathBuf,

    pub broker: NeonBroker,

    // Configuration for the storage controller (1 per neon_local environment)
    pub storage_controller: NeonStorageControllerConf,

    /// This Vec must always contain at least one pageserver
    /// Populdated by [`Self::load_config`] from the individual `pageserver.toml`s.
    /// NB: not used anymore except for informing users that they need to change their `.neon/config`.
    pub pageservers: Vec<PageServerConf>,

    pub safekeepers: Vec<SafekeeperConf>,

    pub endpoint_storage: EndpointStorageConf,

    // Control plane upcall API for pageserver: if None, we will not run storage_controller  If set, this will
    // be propagated into each pageserver's configuration.
    pub control_plane_api: Url,

    // Control plane upcall APIs for storage controller.  If set, this will be propagated into the
    // storage controller's configuration.
    pub control_plane_hooks_api: Option<Url>,

    /// Keep human-readable aliases in memory (and persist them to config), to hide ZId hex strings from the user.
    // A `HashMap<String, HashMap<TenantId, TimelineId>>` would be more appropriate here,
    // but deserialization into a generic toml object as `toml::Value::try_from` fails with an error.
    // https://toml.io/en/v1.0.0 does not contain a concept of "a table inside another table".
    pub branch_name_mappings: HashMap<String, Vec<(TenantId, TimelineId)>>,

    /// Flag to generate SSL certificates for components that need it.
    /// Also generates root CA certificate that is used to sign all other certificates.
    pub generate_local_ssl_certs: bool,
}

/// On-disk state stored in `.neon/config`.
#[derive(PartialEq, Eq, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OnDiskConfig {
    pub pg_distrib_dir: PathBuf,
    pub neon_distrib_dir: PathBuf,
    pub default_tenant_id: Option<TenantId>,
    pub private_key_path: PathBuf,
    pub public_key_path: PathBuf,
    pub broker: NeonBroker,
    pub storage_controller: NeonStorageControllerConf,
    #[serde(
        skip_serializing,
        deserialize_with = "fail_if_pageservers_field_specified"
    )]
    pub pageservers: Vec<PageServerConf>,
    pub safekeepers: Vec<SafekeeperConf>,
    pub endpoint_storage: EndpointStorageConf,
    pub control_plane_api: Option<Url>,
    pub control_plane_hooks_api: Option<Url>,
    pub control_plane_compute_hook_api: Option<Url>,
    branch_name_mappings: HashMap<String, Vec<(TenantId, TimelineId)>>,
    // Note: skip serializing because in compat tests old storage controller fails
    // to load new config file. May be removed after this field is in release branch.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub generate_local_ssl_certs: bool,
}

fn fail_if_pageservers_field_specified<'de, D>(_: D) -> Result<Vec<PageServerConf>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Err(serde::de::Error::custom(
        "The 'pageservers' field is no longer used; pageserver.toml is now authoritative; \
         Please remove the `pageservers` from your .neon/config.",
    ))
}

/// The description of the neon_local env to be initialized by `neon_local init --config`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NeonLocalInitConf {
    // TODO: do we need this? Seems unused
    pub pg_distrib_dir: Option<PathBuf>,
    // TODO: do we need this? Seems unused
    pub neon_distrib_dir: Option<PathBuf>,
    pub default_tenant_id: TenantId,
    pub broker: NeonBroker,
    pub storage_controller: Option<NeonStorageControllerConf>,
    pub pageservers: Vec<NeonLocalInitPageserverConf>,
    pub safekeepers: Vec<SafekeeperConf>,
    pub endpoint_storage: EndpointStorageConf,
    pub control_plane_api: Option<Url>,
    pub control_plane_hooks_api: Option<Url>,
    pub generate_local_ssl_certs: bool,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
#[serde(default)]
pub struct EndpointStorageConf {
    pub listen_addr: SocketAddr,
}

/// Broker config for cluster internal communication.
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, Default)]
#[serde(default)]
pub struct NeonBroker {
    /// Broker listen HTTP address for storage nodes coordination, e.g. '127.0.0.1:50051'.
    /// At least one of listen_addr or listen_https_addr must be set.
    pub listen_addr: Option<SocketAddr>,
    /// Broker listen HTTPS address for storage nodes coordination, e.g. '127.0.0.1:50051'.
    /// At least one of listen_addr or listen_https_addr must be set.
    /// listen_https_addr is preferred over listen_addr in neon_local.
    pub listen_https_addr: Option<SocketAddr>,
}

/// A part of storage controller's config the neon_local knows about.
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
#[serde(default)]
pub struct NeonStorageControllerConf {
    /// Heartbeat timeout before marking a node offline
    #[serde(with = "humantime_serde")]
    pub max_offline: Duration,

    #[serde(with = "humantime_serde")]
    pub max_warming_up: Duration,

    pub start_as_candidate: bool,

    /// Database url used when running multiple storage controller instances
    pub database_url: Option<SocketAddr>,

    /// Thresholds for auto-splitting a tenant into shards.
    pub split_threshold: Option<u64>,
    pub max_split_shards: Option<u8>,
    pub initial_split_threshold: Option<u64>,
    pub initial_split_shards: Option<u8>,

    pub max_secondary_lag_bytes: Option<u64>,

    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,

    #[serde(with = "humantime_serde")]
    pub long_reconcile_threshold: Option<Duration>,

    pub use_https_pageserver_api: bool,

    pub timelines_onto_safekeepers: bool,

    pub use_https_safekeeper_api: bool,

    pub use_local_compute_notifications: bool,
}

impl NeonStorageControllerConf {
    // Use a shorter pageserver unavailability interval than the default to speed up tests.
    const DEFAULT_MAX_OFFLINE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

    const DEFAULT_MAX_WARMING_UP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

    // Very tight heartbeat interval to speed up tests
    const DEFAULT_HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1000);
}

impl Default for NeonStorageControllerConf {
    fn default() -> Self {
        Self {
            max_offline: Self::DEFAULT_MAX_OFFLINE_INTERVAL,
            max_warming_up: Self::DEFAULT_MAX_WARMING_UP_INTERVAL,
            start_as_candidate: false,
            database_url: None,
            split_threshold: None,
            max_split_shards: None,
            initial_split_threshold: None,
            initial_split_shards: None,
            max_secondary_lag_bytes: None,
            heartbeat_interval: Self::DEFAULT_HEARTBEAT_INTERVAL,
            long_reconcile_threshold: None,
            use_https_pageserver_api: false,
            timelines_onto_safekeepers: true,
            use_https_safekeeper_api: false,
            use_local_compute_notifications: true,
        }
    }
}

impl Default for EndpointStorageConf {
    fn default() -> Self {
        Self {
            listen_addr: ENDPOINT_STORAGE_DEFAULT_ADDR,
        }
    }
}

impl NeonBroker {
    pub fn client_url(&self) -> Url {
        let url = if let Some(addr) = self.listen_https_addr {
            format!("https://{}", addr)
        } else {
            format!(
                "http://{}",
                self.listen_addr
                    .expect("at least one address should be set")
            )
        };

        Url::parse(&url).expect("failed to construct url")
    }
}

// neon_local needs to know this subset of pageserver configuration.
// For legacy reasons, this information is duplicated from `pageserver.toml` into `.neon/config`.
// It can get stale if `pageserver.toml` is changed.
// TODO(christian): don't store this at all in `.neon/config`, always load it from `pageserver.toml`
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
#[serde(default, deny_unknown_fields)]
pub struct PageServerConf {
    pub id: NodeId,
    pub listen_pg_addr: String,
    pub listen_http_addr: String,
    pub listen_https_addr: Option<String>,
    pub pg_auth_type: AuthType,
    pub http_auth_type: AuthType,
    pub no_sync: bool,
}

impl Default for PageServerConf {
    fn default() -> Self {
        Self {
            id: NodeId(0),
            listen_pg_addr: String::new(),
            listen_http_addr: String::new(),
            listen_https_addr: None,
            pg_auth_type: AuthType::Trust,
            http_auth_type: AuthType::Trust,
            no_sync: false,
        }
    }
}

/// The toml that can be passed to `neon_local init --config`.
/// This is a subset of the `pageserver.toml` configuration.
// TODO(christian): use pageserver_api::config::ConfigToml (PR #7656)
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct NeonLocalInitPageserverConf {
    pub id: NodeId,
    pub listen_pg_addr: String,
    pub listen_http_addr: String,
    pub listen_https_addr: Option<String>,
    pub pg_auth_type: AuthType,
    pub http_auth_type: AuthType,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub no_sync: bool,
    #[serde(flatten)]
    pub other: HashMap<String, toml::Value>,
}

impl From<&NeonLocalInitPageserverConf> for PageServerConf {
    fn from(conf: &NeonLocalInitPageserverConf) -> Self {
        let NeonLocalInitPageserverConf {
            id,
            listen_pg_addr,
            listen_http_addr,
            listen_https_addr,
            pg_auth_type,
            http_auth_type,
            no_sync,
            other: _,
        } = conf;
        Self {
            id: *id,
            listen_pg_addr: listen_pg_addr.clone(),
            listen_http_addr: listen_http_addr.clone(),
            listen_https_addr: listen_https_addr.clone(),
            pg_auth_type: *pg_auth_type,
            http_auth_type: *http_auth_type,
            no_sync: *no_sync,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
#[serde(default)]
pub struct SafekeeperConf {
    pub id: NodeId,
    pub pg_port: u16,
    pub pg_tenant_only_port: Option<u16>,
    pub http_port: u16,
    pub https_port: Option<u16>,
    pub sync: bool,
    pub remote_storage: Option<String>,
    pub backup_threads: Option<u32>,
    pub auth_enabled: bool,
    pub listen_addr: Option<String>,
}

impl Default for SafekeeperConf {
    fn default() -> Self {
        Self {
            id: NodeId(0),
            pg_port: 0,
            pg_tenant_only_port: None,
            http_port: 0,
            https_port: None,
            sync: true,
            remote_storage: None,
            backup_threads: None,
            auth_enabled: false,
            listen_addr: None,
        }
    }
}

#[derive(Clone, Copy)]
pub enum InitForceMode {
    MustNotExist,
    EmptyDirOk,
    RemoveAllContents,
}

impl ValueEnum for InitForceMode {
    fn value_variants<'a>() -> &'a [Self] {
        &[
            Self::MustNotExist,
            Self::EmptyDirOk,
            Self::RemoveAllContents,
        ]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(clap::builder::PossibleValue::new(match self {
            InitForceMode::MustNotExist => "must-not-exist",
            InitForceMode::EmptyDirOk => "empty-dir-ok",
            InitForceMode::RemoveAllContents => "remove-all-contents",
        }))
    }
}

impl SafekeeperConf {
    /// Compute is served by port on which only tenant scoped tokens allowed, if
    /// it is configured.
    pub fn get_compute_port(&self) -> u16 {
        self.pg_tenant_only_port.unwrap_or(self.pg_port)
    }
}

impl LocalEnv {
    pub fn pg_distrib_dir_raw(&self) -> PathBuf {
        self.pg_distrib_dir.clone()
    }

    pub fn pg_distrib_dir(&self, pg_version: u32) -> anyhow::Result<PathBuf> {
        let path = self.pg_distrib_dir.clone();

        #[allow(clippy::manual_range_patterns)]
        match pg_version {
            14 | 15 | 16 | 17 => Ok(path.join(format!("v{pg_version}"))),
            _ => bail!("Unsupported postgres version: {}", pg_version),
        }
    }

    pub fn pg_dir(&self, pg_version: u32, dir_name: &str) -> anyhow::Result<PathBuf> {
        Ok(self.pg_distrib_dir(pg_version)?.join(dir_name))
    }

    pub fn pg_bin_dir(&self, pg_version: u32) -> anyhow::Result<PathBuf> {
        self.pg_dir(pg_version, "bin")
    }

    pub fn pg_lib_dir(&self, pg_version: u32) -> anyhow::Result<PathBuf> {
        self.pg_dir(pg_version, "lib")
    }

    pub fn endpoint_storage_bin(&self) -> PathBuf {
        self.neon_distrib_dir.join("endpoint_storage")
    }

    pub fn pageserver_bin(&self) -> PathBuf {
        self.neon_distrib_dir.join("pageserver")
    }

    pub fn storage_controller_bin(&self) -> PathBuf {
        // Irrespective of configuration, storage controller binary is always
        // run from the same location as neon_local.  This means that for compatibility
        // tests that run old pageserver/safekeeper, they still run latest storage controller.
        let neon_local_bin_dir = env::current_exe().unwrap().parent().unwrap().to_owned();
        neon_local_bin_dir.join("storage_controller")
    }

    pub fn safekeeper_bin(&self) -> PathBuf {
        self.neon_distrib_dir.join("safekeeper")
    }

    pub fn storage_broker_bin(&self) -> PathBuf {
        self.neon_distrib_dir.join("storage_broker")
    }

    pub fn endpoints_path(&self) -> PathBuf {
        self.base_data_dir.join("endpoints")
    }

    pub fn storage_broker_data_dir(&self) -> PathBuf {
        self.base_data_dir.join("storage_broker")
    }

    pub fn pageserver_data_dir(&self, pageserver_id: NodeId) -> PathBuf {
        self.base_data_dir
            .join(format!("pageserver_{pageserver_id}"))
    }

    pub fn safekeeper_data_dir(&self, data_dir_name: &str) -> PathBuf {
        self.base_data_dir.join("safekeepers").join(data_dir_name)
    }

    pub fn endpoint_storage_data_dir(&self) -> PathBuf {
        self.base_data_dir.join("endpoint_storage")
    }

    pub fn get_pageserver_conf(&self, id: NodeId) -> anyhow::Result<&PageServerConf> {
        if let Some(conf) = self.pageservers.iter().find(|node| node.id == id) {
            Ok(conf)
        } else {
            let have_ids = self
                .pageservers
                .iter()
                .map(|node| format!("{}:{}", node.id, node.listen_http_addr))
                .collect::<Vec<_>>();
            let joined = have_ids.join(",");
            bail!("could not find pageserver {id}, have ids {joined}")
        }
    }

    pub fn ssl_ca_cert_path(&self) -> Option<PathBuf> {
        if self.generate_local_ssl_certs {
            Some(self.base_data_dir.join("rootCA.crt"))
        } else {
            None
        }
    }

    pub fn ssl_ca_key_path(&self) -> Option<PathBuf> {
        if self.generate_local_ssl_certs {
            Some(self.base_data_dir.join("rootCA.key"))
        } else {
            None
        }
    }

    pub fn generate_ssl_ca_cert(&self) -> anyhow::Result<()> {
        let cert_path = self.ssl_ca_cert_path().unwrap();
        let key_path = self.ssl_ca_key_path().unwrap();
        if !fs::exists(cert_path.as_path())? {
            generate_ssl_ca_cert(cert_path.as_path(), key_path.as_path())?;
        }
        Ok(())
    }

    pub fn generate_ssl_cert(&self, cert_path: &Path, key_path: &Path) -> anyhow::Result<()> {
        self.generate_ssl_ca_cert()?;
        generate_ssl_cert(
            cert_path,
            key_path,
            self.ssl_ca_cert_path().unwrap().as_path(),
            self.ssl_ca_key_path().unwrap().as_path(),
        )
    }

    /// Creates HTTP client with local SSL CA certificates.
    pub fn create_http_client(&self) -> reqwest::Client {
        let ssl_ca_certs = self.ssl_ca_cert_path().map(|ssl_ca_file| {
            let buf = std::fs::read(ssl_ca_file).expect("SSL CA file should exist");
            Certificate::from_pem_bundle(&buf).expect("SSL CA file should be valid")
        });

        let mut http_client = reqwest::Client::builder();
        for ssl_ca_cert in ssl_ca_certs.unwrap_or_default() {
            http_client = http_client.add_root_certificate(ssl_ca_cert);
        }

        http_client
            .build()
            .expect("HTTP client should construct with no error")
    }

    /// Inspect the base data directory and extract the instance id and instance directory path
    /// for all storage controller instances
    pub async fn storage_controller_instances(&self) -> std::io::Result<Vec<(u8, PathBuf)>> {
        let mut instances = Vec::default();

        let dir = std::fs::read_dir(self.base_data_dir.clone())?;
        for dentry in dir {
            let dentry = dentry?;
            let is_dir = dentry.metadata()?.is_dir();
            let filename = dentry.file_name().into_string().unwrap();
            let parsed_instance_id = match filename.strip_prefix("storage_controller_") {
                Some(suffix) => suffix.parse::<u8>().ok(),
                None => None,
            };

            let is_instance_dir = is_dir && parsed_instance_id.is_some();

            if !is_instance_dir {
                continue;
            }

            instances.push((
                parsed_instance_id.expect("Checked previously"),
                dentry.path(),
            ));
        }

        Ok(instances)
    }

    pub fn register_branch_mapping(
        &mut self,
        branch_name: String,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> anyhow::Result<()> {
        let existing_values = self
            .branch_name_mappings
            .entry(branch_name.clone())
            .or_default();

        let existing_ids = existing_values
            .iter()
            .find(|(existing_tenant_id, _)| existing_tenant_id == &tenant_id);

        if let Some((_, old_timeline_id)) = existing_ids {
            if old_timeline_id == &timeline_id {
                Ok(())
            } else {
                bail!(
                    "branch '{branch_name}' is already mapped to timeline {old_timeline_id}, cannot map to another timeline {timeline_id}"
                );
            }
        } else {
            existing_values.push((tenant_id, timeline_id));
            Ok(())
        }
    }

    pub fn get_branch_timeline_id(
        &self,
        branch_name: &str,
        tenant_id: TenantId,
    ) -> Option<TimelineId> {
        self.branch_name_mappings
            .get(branch_name)?
            .iter()
            .find(|(mapped_tenant_id, _)| mapped_tenant_id == &tenant_id)
            .map(|&(_, timeline_id)| timeline_id)
    }

    pub fn timeline_name_mappings(&self) -> HashMap<TenantTimelineId, String> {
        self.branch_name_mappings
            .iter()
            .flat_map(|(name, tenant_timelines)| {
                tenant_timelines.iter().map(|&(tenant_id, timeline_id)| {
                    (TenantTimelineId::new(tenant_id, timeline_id), name.clone())
                })
            })
            .collect()
    }

    ///  Construct `Self` from on-disk state.
    pub fn load_config(repopath: &Path) -> anyhow::Result<Self> {
        if !repopath.exists() {
            bail!(
                "Neon config is not found in {}. You need to run 'neon_local init' first",
                repopath.to_str().unwrap()
            );
        }

        // TODO: check that it looks like a neon repository

        // load and parse file
        let config_file_contents = fs::read_to_string(repopath.join("config"))?;
        let on_disk_config: OnDiskConfig = toml::from_str(config_file_contents.as_str())?;
        let mut env = {
            let OnDiskConfig {
                pg_distrib_dir,
                neon_distrib_dir,
                default_tenant_id,
                private_key_path,
                public_key_path,
                broker,
                storage_controller,
                pageservers,
                safekeepers,
                control_plane_api,
                control_plane_hooks_api,
                control_plane_compute_hook_api: _,
                branch_name_mappings,
                generate_local_ssl_certs,
                endpoint_storage,
            } = on_disk_config;
            LocalEnv {
                base_data_dir: repopath.to_owned(),
                pg_distrib_dir,
                neon_distrib_dir,
                default_tenant_id,
                private_key_path,
                public_key_path,
                broker,
                storage_controller,
                pageservers,
                safekeepers,
                control_plane_api: control_plane_api.unwrap(),
                control_plane_hooks_api,
                branch_name_mappings,
                generate_local_ssl_certs,
                endpoint_storage,
            }
        };

        // The source of truth for pageserver configuration is the pageserver.toml.
        assert!(
            env.pageservers.is_empty(),
            "we ensure this during deserialization"
        );
        env.pageservers = {
            let iter = std::fs::read_dir(repopath).context("open dir")?;
            let mut pageservers = Vec::new();
            for res in iter {
                let dentry = res?;
                const PREFIX: &str = "pageserver_";
                let dentry_name = dentry
                    .file_name()
                    .into_string()
                    .ok()
                    .with_context(|| format!("non-utf8 dentry: {:?}", dentry.path()))
                    .unwrap();
                if !dentry_name.starts_with(PREFIX) {
                    continue;
                }
                if !dentry.file_type().context("determine file type")?.is_dir() {
                    anyhow::bail!("expected a directory, got {:?}", dentry.path());
                }
                let id = dentry_name[PREFIX.len()..]
                    .parse::<NodeId>()
                    .with_context(|| format!("parse id from {:?}", dentry.path()))?;
                // TODO(christian): use pageserver_api::config::ConfigToml (PR #7656)
                #[derive(serde::Serialize, serde::Deserialize)]
                // (allow unknown fields, unlike PageServerConf)
                struct PageserverConfigTomlSubset {
                    listen_pg_addr: String,
                    listen_http_addr: String,
                    listen_https_addr: Option<String>,
                    pg_auth_type: AuthType,
                    http_auth_type: AuthType,
                    #[serde(default)]
                    no_sync: bool,
                }
                let config_toml_path = dentry.path().join("pageserver.toml");
                let config_toml: PageserverConfigTomlSubset = toml_edit::de::from_str(
                    &std::fs::read_to_string(&config_toml_path)
                        .with_context(|| format!("read {:?}", config_toml_path))?,
                )
                .context("parse pageserver.toml")?;
                let identity_toml_path = dentry.path().join("identity.toml");
                #[derive(serde::Serialize, serde::Deserialize)]
                struct IdentityTomlSubset {
                    id: NodeId,
                }
                let identity_toml: IdentityTomlSubset = toml_edit::de::from_str(
                    &std::fs::read_to_string(&identity_toml_path)
                        .with_context(|| format!("read {:?}", identity_toml_path))?,
                )
                .context("parse identity.toml")?;
                let PageserverConfigTomlSubset {
                    listen_pg_addr,
                    listen_http_addr,
                    listen_https_addr,
                    pg_auth_type,
                    http_auth_type,
                    no_sync,
                } = config_toml;
                let IdentityTomlSubset {
                    id: identity_toml_id,
                } = identity_toml;
                let conf = PageServerConf {
                    id: {
                        anyhow::ensure!(
                            identity_toml_id == id,
                            "id mismatch: identity.toml:id={identity_toml_id} pageserver_(.*) id={id}",
                        );
                        id
                    },
                    listen_pg_addr,
                    listen_http_addr,
                    listen_https_addr,
                    pg_auth_type,
                    http_auth_type,
                    no_sync,
                };
                pageservers.push(conf);
            }
            pageservers
        };

        Ok(env)
    }

    pub fn persist_config(&self) -> anyhow::Result<()> {
        Self::persist_config_impl(
            &self.base_data_dir,
            &OnDiskConfig {
                pg_distrib_dir: self.pg_distrib_dir.clone(),
                neon_distrib_dir: self.neon_distrib_dir.clone(),
                default_tenant_id: self.default_tenant_id,
                private_key_path: self.private_key_path.clone(),
                public_key_path: self.public_key_path.clone(),
                broker: self.broker.clone(),
                storage_controller: self.storage_controller.clone(),
                pageservers: vec![], // it's skip_serializing anyway
                safekeepers: self.safekeepers.clone(),
                control_plane_api: Some(self.control_plane_api.clone()),
                control_plane_hooks_api: self.control_plane_hooks_api.clone(),
                control_plane_compute_hook_api: None,
                branch_name_mappings: self.branch_name_mappings.clone(),
                generate_local_ssl_certs: self.generate_local_ssl_certs,
                endpoint_storage: self.endpoint_storage.clone(),
            },
        )
    }

    pub fn persist_config_impl(base_path: &Path, config: &OnDiskConfig) -> anyhow::Result<()> {
        let conf_content = &toml::to_string_pretty(config)?;
        let target_config_path = base_path.join("config");
        fs::write(&target_config_path, conf_content).with_context(|| {
            format!(
                "Failed to write config file into path '{}'",
                target_config_path.display()
            )
        })
    }

    // this function is used only for testing purposes in CLI e g generate tokens during init
    pub fn generate_auth_token<S: Serialize>(&self, claims: &S) -> anyhow::Result<String> {
        let key = self.read_private_key()?;
        encode_from_key_file(claims, &key)
    }

    /// Get the path to the private key.
    pub fn get_private_key_path(&self) -> PathBuf {
        if self.private_key_path.is_absolute() {
            self.private_key_path.to_path_buf()
        } else {
            self.base_data_dir.join(&self.private_key_path)
        }
    }

    /// Get the path to the public key.
    pub fn get_public_key_path(&self) -> PathBuf {
        if self.public_key_path.is_absolute() {
            self.public_key_path.to_path_buf()
        } else {
            self.base_data_dir.join(&self.public_key_path)
        }
    }

    /// Read the contents of the private key file.
    pub fn read_private_key(&self) -> anyhow::Result<Pem> {
        let private_key_path = self.get_private_key_path();
        let pem = pem::parse(fs::read(private_key_path)?)?;
        Ok(pem)
    }

    /// Read the contents of the public key file.
    pub fn read_public_key(&self) -> anyhow::Result<Pem> {
        let public_key_path = self.get_public_key_path();
        let pem = pem::parse(fs::read(public_key_path)?)?;
        Ok(pem)
    }

    /// Materialize the [`NeonLocalInitConf`] to disk. Called during [`neon_local init`].
    pub fn init(conf: NeonLocalInitConf, force: &InitForceMode) -> anyhow::Result<()> {
        let base_path = base_path();
        assert_ne!(base_path, Path::new(""));
        let base_path = &base_path;

        // create base_path dir
        if base_path.exists() {
            match force {
                InitForceMode::MustNotExist => {
                    bail!(
                        "directory '{}' already exists. Perhaps already initialized?",
                        base_path.display()
                    );
                }
                InitForceMode::EmptyDirOk => {
                    if let Some(res) = std::fs::read_dir(base_path)?.next() {
                        res.context("check if directory is empty")?;
                        anyhow::bail!("directory not empty: {base_path:?}");
                    }
                }
                InitForceMode::RemoveAllContents => {
                    println!("removing all contents of '{}'", base_path.display());
                    // instead of directly calling `remove_dir_all`, we keep the original dir but removing
                    // all contents inside. This helps if the developer symbol links another directory (i.e.,
                    // S3 local SSD) to the `.neon` base directory.
                    for entry in std::fs::read_dir(base_path)? {
                        let entry = entry?;
                        let path = entry.path();
                        if path.is_dir() {
                            fs::remove_dir_all(&path)?;
                        } else {
                            fs::remove_file(&path)?;
                        }
                    }
                }
            }
        }
        if !base_path.exists() {
            fs::create_dir(base_path)?;
        }

        let NeonLocalInitConf {
            pg_distrib_dir,
            neon_distrib_dir,
            default_tenant_id,
            broker,
            storage_controller,
            pageservers,
            safekeepers,
            control_plane_api,
            generate_local_ssl_certs,
            control_plane_hooks_api,
            endpoint_storage,
        } = conf;

        // Find postgres binaries.
        // Follow POSTGRES_DISTRIB_DIR if set, otherwise look in "pg_install".
        // Note that later in the code we assume, that distrib dirs follow the same pattern
        // for all postgres versions.
        let pg_distrib_dir = pg_distrib_dir.unwrap_or_else(|| {
            if let Some(postgres_bin) = env::var_os("POSTGRES_DISTRIB_DIR") {
                postgres_bin.into()
            } else {
                let cwd = env::current_dir().unwrap();
                cwd.join("pg_install")
            }
        });

        // Find neon binaries.
        let neon_distrib_dir = neon_distrib_dir
            .unwrap_or_else(|| env::current_exe().unwrap().parent().unwrap().to_owned());

        // Generate keypair for JWT.
        //
        // The keypair is only needed if authentication is enabled in any of the
        // components. For convenience, we generate the keypair even if authentication
        // is not enabled, so that you can easily enable it after the initialization
        // step.
        generate_auth_keys(
            base_path.join("auth_private_key.pem").as_path(),
            base_path.join("auth_public_key.pem").as_path(),
        )
        .context("generate auth keys")?;
        let private_key_path = PathBuf::from("auth_private_key.pem");
        let public_key_path = PathBuf::from("auth_public_key.pem");

        // create the runtime type because the remaining initialization code below needs
        // a LocalEnv instance op operation
        // TODO: refactor to avoid this, LocalEnv should only be constructed from on-disk state
        let env = LocalEnv {
            base_data_dir: base_path.clone(),
            pg_distrib_dir,
            neon_distrib_dir,
            default_tenant_id: Some(default_tenant_id),
            private_key_path,
            public_key_path,
            broker,
            storage_controller: storage_controller.unwrap_or_default(),
            pageservers: pageservers.iter().map(Into::into).collect(),
            safekeepers,
            control_plane_api: control_plane_api.unwrap(),
            control_plane_hooks_api,
            branch_name_mappings: Default::default(),
            generate_local_ssl_certs,
            endpoint_storage,
        };

        if generate_local_ssl_certs {
            env.generate_ssl_ca_cert()?;
        }

        // create endpoints dir
        fs::create_dir_all(env.endpoints_path())?;

        // create storage broker dir
        fs::create_dir_all(env.storage_broker_data_dir())?;
        StorageBroker::from_env(&env)
            .initialize()
            .context("storage broker init failed")?;

        // create safekeeper dirs
        for safekeeper in &env.safekeepers {
            fs::create_dir_all(SafekeeperNode::datadir_path_by_id(&env, safekeeper.id))?;
            SafekeeperNode::from_env(&env, safekeeper)
                .initialize()
                .context("safekeeper init failed")?;
        }

        // initialize pageserver state
        for (i, ps) in pageservers.into_iter().enumerate() {
            let runtime_ps = &env.pageservers[i];
            assert_eq!(&PageServerConf::from(&ps), runtime_ps);
            fs::create_dir(env.pageserver_data_dir(ps.id))?;
            PageServerNode::from_env(&env, runtime_ps)
                .initialize(ps)
                .context("pageserver init failed")?;
        }

        EndpointStorage::from_env(&env)
            .init()
            .context("object storage init failed")?;

        // setup remote remote location for default LocalFs remote storage
        std::fs::create_dir_all(env.base_data_dir.join(PAGESERVER_REMOTE_STORAGE_DIR))?;
        std::fs::create_dir_all(env.base_data_dir.join(ENDPOINT_STORAGE_REMOTE_STORAGE_DIR))?;

        env.persist_config()
    }
}

pub fn base_path() -> PathBuf {
    let path = match std::env::var_os("NEON_REPO_DIR") {
        Some(val) => {
            let path = PathBuf::from(val);
            if !path.is_absolute() {
                // repeat the env var in the error because our default is always absolute
                panic!("NEON_REPO_DIR must be an absolute path, got {path:?}");
            }
            path
        }
        None => {
            let pwd = std::env::current_dir()
                // technically this can fail but it's quite unlikeley
                .expect("determine current directory");
            let pwd_abs = pwd.canonicalize().expect("canonicalize current directory");
            pwd_abs.join(".neon")
        }
    };
    assert!(path.is_absolute());
    path
}

/// Generate a public/private key pair for JWT authentication
fn generate_auth_keys(private_key_path: &Path, public_key_path: &Path) -> anyhow::Result<()> {
    // Generate the key pair
    //
    // openssl genpkey -algorithm ed25519 -out auth_private_key.pem
    let keygen_output = Command::new("openssl")
        .arg("genpkey")
        .args(["-algorithm", "ed25519"])
        .args(["-out", private_key_path.to_str().unwrap()])
        .stdout(Stdio::null())
        .output()
        .context("failed to generate auth private key")?;
    if !keygen_output.status.success() {
        bail!(
            "openssl failed: '{}'",
            String::from_utf8_lossy(&keygen_output.stderr)
        );
    }

    // Extract the public key from the private key file
    //
    // openssl pkey -in auth_private_key.pem -pubout -out auth_public_key.pem
    let keygen_output = Command::new("openssl")
        .arg("pkey")
        .args(["-in", private_key_path.to_str().unwrap()])
        .arg("-pubout")
        .args(["-out", public_key_path.to_str().unwrap()])
        .output()
        .context("failed to extract public key from private key")?;
    if !keygen_output.status.success() {
        bail!(
            "openssl failed: '{}'",
            String::from_utf8_lossy(&keygen_output.stderr)
        );
    }

    Ok(())
}

fn generate_ssl_ca_cert(cert_path: &Path, key_path: &Path) -> anyhow::Result<()> {
    // openssl req -x509 -newkey rsa:2048 -nodes -subj "/CN=Neon Local CA" -days 36500 \
    // -out rootCA.crt -keyout rootCA.key
    let keygen_output = Command::new("openssl")
        .args([
            "req", "-x509", "-newkey", "ed25519", "-nodes", "-days", "36500",
        ])
        .args(["-subj", "/CN=Neon Local CA"])
        .args(["-out", cert_path.to_str().unwrap()])
        .args(["-keyout", key_path.to_str().unwrap()])
        .output()
        .context("failed to generate CA certificate")?;
    if !keygen_output.status.success() {
        bail!(
            "openssl failed: '{}'",
            String::from_utf8_lossy(&keygen_output.stderr)
        );
    }
    Ok(())
}

fn generate_ssl_cert(
    cert_path: &Path,
    key_path: &Path,
    ca_cert_path: &Path,
    ca_key_path: &Path,
) -> anyhow::Result<()> {
    // Generate Certificate Signing Request (CSR).
    let mut csr_path = cert_path.to_path_buf();
    csr_path.set_extension(".csr");

    // openssl req -new -nodes -newkey rsa:2048 -keyout server.key -out server.csr \
    // -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
    let keygen_output = Command::new("openssl")
        .args(["req", "-new", "-nodes"])
        .args(["-newkey", "ed25519"])
        .args(["-subj", "/CN=localhost"])
        .args(["-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"])
        .args(["-keyout", key_path.to_str().unwrap()])
        .args(["-out", csr_path.to_str().unwrap()])
        .output()
        .context("failed to generate CSR")?;
    if !keygen_output.status.success() {
        bail!(
            "openssl failed: '{}'",
            String::from_utf8_lossy(&keygen_output.stderr)
        );
    }

    // Sign CSR with CA key.
    //
    // openssl x509 -req -in server.csr -CA rootCA.crt -CAkey rootCA.key -CAcreateserial \
    // -out server.crt -days 36500 -copy_extensions copyall
    let keygen_output = Command::new("openssl")
        .args(["x509", "-req"])
        .args(["-in", csr_path.to_str().unwrap()])
        .args(["-CA", ca_cert_path.to_str().unwrap()])
        .args(["-CAkey", ca_key_path.to_str().unwrap()])
        .arg("-CAcreateserial")
        .args(["-out", cert_path.to_str().unwrap()])
        .args(["-days", "36500"])
        .args(["-copy_extensions", "copyall"])
        .output()
        .context("failed to sign CSR")?;
    if !keygen_output.status.success() {
        bail!(
            "openssl failed: '{}'",
            String::from_utf8_lossy(&keygen_output.stderr)
        );
    }

    // Remove CSR file as it's not needed anymore.
    fs::remove_file(csr_path)?;

    Ok(())
}
