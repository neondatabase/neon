//! This module is responsible for locating and loading paths in a local setup.
//!
//! Now it also provides init method which acts like a stub for proper installation
//! script which will use local paths.

use anyhow::{bail, ensure, Context};

use postgres_backend::AuthType;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use utils::{
    auth::{encode_from_key_file, Claims},
    id::{NodeId, TenantId, TenantTimelineId, TimelineId},
};

use crate::safekeeper::SafekeeperNode;

pub const DEFAULT_PG_VERSION: u32 = 15;

//
// This data structures represents neon_local CLI config
//
// It is deserialized from the .neon/config file, or the config file passed
// to 'neon_local init --config=<path>' option. See control_plane/simple.conf for
// an example.
//
#[serde_as]
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct LocalEnv {
    // Base directory for all the nodes (the pageserver, safekeepers and
    // compute endpoints).
    //
    // This is not stored in the config file. Rather, this is the path where the
    // config file itself is. It is read from the NEON_REPO_DIR env variable or
    // '.neon' if not given.
    #[serde(skip)]
    pub base_data_dir: PathBuf,

    // Path to postgres distribution. It's expected that "bin", "include",
    // "lib", "share" from postgres distribution are there. If at some point
    // in time we will be able to run against vanilla postgres we may split that
    // to four separate paths and match OS-specific installation layout.
    #[serde(default)]
    pub pg_distrib_dir: PathBuf,

    // Path to pageserver binary.
    #[serde(default)]
    pub neon_distrib_dir: PathBuf,

    // Default tenant ID to use with the 'neon_local' command line utility, when
    // --tenant_id is not explicitly specified.
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub default_tenant_id: Option<TenantId>,

    // used to issue tokens during e.g pg start
    #[serde(default)]
    pub private_key_path: PathBuf,

    pub broker: NeonBroker,

    pub pageserver: PageServerConf,

    #[serde(default)]
    pub safekeepers: Vec<SafekeeperConf>,

    /// Keep human-readable aliases in memory (and persist them to config), to hide ZId hex strings from the user.
    #[serde(default)]
    // A `HashMap<String, HashMap<TenantId, TimelineId>>` would be more appropriate here,
    // but deserialization into a generic toml object as `toml::Value::try_from` fails with an error.
    // https://toml.io/en/v1.0.0 does not contain a concept of "a table inside another table".
    #[serde_as(as = "HashMap<_, Vec<(DisplayFromStr, DisplayFromStr)>>")]
    branch_name_mappings: HashMap<String, Vec<(TenantId, TimelineId)>>,
}

/// Broker config for cluster internal communication.
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
#[serde(default)]
pub struct NeonBroker {
    /// Broker listen address for storage nodes coordination, e.g. '127.0.0.1:50051'.
    pub listen_addr: SocketAddr,
}

// Dummy Default impl to satisfy Deserialize derive.
impl Default for NeonBroker {
    fn default() -> Self {
        NeonBroker {
            listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
        }
    }
}

impl NeonBroker {
    pub fn client_url(&self) -> Url {
        Url::parse(&format!("http://{}", self.listen_addr)).expect("failed to construct url")
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
#[serde(default)]
pub struct PageServerConf {
    // node id
    pub id: NodeId,

    // Pageserver connection settings
    pub listen_pg_addr: String,
    pub listen_http_addr: String,

    // auth type used for the PG and HTTP ports
    pub pg_auth_type: AuthType,
    pub http_auth_type: AuthType,
}

impl Default for PageServerConf {
    fn default() -> Self {
        Self {
            id: NodeId(0),
            listen_pg_addr: String::new(),
            listen_http_addr: String::new(),
            pg_auth_type: AuthType::Trust,
            http_auth_type: AuthType::Trust,
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
    pub sync: bool,
    pub remote_storage: Option<String>,
    pub backup_threads: Option<u32>,
    pub auth_enabled: bool,
}

impl Default for SafekeeperConf {
    fn default() -> Self {
        Self {
            id: NodeId(0),
            pg_port: 0,
            pg_tenant_only_port: None,
            http_port: 0,
            sync: true,
            remote_storage: None,
            backup_threads: None,
            auth_enabled: false,
        }
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

    pub fn pageserver_bin(&self) -> PathBuf {
        self.neon_distrib_dir.join("pageserver")
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

    // TODO: move pageserver files into ./pageserver
    pub fn pageserver_data_dir(&self) -> PathBuf {
        self.base_data_dir.clone()
    }

    pub fn safekeeper_data_dir(&self, data_dir_name: &str) -> PathBuf {
        self.base_data_dir.join("safekeepers").join(data_dir_name)
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
                bail!("branch '{branch_name}' is already mapped to timeline {old_timeline_id}, cannot map to another timeline {timeline_id}");
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
            .map(TimelineId::from)
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

    /// Create a LocalEnv from a config file.
    ///
    /// Unlike 'load_config', this function fills in any defaults that are missing
    /// from the config file.
    pub fn parse_config(toml: &str) -> anyhow::Result<Self> {
        let mut env: LocalEnv = toml::from_str(toml)?;

        // Find postgres binaries.
        // Follow POSTGRES_DISTRIB_DIR if set, otherwise look in "pg_install".
        // Note that later in the code we assume, that distrib dirs follow the same pattern
        // for all postgres versions.
        if env.pg_distrib_dir == Path::new("") {
            if let Some(postgres_bin) = env::var_os("POSTGRES_DISTRIB_DIR") {
                env.pg_distrib_dir = postgres_bin.into();
            } else {
                let cwd = env::current_dir()?;
                env.pg_distrib_dir = cwd.join("pg_install")
            }
        }

        // Find neon binaries.
        if env.neon_distrib_dir == Path::new("") {
            env.neon_distrib_dir = env::current_exe()?.parent().unwrap().to_owned();
        }

        env.base_data_dir = base_path();

        Ok(env)
    }

    /// Locate and load config
    pub fn load_config() -> anyhow::Result<Self> {
        let repopath = base_path();

        if !repopath.exists() {
            bail!(
                "Neon config is not found in {}. You need to run 'neon_local init' first",
                repopath.to_str().unwrap()
            );
        }

        // TODO: check that it looks like a neon repository

        // load and parse file
        let config = fs::read_to_string(repopath.join("config"))?;
        let mut env: LocalEnv = toml::from_str(config.as_str())?;

        env.base_data_dir = repopath;

        Ok(env)
    }

    pub fn persist_config(&self, base_path: &Path) -> anyhow::Result<()> {
        // Currently, the user first passes a config file with 'neon_local init --config=<path>'
        // We read that in, in `create_config`, and fill any missing defaults. Then it's saved
        // to .neon/config. TODO: We lose any formatting and comments along the way, which is
        // a bit sad.
        let mut conf_content = r#"# This file describes a locale deployment of the page server
# and safekeeeper node. It is read by the 'neon_local' command-line
# utility.
"#
        .to_string();

        // Convert the LocalEnv to a toml file.
        //
        // This could be as simple as this:
        //
        // conf_content += &toml::to_string_pretty(env)?;
        //
        // But it results in a "values must be emitted before tables". I'm not sure
        // why, AFAICS the table, i.e. 'safekeepers: Vec<SafekeeperConf>' is last.
        // Maybe rust reorders the fields to squeeze avoid padding or something?
        // In any case, converting to toml::Value first, and serializing that, works.
        // See https://github.com/alexcrichton/toml-rs/issues/142
        conf_content += &toml::to_string_pretty(&toml::Value::try_from(self)?)?;

        let target_config_path = base_path.join("config");
        fs::write(&target_config_path, conf_content).with_context(|| {
            format!(
                "Failed to write config file into path '{}'",
                target_config_path.display()
            )
        })
    }

    // this function is used only for testing purposes in CLI e g generate tokens during init
    pub fn generate_auth_token(&self, claims: &Claims) -> anyhow::Result<String> {
        let private_key_path = if self.private_key_path.is_absolute() {
            self.private_key_path.to_path_buf()
        } else {
            self.base_data_dir.join(&self.private_key_path)
        };

        let key_data = fs::read(private_key_path)?;
        encode_from_key_file(claims, &key_data)
    }

    //
    // Initialize a new Neon repository
    //
    pub fn init(&mut self, pg_version: u32, force: bool) -> anyhow::Result<()> {
        // check if config already exists
        let base_path = &self.base_data_dir;
        ensure!(
            base_path != Path::new(""),
            "repository base path is missing"
        );

        if base_path.exists() {
            if force {
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
            } else {
                bail!(
                    "directory '{}' already exists. Perhaps already initialized? (Hint: use --force to remove all contents)",
                    base_path.display()
                );
            }
        }

        if !self.pg_bin_dir(pg_version)?.join("postgres").exists() {
            bail!(
                "Can't find postgres binary at {}",
                self.pg_bin_dir(pg_version)?.display()
            );
        }
        for binary in ["pageserver", "safekeeper"] {
            if !self.neon_distrib_dir.join(binary).exists() {
                bail!(
                    "Can't find binary '{binary}' in neon distrib dir '{}'",
                    self.neon_distrib_dir.display()
                );
            }
        }

        if !base_path.exists() {
            fs::create_dir(base_path)?;
        }

        // Generate keypair for JWT.
        //
        // The keypair is only needed if authentication is enabled in any of the
        // components. For convenience, we generate the keypair even if authentication
        // is not enabled, so that you can easily enable it after the initialization
        // step. However, if the key generation fails, we treat it as non-fatal if
        // authentication was not enabled.
        if self.private_key_path == PathBuf::new() {
            match generate_auth_keys(
                base_path.join("auth_private_key.pem").as_path(),
                base_path.join("auth_public_key.pem").as_path(),
            ) {
                Ok(()) => {
                    self.private_key_path = PathBuf::from("auth_private_key.pem");
                }
                Err(e) => {
                    if !self.auth_keys_needed() {
                        eprintln!("Could not generate keypair for JWT authentication: {e}");
                        eprintln!("Continuing anyway because authentication was not enabled");
                        self.private_key_path = PathBuf::from("auth_private_key.pem");
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        fs::create_dir_all(self.endpoints_path())?;

        for safekeeper in &self.safekeepers {
            fs::create_dir_all(SafekeeperNode::datadir_path_by_id(self, safekeeper.id))?;
        }

        self.persist_config(base_path)
    }

    fn auth_keys_needed(&self) -> bool {
        self.pageserver.pg_auth_type == AuthType::NeonJWT
            || self.pageserver.http_auth_type == AuthType::NeonJWT
            || self.safekeepers.iter().any(|sk| sk.auth_enabled)
    }
}

fn base_path() -> PathBuf {
    match std::env::var_os("NEON_REPO_DIR") {
        Some(val) => PathBuf::from(val),
        None => PathBuf::from(".neon"),
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_conf_parsing() {
        let simple_conf_toml = include_str!("../simple.conf");
        let simple_conf_parse_result = LocalEnv::parse_config(simple_conf_toml);
        assert!(
            simple_conf_parse_result.is_ok(),
            "failed to parse simple config {simple_conf_toml}, reason: {simple_conf_parse_result:?}"
        );

        let string_to_replace = "listen_addr = '127.0.0.1:50051'";
        let spoiled_url_str = "listen_addr = '!@$XOXO%^&'";
        let spoiled_url_toml = simple_conf_toml.replace(string_to_replace, spoiled_url_str);
        assert!(
            spoiled_url_toml.contains(spoiled_url_str),
            "Failed to replace string {string_to_replace} in the toml file {simple_conf_toml}"
        );
        let spoiled_url_parse_result = LocalEnv::parse_config(&spoiled_url_toml);
        assert!(
            spoiled_url_parse_result.is_err(),
            "expected toml with invalid Url {spoiled_url_toml} to fail the parsing, but got {spoiled_url_parse_result:?}"
        );
    }
}
