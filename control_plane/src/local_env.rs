//! This module is responsible for locating and loading paths in a local setup.
//!
//! Now it also provides init method which acts like a stub for proper installation
//! script which will use local paths.

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt::Write;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use zenith_utils::auth::{encode_from_key_file, Claims, Scope};
use zenith_utils::postgres_backend::AuthType;
use zenith_utils::zid::{opt_display_serde, ZNodeId, ZTenantId};

use crate::safekeeper::SafekeeperNode;

//
// This data structures represents zenith CLI config
//
// It is deserialized from the .zenith/config file, or the config file passed
// to 'zenith init --config=<path>' option. See control_plane/simple.conf for
// an example.
//
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LocalEnv {
    // Base directory for all the nodes (the pageserver, safekeepers and
    // compute nodes).
    //
    // This is not stored in the config file. Rather, this is the path where the
    // config file itself is. It is read from the ZENITH_REPO_DIR env variable or
    // '.zenith' if not given.
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
    pub zenith_distrib_dir: PathBuf,

    // Default tenant ID to use with the 'zenith' command line utility, when
    // --tenantid is not explicitly specified.
    #[serde(with = "opt_display_serde")]
    #[serde(default)]
    pub default_tenantid: Option<ZTenantId>,

    // used to issue tokens during e.g pg start
    #[serde(default)]
    pub private_key_path: PathBuf,

    pub pageserver: PageServerConf,

    #[serde(default)]
    pub safekeepers: Vec<SafekeeperConf>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct PageServerConf {
    // Pageserver connection settings
    pub listen_pg_addr: String,
    pub listen_http_addr: String,

    // used to determine which auth type is used
    pub auth_type: AuthType,

    // jwt auth token used for communication with pageserver
    pub auth_token: String,
}

impl Default for PageServerConf {
    fn default() -> Self {
        Self {
            listen_pg_addr: String::new(),
            listen_http_addr: String::new(),
            auth_type: AuthType::Trust,
            auth_token: String::new(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct SafekeeperConf {
    pub id: ZNodeId,
    pub pg_port: u16,
    pub http_port: u16,
    pub sync: bool,
}

impl Default for SafekeeperConf {
    fn default() -> Self {
        Self {
            id: ZNodeId(0),
            pg_port: 0,
            http_port: 0,
            sync: true,
        }
    }
}

impl LocalEnv {
    // postgres installation paths
    pub fn pg_bin_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("bin")
    }
    pub fn pg_lib_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("lib")
    }

    pub fn pageserver_bin(&self) -> anyhow::Result<PathBuf> {
        Ok(self.zenith_distrib_dir.join("pageserver"))
    }

    pub fn safekeeper_bin(&self) -> anyhow::Result<PathBuf> {
        Ok(self.zenith_distrib_dir.join("safekeeper"))
    }

    pub fn pg_data_dirs_path(&self) -> PathBuf {
        self.base_data_dir.join("pgdatadirs").join("tenants")
    }

    pub fn pg_data_dir(&self, tenantid: &ZTenantId, branch_name: &str) -> PathBuf {
        self.pg_data_dirs_path()
            .join(tenantid.to_string())
            .join(branch_name)
    }

    // TODO: move pageserver files into ./pageserver
    pub fn pageserver_data_dir(&self) -> PathBuf {
        self.base_data_dir.clone()
    }

    pub fn safekeeper_data_dir(&self, data_dir_name: &str) -> PathBuf {
        self.base_data_dir.join("safekeepers").join(data_dir_name)
    }

    /// Create a LocalEnv from a config file.
    ///
    /// Unlike 'load_config', this function fills in any defaults that are missing
    /// from the config file.
    pub fn create_config(toml: &str) -> anyhow::Result<Self> {
        let mut env: LocalEnv = toml::from_str(toml)?;

        // Find postgres binaries.
        // Follow POSTGRES_DISTRIB_DIR if set, otherwise look in "tmp_install".
        if env.pg_distrib_dir == Path::new("") {
            if let Some(postgres_bin) = env::var_os("POSTGRES_DISTRIB_DIR") {
                env.pg_distrib_dir = postgres_bin.into();
            } else {
                let cwd = env::current_dir()?;
                env.pg_distrib_dir = cwd.join("tmp_install")
            }
        }
        if !env.pg_distrib_dir.join("bin/postgres").exists() {
            bail!(
                "Can't find postgres binary at {}",
                env.pg_distrib_dir.display()
            );
        }

        // Find zenith binaries.
        if env.zenith_distrib_dir == Path::new("") {
            env.zenith_distrib_dir = env::current_exe()?.parent().unwrap().to_owned();
        }
        for binary in ["pageserver", "safekeeper"] {
            if !env.zenith_distrib_dir.join(binary).exists() {
                bail!(
                    "Can't find binary '{}' in zenith distrib dir '{}'",
                    binary,
                    env.zenith_distrib_dir.display()
                );
            }
        }

        // If no initial tenant ID was given, generate it.
        if env.default_tenantid.is_none() {
            env.default_tenantid = Some(ZTenantId::generate());
        }

        env.base_data_dir = base_path();

        Ok(env)
    }

    /// Locate and load config
    pub fn load_config() -> anyhow::Result<Self> {
        let repopath = base_path();

        if !repopath.exists() {
            bail!(
                "Zenith config is not found in {}. You need to run 'zenith init' first",
                repopath.to_str().unwrap()
            );
        }

        // TODO: check that it looks like a zenith repository

        // load and parse file
        let config = fs::read_to_string(repopath.join("config"))?;
        let mut env: LocalEnv = toml::from_str(config.as_str())?;

        env.base_data_dir = repopath;

        Ok(env)
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
    // Initialize a new Zenith repository
    //
    pub fn init(&mut self) -> anyhow::Result<()> {
        // check if config already exists
        let base_path = &self.base_data_dir;
        if base_path == Path::new("") {
            bail!("repository base path is missing");
        }
        if base_path.exists() {
            bail!(
                "directory '{}' already exists. Perhaps already initialized?",
                base_path.to_str().unwrap()
            );
        }

        fs::create_dir(&base_path)?;

        // generate keys for jwt
        // openssl genrsa -out private_key.pem 2048
        let private_key_path;
        if self.private_key_path == PathBuf::new() {
            private_key_path = base_path.join("auth_private_key.pem");
            let keygen_output = Command::new("openssl")
                .arg("genrsa")
                .args(&["-out", private_key_path.to_str().unwrap()])
                .arg("2048")
                .stdout(Stdio::null())
                .output()
                .context("failed to generate auth private key")?;
            if !keygen_output.status.success() {
                bail!(
                    "openssl failed: '{}'",
                    String::from_utf8_lossy(&keygen_output.stderr)
                );
            }
            self.private_key_path = PathBuf::from("auth_private_key.pem");

            let public_key_path = base_path.join("auth_public_key.pem");
            // openssl rsa -in private_key.pem -pubout -outform PEM -out public_key.pem
            let keygen_output = Command::new("openssl")
                .arg("rsa")
                .args(&["-in", private_key_path.to_str().unwrap()])
                .arg("-pubout")
                .args(&["-outform", "PEM"])
                .args(&["-out", public_key_path.to_str().unwrap()])
                .stdout(Stdio::null())
                .output()
                .context("failed to generate auth private key")?;
            if !keygen_output.status.success() {
                bail!(
                    "openssl failed: '{}'",
                    String::from_utf8_lossy(&keygen_output.stderr)
                );
            }
        }

        self.pageserver.auth_token =
            self.generate_auth_token(&Claims::new(None, Scope::PageServerApi))?;

        fs::create_dir_all(self.pg_data_dirs_path())?;

        for safekeeper in &self.safekeepers {
            fs::create_dir_all(SafekeeperNode::datadir_path_by_id(self, safekeeper.id))?;
        }

        let mut conf_content = String::new();

        // Currently, the user first passes a config file with 'zenith init --config=<path>'
        // We read that in, in `create_config`, and fill any missing defaults. Then it's saved
        // to .zenith/config. TODO: We lose any formatting and comments along the way, which is
        // a bit sad.
        write!(
            &mut conf_content,
            r#"# This file describes a locale deployment of the page server
# and safekeeeper node. It is read by the 'zenith' command-line
# utility.
"#
        )?;

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
        conf_content += &toml::to_string_pretty(&toml::Value::try_from(&self)?)?;

        fs::write(base_path.join("config"), conf_content)?;

        Ok(())
    }
}

fn base_path() -> PathBuf {
    match std::env::var_os("ZENITH_REPO_DIR") {
        Some(val) => PathBuf::from(val.to_str().unwrap()),
        None => ".zenith".into(),
    }
}
