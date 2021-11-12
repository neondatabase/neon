//
// This module is responsible for locating and loading paths in a local setup.
//
// Now it also provides init method which acts like a stub for proper installation
// script which will use local paths.
//
use anyhow::{bail, Context, Result};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::str::FromStr;
use zenith_utils::auth::{encode_from_key_file, Claims, Scope};
use zenith_utils::postgres_backend::AuthType;
use zenith_utils::zid::ZTenantId;

use toml_edit::{Document, Item, Table, Value};

//
// This data structures represents zenith CLI config
//
// It is deserialized from the .zenith/config file, or the config file passed
// to 'zenith init --config=<path>' option. See control_plane/simple.conf for
// an example.
//
#[derive(Clone, Debug, Default)]
pub struct LocalEnv {
    // Base directory for all the nodes (the pageserver, safekeepers and
    // compute nodes).
    //
    // This is not stored in the config file. Rather, this is the path where the
    // config file itself is. It is read from the ZENITH_REPO_DIR env variable or
    // '.zenith' if not given.
    pub base_data_dir: PathBuf,

    // Path to postgres distribution. It's expected that "bin", "include",
    // "lib", "share" from postgres distribution are there. If at some point
    // in time we will be able to run against vanilla postgres we may split that
    // to four separate paths and match OS-specific installation layout.
    pub pg_distrib_dir: PathBuf,

    // Path to pageserver binary.
    pub zenith_distrib_dir: PathBuf,

    // Default tenant ID to use with the 'zenith' command line utility, when
    // --tenantid is not explicitly specified.
    pub default_tenantid: Option<ZTenantId>,

    // used to issue tokens during e.g pg start
    pub private_key_path: PathBuf,

    pub pageserver: PageServerConf,

    pub safekeepers: Vec<SafekeeperConf>,

    // The original toml file (with possible changes from defaults and overrides
    // from command line). All the fields above are derived from this.
    pub toml: Document,
}

#[derive(Clone, Debug)]
pub struct PageServerConf {
    // Pageserver connection settings
    pub listen_pg_addr: String,
    pub listen_http_addr: String,

    // used to determine which auth type is used
    pub auth_type: AuthType,

    // jwt auth token used for communication with pageserver
    pub auth_token: String,

    pub toml: Table,
}

impl Default for PageServerConf {
    fn default() -> Self {
        Self {
            listen_pg_addr: "".to_string(),
            listen_http_addr: "".to_string(),
            auth_type: AuthType::Trust,
            auth_token: "".to_string(),
            toml: Table::new(),
        }
    }
}

impl PageServerConf {
    fn from_toml(toml: &Table) -> Result<Self> {
        let mut result = PageServerConf {
            toml: toml.clone(),
            ..Default::default()
        };

        for (key, value) in toml.iter() {
            match key {
                "listen_pg_addr" => result.listen_pg_addr = parse_str(key, value)?,
                "listen_http_addr" => result.listen_http_addr = parse_str(key, value)?,
                "auth_type" => result.auth_type = AuthType::from_str(&parse_str(key, value)?)?,
                "auth_token" => result.auth_token = parse_str(key, value)?,
                _ => {}
            }
        }
        Ok(result)
    }
}

fn parse_str(name: &str, val: &Item) -> Result<String> {
    if let Item::Value(Value::String(val)) = val {
        Ok(val.value().to_string())
    } else {
        bail!("option {} is not a string", name);
    }
}

fn parse_port(name: &str, val: &Item) -> Result<u16> {
    if let Item::Value(Value::Integer(val)) = val {
        let val = *val.value();
        if val <= 0 || val > u16::MAX as i64 {
            bail!("value {} for option {} is out of range", val, name);
        }
        Ok(val as u16)
    } else {
        bail!("option {} is not an integer", name);
    }
}

fn parse_bool(name: &str, val: &Item) -> Result<bool> {
    if let Item::Value(Value::Boolean(val)) = val {
        Ok(*val.value())
    } else {
        bail!("option {} is not a boolean", name);
    }
}

#[derive(Clone, Debug)]
pub struct SafekeeperConf {
    pub name: String,
    pub pg_port: u16,
    pub http_port: u16,
    pub sync: bool,
}
impl Default for SafekeeperConf {
    fn default() -> Self {
        Self {
            name: "".to_string(),
            pg_port: 0,
            http_port: 0,
            sync: true,
        }
    }
}

impl SafekeeperConf {
    fn from_toml(toml: &Table) -> Result<Self> {
        let mut result = SafekeeperConf {
            ..Default::default()
        };

        for (key, value) in toml.iter() {
            match key {
                "name" => result.name = parse_str(key, value)?,
                "pg_port" => result.pg_port = parse_port(key, value)?,
                "http_port" => result.http_port = parse_port(key, value)?,
                "sync" => result.sync = parse_bool(key, value)?,
                _ => {}
            }
        }
        Ok(result)
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

    pub fn pageserver_bin(&self) -> Result<PathBuf> {
        Ok(self.zenith_distrib_dir.join("pageserver"))
    }

    pub fn safekeeper_bin(&self) -> Result<PathBuf> {
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

    pub fn safekeeper_data_dir(&self, node_name: &str) -> PathBuf {
        self.base_data_dir.join("safekeepers").join(node_name)
    }

    /// Fill in any missing required values with defaults.
    pub fn fill_defaults(&mut self) -> Result<()> {
        // Find postgres binaries.
        // Follow POSTGRES_DISTRIB_DIR if set, otherwise look in "tmp_install".
        if self.pg_distrib_dir == Path::new("") {
            if let Some(postgres_bin) = env::var_os("POSTGRES_DISTRIB_DIR") {
                self.pg_distrib_dir = postgres_bin.into();
            } else {
                let cwd = env::current_dir()?;
                self.pg_distrib_dir = cwd.join("tmp_install")
            }
            self.toml.insert(
                "pg_distrib_dir",
                toml_edit::value(&self.pg_distrib_dir.display().to_string()),
            );
        }
        if !self.pg_distrib_dir.join("bin/postgres").exists() {
            anyhow::bail!(
                "Can't find postgres binary at {}",
                self.pg_distrib_dir.display()
            );
        }

        // Find zenith binaries.
        if self.zenith_distrib_dir == Path::new("") {
            self.zenith_distrib_dir = env::current_exe()?.parent().unwrap().to_owned();
            self.toml.insert(
                "zenith_distrib_dir",
                toml_edit::value(&self.zenith_distrib_dir.display().to_string()),
            );
        }
        if !self.zenith_distrib_dir.join("pageserver").exists() {
            anyhow::bail!("Can't find pageserver binary.");
        }
        if !self.zenith_distrib_dir.join("safekeeper").exists() {
            anyhow::bail!("Can't find safekeeper binary.");
        }

        // If no initial tenant ID was given, generate it.
        if self.default_tenantid.is_none() {
            let tenantid = ZTenantId::generate();
            self.default_tenantid = Some(tenantid);
            self.toml
                .insert("default_tenantid", toml_edit::value(tenantid.to_string()));
        }

        self.base_data_dir = base_path();

        Ok(())
    }

    /// Locate and load config
    pub fn load_config() -> Result<LocalEnv> {
        let repopath = base_path();

        if !repopath.exists() {
            anyhow::bail!(
                "Zenith config is not found in {}. You need to run 'zenith init' first",
                repopath.to_str().unwrap()
            );
        }

        let config = fs::read_to_string(repopath.join("config"))?;
        let mut result = Self::parse_config(&config)?;
        result.base_data_dir = repopath;

        Ok(result)
    }

    /// Locate and load config
    pub fn parse_config(toml_str: &str) -> Result<LocalEnv> {
        // load and parse file
        let toml = toml_str.parse::<Document>()?;

        let mut env = LocalEnv {
            toml,
            ..Default::default()
        };
        for (key, value) in env.toml.iter() {
            match key {
                "pg_distrib_dir" => {
                    if let Some(val) = value.as_str() {
                        env.pg_distrib_dir = PathBuf::from(val);
                    } else {
                        bail!("value of option {} is not a string", key);
                    }
                }
                "zenith_distrib_dir" => {
                    if let Some(val) = value.as_str() {
                        env.zenith_distrib_dir = PathBuf::from(val);
                    } else {
                        bail!("value of option {} is not a string", key);
                    }
                }
                "default_tenantid" => {
                    if let Some(val) = value.as_str() {
                        env.default_tenantid = Some(ZTenantId::from_str(val)?);
                    } else {
                        bail!("value of option {} is not a string", key);
                    }
                }
                "private_key_path" => {
                    if let Some(val) = value.as_str() {
                        env.private_key_path = PathBuf::from(val);
                    } else {
                        bail!("value of option {} is not a string", key);
                    }
                }
                "pageserver" => {
                    // We pass any pageserver options as is to the pageserver.toml config
                    // file, but we also extract and parse some options that we need to know
                    // about.
                    if let Some(val) = value.as_table() {
                        env.pageserver = PageServerConf::from_toml(val)?;
                    } else {
                        bail!("value of option {} is not a table", key);
                    }
                }
                "safekeepers" => {
                    if let Some(aot) = value.as_array_of_tables() {
                        let mut safekeepers = Vec::new();
                        for tbl in aot.iter() {
                            safekeepers.push(SafekeeperConf::from_toml(tbl)?);
                        }
                        env.safekeepers = safekeepers;
                    } else {
                        bail!("'safekeepers' is not an array of tables");
                    }
                }

                _ => bail!("unrecognized option \"{}\" in config file", key),
            }
        }

        Ok(env)
    }

    // this function is used only for testing purposes in CLI e g generate tokens during init
    pub fn generate_auth_token(&self, claims: &Claims) -> Result<String> {
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
    pub fn init(&mut self) -> Result<()> {
        // check if config already exists
        let base_path = &self.base_data_dir;
        if base_path == Path::new("") {
            anyhow::bail!("repository base path is missing");
        }
        if base_path.exists() {
            anyhow::bail!(
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
                .with_context(|| "failed to generate auth private key")?;
            if !keygen_output.status.success() {
                anyhow::bail!(
                    "openssl failed: '{}'",
                    String::from_utf8_lossy(&keygen_output.stderr)
                );
            }
            self.private_key_path = Path::new("auth_private_key.pem").to_path_buf();

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
                .with_context(|| "failed to generate auth private key")?;
            if !keygen_output.status.success() {
                anyhow::bail!(
                    "openssl failed: '{}'",
                    String::from_utf8_lossy(&keygen_output.stderr)
                );
            }
        }

        self.pageserver.auth_token =
            self.generate_auth_token(&Claims::new(None, Scope::PageServerApi))?;

        fs::create_dir_all(self.pg_data_dirs_path())?;

        for safekeeper in self.safekeepers.iter() {
            fs::create_dir_all(self.safekeeper_data_dir(&safekeeper.name))?;
        }

        // Currently, the user first passes a config file with 'zenith init --config=<path>'
        // We read that in, in `create_config`, and fill any missing defaults. Then it's saved
        // to .zenith/config.

        fs::write(base_path.join("config"), self.toml.to_string())?;

        Ok(())
    }
}

fn base_path() -> PathBuf {
    match std::env::var_os("ZENITH_REPO_DIR") {
        Some(val) => PathBuf::from(val.to_str().unwrap()),
        None => ".zenith".into(),
    }
}
