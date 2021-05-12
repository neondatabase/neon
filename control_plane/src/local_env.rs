//
// This module is responsible for locating and loading paths in a local setup.
//
// Now it also provides init method which acts like a stub for proper installation
// script which will use local paths.
//
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, env};
use std::fs;
use std::path::PathBuf;
use url::Url;

use crate::remotes;

//
// This data structures represent deserialized zenith CLI config
//
#[derive(Serialize, Deserialize, Clone)]
pub struct LocalEnv {
    // Pageserver connection strings
    pub pageserver_connstring: String,

    // Base directory for both pageserver and compute nodes
    pub base_data_dir: PathBuf,

    // Path to postgres distribution. It's expected that "bin", "include",
    // "lib", "share" from postgres distribution are there. If at some point
    // in time we will be able to run against vanilla postgres we may split that
    // to four separate paths and match OS-specific installation layout.
    pub pg_distrib_dir: PathBuf,

    // Path to pageserver binary. Empty for remote pageserver.
    pub zenith_distrib_dir: Option<PathBuf>,
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
        Ok(self
            .zenith_distrib_dir
            .as_ref()
            .ok_or(anyhow!("Can not manage remote pageserver"))?
            .join("pageserver"))
    }

    pub fn pg_data_dirs_path(&self) -> PathBuf {
        self.base_data_dir.join("pgdatadirs")
    }

    pub fn pg_data_dir(&self, name: &str) -> PathBuf {
        self.pg_data_dirs_path().join(name)
    }

    // TODO: move pageserver files into ./pageserver
    pub fn pageserver_data_dir(&self) -> PathBuf {
        self.base_data_dir.clone()
    }
}

fn base_path() -> PathBuf {
    match std::env::var_os("ZENITH_REPO_DIR") {
        Some(val) => PathBuf::from(val.to_str().unwrap()),
        None => ".zenith".into(),
    }
}

//
// Initialize a new Zenith repository
//
pub fn init(remote_pageserver: Option<&str>) -> Result<()> {
    // check if config already exists
    let base_path = base_path();
    if base_path.exists() {
        anyhow::bail!(
            "{} already exists. Perhaps already initialized?",
            base_path.to_str().unwrap()
        );
    }

    // ok, now check that expected binaries are present

    // Find postgres binaries. Follow POSTGRES_DISTRIB_DIR if set, otherwise look in "tmp_install".
    let pg_distrib_dir: PathBuf = {
        if let Some(postgres_bin) = env::var_os("POSTGRES_DISTRIB_DIR") {
            postgres_bin.into()
        } else {
            let cwd = env::current_dir()?;
            cwd.join("tmp_install")
        }
    };
    if !pg_distrib_dir.join("bin/postgres").exists() {
        anyhow::bail!("Can't find postgres binary at {:?}", pg_distrib_dir);
    }

    fs::create_dir(&base_path)?;
    fs::create_dir(base_path.join("pgdatadirs"))?;

    let conf = if let Some(addr) = remote_pageserver {
        // check that addr is parsable
        let _uri = Url::parse(addr).map_err(|e| anyhow!("{}: {}", addr, e))?;

        LocalEnv {
            pageserver_connstring: format!("postgresql://{}/", addr),
            pg_distrib_dir,
            zenith_distrib_dir: None,
            base_data_dir: base_path,
        }
    } else {
        // Find zenith binaries.
        let zenith_distrib_dir = env::current_exe()?.parent().unwrap().to_owned();
        if !zenith_distrib_dir.join("pageserver").exists() {
            anyhow::bail!("Can't find pageserver binary.",);
        }

        LocalEnv {
            pageserver_connstring: "postgresql://127.0.0.1:6400".to_string(),
            pg_distrib_dir,
            zenith_distrib_dir: Some(zenith_distrib_dir),
            base_data_dir: base_path,
        }
    };

    let toml = toml::to_string(&conf)?;
    fs::write(conf.base_data_dir.join("config"), toml)?;
    // initialize remotes file
    remotes::save_remotes(&conf, &BTreeMap::default())?;

    Ok(())
}

// Locate and load config
pub fn load_config() -> Result<LocalEnv> {
    let repopath = base_path();

    if !repopath.exists() {
        anyhow::bail!(
            "Zenith config is not found in {}. You need to run 'zenith init' first",
            repopath.to_str().unwrap()
        );
    }

    // TODO: check that it looks like a zenith repository

    // load and parse file
    let config = fs::read_to_string(repopath.join("config"))?;
    toml::from_str(config.as_str()).map_err(|e| e.into())
}

// Find the directory where the binaries were put (i.e. target/debug/)
pub fn cargo_bin_dir() -> PathBuf {
    let mut pathbuf = std::env::current_exe().unwrap();

    pathbuf.pop();
    if pathbuf.ends_with("deps") {
        pathbuf.pop();
    }

    pathbuf
}
