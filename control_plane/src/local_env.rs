//
// This module is responsible for locating and loading paths in a local setup.
//
// Now it also provides init method which acts like a stub for proper installation
// script which will use local paths.
//
use std::env;
use std::error;
use std::fs;
use std::path::{Path, PathBuf};

use home;
use serde_derive::{Deserialize, Serialize};

type Result<T> = std::result::Result<T, Box<dyn error::Error>>;

//
// This data structure represents deserialized zenith config, which should be
// located in ~/.zenith
//
// TODO: should we also support ZENITH_CONF env var?
//
#[derive(Serialize, Deserialize, Clone)]
pub struct LocalEnv {
    // Here page server and compute nodes will create and store their data.
    pub data_dir: PathBuf,

    // Path to postgres distribution. It expected that "bin", "include",
    // "lib", "share" from postgres distribution will be there. If at some point
    // in time we will be able to run against vanilla postgres we may split that
    // to four separate paths and match OS-specific installation layout.
    pub pg_distrib_dir: PathBuf,

    // Path to pageserver binary.
    pub zenith_distrib_dir: PathBuf,
}

impl LocalEnv {
    pub fn pg_bin_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("bin")
    }
    pub fn pg_lib_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("lib")
    }

    pub fn pageserver_data_dir(&self) -> PathBuf {
        self.data_dir.join("pageserver")
    }
    pub fn pageserver_log(&self) -> PathBuf {
        self.pageserver_data_dir().join("pageserver.log")
    }
    pub fn pageserver_pidfile(&self) -> PathBuf {
        self.pageserver_data_dir().join("pageserver.pid")
    }
}

//
// Issues in rust-lang repo has several discussions about proper library to check
// home directory in a cross-platform way. Seems that current consensus is around
// home crate and cargo uses it.
//
fn get_home() -> Result<PathBuf> {
    match home::home_dir() {
        Some(path) => Ok(path),
        None => {
            return Err(Box::<dyn error::Error>::from(
                "can not determine home directory path",
            ));
        }
    }
}

pub fn init() -> Result<()> {
    let home_dir = get_home()?;

    // check if config already exists
    let cfg_path = home_dir.join(".zenith");
    if cfg_path.exists() {
        let err_msg = format!(
            "{} already exists. Perhaps already initialized?",
            cfg_path.to_str().unwrap()
        );
        return Err(Box::<dyn error::Error>::from(err_msg));
    }

    // Now we can run init only from crate directory, so check that current dir is our crate.
    // Use 'pageserver/Cargo.toml' existence as evidendce.
    let cargo_path = env::current_dir()?;
    if !cargo_path.join("pageserver/Cargo.toml").exists() {
        let err_msg = "Current dirrectory does not look like a zenith repo. \
            Please, run 'init' from zenith repo root.";
        return Err(Box::<dyn error::Error>::from(err_msg));
    }

    // ok, now check that expected binaries are present

    // check postgres
    let pg_distrib_dir = cargo_path.join("tmp_install");
    let pg_path = pg_distrib_dir.join("bin/postgres");
    if !pg_path.exists() {
        let err_msg = format!(
            "Can't find postres binary at {}. \
            Perhaps './pgbuild.sh' is needed to build it first.",
            pg_path.to_str().unwrap()
        );
        return Err(Box::<dyn error::Error>::from(err_msg));
    }

    // check pageserver
    let zenith_distrib_dir = cargo_path.join("target/debug/");
    let pageserver_path = zenith_distrib_dir.join("pageserver");
    if !pageserver_path.exists() {
        let err_msg = format!(
            "Can't find pageserver binary at {}. Please build it.",
            pageserver_path.to_str().unwrap()
        );
        return Err(Box::<dyn error::Error>::from(err_msg));
    }

    // ok, we are good to go

    // create data dir
    let data_dir = cargo_path.join("tmp_install");
    match fs::create_dir(data_dir.clone()) {
        Ok(_) => {}
        Err(e) => match e.kind() {
            std::io::ErrorKind::AlreadyExists => {}
            _ => {
                let err_msg = format!(
                    "Failed to create data directory in '{}': {}",
                    data_dir.to_str().unwrap(),
                    e
                );
                return Err(Box::<dyn error::Error>::from(err_msg));
            }
        },
    }

    // write config
    let conf = LocalEnv {
        data_dir,
        pg_distrib_dir,
        zenith_distrib_dir,
    };
    let toml = toml::to_string(&conf)?;
    fs::write(cfg_path, toml)?;

    Ok(())
}

// check that config file is present
pub fn load_config() -> Result<LocalEnv> {
    // home
    let home_dir = get_home()?;

    // check file exists
    let cfg_path = home_dir.join(".zenith");
    if !cfg_path.exists() {
        let err_msg = format!(
            "Zenith config is not found in {}. You need to run 'zenith init' first",
            cfg_path.to_str().unwrap()
        );
        return Err(Box::<dyn error::Error>::from(err_msg));
    }

    // load and parse file
    let config = fs::read_to_string(cfg_path)?;
    match toml::from_str(config.as_str()) {
        Ok(cfg) => Ok(cfg),
        Err(e) => Err(Box::<dyn error::Error>::from(e)),
    }
}

// local env for tests
pub fn test_env() -> LocalEnv {
    let data_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tmp_check");
    fs::create_dir_all(data_dir.clone()).unwrap();
    LocalEnv {
        data_dir,
        pg_distrib_dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("../tmp_install"),
        zenith_distrib_dir: cargo_bin_dir(),
    }
}

// Find the directory where the binaries were put (i.e. target/debug/)
pub fn cargo_bin_dir() -> PathBuf {
    let mut pathbuf = std::env::current_exe().ok().unwrap();

    pathbuf.pop();
    if pathbuf.ends_with("deps") {
        pathbuf.pop();
    }

    return pathbuf;
}
