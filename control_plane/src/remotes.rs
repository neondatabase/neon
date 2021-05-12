//! Utility module for managing the remote pageserver file
//! Currently a TOML file is used to map remote names to URIs

use anyhow::Result;
use std::collections::BTreeMap;
use std::{fs, path::PathBuf};

use crate::local_env::LocalEnv;

pub type Remotes = BTreeMap<String, String>;

fn remotes_path(local_env: &LocalEnv) -> PathBuf {
    local_env.base_data_dir.join("remotes")
}

pub fn load_remotes(local_env: &LocalEnv) -> Result<Remotes> {
    let remotes_str = fs::read_to_string(remotes_path(local_env))?;
    Ok(toml::from_str(&remotes_str)?)
}

pub fn save_remotes(local_env: &LocalEnv, remotes: &Remotes) -> Result<()> {
    let remotes_str = toml::to_string_pretty(remotes)?;
    fs::write(remotes_path(local_env), remotes_str)?;
    Ok(())
}
