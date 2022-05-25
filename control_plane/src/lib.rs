//
// Local control plane.
//
// Can start, configure and stop postgres instances running as a local processes.
//
// Intended to be used in integration tests and in CLI tools for
// local installations.
//
use anyhow::{anyhow, bail, Context, Result};
use std::fs;
use std::path::Path;
use std::process::Command;

pub mod compute;
pub mod etcd;
pub mod local_env;
pub mod postgresql_conf;
pub mod safekeeper;
pub mod storage;

/// Read a PID file
///
/// We expect a file that contains a single integer.
/// We return an i32 for compatibility with libc and nix.
pub fn read_pidfile(pidfile: &Path) -> Result<i32> {
    let pid_str = fs::read_to_string(pidfile)
        .with_context(|| format!("failed to read pidfile {:?}", pidfile))?;
    let pid: i32 = pid_str
        .parse()
        .map_err(|_| anyhow!("failed to parse pidfile {:?}", pidfile))?;
    if pid < 1 {
        bail!("pidfile {:?} contained bad value '{}'", pidfile, pid);
    }
    Ok(pid)
}

fn fill_rust_env_vars(cmd: &mut Command) -> &mut Command {
    let cmd = cmd.env_clear().env("RUST_BACKTRACE", "1");

    let var = "LLVM_PROFILE_FILE";
    if let Some(val) = std::env::var_os(var) {
        cmd.env(var, val);
    }

    const RUST_LOG_KEY: &str = "RUST_LOG";
    if let Ok(rust_log_value) = std::env::var(RUST_LOG_KEY) {
        cmd.env(RUST_LOG_KEY, rust_log_value)
    } else {
        cmd
    }
}
