//
// Local control plane.
//
// Can start, cofigure and stop postgres instances running as a local processes.
//
// Intended to be used in integration tests and in CLI tools for
// local installations.
//
use anyhow::{anyhow, bail, Context, Result};
use std::fs;
use std::path::Path;

pub mod compute;
pub mod local_env;
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
