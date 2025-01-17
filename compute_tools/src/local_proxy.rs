//! Local Proxy is a feature of our BaaS Neon Authorize project.
//!
//! Local Proxy validates JWTs and manages the pg_session_jwt extension.
//! It also maintains a connection pool to postgres.

use anyhow::{Context, Result};
use camino::Utf8Path;
use compute_api::spec::LocalProxySpec;
use nix::sys::signal::Signal;
use utils::pid_file::{self, PidFileRead};

pub fn configure(local_proxy: &LocalProxySpec) -> Result<()> {
    write_local_proxy_conf("/etc/local_proxy/config.json".as_ref(), local_proxy)?;
    notify_local_proxy("/etc/local_proxy/pid".as_ref())?;

    Ok(())
}

/// Create or completely rewrite configuration file specified by `path`
fn write_local_proxy_conf(path: &Utf8Path, local_proxy: &LocalProxySpec) -> Result<()> {
    let config =
        serde_json::to_string_pretty(local_proxy).context("serializing LocalProxySpec to json")?;
    std::fs::write(path, config).with_context(|| format!("writing {path}"))?;

    Ok(())
}

/// Notify local proxy about a new config file.
fn notify_local_proxy(path: &Utf8Path) -> Result<()> {
    match pid_file::read(path)? {
        // if the file doesn't exist, or isn't locked, local_proxy isn't running
        // and will naturally pick up our config later
        PidFileRead::NotExist | PidFileRead::NotHeldByAnyProcess(_) => {}
        PidFileRead::LockedByOtherProcess(pid) => {
            // From the pid_file docs:
            //
            // > 1. The other process might exit at any time, turning the given PID stale.
            // > 2. There is a small window in which `claim_for_current_process` has already
            // >    locked the file but not yet updates its contents. [`read`] will return
            // >    this variant here, but with the old file contents, i.e., a stale PID.
            // >
            // > The kernel is free to recycle PID once it has been `wait(2)`ed upon by
            // > its creator. Thus, acting upon a stale PID, e.g., by issuing a `kill`
            // > system call on it, bears the risk of killing an unrelated process.
            // > This is an inherent limitation of using pidfiles.
            // > The only race-free solution is to have a supervisor-process with a lifetime
            // > that exceeds that of all of its child-processes (e.g., `runit`, `supervisord`).
            //
            // This is an ok risk as we only send a SIGHUP which likely won't actually
            // kill the process, only reload config.
            nix::sys::signal::kill(pid, Signal::SIGHUP).context("sending signal to local_proxy")?;
        }
    }

    Ok(())
}
