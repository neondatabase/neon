//! Spawns and kills background processes that are needed by Neon CLI.
//! Applies common set-up such as log and pid files (if needed) to every process.
//!
//! Neon CLI does not run in background, so it needs to store the information about
//! spawned processes, which it does in this module.
//! We do that by storing the pid of the process in the "${process_name}.pid" file.
//! The pid file can be created by the process itself
//! (Neon storage binaries do that and also ensure that a lock is taken onto that file)
//! or we create such file after starting the process
//! (non-Neon binaries don't necessarily follow our pidfile conventions).
//! The pid stored in the file is later used to stop the service.
//!
//! See [`lock_file`] module for more info.

use std::ffi::OsStr;
use std::io::Write;
use std::path::Path;
use std::process::{Child, Command};
use std::time::Duration;
use std::{fs, io, thread};

use anyhow::{anyhow, bail, Context, Result};
use nix::errno::Errno;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;

use utils::lock_file;

// These constants control the loop used to poll for process start / stop.
//
// The loop waits for at most 10 seconds, polling every 100 ms.
// Once a second, it prints a dot ("."), to give the user an indication that
// it's waiting. If the process hasn't started/stopped after 5 seconds,
// it prints a notice that it's taking long, but keeps waiting.
//
const RETRY_UNTIL_SECS: u64 = 10;
const RETRIES: u64 = (RETRY_UNTIL_SECS * 1000) / RETRY_INTERVAL_MILLIS;
const RETRY_INTERVAL_MILLIS: u64 = 100;
const DOT_EVERY_RETRIES: u64 = 10;
const NOTICE_AFTER_RETRIES: u64 = 50;

/// Argument to `start_process`, to indicate whether it should create pidfile or if the process creates
/// it itself.
pub enum InitialPidFile<'t> {
    /// Create a pidfile, to allow future CLI invocations to manipulate the process.
    Create(&'t Path),
    /// The process will create the pidfile itself, need to wait for that event.
    Expect(&'t Path),
}

/// Start a background child process using the parameters given.
pub fn start_process<F, S: AsRef<OsStr>>(
    process_name: &str,
    datadir: &Path,
    command: &Path,
    args: &[S],
    initial_pid_file: InitialPidFile,
    process_status_check: F,
) -> anyhow::Result<Child>
where
    F: Fn() -> anyhow::Result<bool>,
{
    let log_path = datadir.join(format!("{process_name}.log"));
    let process_log_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&log_path)
        .with_context(|| {
            format!("Could not open {process_name} log file {log_path:?} for writing")
        })?;
    let same_file_for_stderr = process_log_file.try_clone().with_context(|| {
        format!("Could not reuse {process_name} log file {log_path:?} for writing stderr")
    })?;

    let mut command = Command::new(command);
    let background_command = command
        .stdout(process_log_file)
        .stderr(same_file_for_stderr)
        .args(args);
    let filled_cmd = fill_aws_secrets_vars(fill_rust_env_vars(background_command));

    let mut spawned_process = filled_cmd.spawn().with_context(|| {
        format!("Could not spawn {process_name}, see console output and log files for details.")
    })?;
    let pid = spawned_process.id();
    let pid = Pid::from_raw(
        i32::try_from(pid)
            .with_context(|| format!("Subprocess {process_name} has invalid pid {pid}"))?,
    );

    let pid_file_to_check = match initial_pid_file {
        InitialPidFile::Create(target_pid_file_path) => {
            match lock_file::create_lock_file(target_pid_file_path, pid.to_string()) {
                lock_file::LockCreationResult::Created { .. } => {
                    // We use "lock" file here only to create the pid file. The lock on the pidfile will be dropped as soon
                    // as this CLI invocation exits, so it's a bit useless, but doesn't any harm either.
                }
                lock_file::LockCreationResult::AlreadyLocked { .. } => {
                    anyhow::bail!("Cannot write pid file for {process_name} at path {target_pid_file_path:?}: file is already locked by another process")
                }
                lock_file::LockCreationResult::CreationFailed(e) => {
                    return Err(e.context(format!(
                    "Failed to create pid file for {process_name} at path {target_pid_file_path:?}"
                )))
                }
            }
            None
        }
        InitialPidFile::Expect(pid_file_path) => Some(pid_file_path),
    };

    for retries in 0..RETRIES {
        match process_started(pid, pid_file_to_check, &process_status_check) {
            Ok(true) => {
                println!("\n{process_name} started, pid: {pid}");
                return Ok(spawned_process);
            }
            Ok(false) => {
                if retries == NOTICE_AFTER_RETRIES {
                    // The process is taking a long time to start up. Keep waiting, but
                    // print a message
                    print!("\n{process_name} has not started yet, continuing to wait");
                }
                if retries % DOT_EVERY_RETRIES == 0 {
                    print!(".");
                    io::stdout().flush().unwrap();
                }
                thread::sleep(Duration::from_millis(RETRY_INTERVAL_MILLIS));
            }
            Err(e) => {
                println!("{process_name} failed to start: {e:#}");
                if let Err(e) = spawned_process.kill() {
                    println!("Could not stop {process_name} subprocess: {e:#}")
                };
                return Err(e);
            }
        }
    }
    println!();
    anyhow::bail!("{process_name} did not start in {RETRY_UNTIL_SECS} seconds");
}

/// Stops the process, using the pid file given. Returns Ok also if the process is already not running.
pub fn stop_process(immediate: bool, process_name: &str, pid_file: &Path) -> anyhow::Result<()> {
    if !pid_file.exists() {
        println!("{process_name} is already stopped: no pid file {pid_file:?} is present");
        return Ok(());
    }
    let pid = read_pidfile(pid_file)?;

    let sig = if immediate {
        print!("Stopping {process_name} with pid {pid} immediately..");
        Signal::SIGQUIT
    } else {
        print!("Stopping {process_name} with pid {pid} gracefully..");
        Signal::SIGTERM
    };
    io::stdout().flush().unwrap();
    match kill(pid, sig) {
        Ok(()) => (),
        Err(Errno::ESRCH) => {
            println!(
                "{process_name} with pid {pid} does not exist, but a pid file {pid_file:?} was found"
            );
            return Ok(());
        }
        Err(e) => anyhow::bail!("Failed to send signal to {process_name} with pid {pid}: {e}"),
    }

    // Wait until process is gone
    for retries in 0..RETRIES {
        match process_has_stopped(pid) {
            Ok(true) => {
                println!("\n{process_name} stopped");
                if let Err(e) = fs::remove_file(pid_file) {
                    if e.kind() != io::ErrorKind::NotFound {
                        eprintln!("Failed to remove pid file {pid_file:?} after stopping the process: {e:#}");
                    }
                }
                return Ok(());
            }
            Ok(false) => {
                if retries == NOTICE_AFTER_RETRIES {
                    // The process is taking a long time to start up. Keep waiting, but
                    // print a message
                    print!("\n{process_name} has not stopped yet, continuing to wait");
                }
                if retries % DOT_EVERY_RETRIES == 0 {
                    print!(".");
                    io::stdout().flush().unwrap();
                }
                thread::sleep(Duration::from_millis(RETRY_INTERVAL_MILLIS));
            }
            Err(e) => {
                println!("{process_name} with pid {pid} failed to stop: {e:#}");
                return Err(e);
            }
        }
    }
    println!();
    anyhow::bail!("{process_name} with pid {pid} did not stop in {RETRY_UNTIL_SECS} seconds");
}

fn fill_rust_env_vars(cmd: &mut Command) -> &mut Command {
    let mut filled_cmd = cmd.env_clear().env("RUST_BACKTRACE", "1");

    let var = "LLVM_PROFILE_FILE";
    if let Some(val) = std::env::var_os(var) {
        filled_cmd = filled_cmd.env(var, val);
    }

    const RUST_LOG_KEY: &str = "RUST_LOG";
    if let Ok(rust_log_value) = std::env::var(RUST_LOG_KEY) {
        filled_cmd.env(RUST_LOG_KEY, rust_log_value)
    } else {
        filled_cmd
    }
}

fn fill_aws_secrets_vars(mut cmd: &mut Command) -> &mut Command {
    for env_key in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    ] {
        if let Ok(value) = std::env::var(env_key) {
            cmd = cmd.env(env_key, value);
        }
    }
    cmd
}

fn process_started<F>(
    pid: Pid,
    pid_file_to_check: Option<&Path>,
    status_check: &F,
) -> anyhow::Result<bool>
where
    F: Fn() -> anyhow::Result<bool>,
{
    match status_check() {
        Ok(true) => match pid_file_to_check {
            Some(pid_file_path) => {
                if pid_file_path.exists() {
                    let pid_in_file = read_pidfile(pid_file_path)?;
                    Ok(pid_in_file == pid)
                } else {
                    Ok(false)
                }
            }
            None => Ok(true),
        },
        Ok(false) => Ok(false),
        Err(e) => anyhow::bail!("process failed to start: {e}"),
    }
}

/// Read a PID file
///
/// We expect a file that contains a single integer.
fn read_pidfile(pidfile: &Path) -> Result<Pid> {
    let pid_str = fs::read_to_string(pidfile)
        .with_context(|| format!("failed to read pidfile {pidfile:?}"))?;
    let pid: i32 = pid_str
        .parse()
        .map_err(|_| anyhow!("failed to parse pidfile {pidfile:?}"))?;
    if pid < 1 {
        bail!("pidfile {pidfile:?} contained bad value '{pid}'");
    }
    Ok(Pid::from_raw(pid))
}

fn process_has_stopped(pid: Pid) -> anyhow::Result<bool> {
    match kill(pid, None) {
        // Process exists, keep waiting
        Ok(_) => Ok(false),
        // Process not found, we're done
        Err(Errno::ESRCH) => Ok(true),
        Err(err) => anyhow::bail!("Failed to send signal to process with pid {pid}: {err}"),
    }
}
