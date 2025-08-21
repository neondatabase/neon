use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader, Cursor, Write},
    ops::{Deref, DerefMut},
    path::PathBuf,
};

use anyhow::{Context, anyhow};
use flate2::{Compression, write::GzEncoder};
use inferno::collapse::Collapse;
use nix::{libc::pid_t, unistd::Pid};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tracing::instrument;

const SUDO_PATH: &str = "/usr/bin/sudo";
const PERF_BINARY_DEFAULT_PATH: &str = "perf";
const BCC_PROFILE_BINARY_DEFAULT_PATH: &str = "/usr/share/bcc/tools/profile";
const STANDARD_PATH_PATHS: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// Either returns the path to the `sudo` binary
/// from the environment variable `SUDO_BINARY_PATH`
/// or returns the default path defined by
/// [`SUDO_PATH`].
fn sudo_path() -> String {
    std::env::var("SUDO_BINARY_PATH").unwrap_or_else(|_| SUDO_PATH.to_owned())
}

/// Either returns the path to the `perf` binary
/// from the environment variable `PERF_BINARY_PATH`
/// or returns the default path defined by
/// [`PERF_BINARY_DEFAULT_PATH`].
fn perf_binary_path() -> String {
    std::env::var("PERF_BINARY_PATH").unwrap_or_else(|_| PERF_BINARY_DEFAULT_PATH.to_owned())
}

/// Either returns the path to the `bcc-profile` binary
/// from the environment variable `BCC_PROFILE_BINARY_PATH`
/// or returns the default path defined by
/// [`BCC_PROFILE_BINARY_DEFAULT_PATH`].
fn bcc_profile_binary_path() -> String {
    std::env::var("BCC_PROFILE_BINARY_PATH")
        .unwrap_or_else(|_| BCC_PROFILE_BINARY_DEFAULT_PATH.to_owned())
}

/// Probes the executable at the given paths for the provided binary
/// names.
///
/// To check that the binary can run, it is invoked with the arguments.
fn probe_executable_at_paths(
    exact_matches: &[&str],
    binary_names: &[&str],
    paths: &[PathBuf],
    arg: Option<&str>,
    path_env_override: Option<String>,
) -> anyhow::Result<String> {
    let check_binary_runs = move |path: &str| {
        let mut command = vec![path];

        if let Some(arg) = arg {
            command.push(arg);
        }

        check_binary_runs(&command, path_env_override.clone())
    };

    let mut probed = Vec::new();

    for exact_match in exact_matches {
        match check_binary_runs(exact_match) {
            Ok(_) => {
                tracing::trace!("Found exact match for binary: {exact_match}");
                return Ok(exact_match.to_owned().to_owned());
            }
            Err(e) => {
                probed.push(exact_match.to_owned().to_owned());
                tracing::trace!("Failed to run the binary at path: {exact_match}: {e:?}");
            }
        }
    }

    for path in paths {
        for name in binary_names {
            let full_path = path.join(name);

            tracing::trace!("Looking at path: {}", full_path.to_string_lossy());

            if full_path.exists() && full_path.is_file() {
                tracing::trace!(
                    "There is an existing file at path: {}",
                    full_path.to_string_lossy()
                );

                match check_binary_runs(full_path.to_string_lossy().as_ref()) {
                    Ok(_) => {
                        tracing::trace!(
                            "Found valid binary at path: {}",
                            full_path.to_string_lossy()
                        );

                        return Ok(full_path.to_string_lossy().into_owned());
                    }
                    Err(e) => {
                        tracing::trace!(
                            "Failed to run the binary at path: {}: {e:?}",
                            full_path.to_string_lossy(),
                        );
                    }
                }
            }

            probed.push(full_path.to_string_lossy().into_owned());
        }
    }

    Err(anyhow!(
        "No valid binary found in the paths: {paths:?}, probed paths: {probed:?}"
    ))
}

/// Probes the executable in the known paths for the provided binary
/// names. At first, the function checks the exact matches
/// provided in the `exact_matches` slice. If any of those binaries
/// can be run with the provided `arg`, it returns the path to that
/// binary. If no exact matches are found, it checks the standard
/// paths defined by [`STANDARD_PATH_PATHS`] and looks for the binaries
/// with the names provided in the `binary_names` slice.
/// If any of those binaries can be run with the provided `arg`, it
/// returns the path to that binary.
/// If no valid binary is found, it returns an error.
fn probe_executable_in_known_paths(
    exact_matches: &[&str],
    binary_names: &[&str],
    arg: Option<&str>,
    path_env_override: Option<String>,
) -> anyhow::Result<String> {
    let paths = STANDARD_PATH_PATHS
        .split(':')
        .map(PathBuf::from)
        .collect::<Vec<_>>();

    probe_executable_at_paths(exact_matches, binary_names, &paths, arg, path_env_override)
}

/// Returns the path to the `sudo` binary if it can be run.
fn probe_sudo_paths() -> anyhow::Result<String> {
    let sudo_path = sudo_path();
    let exact_matches = [sudo_path.as_ref()];
    let binary_names = ["sudo"];

    probe_executable_in_known_paths(&exact_matches, &binary_names, Some("--version"), None)
}

/// Checks the known paths for `bcc-profile` binary and verifies that it
/// can be run.
///
/// First, if the provided path is `Some`, it checks if the binary
/// at that path can be run with `--help` argument. If it can, it
/// returns the path.
///
/// If the provided path is `None`, it checks the default `bcc-profile`
/// binary path defined by [`BCC_PROFILE_BINARY_DEFAULT_PATH`] constant.
/// If that binary can be run, it returns the path.
///
/// If neither the provided path nor the default path can be run,
/// it checks the standard paths defined by [`STANDARD_PATH_PATHS`]
/// and looks for known binary names like `bcc-profile`, `profile.py`,
/// or `profile-bpfcc` and other known names from various linux
/// distributions. If any of those binaries can be run, it returns the
/// path to that binary. If no valid binary is found, it returns an
/// error.
fn probe_bcc_profile_binary_paths(provided_path: Option<PathBuf>) -> anyhow::Result<String> {
    let binary_names = ["bcc-profile", "profile.py", "profile-bpfcc", "profile"];

    let mut exact_matches = Vec::new();

    if let Some(path) = provided_path.as_ref() {
        if let Some(path) = path.to_str() {
            exact_matches.push(path);
        }
    }

    let bcc_profile_binary_path = bcc_profile_binary_path();
    exact_matches.push(&bcc_profile_binary_path);

    probe_executable_in_known_paths(
        &exact_matches,
        &binary_names,
        Some("--help"),
        Some(get_override_path_env()),
    )
}

/// Represents the pprof data generated from a profiling tool.
#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct PprofData(pub(crate) Vec<u8>);

impl PprofData {
    /// Dumps the pprof data to a file.
    pub fn write_to_file(&self, path: &PathBuf) -> anyhow::Result<()> {
        let mut file = std::fs::File::create(path).context(format!(
            "couldn't create a file for dumping pprof data at path: {path:?}"
        ))?;
        file.write_all(&self.0)
            .context("couldn't write pprof raw data to the file at path: {path:?}")?;
        Ok(())
    }
}

impl Deref for PprofData {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PprofData {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Returns a list of child processes for a given parent process ID.
fn list_children_processes(parent_pid: Pid) -> anyhow::Result<Vec<Pid>> {
    use libproc::processes::{ProcFilter, pids_by_type};

    let filter = ProcFilter::ByParentProcess {
        ppid: parent_pid.as_raw() as _,
    };

    Ok(pids_by_type(filter)
        .context("failed to list child processes")?
        .into_iter()
        .map(|p| Pid::from_raw(p as _))
        .collect())
}

fn check_binary_runs(command: &[&str], override_path: Option<String>) -> anyhow::Result<()> {
    if command.is_empty() {
        return Err(anyhow!("Command cannot be empty"));
    }

    // One by one the strings from the command are joined to form the
    // word in the invocation of the binary.
    let (first, rest) = command
        .split_first()
        .ok_or_else(|| anyhow!("Command must contain at least one argument (the binary name)"))?;

    let mut output = std::process::Command::new(first);

    for arg in rest {
        output.arg(arg);
    }

    let command_invocation = get_command_invocation_string(&output);

    if let Some(path) = override_path {
        output.env("PATH", path);
    }

    let output = output.output().context(format!(
        "failed to run the command to check if it can be run: {command_invocation}"
    ))?;

    if !output.status.success() {
        return Err(anyhow!(
            "The command invocation is not possible: {}",
            output.status
        ));
    }

    Ok(())
}

fn check_perf_runs(perf_binary_path: &str, run_with_sudo: bool) -> anyhow::Result<()> {
    let sudo_path = sudo_path();

    let command = if run_with_sudo {
        vec![&sudo_path, perf_binary_path, "version"]
    } else {
        vec![perf_binary_path, "version"]
    };

    check_binary_runs(&command, None)
}

/// Returns the command invocation string for a given
/// [`tokio::process::Command`].
fn get_command_invocation_string(command: &std::process::Command) -> String {
    let mut command_str = command.get_program().to_string_lossy().into_owned();

    for arg in command.get_args() {
        command_str.push(' ');
        command_str.push_str(&arg.to_string_lossy());
    }

    command_str
}

/// Returns the clear `PATH` environment variable, so that even the
/// Python scripts can run cleanly without any virtualenv or pyenv
/// interference.
fn get_override_path_env() -> String {
    let path = std::env::var("PATH").unwrap_or_else(|_| STANDARD_PATH_PATHS.to_string());

    #[cfg(feature = "testing")]
    let path = path
        .split(':')
        .filter(|p| {
            let p = p.to_lowercase();
            !p.contains("virtualenv")
                && !p.contains("venv")
                && !p.contains("pyenv")
                && !p.contains("pypoetry")
                && !p.contains("pyenv-virtualenv")
                && !p.contains("pyenv-virtualenvs")
        })
        .collect::<Vec<_>>()
        .join(":");

    path
}

/// The generator for the pprof profile.
///
/// The generator tools have the path to the binary
/// that will be used to generate the profile.
/// If the path is [`None`], the tool will be searched
/// in the system's `PATH` using the default name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProfileGenerator {
    /// The `bcc` tool for generating profiles.
    BccProfile(Option<PathBuf>),
    /// The Linux `perf` tool.
    Perf(Option<PathBuf>),
}

impl ProfileGenerator {
    /// Returns the path to the `profile` binary if it is set.
    pub fn get_bcc_profile_binary_path(&self) -> Option<PathBuf> {
        match self {
            ProfileGenerator::BccProfile(path) => path.clone(),
            _ => None,
        }
    }
}

/// The options for generating a pprof profile using the `perf` tool.
#[derive(Debug, Clone)]
pub struct ProfileGenerationOptions {
    pub profiler: ProfileGenerator,
    /// If set to `true`, the `perf` command will be run with `sudo`.
    /// This is useful for profiling processes that require elevated
    /// privileges.
    /// If `false`, the command will be run without `sudo`.
    pub run_with_sudo: bool,
    /// The PID of the process to profile. This can be the main process
    /// or a child process. For targeting postgres, this should be the
    /// PID of the main postgres process.
    pub pids: HashSet<Pid>,
    /// If set to `true`, the `perf` command will follow forks.
    /// This means that it will continue to profile child processes
    /// that are created by the main process after it has already
    /// started profiling.
    pub follow_forks: bool,
    /// The sampling frequency in Hz.
    /// This is the frequency at which stack traces will be sampled.
    pub sampling_frequency: u32,
    /// A list of symbols to block from the profiling output.
    /// This is useful for filtering out noise from the profiling data,
    /// such as system libraries or other irrelevant symbols.
    pub blocklist_symbols: Vec<String>,
    /// Whether to archive the pprof profile data or not. If yes,
    /// the profile data will be gzipped before returning.
    pub archive: bool,
}

/// The options for the task for generating a pprof profile using the
/// `perf` tool.
#[derive(Debug, Clone)]
pub struct ProfileGenerationTaskOptions {
    /// The generation options. See [`ProfileGenerationOptions`].
    pub options: ProfileGenerationOptions,
    /// The duration for which the profiling should run.
    /// This is the maximum time the profiling will run before it is
    /// stopped. If the profiling is not stopped manually, it will run
    /// for this duration.
    pub timeout: std::time::Duration,
    /// An optional channel receiver that can be used to signal the
    /// profiling to stop early. If provided, the profiling will stop
    /// when a message is received on this channel or when the timeout
    /// is reached, whichever comes first.
    pub should_stop: Option<tokio::sync::broadcast::Sender<()>>,
}

/// Profiles the processes using the `bcc` framework and `profile.py`
/// tool and returns the collapsed (folded) stack output as [`Vec<u8>`].
#[instrument]
async fn profile_with_bcc_profile(
    options: ProfileGenerationTaskOptions,
) -> anyhow::Result<Vec<u8>> {
    let bcc_profile_binary_path =
        probe_bcc_profile_binary_paths(options.options.profiler.get_bcc_profile_binary_path())
            .context("failed to probe bcc profile binary paths")?;

    let path = get_override_path_env();

    let mut command = tokio::process::Command::new(
        probe_sudo_paths().context("failed to find the sudo executable")?,
    );

    command
        .arg(bcc_profile_binary_path)
        .arg("-f")
        .arg("-F")
        .arg(options.options.sampling_frequency.to_string())
        .arg("-p")
        .arg(
            options
                .options
                .pids
                .iter()
                .map(|p| p.as_raw().to_string())
                .collect::<Vec<_>>()
                .join(","),
        )
        .arg(options.timeout.as_secs().to_string())
        .env_clear()
        .env("PATH", path);

    let command_str = get_command_invocation_string(command.as_std());

    let result = command.output();

    let result = match options.should_stop.as_ref().map(|s| s.subscribe()) {
        Some(mut rx) => {
            tokio::select! {
                _ = rx.recv() => {
                    tracing::trace!("Received shutdown signal, stopping perf...");

                    return Err(anyhow!("Profiling was stopped by shutdown signal"));
                }
                result = result => {
                    tracing::trace!("Profiling completed, processing result...");
                    result
                }
            }
        }
        None => {
            tracing::trace!("No shutdown signal receiver provided, running bcc profile...");
            result.await
        }
    }
    .context(format!("failed to invoke bcc-profile using {command_str}"))?;

    match result.status.success() {
        true => Ok(result.stdout),
        false => {
            return Err(anyhow!(
                "failed to run bcc profiler, invocation: {command_str}, stderr: {}",
                String::from_utf8_lossy(&result.stderr)
            ));
        }
    }
}

/// Profiles the processes using the `perf` tool and returns the
/// collapsed (folded) stack output as [`Vec<u8>`].
#[allow(unsafe_code)]
#[instrument]
async fn profile_with_perf(options: ProfileGenerationTaskOptions) -> anyhow::Result<Vec<u8>> {
    let perf_binary_path = match options.options.profiler {
        ProfileGenerator::BccProfile(_) => {
            return Err(anyhow!(
                "bcc profile generator is not supported for perf profiling"
            ));
        }
        ProfileGenerator::Perf(v) => v,
    }
    .unwrap_or_else(|| PathBuf::from(perf_binary_path()));

    check_perf_runs(
        perf_binary_path
            .to_str()
            .context("couldn't reconstruct perf binary path as string.")?,
        options.options.run_with_sudo,
    )
    .context("couldn't check that perf is available and can be run")?;

    let temp_file = tempfile::NamedTempFile::new()
        .context("failed to create temporary file for perf output")?;
    let temp_file_path = temp_file.path();

    // Step 1: Run perf to collect stack traces
    let mut perf_record_command = if options.options.run_with_sudo {
        let mut cmd =
            tokio::process::Command::new(probe_sudo_paths().context("failed to find sudo")?);
        cmd.arg(&perf_binary_path);
        cmd
    } else {
        tokio::process::Command::new(&perf_binary_path)
    };

    let mut perf_record_command = perf_record_command
        .arg("record")
        // Target the specified process IDs.
        .arg("-a")
        // Specify the sampling frequency or default to 99 Hz.
        .arg("-F")
        .arg(options.options.sampling_frequency.to_string())
        // Enable call-graph (stack chain/backtrace) recording for both
        // kernel space and user space.
        .arg("-g")
        // Disable buffering to ensure that the output is written
        // immediately to the temporary file.
        // This is important for real-time profiling and ensures that
        // we get the most accurate stack traces and that after we "ask"
        // perf to stop, we get the data dumped to the file properly.
        .arg("--no-buffering");

    if options.options.follow_forks {
        perf_record_command = perf_record_command.arg("--inherit");
    }

    // Make it write to stdout instead of a file.
    let perf_record_command = perf_record_command
        .arg("-o")
        .arg(temp_file_path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());

    let mut perf_record_command = perf_record_command
        .spawn()
        .context("failed to spawn a process for perf record")?;

    let rx = options.should_stop.as_ref().map(|s| s.subscribe());

    if let Some(mut rx) = rx {
        tokio::select! {
            _ = rx.recv() => {
                tracing::trace!("Received shutdown signal, stopping perf...");
            }
            _ = tokio::time::sleep(options.timeout) => {
                tracing::trace!("Timeout reached, stopping perf...");
            }
        }
    } else {
        tokio::time::sleep(options.timeout).await;
        tracing::trace!("Timeout reached, stopping perf...");
    }

    let perf_record_pid = perf_record_command
        .id()
        .ok_or_else(|| anyhow!("failed to get perf record process ID"))?;

    // Send SIGTERM to the perf record process to stop it
    // gracefully and wait for it to finish.
    let _ = nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(perf_record_pid as i32),
        nix::sys::signal::Signal::SIGTERM,
    );

    tracing::trace!(
        "Waiting for perf record to finish for pids = {:?}...",
        options.options.pids
    );

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    if perf_record_command.try_wait().is_err() {
        perf_record_command
            .kill()
            .await
            .context("failed to kill perf record command")?;
    }

    let _ = perf_record_command
        .wait()
        .await
        .context("failed to wait for perf record command")?;

    let mut perf_record_stderr = perf_record_command
        .stderr
        .take()
        .context("failed to take stderr from perf record command")?;

    let mut perf_record_stderr_string: String = String::new();
    perf_record_stderr
        .read_to_string(&mut perf_record_stderr_string)
        .await
        .context("failed to read stderr from perf record command")?;

    let temp_file_size = std::fs::metadata(temp_file_path)
        .context(format!(
            "couldn't get metadata for temp file: {temp_file_path:?}"
        ))?
        .len();

    if temp_file_size == 0 {
        tracing::warn!(
            "perf record output file is empty, no data collected.\nPerf stderr: {perf_record_stderr_string}"
        );

        return Err(anyhow!(
            "perf record output file is empty, no data collected"
        ));
    }

    let perf_script_output = tokio::process::Command::new(&perf_binary_path)
        .arg("script")
        .arg("-i")
        .arg(temp_file_path)
        .arg(format!(
            "--pid={}",
            options
                .options
                .pids
                .iter()
                .map(|p| p.as_raw().to_string())
                .collect::<Vec<_>>()
                .join(",")
        ))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .context(format!("couldn't run perf script -i {temp_file_path:?}"))?;

    if !perf_script_output.status.success() {
        return Err(anyhow!(
            "perf script command failed: {}",
            String::from_utf8(perf_script_output.stderr)
                .unwrap_or_else(|_| "Invalid UTF-8 output".to_string())
        ));
    }

    if perf_script_output.stdout.is_empty() {
        return Err(anyhow!(format!(
            "Perf script output is empty for pid = {:?}.\n",
            options.options.pids
        )));
    }

    // Step 2: Collapse the stack traces into a folded stack trace
    let mut folder = {
        let mut options = inferno::collapse::perf::Options::default();
        options.annotate_jit = true; // Enable JIT annotation if needed
        options.annotate_kernel = true; // Enable kernel annotation if needed
        options.include_addrs = true; // Include addresses in the output
        options.include_pid = true; // Include PIDs in the output
        options.include_tid = true; // Include TIDs in the output
        inferno::collapse::perf::Folder::from(options)
    };

    let mut collapsed = Vec::new();

    folder
        .collapse(perf_script_output.stdout.as_slice(), &mut collapsed)
        .context("couldn't collapse the output of perf script into the folded format")?;

    if collapsed.is_empty() {
        tracing::error!(
            "collapsed stack trace is empty for output: {}",
            String::from_utf8_lossy(&perf_script_output.stdout)
        );

        return Err(anyhow!("collapsed stack trace is empty"));
    }

    Ok(collapsed)
}

/// Run perf against a process with the given name and generate a pprof
/// profile from the output.
///
/// The pipeline is as follows:
/// 1. Running perf for the specified process name and duration.
/// 2. Collapsing the perf output into a folded stack trace.
/// 3. Generating a pprof profile from the collapsed stack trace.
///
/// This function blocks until the profiling is complete or the timeout
/// is reached.
///
/// If the perf binary path is not provided, it defaults to "perf" in
/// the system's `PATH`.
pub async fn generate_pprof_profile(
    mut options: ProfileGenerationTaskOptions,
) -> anyhow::Result<PprofData> {
    options.options.pids = options
        .options
        .pids
        .into_iter()
        .flat_map(list_children_processes)
        .flatten()
        .collect::<HashSet<Pid>>();

    let blocklist_symbols = options.options.blocklist_symbols.clone();
    let archive = options.options.archive;

    let collapsed = match options.options.profiler {
        ProfileGenerator::BccProfile(_) => profile_with_bcc_profile(options).await,
        ProfileGenerator::Perf(_) => profile_with_perf(options).await,
    }
    .context("failed to run the profiler")?;

    if collapsed.is_empty() {
        return Err(anyhow!("collapsed output is empty, no data collected"));
    }

    generate_pprof_from_collapsed(&collapsed, &blocklist_symbols, archive)
        .context("failed to generate pprof profile from collapsed stack trace")
}

fn archive_bytes<W: Write>(bytes: &[u8], output: &mut W) -> anyhow::Result<()> {
    let mut encoder = GzEncoder::new(output, Compression::default());

    encoder
        .write_all(bytes)
        .context("couldn't write the bytes into the gz encoder")?;
    encoder
        .finish()
        .context("couldn't finish the gz encoder session")?;

    Ok(())
}

/// Returns the executable path for a given process ID.
fn get_exe_from_pid(pid: Pid, fallback_name: &str) -> String {
    libproc::proc_pid::name(pid.as_raw()).unwrap_or_else(|_| fallback_name.to_owned())
}

/// Generate a pprof profile from a collapsed stack trace file.
pub fn generate_pprof_from_collapsed<S: AsRef<str>>(
    collapsed_bytes: &[u8],
    blocklist_symbols: &[S],
    archive: bool,
) -> anyhow::Result<PprofData> {
    use pprof::protos::Message;
    use pprof::protos::profile::{Function, Line, Location, Profile, Sample, ValueType};

    let mut profile = Profile::default();
    let mut value_type = ValueType::new();
    value_type.ty = profile.string_table.len() as i64;
    value_type.unit = profile.string_table.len() as i64 + 1;

    profile.sample_type.push(value_type);
    profile.string_table.push("".to_string()); // 0
    profile.string_table.push("samples".to_string()); // 1
    profile.string_table.push("count".to_string()); // 2

    let mut mapping_map = HashMap::new(); // binary_name -> mapping_id
    let mut function_map = HashMap::new(); // (function_name) -> function_id
    let mut location_map = HashMap::new(); // (function_id) -> location_id

    let reader = BufReader::new(Cursor::new(collapsed_bytes));

    for line in reader.lines() {
        let line = line.context("invalid line found")?;
        let Some((stack_str, count_str)) = line.rsplit_once(' ') else {
            continue;
        };
        let count: i64 = count_str
            .trim()
            .parse()
            .context("failed to parse the counter")?;

        let mut parts = stack_str.trim().split(';');

        // Extract binary name and function stack
        let raw_binary = parts.next().unwrap_or("[unknown]");
        let (bin_part, pid) = raw_binary
            .rsplit_once('/')
            .unwrap_or((raw_binary, "unknown"));
        let binary_name = bin_part.strip_suffix("-?").unwrap_or(bin_part);

        let mapping_id = *mapping_map
            .entry(binary_name.to_string())
            .or_insert_with(|| {
                let exe =
                    get_exe_from_pid(Pid::from_raw(pid.parse::<pid_t>().unwrap_or(0)), "unknown");
                let id = profile.mapping.len() as u64 + 1;
                profile.string_table.push(exe);
                profile.mapping.push(pprof::protos::profile::Mapping {
                    id,
                    filename: profile.string_table.len() as i64 - 1,
                    ..Default::default()
                });
                id
            });

        let stack: Vec<&str> = stack_str
            .trim()
            .split(';')
            .filter(|s| {
                // Filter out blocklisted symbols
                !blocklist_symbols.iter().any(|b| s.contains(b.as_ref()))
            })
            .collect();

        let mut location_ids = Vec::new();

        for func_name in stack.iter() {
            let func_id = *function_map
                .entry(func_name.to_string())
                .or_insert_with(|| {
                    let id = profile.function.len() as u64 + 1;
                    profile.string_table.push(func_name.to_string());
                    profile.function.push(Function {
                        id,
                        name: profile.string_table.len() as i64 - 1,
                        system_name: profile.string_table.len() as i64 - 1,
                        filename: 0,
                        start_line: 0,
                        special_fields: Default::default(),
                    });
                    id
                });

            let loc_id = *location_map.entry(func_id).or_insert_with(|| {
                let id = profile.location.len() as u64 + 1;
                profile.location.push(Location {
                    id,
                    mapping_id,
                    address: 0,
                    line: vec![Line {
                        function_id: func_id,
                        line: 0,
                        special_fields: Default::default(),
                    }],
                    is_folded: true,
                    special_fields: Default::default(),
                });
                id
            });

            location_ids.push(loc_id);
        }

        profile.sample.push(Sample {
            location_id: location_ids.into_iter().rev().collect(),
            value: vec![count],
            label: vec![],
            special_fields: Default::default(),
        });
    }

    let mut protobuf_encoded = Vec::new();
    profile
        .write_to_vec(&mut protobuf_encoded)
        .context("failed to write the encoded pprof-protobuf message into a vec")?;

    Ok(PprofData(if archive {
        let mut archived = Vec::new();
        archive_bytes(&protobuf_encoded, &mut archived)
            .context("couldn't archive the pprof-protobuf message data")?;
        archived
    } else {
        protobuf_encoded
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pprof_from_collapsed() {
        const COLLAPSED_DATA: &str = r#"fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;_copy_from_iter;_copy_from_iter 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;_copy_from_iter;_copy_from_iter 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;__wake_up;__wake_up 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;vfs_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;do_output_char;do_output_char 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;tty_update_time;tty_update_time 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;n_tty_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;tty_ldisc_deref;tty_ldisc_deref 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;n_tty_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;__x64_sys_write;__x64_sys_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;tty_update_time;ktime_get_real_seconds;ktime_get_real_seconds 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;n_tty_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;process_echoes;process_echoes 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;n_tty_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;tty_write;tty_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;n_tty_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;std::io::stdio::print_to_buffer_if_capture_used::hcd24b3f3cecb2f16 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;n_tty_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;ldsem_up_read;ldsem_up_read 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;tty_update_time;_raw_spin_lock;_raw_spin_lock 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;__check_object_size;__virt_addr_valid;__virt_addr_valid 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;n_tty_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;[unknown] 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;file_tty_write.isra.0 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;tty_update_time;tty_update_time 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;__wake_up;__wake_up 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;tty_update_time;_raw_spin_lock;_raw_spin_lock 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;n_tty_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;fdget_pos;fdget_pos 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;do_output_char;do_output_char 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;file_tty_write.isra.0 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;n_tty_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;mutex_unlock;mutex_unlock 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;remove_wait_queue;_raw_spin_lock_irqsave;_raw_spin_lock_irqsave 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;add_wait_queue;_raw_spin_lock_irqsave;_raw_spin_lock_irqsave 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;__check_object_size;__virt_addr_valid;__virt_addr_valid 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;do_output_char;pty_write;tty_insert_flip_string_and_push_buffer;tty_insert_flip_string_and_push_buffer 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;n_tty_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;mutex_unlock;mutex_unlock 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;n_tty_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;file_tty_write.isra.0 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;pty_write_room;pty_write_room 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;mutex_trylock;mutex_trylock 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;__check_object_size;__check_heap_object;__check_heap_object 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;mutex_lock;mutex_lock 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;file_tty_write.isra.0 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;do_output_char;do_output_char 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;__check_object_size;__check_heap_object;__check_heap_object 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;pty_write;tty_insert_flip_string_and_push_buffer;queue_work_on;queue_work_on 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;vfs_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;mutex_unlock;mutex_unlock 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;pty_write;tty_insert_flip_string_and_push_buffer;_raw_spin_lock_irqsave;_raw_spin_lock_irqsave 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;file_tty_write.isra.0 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;__wake_up;__wake_up 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;vfs_write 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;srso_alias_return_thunk;srso_alias_safe_ret;srso_alias_safe_ret 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;__check_object_size;__check_object_size 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;tty_write_room;tty_write_room 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;rw_verify_area;rw_verify_area 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;file_tty_write.isra.0 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;__wake_up;_raw_spin_lock_irqsave;_raw_spin_lock_irqsave 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;do_output_char;do_output_char 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;__check_object_size;__check_heap_object;__check_heap_object 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;__check_object_size;__check_heap_object;__check_heap_object 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;_copy_from_iter;_copy_from_iter 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;do_output_char;pty_write;tty_insert_flip_string_and_push_buffer;queue_work_on;queue_work_on 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;down_read;down_read 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;rw_verify_area;rw_verify_area 1
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;tty_update_time;tty_update_time 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;n_tty_write 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;pty_write_room;pty_write_room 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;add_wait_queue;_raw_spin_lock_irqsave;_raw_spin_lock_irqsave 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;n_tty_write 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;_copy_from_iter;_copy_from_iter 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;tty_update_time;tty_update_time 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;file_tty_write.isra.0 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;pty_write;pty_write 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;fdget_pos;fdget_pos 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;pty_write;tty_insert_flip_string_and_push_buffer;tty_insert_flip_string_and_push_buffer 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;_raw_spin_unlock_irqrestore;_raw_spin_unlock_irqrestore 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;pty_write;tty_insert_flip_string_and_push_buffer;tty_insert_flip_string_and_push_buffer 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;mutex_unlock;mutex_unlock 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;vfs_write 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;n_tty_write 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;pty_write_room;pty_write_room 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;[unknown] 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;process_echoes;process_echoes 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;file_tty_write.isra.0 2
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;file_tty_write.isra.0 3
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;__wake_up;_raw_spin_unlock_irqrestore;_raw_spin_unlock_irqrestore 3
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;tty_update_time;tty_update_time 3
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;std::io::stdio::_print::h83d703bcf3ee60d9 3
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;do_syscall_64 3
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown] 3
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;pty_write_room;pty_write_room 4
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;pty_write_room;pty_write_room 4
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;fdget_pos;fdget_pos 4
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;file_tty_write.isra.0 5
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;n_tty_write 5
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;mutex_lock;mutex_lock 6
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;_raw_spin_unlock_irqrestore;_raw_spin_unlock_irqrestore 7
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;do_syscall_64 13
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;do_output_char;pty_write;tty_insert_flip_string_and_push_buffer;_raw_spin_unlock_irqrestore;_raw_spin_unlock_irqrestore 13
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;pty_write;tty_insert_flip_string_and_push_buffer;_raw_spin_unlock_irqrestore;_raw_spin_unlock_irqrestore 14
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;do_output_char;pty_write;tty_insert_flip_string_and_push_buffer;queue_work_on;queue_work_on 15
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown] 72
fxtest;_start;__libc_start_main;std::rt::lang_start::h1a0450dcf98649e3;test::main::h02002fad832e89f3;std::io::stdio::_print::h83d703bcf3ee60d9;_$LT$$RF$std..io..stdio..Stdout$u20$as$u20$std..io..Write$GT$::write_fmt::h66e8a2ffb8c1eda1;core::fmt::write::hfe57b7174b7d8eab;_$LT$std..io..default_write_fmt..Adapter$LT$T$GT$$u20$as$u20$core..fmt..Write$GT$::write_str::hb8e2dcabc266bbd5;_$LT$std..io..stdio..StdoutLock$u20$as$u20$std..io..Write$GT$::write_all::h69a9b485ff934b93;write;[unknown];[unknown];entry_SYSCALL_64_after_hwframe;do_syscall_64;ksys_write;vfs_write;file_tty_write.isra.0;n_tty_write;pty_write;tty_insert_flip_string_and_push_buffer;queue_work_on;queue_work_on 188"#;
        const BLOCKLIST_SYMBOLS: &[&str] = &[];
        const EXPECTED: [u8; 5348] = [
            10, 2, 16, 1, 18, 25, 10, 20, 18, 18, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6,
            5, 4, 3, 2, 1, 18, 1, 1, 18, 15, 10, 10, 11, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18,
            25, 10, 20, 18, 18, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18,
            1, 1, 18, 15, 10, 10, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 25, 10, 20, 19, 19,
            17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 23, 10,
            18, 16, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 26,
            10, 21, 21, 21, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18,
            1, 1, 18, 25, 10, 20, 22, 22, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3,
            2, 1, 18, 1, 1, 18, 25, 10, 20, 20, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6,
            5, 4, 3, 2, 1, 18, 1, 1, 18, 25, 10, 20, 23, 23, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9,
            8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 25, 10, 20, 20, 20, 17, 16, 15, 14, 13, 12, 12,
            11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 22, 10, 17, 24, 24, 14, 13, 12, 12,
            11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 26, 10, 21, 25, 25, 22, 17, 16, 15,
            14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 25, 10, 20, 20, 20,
            17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 13, 10, 8,
            9, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 26, 10, 21, 26, 26, 20, 17, 16, 15, 14, 13, 12,
            12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 12, 10, 7, 7, 6, 5, 4, 3, 2, 1,
            18, 1, 1, 18, 25, 10, 20, 20, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4,
            3, 2, 1, 18, 1, 1, 18, 24, 10, 19, 27, 27, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6,
            5, 4, 3, 2, 1, 18, 1, 1, 18, 25, 10, 20, 20, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9,
            8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 12, 10, 7, 28, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18,
            15, 10, 10, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 12, 10, 7, 7, 6, 5, 4, 3, 2,
            1, 18, 1, 1, 18, 25, 10, 20, 20, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5,
            4, 3, 2, 1, 18, 1, 1, 18, 25, 10, 20, 29, 29, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8,
            7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 26, 10, 21, 30, 30, 22, 17, 16, 15, 14, 13, 12, 12,
            11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 26, 10, 21, 32, 32, 31, 17, 16, 15,
            14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 25, 10, 20, 20, 20,
            17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 15, 10,
            10, 12, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 24, 10, 19, 17, 17, 16, 15, 14, 13,
            12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 15, 10, 10, 10, 9, 8, 7, 6, 5,
            4, 3, 2, 1, 18, 1, 1, 18, 25, 10, 20, 22, 22, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8,
            7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 25, 10, 20, 19, 19, 17, 16, 15, 14, 13, 12, 12, 11,
            10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 26, 10, 21, 30, 30, 22, 17, 16, 15, 14,
            13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 25, 10, 20, 20, 20, 17,
            16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 23, 10, 18,
            33, 33, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 26, 10,
            21, 21, 21, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1,
            1, 18, 24, 10, 19, 17, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1,
            18, 1, 1, 18, 25, 10, 20, 20, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4,
            3, 2, 1, 18, 1, 1, 18, 26, 10, 21, 34, 34, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9,
            8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 27, 10, 22, 36, 36, 35, 20, 17, 16, 15, 14, 13,
            12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 27, 10, 22, 36, 36, 37, 20,
            17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 26, 10,
            21, 32, 32, 31, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1,
            1, 18, 28, 10, 23, 39, 39, 38, 21, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6,
            5, 4, 3, 2, 1, 18, 1, 1, 18, 25, 10, 20, 20, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9,
            8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 12, 10, 7, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 25,
            10, 20, 34, 34, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1,
            1, 18, 25, 10, 20, 20, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2,
            1, 18, 1, 1, 18, 24, 10, 19, 17, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4,
            3, 2, 1, 18, 1, 1, 18, 13, 10, 8, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 26, 10, 21, 40,
            40, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18,
            25, 10, 20, 41, 41, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18,
            1, 1, 18, 26, 10, 21, 42, 42, 31, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4,
            3, 2, 1, 18, 1, 1, 18, 26, 10, 21, 43, 43, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9,
            8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 24, 10, 19, 17, 17, 16, 15, 14, 13, 12, 12, 11,
            10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 26, 10, 21, 21, 21, 20, 17, 16, 15, 14,
            13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 26, 10, 21, 42, 42, 31,
            17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 28, 10,
            23, 44, 44, 39, 38, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1,
            18, 1, 1, 18, 23, 10, 18, 16, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2,
            1, 18, 1, 1, 18, 26, 10, 21, 34, 34, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7,
            6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 28, 10, 23, 36, 36, 39, 38, 20, 17, 16, 15, 14, 13, 12,
            12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 24, 10, 19, 17, 17, 16, 15, 14,
            13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 25, 10, 20, 19, 19, 17,
            16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 23, 10, 18,
            16, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 11, 10, 6,
            6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 14, 10, 9, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 27,
            10, 22, 46, 46, 45, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1,
            18, 1, 1, 18, 25, 10, 20, 31, 31, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4,
            3, 2, 1, 18, 1, 1, 18, 26, 10, 21, 47, 47, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9,
            8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 24, 10, 19, 48, 48, 16, 15, 14, 13, 12, 12, 11,
            10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 24, 10, 19, 17, 17, 16, 15, 14, 13, 12,
            12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 26, 10, 21, 36, 36, 19, 17, 16,
            15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 26, 10, 21, 21,
            21, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18,
            26, 10, 21, 42, 42, 31, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1,
            18, 1, 1, 18, 26, 10, 21, 42, 42, 31, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6,
            5, 4, 3, 2, 1, 18, 1, 1, 18, 25, 10, 20, 18, 18, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9,
            8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 14, 10, 9, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1,
            18, 12, 10, 7, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 29, 10, 24, 44, 44, 39, 38, 21, 20,
            17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 26, 10,
            21, 49, 49, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1,
            1, 18, 11, 10, 6, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 24, 10, 19, 48, 48, 16, 15, 14, 13,
            12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 1, 18, 25, 10, 20, 22, 22, 17, 16,
            15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 2, 18, 25, 10, 20, 20,
            20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 2, 18, 26,
            10, 21, 40, 40, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18,
            1, 2, 18, 27, 10, 22, 36, 36, 37, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6,
            5, 4, 3, 2, 1, 18, 1, 2, 18, 25, 10, 20, 20, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9,
            8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 2, 18, 25, 10, 20, 18, 18, 17, 16, 15, 14, 13, 12, 12,
            11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 2, 18, 25, 10, 20, 22, 22, 17, 16, 15, 14,
            13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 2, 18, 24, 10, 19, 17, 17, 16,
            15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 2, 18, 26, 10, 21, 38,
            38, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 2, 18,
            23, 10, 18, 33, 33, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 2,
            18, 27, 10, 22, 39, 39, 38, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4,
            3, 2, 1, 18, 1, 2, 18, 26, 10, 21, 50, 50, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9,
            8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 2, 18, 27, 10, 22, 39, 39, 38, 20, 17, 16, 15, 14, 13,
            12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 2, 18, 10, 10, 5, 5, 4, 3, 2, 1, 18,
            1, 2, 18, 26, 10, 21, 34, 34, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4,
            3, 2, 1, 18, 1, 2, 18, 23, 10, 18, 16, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5,
            4, 3, 2, 1, 18, 1, 2, 18, 25, 10, 20, 20, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8,
            7, 6, 5, 4, 3, 2, 1, 18, 1, 2, 18, 26, 10, 21, 40, 40, 20, 17, 16, 15, 14, 13, 12, 12,
            11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 2, 18, 16, 10, 11, 12, 10, 9, 8, 7, 6, 5, 4,
            3, 2, 1, 18, 1, 2, 18, 26, 10, 21, 26, 26, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9,
            8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 2, 18, 24, 10, 19, 17, 17, 16, 15, 14, 13, 12, 12, 11,
            10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 2, 18, 24, 10, 19, 17, 17, 16, 15, 14, 13, 12,
            12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 3, 18, 26, 10, 21, 50, 50, 19, 17, 16,
            15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 3, 18, 25, 10, 20, 22,
            22, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 3, 18, 10,
            10, 5, 6, 4, 3, 2, 1, 18, 1, 3, 18, 21, 10, 16, 14, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6,
            5, 4, 3, 2, 1, 18, 1, 3, 18, 17, 10, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1,
            3, 18, 26, 10, 21, 40, 40, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3,
            2, 1, 18, 1, 4, 18, 26, 10, 21, 40, 40, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8,
            7, 6, 5, 4, 3, 2, 1, 18, 1, 4, 18, 23, 10, 18, 33, 33, 15, 14, 13, 12, 12, 11, 10, 9,
            8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 4, 18, 24, 10, 19, 17, 17, 16, 15, 14, 13, 12, 12, 11,
            10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 5, 18, 25, 10, 20, 20, 20, 17, 16, 15, 14, 13,
            12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 5, 18, 26, 10, 21, 43, 43, 20, 17,
            16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 6, 18, 26, 10, 21,
            50, 50, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 7,
            18, 21, 10, 16, 14, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 13, 18,
            29, 10, 24, 50, 50, 39, 38, 21, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5,
            4, 3, 2, 1, 18, 1, 13, 18, 28, 10, 23, 50, 50, 39, 38, 20, 17, 16, 15, 14, 13, 12, 12,
            11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 14, 18, 29, 10, 24, 44, 44, 39, 38, 21, 20,
            17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 15, 18, 18, 10,
            13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 1, 72, 18, 29, 10, 23, 44, 44, 39,
            38, 20, 17, 16, 15, 14, 13, 12, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 18, 2, 188, 1,
            26, 4, 8, 1, 40, 3, 34, 10, 8, 1, 16, 1, 34, 2, 8, 1, 40, 1, 34, 10, 8, 2, 16, 1, 34,
            2, 8, 2, 40, 1, 34, 10, 8, 3, 16, 1, 34, 2, 8, 3, 40, 1, 34, 10, 8, 4, 16, 1, 34, 2, 8,
            4, 40, 1, 34, 10, 8, 5, 16, 1, 34, 2, 8, 5, 40, 1, 34, 10, 8, 6, 16, 1, 34, 2, 8, 6,
            40, 1, 34, 10, 8, 7, 16, 1, 34, 2, 8, 7, 40, 1, 34, 10, 8, 8, 16, 1, 34, 2, 8, 8, 40,
            1, 34, 10, 8, 9, 16, 1, 34, 2, 8, 9, 40, 1, 34, 10, 8, 10, 16, 1, 34, 2, 8, 10, 40, 1,
            34, 10, 8, 11, 16, 1, 34, 2, 8, 11, 40, 1, 34, 10, 8, 12, 16, 1, 34, 2, 8, 12, 40, 1,
            34, 10, 8, 13, 16, 1, 34, 2, 8, 13, 40, 1, 34, 10, 8, 14, 16, 1, 34, 2, 8, 14, 40, 1,
            34, 10, 8, 15, 16, 1, 34, 2, 8, 15, 40, 1, 34, 10, 8, 16, 16, 1, 34, 2, 8, 16, 40, 1,
            34, 10, 8, 17, 16, 1, 34, 2, 8, 17, 40, 1, 34, 10, 8, 18, 16, 1, 34, 2, 8, 18, 40, 1,
            34, 10, 8, 19, 16, 1, 34, 2, 8, 19, 40, 1, 34, 10, 8, 20, 16, 1, 34, 2, 8, 20, 40, 1,
            34, 10, 8, 21, 16, 1, 34, 2, 8, 21, 40, 1, 34, 10, 8, 22, 16, 1, 34, 2, 8, 22, 40, 1,
            34, 10, 8, 23, 16, 1, 34, 2, 8, 23, 40, 1, 34, 10, 8, 24, 16, 1, 34, 2, 8, 24, 40, 1,
            34, 10, 8, 25, 16, 1, 34, 2, 8, 25, 40, 1, 34, 10, 8, 26, 16, 1, 34, 2, 8, 26, 40, 1,
            34, 10, 8, 27, 16, 1, 34, 2, 8, 27, 40, 1, 34, 10, 8, 28, 16, 1, 34, 2, 8, 28, 40, 1,
            34, 10, 8, 29, 16, 1, 34, 2, 8, 29, 40, 1, 34, 10, 8, 30, 16, 1, 34, 2, 8, 30, 40, 1,
            34, 10, 8, 31, 16, 1, 34, 2, 8, 31, 40, 1, 34, 10, 8, 32, 16, 1, 34, 2, 8, 32, 40, 1,
            34, 10, 8, 33, 16, 1, 34, 2, 8, 33, 40, 1, 34, 10, 8, 34, 16, 1, 34, 2, 8, 34, 40, 1,
            34, 10, 8, 35, 16, 1, 34, 2, 8, 35, 40, 1, 34, 10, 8, 36, 16, 1, 34, 2, 8, 36, 40, 1,
            34, 10, 8, 37, 16, 1, 34, 2, 8, 37, 40, 1, 34, 10, 8, 38, 16, 1, 34, 2, 8, 38, 40, 1,
            34, 10, 8, 39, 16, 1, 34, 2, 8, 39, 40, 1, 34, 10, 8, 40, 16, 1, 34, 2, 8, 40, 40, 1,
            34, 10, 8, 41, 16, 1, 34, 2, 8, 41, 40, 1, 34, 10, 8, 42, 16, 1, 34, 2, 8, 42, 40, 1,
            34, 10, 8, 43, 16, 1, 34, 2, 8, 43, 40, 1, 34, 10, 8, 44, 16, 1, 34, 2, 8, 44, 40, 1,
            34, 10, 8, 45, 16, 1, 34, 2, 8, 45, 40, 1, 34, 10, 8, 46, 16, 1, 34, 2, 8, 46, 40, 1,
            34, 10, 8, 47, 16, 1, 34, 2, 8, 47, 40, 1, 34, 10, 8, 48, 16, 1, 34, 2, 8, 48, 40, 1,
            34, 10, 8, 49, 16, 1, 34, 2, 8, 49, 40, 1, 34, 10, 8, 50, 16, 1, 34, 2, 8, 50, 40, 1,
            42, 6, 8, 1, 16, 4, 24, 4, 42, 6, 8, 2, 16, 5, 24, 5, 42, 6, 8, 3, 16, 6, 24, 6, 42, 6,
            8, 4, 16, 7, 24, 7, 42, 6, 8, 5, 16, 8, 24, 8, 42, 6, 8, 6, 16, 9, 24, 9, 42, 6, 8, 7,
            16, 10, 24, 10, 42, 6, 8, 8, 16, 11, 24, 11, 42, 6, 8, 9, 16, 12, 24, 12, 42, 6, 8, 10,
            16, 13, 24, 13, 42, 6, 8, 11, 16, 14, 24, 14, 42, 6, 8, 12, 16, 15, 24, 15, 42, 6, 8,
            13, 16, 16, 24, 16, 42, 6, 8, 14, 16, 17, 24, 17, 42, 6, 8, 15, 16, 18, 24, 18, 42, 6,
            8, 16, 16, 19, 24, 19, 42, 6, 8, 17, 16, 20, 24, 20, 42, 6, 8, 18, 16, 21, 24, 21, 42,
            6, 8, 19, 16, 22, 24, 22, 42, 6, 8, 20, 16, 23, 24, 23, 42, 6, 8, 21, 16, 24, 24, 24,
            42, 6, 8, 22, 16, 25, 24, 25, 42, 6, 8, 23, 16, 26, 24, 26, 42, 6, 8, 24, 16, 27, 24,
            27, 42, 6, 8, 25, 16, 28, 24, 28, 42, 6, 8, 26, 16, 29, 24, 29, 42, 6, 8, 27, 16, 30,
            24, 30, 42, 6, 8, 28, 16, 31, 24, 31, 42, 6, 8, 29, 16, 32, 24, 32, 42, 6, 8, 30, 16,
            33, 24, 33, 42, 6, 8, 31, 16, 34, 24, 34, 42, 6, 8, 32, 16, 35, 24, 35, 42, 6, 8, 33,
            16, 36, 24, 36, 42, 6, 8, 34, 16, 37, 24, 37, 42, 6, 8, 35, 16, 38, 24, 38, 42, 6, 8,
            36, 16, 39, 24, 39, 42, 6, 8, 37, 16, 40, 24, 40, 42, 6, 8, 38, 16, 41, 24, 41, 42, 6,
            8, 39, 16, 42, 24, 42, 42, 6, 8, 40, 16, 43, 24, 43, 42, 6, 8, 41, 16, 44, 24, 44, 42,
            6, 8, 42, 16, 45, 24, 45, 42, 6, 8, 43, 16, 46, 24, 46, 42, 6, 8, 44, 16, 47, 24, 47,
            42, 6, 8, 45, 16, 48, 24, 48, 42, 6, 8, 46, 16, 49, 24, 49, 42, 6, 8, 47, 16, 50, 24,
            50, 42, 6, 8, 48, 16, 51, 24, 51, 42, 6, 8, 49, 16, 52, 24, 52, 42, 6, 8, 50, 16, 53,
            24, 53, 50, 0, 50, 7, 115, 97, 109, 112, 108, 101, 115, 50, 5, 99, 111, 117, 110, 116,
            50, 7, 117, 110, 107, 110, 111, 119, 110, 50, 6, 102, 120, 116, 101, 115, 116, 50, 6,
            95, 115, 116, 97, 114, 116, 50, 17, 95, 95, 108, 105, 98, 99, 95, 115, 116, 97, 114,
            116, 95, 109, 97, 105, 110, 50, 38, 115, 116, 100, 58, 58, 114, 116, 58, 58, 108, 97,
            110, 103, 95, 115, 116, 97, 114, 116, 58, 58, 104, 49, 97, 48, 52, 53, 48, 100, 99,
            102, 57, 56, 54, 52, 57, 101, 51, 50, 29, 116, 101, 115, 116, 58, 58, 109, 97, 105,
            110, 58, 58, 104, 48, 50, 48, 48, 50, 102, 97, 100, 56, 51, 50, 101, 56, 57, 102, 51,
            50, 41, 115, 116, 100, 58, 58, 105, 111, 58, 58, 115, 116, 100, 105, 111, 58, 58, 95,
            112, 114, 105, 110, 116, 58, 58, 104, 56, 51, 100, 55, 48, 51, 98, 99, 102, 51, 101,
            101, 54, 48, 100, 57, 50, 91, 95, 36, 76, 84, 36, 36, 82, 70, 36, 115, 116, 100, 46,
            46, 105, 111, 46, 46, 115, 116, 100, 105, 111, 46, 46, 83, 116, 100, 111, 117, 116, 36,
            117, 50, 48, 36, 97, 115, 36, 117, 50, 48, 36, 115, 116, 100, 46, 46, 105, 111, 46, 46,
            87, 114, 105, 116, 101, 36, 71, 84, 36, 58, 58, 119, 114, 105, 116, 101, 95, 102, 109,
            116, 58, 58, 104, 54, 54, 101, 56, 97, 50, 102, 102, 98, 56, 99, 49, 101, 100, 97, 49,
            50, 35, 99, 111, 114, 101, 58, 58, 102, 109, 116, 58, 58, 119, 114, 105, 116, 101, 58,
            58, 104, 102, 101, 53, 55, 98, 55, 49, 55, 52, 98, 55, 100, 56, 101, 97, 98, 50, 111,
            95, 36, 76, 84, 36, 115, 116, 100, 46, 46, 105, 111, 46, 46, 100, 101, 102, 97, 117,
            108, 116, 95, 119, 114, 105, 116, 101, 95, 102, 109, 116, 46, 46, 65, 100, 97, 112,
            116, 101, 114, 36, 76, 84, 36, 84, 36, 71, 84, 36, 36, 117, 50, 48, 36, 97, 115, 36,
            117, 50, 48, 36, 99, 111, 114, 101, 46, 46, 102, 109, 116, 46, 46, 87, 114, 105, 116,
            101, 36, 71, 84, 36, 58, 58, 119, 114, 105, 116, 101, 95, 115, 116, 114, 58, 58, 104,
            98, 56, 101, 50, 100, 99, 97, 98, 99, 50, 54, 54, 98, 98, 100, 53, 50, 91, 95, 36, 76,
            84, 36, 115, 116, 100, 46, 46, 105, 111, 46, 46, 115, 116, 100, 105, 111, 46, 46, 83,
            116, 100, 111, 117, 116, 76, 111, 99, 107, 36, 117, 50, 48, 36, 97, 115, 36, 117, 50,
            48, 36, 115, 116, 100, 46, 46, 105, 111, 46, 46, 87, 114, 105, 116, 101, 36, 71, 84,
            36, 58, 58, 119, 114, 105, 116, 101, 95, 97, 108, 108, 58, 58, 104, 54, 57, 97, 57, 98,
            52, 56, 53, 102, 102, 57, 51, 52, 98, 57, 51, 50, 5, 119, 114, 105, 116, 101, 50, 9,
            91, 117, 110, 107, 110, 111, 119, 110, 93, 50, 30, 101, 110, 116, 114, 121, 95, 83, 89,
            83, 67, 65, 76, 76, 95, 54, 52, 95, 97, 102, 116, 101, 114, 95, 104, 119, 102, 114, 97,
            109, 101, 50, 13, 100, 111, 95, 115, 121, 115, 99, 97, 108, 108, 95, 54, 52, 50, 10,
            107, 115, 121, 115, 95, 119, 114, 105, 116, 101, 50, 9, 118, 102, 115, 95, 119, 114,
            105, 116, 101, 50, 21, 102, 105, 108, 101, 95, 116, 116, 121, 95, 119, 114, 105, 116,
            101, 46, 105, 115, 114, 97, 46, 48, 50, 15, 95, 99, 111, 112, 121, 95, 102, 114, 111,
            109, 95, 105, 116, 101, 114, 50, 9, 95, 95, 119, 97, 107, 101, 95, 117, 112, 50, 11,
            110, 95, 116, 116, 121, 95, 119, 114, 105, 116, 101, 50, 14, 100, 111, 95, 111, 117,
            116, 112, 117, 116, 95, 99, 104, 97, 114, 50, 15, 116, 116, 121, 95, 117, 112, 100, 97,
            116, 101, 95, 116, 105, 109, 101, 50, 15, 116, 116, 121, 95, 108, 100, 105, 115, 99,
            95, 100, 101, 114, 101, 102, 50, 15, 95, 95, 120, 54, 52, 95, 115, 121, 115, 95, 119,
            114, 105, 116, 101, 50, 22, 107, 116, 105, 109, 101, 95, 103, 101, 116, 95, 114, 101,
            97, 108, 95, 115, 101, 99, 111, 110, 100, 115, 50, 14, 112, 114, 111, 99, 101, 115,
            115, 95, 101, 99, 104, 111, 101, 115, 50, 9, 116, 116, 121, 95, 119, 114, 105, 116,
            101, 50, 66, 115, 116, 100, 58, 58, 105, 111, 58, 58, 115, 116, 100, 105, 111, 58, 58,
            112, 114, 105, 110, 116, 95, 116, 111, 95, 98, 117, 102, 102, 101, 114, 95, 105, 102,
            95, 99, 97, 112, 116, 117, 114, 101, 95, 117, 115, 101, 100, 58, 58, 104, 99, 100, 50,
            52, 98, 51, 102, 51, 99, 101, 99, 98, 50, 102, 49, 54, 50, 13, 108, 100, 115, 101, 109,
            95, 117, 112, 95, 114, 101, 97, 100, 50, 14, 95, 114, 97, 119, 95, 115, 112, 105, 110,
            95, 108, 111, 99, 107, 50, 19, 95, 95, 99, 104, 101, 99, 107, 95, 111, 98, 106, 101,
            99, 116, 95, 115, 105, 122, 101, 50, 17, 95, 95, 118, 105, 114, 116, 95, 97, 100, 100,
            114, 95, 118, 97, 108, 105, 100, 50, 9, 102, 100, 103, 101, 116, 95, 112, 111, 115, 50,
            12, 109, 117, 116, 101, 120, 95, 117, 110, 108, 111, 99, 107, 50, 17, 114, 101, 109,
            111, 118, 101, 95, 119, 97, 105, 116, 95, 113, 117, 101, 117, 101, 50, 22, 95, 114, 97,
            119, 95, 115, 112, 105, 110, 95, 108, 111, 99, 107, 95, 105, 114, 113, 115, 97, 118,
            101, 50, 14, 97, 100, 100, 95, 119, 97, 105, 116, 95, 113, 117, 101, 117, 101, 50, 9,
            112, 116, 121, 95, 119, 114, 105, 116, 101, 50, 38, 116, 116, 121, 95, 105, 110, 115,
            101, 114, 116, 95, 102, 108, 105, 112, 95, 115, 116, 114, 105, 110, 103, 95, 97, 110,
            100, 95, 112, 117, 115, 104, 95, 98, 117, 102, 102, 101, 114, 50, 14, 112, 116, 121,
            95, 119, 114, 105, 116, 101, 95, 114, 111, 111, 109, 50, 13, 109, 117, 116, 101, 120,
            95, 116, 114, 121, 108, 111, 99, 107, 50, 19, 95, 95, 99, 104, 101, 99, 107, 95, 104,
            101, 97, 112, 95, 111, 98, 106, 101, 99, 116, 50, 10, 109, 117, 116, 101, 120, 95, 108,
            111, 99, 107, 50, 13, 113, 117, 101, 117, 101, 95, 119, 111, 114, 107, 95, 111, 110,
            50, 23, 115, 114, 115, 111, 95, 97, 108, 105, 97, 115, 95, 114, 101, 116, 117, 114,
            110, 95, 116, 104, 117, 110, 107, 50, 19, 115, 114, 115, 111, 95, 97, 108, 105, 97,
            115, 95, 115, 97, 102, 101, 95, 114, 101, 116, 50, 14, 116, 116, 121, 95, 119, 114,
            105, 116, 101, 95, 114, 111, 111, 109, 50, 14, 114, 119, 95, 118, 101, 114, 105, 102,
            121, 95, 97, 114, 101, 97, 50, 9, 100, 111, 119, 110, 95, 114, 101, 97, 100, 50, 27,
            95, 114, 97, 119, 95, 115, 112, 105, 110, 95, 117, 110, 108, 111, 99, 107, 95, 105,
            114, 113, 114, 101, 115, 116, 111, 114, 101,
        ];

        assert_eq!(
            generate_pprof_from_collapsed(COLLAPSED_DATA.as_bytes(), BLOCKLIST_SYMBOLS, false)
                .unwrap()
                .0,
            EXPECTED
        );
    }
}
