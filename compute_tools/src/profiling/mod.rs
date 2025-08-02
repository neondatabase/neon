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
