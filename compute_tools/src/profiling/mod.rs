use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader, Cursor, Write},
    ops::{Deref, DerefMut},
    path::PathBuf,
};

use anyhow::{Context, anyhow};
use aws_config::profile::Profile;
use flate2::{Compression, write::GzEncoder};
use futures::{StreamExt, stream::FuturesUnordered};
use inferno::collapse::Collapse;
use nix::{libc::pid_t, unistd::Pid};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tracing::{Instrument, error, instrument};
use x509_cert::der::oid::db::rfc4519::L;

const SUDO_PATH: &str = "/usr/bin/sudo";

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
    Ok(procfs::process::all_processes()
        .context("couldn't list the processes running in the system")?
        .filter_map(|proc| {
            if let Ok(stat) = proc.and_then(|p| p.stat()) {
                if stat.ppid == parent_pid.as_raw() {
                    tracing::error!("Found child process with PID: {} ({})", stat.pid, stat.comm);
                    return Some(Pid::from_raw(stat.pid));
                }
            }
            None
        })
        .collect())
}

fn check_binary_runs(command: &[&str]) -> anyhow::Result<()> {
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

    let output = output
        .output()
        .context("failed to run the command to check if it can be run")?;

    if !output.status.success() {
        return Err(anyhow!(
            "The command invocation is not possible: {}",
            output.status
        ));
    }

    Ok(())
}

fn check_perf_runs(perf_binary_path: &str, run_with_sudo: bool) -> anyhow::Result<()> {
    let command = if run_with_sudo {
        vec![SUDO_PATH, perf_binary_path, "version"]
    } else {
        vec![perf_binary_path, "version"]
    };

    check_binary_runs(&command)
}

fn pause_processes(pids: &HashSet<Pid>) -> anyhow::Result<()> {
    for pid in pids {
        if let Err(e) = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid.as_raw()),
            nix::sys::signal::Signal::SIGSTOP,
        ) {
            tracing::error!("Failed to pause process with PID: {pid}: {e}");
        }
    }

    Ok(())
}

fn unpause_processes(pids: &HashSet<Pid>) -> anyhow::Result<()> {
    for pid in pids {
        if let Err(e) = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid.as_raw()),
            nix::sys::signal::Signal::SIGCONT,
        ) {
            tracing::error!("Failed to unpause process with PID: {pid}: {e}");
        }
    }

    Ok(())
}

/// The options for generating a pprof profile using the `perf` tool.
#[derive(Debug, Clone)]
pub struct ProfileGenerationOptions {
    /// If set to `true`, the `perf` command will be run with `sudo`.
    /// This is useful for profiling processes that require elevated
    /// privileges.
    /// If `false`, the command will be run without `sudo`.
    pub run_with_sudo: bool,
    /// The path to the `perf` binary. If `None`, it defaults to "perf",
    /// so it will look for `perf` in the system's `PATH`.
    pub perf_binary_path: Option<PathBuf>,
    /// The PID of the process to profile. This can be the main process
    /// or a child process. For targeting postgres, this should be the
    /// PID of the main postgres process.
    pub pid: Pid,
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
}

/// The options for the task for generating a pprof profile using the
/// `perf` tool.
#[derive(Debug, Clone)]
pub struct ProfileGenerationTaskOptions {
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

async fn profile_single_process_bcc_profile(
    _perf_binary_path: PathBuf,
    pids: HashSet<Pid>,
    options: ProfileGenerationTaskOptions,
) -> anyhow::Result<Vec<u8>> {
    let path = std::env::var("PATH").unwrap_or_else(|_| {
        "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string()
    });

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

    let cmd = tokio::process::Command::new(SUDO_PATH)
        .arg("/usr/share/bcc/tools/profile")
        .arg("-f")
        .arg("-F")
        .arg(options.options.sampling_frequency.to_string())
        .arg("-p")
        .arg(
            pids.iter()
                .map(|p| p.as_raw().to_string())
                .collect::<Vec<_>>()
                .join(","),
        )
        .arg(options.timeout.as_secs().to_string())
        .env_clear()
        .env("PATH", path)
        .output()
        .await
        .context("failed to run bcc profile command")?;

    Ok(cmd.stdout)
}

/// Profiles a single process using the `perf` tool and returns the
/// output of `perf script` as [`Vec<u8>`].
#[allow(unsafe_code)]
#[instrument]
async fn profile_single_process_perf(
    perf_binary_path: PathBuf,
    pids: HashSet<Pid>,
    options: ProfileGenerationTaskOptions,
) -> anyhow::Result<Vec<u8>> {
    let temp_file = tempfile::NamedTempFile::new()
        .context("failed to create temporary file for perf output")?;
    let temp_file_path = temp_file.path();

    // Step 1: Run perf to collect stack traces
    let mut perf_record_command = if options.options.run_with_sudo {
        let mut cmd = tokio::process::Command::new(SUDO_PATH);
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
        .arg("--no-buffering")
        // #[cfg(feature = "testing")]
        .arg("-v");

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
        "Waiting for perf record to finish for pid = {}...",
        options.options.pid
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
            pids.iter()
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

    error!(
        "Reading perf script stdout for pid = {}",
        options.options.pid
    );

    if perf_script_output.stdout.is_empty() {
        return Err(anyhow!(format!(
            "Perf script output is empty for pid = {}.\n",
            options.options.pid
        )));
    }

    error!("Profiling succeeded for pid = {}", options.options.pid);

    Ok(perf_script_output.stdout)
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
    options: ProfileGenerationTaskOptions,
) -> anyhow::Result<PprofData> {
    let perf_binary_path = options
        .options
        .perf_binary_path
        .clone()
        .map_or_else(|| PathBuf::from("perf"), |p| p.to_owned());

    check_perf_runs(
        perf_binary_path
            .to_str()
            .context("couldn't reconstruct perf binary path as string.")?,
        options.options.run_with_sudo,
    )
    .context("couldn't check that perf is available and can be run")?;

    let mut pids = list_children_processes(options.options.pid)
        .unwrap_or_default()
        .into_iter()
        .chain(std::iter::once(options.options.pid))
        .collect::<HashSet<_>>();
    // let mut pids = HashSet::new();
    // pids.insert(options.options.pid);

    pause_processes(&pids).context("couldn't pause the processes for profiling")?;

    let mut futures_unordered = FuturesUnordered::new();

    {
        let options = options.clone();
        let perf_binary_path = perf_binary_path.clone();
        let pids = pids.clone();

        let task = tokio::task::spawn(async move {
            (
                // profile_single_process_perf(perf_binary_path, pids.clone(), options.clone()).await,
                profile_single_process_bcc_profile(perf_binary_path, pids.clone(), options.clone())
                    .await,
                options.options.pid,
            )
        });
        futures_unordered.push(task);
    }
    // for pid in &pids {
    //     let mut options = options.clone();
    //     let pid = *pid;
    //     options.options.pid = pid;
    //     let perf_binary_path = perf_binary_path.clone();

    //     let task = tokio::task::spawn(async move {
    //         (profile_single_process(perf_binary_path, options).await, pid)
    //     });
    //     futures_unordered.push(task);
    // }

    unpause_processes(&pids).context("couldn't pause the processes for profiling")?;

    let collapsed: Vec<u8> = {
        let mut ret = Vec::new();

        let mut count = 1;
        let total = futures_unordered.len();
        // TODO: timeout here as well (tokio::select with timeout)
        while let Some(result) = futures_unordered.next().await {
            match result {
                Ok((Ok(output), pid)) => {
                    pids.remove(&pid);
                    error!(
                        "Per-process profiling task completed successfully {count} / {total} (for pid = {pid})"
                    );
                    error!(
                        "Perf script output for pid {pid}: {}",
                        String::from_utf8_lossy(&output)
                    );
                    ret.push(output);
                }
                Ok((Err(e), pid)) => {
                    pids.remove(&pid);
                    error!("Per-process profiling task failed {count} / {total} (for pid = {pid})");
                    tracing::info!("Empty profiling info for a process: {e}");
                }
                Err(e) => {
                    error!("Per-process profiling task failed {count} / {total}");
                    tracing::error!("Per-process profiling task failed: {e}");
                }
            }

            count += 1;
            error!(
                "Waiting for next profiling task to complete {count} / {total}, remaining pids: {pids:?}"
            );
        }

        error!("Perf script output collected: {count} / {total}");

        ret.into_iter().flatten().collect()
    };

    if collapsed.is_empty() {
        error!("perf script output is empty, no data collected");
        return Err(anyhow!("perf script output is empty, no data collected"));
    }

    // let perf_script_output = futured_unordered
    //     .await
    //     .into_iter()
    //     .collect::<anyhow::Result<Vec<Vec<u8>>>>()
    //     .context("failed to run perf for all processes")?
    //     .into_iter()
    //     .flatten()
    //     .collect::<Vec<_>>();

    // // Step 2: Collapse the stack traces into a folded stack trace
    // let mut folder = {
    //     let mut options = inferno::collapse::perf::Options::default();
    //     options.annotate_jit = true; // Enable JIT annotation if needed
    //     options.annotate_kernel = true; // Enable kernel annotation if needed
    //     options.include_addrs = true; // Include addresses in the output
    //     options.include_pid = true; // Include PIDs in the output
    //     options.include_tid = true; // Include TIDs in the output
    //     inferno::collapse::perf::Folder::from(options)
    // };

    // let mut collapsed = Vec::new();

    // folder
    //     .collapse(perf_script_output.as_slice(), &mut collapsed)
    //     .context("couldn't collapse the output of perf script into the folded format")?;

    // if collapsed.is_empty() {
    //     error!(
    //         "collapsed stack trace is empty for output: {}",
    //         String::from_utf8_lossy(&perf_script_output)
    //     );

    //     return Err(anyhow!("collapsed stack trace is empty"));
    // }

    // Step 3: Generate pprof profile from collapsed stack trace
    generate_pprof_from_collapsed(&collapsed, &options.options.blocklist_symbols, false)
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
    let proc = match procfs::process::Process::new(pid.as_raw()) {
        Ok(proc) => proc,
        Err(_) => {
            return fallback_name.to_owned();
        }
    };

    match proc.exe() {
        Ok(path) => path.to_string_lossy().into_owned(),
        Err(_) => fallback_name.into(),
    }
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
