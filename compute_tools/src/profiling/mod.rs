use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Cursor, Write},
    ops::{Deref, DerefMut},
    os::fd::{AsRawFd, FromRawFd},
    path::{Path, PathBuf},
};

use anyhow::{Context, anyhow};
use flate2::{Compression, write::GzEncoder};
use inferno::collapse::Collapse;
use nix::{libc::pid_t, unistd::Pid};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tracing::{error, instrument};

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

fn get_command_invocation(cmd: &tokio::process::Command) -> String {
    format!(
        "{} {}",
        cmd.as_std().get_program().to_string_lossy(),
        cmd.as_std()
            .get_args()
            .map(|s| s.to_string_lossy().to_string())
            .collect::<Vec<String>>()
            .join(" "),
    )
}

/// The options for generating a pprof profile using the `perf` tool.
#[derive(Debug)]
pub struct ProfileGenerationOptions<'a, S: AsRef<str>> {
    /// If set to `true`, the `perf` command will be run with `sudo`.
    /// This is useful for profiling processes that require elevated
    /// privileges.
    /// If `false`, the command will be run without `sudo`.
    pub run_with_sudo: bool,
    /// The path to the `perf` binary. If `None`, it defaults to "perf",
    /// so it will look for `perf` in the system's `PATH`.
    pub perf_binary_path: Option<&'a Path>,
    /// The PID of the process to profile. This can be the main process
    /// or a child process. For targeting postgres, this should be the
    /// PID of the main postgres process.
    pub process_pid: Pid,
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
    pub blocklist_symbols: &'a [S],
    /// The duration for which the profiling should run.
    /// This is the maximum time the profiling will run before it is
    /// stopped. If the profiling is not stopped manually, it will run
    /// for this duration.
    pub timeout: std::time::Duration,
    /// An optional channel receiver that can be used to signal the
    /// profiling to stop early. If provided, the profiling will stop
    /// when a message is received on this channel or when the timeout
    /// is reached, whichever comes first.
    pub should_stop: Option<tokio::sync::oneshot::Receiver<()>>,
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
#[allow(unsafe_code)]
#[instrument(skip(options), fields(perf_record_cmd, perf_script_cmd))]
pub async fn generate_pprof_using_perf<S: AsRef<str>>(
    options: ProfileGenerationOptions<'_, S>,
) -> anyhow::Result<PprofData> {
    let perf_binary_path = options
        .perf_binary_path
        .map_or_else(|| PathBuf::from("perf"), |p| p.to_owned());

    check_perf_runs(
        perf_binary_path
            .to_str()
            .context("couldn't reconstruct perf binary path as string.")?,
        options.run_with_sudo,
    )
    .context("couldn't check that perf is available and can be run")?;

    let pids = list_children_processes(options.process_pid)
        .unwrap_or_default()
        .into_iter()
        .chain(std::iter::once(options.process_pid))
        .map(|pid| pid.to_string())
        .collect::<Vec<String>>()
        .join(",");

    let temp_file = tempfile::NamedTempFile::new()
        .context("failed to create temporary file for perf output")?;
    let temp_file_path = temp_file.path();

    // Step 1: Run perf to collect stack traces
    let mut perf_record_command = if options.run_with_sudo {
        let mut cmd = tokio::process::Command::new(SUDO_PATH);
        cmd.arg(&perf_binary_path);
        cmd
    } else {
        tokio::process::Command::new(&perf_binary_path)
    };

    let (read_ctl_fd, write_ctl_fd) =
        nix::unistd::pipe().context("couldn't create the pipe for controlling perf record")?;

    let (read_ack_fd, write_ack_fd) =
        nix::unistd::pipe().context("couldn't create the pipe for perf record acknowledgment")?;

    let mut perf_record_command = perf_record_command
        .arg("record")
        // Target the specified process IDs.
        .arg("-p")
        .arg(pids)
        // Specify the sampling frequency or default to 99 Hz.
        .arg("-F")
        .arg(options.sampling_frequency.to_string())
        // Enable call-graph (stack chain/backtrace) recording for both
        // kernel space and user space.
        .arg("-g")
        // Disable buffering to ensure that the output is written
        // immediately to the temporary file.
        // This is important for real-time profiling and ensures that
        // we get the most accurate stack traces and that after we "ask"
        // perf to stop, we get the data dumped to the file properly.
        .arg("--no-buffering")
        .arg("--control")
        // .arg(format!("fd:{}", read_ctl_fd.as_raw_fd(),));
        .arg(format!(
            "fd:{},{}",
            read_ctl_fd.as_raw_fd(),
            write_ack_fd.as_raw_fd()
        ));

    if options.follow_forks {
        perf_record_command = perf_record_command.arg("--inherit");
    }

    // Make it write to stdout instead of a file.
    let perf_record_command = perf_record_command
        .arg("-o")
        .arg(temp_file_path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());

    tracing::debug!("Running perf record command: {perf_record_command:?}");

    let perf_record_cmd_str = get_command_invocation(perf_record_command);
    tracing::Span::current().record("perf_record_cmd", perf_record_cmd_str.clone());

    let mut perf_record_command = perf_record_command
        .spawn()
        .context("failed to spawn a process for perf record")?;

    drop(read_ctl_fd);
    drop(write_ack_fd);

    // nix::unistd::close(read_ctl_fd).context("failed to close the read end of the control pipe")?;
    // nix::unistd::close(write_ack_fd)
    //     .context("failed to close the write end of the acknowledgment pipe")?;

    if let Some(mut rx) = options.should_stop {
        tokio::select! {
            _ = &mut rx => {
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

    error!("Asking perf record to stop via write_ctl_fd");
    // nix::unistd::write(&write_ctl_fd, b"stop\n").context("COULDNT WRITE STOP TO WRITE CTL FD")?; // This will tell perf to stop
    // SAFETY: We own the file descriptor `write_ctl_fd` and are converting it into a tokio::fs::File for async writing.
    // This is safe because no other code will use this fd after this point.
    let mut ctl_writer = unsafe { tokio::fs::File::from_raw_fd(write_ctl_fd.as_raw_fd()) };
    if let Err(e) = ctl_writer.write_all(b"stop\n").await {
        error!("failed to write stop command to perf record: {}", e);
    }
    error!("Wrote stop to write_ctl_fd, now waiting for acknowledgment");
    // let mut bytes = [0; 4096];
    // if let Err(e) = nix::unistd::read(&read_ack_fd, &mut bytes) {
    //     error!("failed to read acknowledgment from perf record: {}", e);
    // }
    // SAFETY: We own the file descriptor `read_ack_fd` and are converting it into a std::fs::File for blocking reading.
    // This is safe because no other code will use this fd after this point.
    // let ack_file = unsafe { std::fs::File::from_raw_fd(read_ack_fd.as_raw_fd()) };
    // let mut buf = String::new();
    // use std::io::BufRead;
    // let mut br = std::io::BufReader::new(ack_file);
    let ack_file = unsafe { tokio::fs::File::from_raw_fd(read_ack_fd.as_raw_fd()) };
    let mut br = tokio::io::BufReader::new(ack_file);
    let mut buf = String::new();
    loop {
        // Read the acknowledgment line from perf record.
        // This is a blocking read, so it will wait until perf record sends the acknowledgment.
        match br.read_line(&mut buf).await {
            Ok(n) => {
                if n > 0 {
                    // EOF reached, no more data to read.
                    break;
                } else {
                    // No data read, continue reading.
                    continue;
                }
            }
            Err(e) => {
                // An error occurred while reading the acknowledgment.
                // Log the error and break the loop.
                error!("failed to read acknowledgment from perf record: {e}");
                break;
            }
        }
        // if !buf.is_empty() {
        //     break;
        // }
    }

    error!("Got ACK from perf record({}): {buf}", buf.len());

    // drop(write_ctl_fd); // Close the write end of the control pipe
    // drop(read_ctl_fd); // Close the read end of the control pipe
    // drop(write_ack_fd); // Close the write end of the acknowledgment pipe
    // drop(read_ack_fd); // Close the read end of the acknowledgment pipe

    // // Send SIGUSR2 to the perf record process to stop it writing to
    // // the output file gracefully.
    // let _ = nix::sys::signal::kill(
    //     nix::unistd::Pid::from_raw(perf_record_pid as i32),
    //     nix::sys::signal::Signal::SIGUSR2,
    // );

    // // Send SIGINT to the perf record process to stop it
    // // gracefully and wait for it to finish.
    // let _ = nix::sys::signal::kill(
    //     nix::unistd::Pid::from_raw(perf_record_pid as i32),
    //     nix::sys::signal::Signal::SIGINT,
    // );

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
        error!(
            "perf record output file is empty, no data collected.\nPerf stderr: {perf_record_stderr_string}"
        );
        return Err(anyhow!(
            "perf record output file is empty, no data collected"
        ));
    }

    let mut perf_script_cmd = tokio::process::Command::new(&perf_binary_path);
    perf_script_cmd
        .arg("script")
        .arg("-i")
        .arg(temp_file_path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    tracing::Span::current().record(
        "perf_script_cmd",
        get_command_invocation(&perf_script_cmd).to_string(),
    );

    let perf_script_output = perf_script_cmd
        .output()
        .await
        .context(format!("couldn't run perf script -i {temp_file_path:?}"))?;

    if !perf_script_output.status.success() {
        error!(
            "perf script command failed with status: {}, command: {}\nTemp file size: {temp_file_size} bytes\nPerf stderr: {perf_record_stderr_string}",
            perf_script_output.status,
            get_command_invocation(&perf_script_cmd),
        );

        return Err(anyhow!(
            "perf script command failed: {}",
            String::from_utf8(perf_script_output.stderr)
                .unwrap_or_else(|_| "Invalid UTF-8 output".to_string())
        ));
    }

    let perf_script_stdout = perf_script_output.stdout;
    let perf_output_str =
        String::from_utf8(perf_script_stdout).context("expected utf-8 output from perf script")?;
    if perf_output_str.is_empty() {
        return Err(anyhow!(format!("Perf script output is empty.\n")));
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
        .collapse(perf_output_str.as_bytes(), &mut collapsed)
        .context("couldn't collapse the output of perf script into the folded format")?;

    if collapsed.is_empty() {
        return Err(anyhow!("collapsed stack trace is empty"));
    }

    // Step 3: Generate pprof profile from collapsed stack trace
    generate_pprof_from_collapsed(&collapsed, options.blocklist_symbols, false)
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
