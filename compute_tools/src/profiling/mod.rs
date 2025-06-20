use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Cursor, Write},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
};

use anyhow::anyhow;
use flate2::{Compression, write::GzEncoder};
use inferno::collapse::Collapse;
use nix::{
    libc::{kill, pid_t},
    unistd::Pid,
};

const SUDO_PATH: &str = "/usr/bin/sudo";

/// Represents the pprof data generated from a profiling tool.
#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct PprofData(pub(crate) Vec<u8>);

impl PprofData {
    /// Dumps the pprof data to a file.
    pub fn write_to_file(&self, path: &PathBuf) -> anyhow::Result<()> {
        let mut file = std::fs::File::create(path)?;
        file.write_all(&self.0)?;
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
    Ok(procfs::process::all_processes()?
        .filter_map(|proc| {
            if let Ok(stat) = proc.ok()?.stat() {
                if stat.ppid == parent_pid.as_raw() {
                    return Some(Pid::from_raw(stat.pid));
                }
            }
            None
        })
        .collect())
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
    /// The sampling frequency in Hz. If `None`, it defaults to 99 Hz.
    /// This is the frequency at which stack traces will be sampled.
    pub sampling_frequency: Option<u32>,
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
    pub should_stop: Option<crossbeam_channel::Receiver<()>>,
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
pub fn generate_pprof_using_perf<S: AsRef<str>>(
    options: ProfileGenerationOptions<S>,
) -> anyhow::Result<PprofData> {
    let pids = list_children_processes(options.process_pid)
        .unwrap_or_default()
        .into_iter()
        .chain(std::iter::once(options.process_pid))
        .map(|pid| pid.to_string())
        .collect::<Vec<String>>()
        .join(",");

    // Step 1: Run perf to collect stack traces
    let perf_binary_path = options
        .perf_binary_path
        .map_or_else(|| PathBuf::from("perf"), |p| p.to_owned());

    let mut perf_record_command = if options.run_with_sudo {
        std::process::Command::new(SUDO_PATH)
    } else {
        std::process::Command::new(&perf_binary_path)
    };

    let perf_record_command = if options.run_with_sudo {
        perf_record_command.arg(&perf_binary_path)
    } else {
        perf_record_command.arg("record")
    };

    let mut perf_record_command = perf_record_command
        // Target the specified process IDs.
        .arg("-p")
        .arg(pids)
        // Specify the sampling frequency or default to 99 Hz.
        .arg("-F")
        .arg(options.sampling_frequency.unwrap_or(99).to_string())
        // Enable call-graph (stack chain/backtrace) recording for both
        // kernel space and user space.
        .arg("-g");

    if options.follow_forks {
        perf_record_command = perf_record_command.arg("--inherit");
    }

    // Make it write to stdout instead of a file.
    let perf_record_command = perf_record_command.arg("-o").arg("-");

    let mut perf_record_command = perf_record_command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let perf_record_pid = perf_record_command.id();

    let perf_script_command = std::process::Command::new(&perf_binary_path)
        .arg("script")
        .arg("-i")
        .arg("-")
        .stdin(
            perf_record_command
                .stdout
                .take()
                .expect("Failed to capture perf output"),
        )
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    std::thread::spawn(move || {
        use crossbeam_channel::{after, select};
        use nix::sys::signal::Signal;

        if let Some(rx) = options.should_stop {
            select! {
                recv(rx) -> _ => {
                    println!("Received shutdown signal, stopping perf...");
                }
                recv(after(options.timeout)) -> _ => {
                    println!("Timeout reached, stopping perf...");
                }
            }
        } else {
            std::thread::sleep(options.timeout);
        }

        // SAFETY:
        // unsafe because of the use of `kill` to send a signal to the
        // process.
        unsafe {
            let _ = kill(perf_record_pid as i32, Signal::SIGINT as i32);
        }
    });

    let perf_script_output = perf_script_command.wait_with_output()?;
    if !perf_script_output.status.success() {
        return Err(anyhow!("perf script command failed"));
    }

    let perf_script_output = perf_script_output.stdout;
    let perf_output_str = String::from_utf8(perf_script_output)?;
    if perf_output_str.is_empty() {
        return Err(anyhow!("perf script output is empty"));
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
        .map_err(|e| Box::new(std::io::Error::other(e)))?;

    if collapsed.is_empty() {
        return Err(anyhow!("collapsed stack trace is empty"));
    }

    // Step 3: Generate pprof profile from collapsed stack trace
    generate_pprof_from_collapsed(&collapsed, options.blocklist_symbols, false)
}

fn archive_bytes<W: Write>(bytes: &[u8], output: &mut W) -> anyhow::Result<()> {
    let mut encoder = GzEncoder::new(output, Compression::default());

    encoder.write_all(bytes)?;
    encoder.finish()?;

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
        let line = line?;
        let Some((stack_str, count_str)) = line.rsplit_once(' ') else {
            continue;
        };
        let count: i64 = count_str.trim().parse()?;

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
    profile.write_to_vec(&mut protobuf_encoded)?;

    Ok(PprofData(if archive {
        let mut archived = Vec::new();
        archive_bytes(&protobuf_encoded, &mut archived)?;
        archived
    } else {
        protobuf_encoded
    }))
}
