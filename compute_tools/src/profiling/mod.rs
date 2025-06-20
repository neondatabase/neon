use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Cursor, Write},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
};

use flate2::{Compression, write::GzEncoder};
use inferno::collapse::Collapse;
use nix::libc::kill;

const SUDO_PATH: &str = "/usr/bin/sudo";

/// Represents the pprof data generated from a profiling tool.
#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct PprofData(pub(crate) Vec<u8>);

impl PprofData {
    /// Dumps the pprof data to a file.
    pub fn write_to_file(
        &self,
        path: &PathBuf,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
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

#[repr(transparent)]
#[derive(Debug, Copy, Clone)]
pub struct Pid(u32);

impl From<u32> for Pid {
    fn from(pid: u32) -> Self {
        Pid(pid)
    }
}

impl Deref for Pid {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Pid {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Returns a list of child processes for a given parent process ID.
fn list_children_processes(parent_pid: Pid) -> Result<Vec<Pid>, Box<dyn std::error::Error>> {
    Ok(procfs::process::all_processes()?
        .filter_map(|proc| {
            if let Ok(stat) = proc.ok()?.stat() {
                if stat.ppid as u32 == parent_pid.0 {
                    return Some(Pid(stat.pid as u32));
                }
            }
            None
        })
        .collect())
}

#[derive(Debug)]
pub struct ProfileGenerationOptions<'a, S: AsRef<str>> {
    pub run_with_sudo: bool,
    pub perf_binary_path: Option<&'a Path>,
    pub process_pid: Pid,
    pub follow_forks: bool,
    pub sampling_frequency: Option<u32>,
    pub blocklist_symbols: &'a [S],
    pub timeout: std::time::Duration,
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
) -> std::result::Result<PprofData, Box<dyn std::error::Error>> {
    let pids = list_children_processes(options.process_pid)
        .unwrap_or_default()
        .into_iter()
        .chain(std::iter::once(options.process_pid))
        .map(|pid| pid.0.to_string())
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
        return Err(Box::new(std::io::Error::other(
            "perf script command failed",
        )));
    }

    let perf_script_output = perf_script_output.stdout;
    let perf_output_str = String::from_utf8(perf_script_output)?;
    if perf_output_str.is_empty() {
        return Err(Box::new(std::io::Error::other(
            "perf script output is empty",
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
        .collapse(perf_output_str.as_bytes(), &mut collapsed)
        .map_err(|e| Box::new(std::io::Error::other(e)))?;

    if collapsed.is_empty() {
        return Err(Box::new(std::io::Error::other(
            "collapsed stack trace is empty",
        )));
    }

    // Step 3: Generate pprof profile from collapsed stack trace
    let pprof_data = generate_pprof_from_collapsed(&collapsed, options.blocklist_symbols, false)?;

    Ok(pprof_data)
}

pub fn archive_bytes<W: Write>(
    bytes: &[u8],
    output: &mut W,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut encoder = GzEncoder::new(output, Compression::default());

    encoder.write_all(bytes)?;
    encoder.finish()?;

    Ok(())
}

/// Returns the executable path for a given process ID.
fn get_exe_from_pid(pid: Pid, fallback_name: &str) -> String {
    let proc = match procfs::process::Process::new(pid.0 as i32) {
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
) -> Result<PprofData, Box<dyn std::error::Error>> {
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
                let exe = get_exe_from_pid(Pid(pid.parse::<u32>().unwrap_or(0)), "unknown");
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

// perf_data_to_pprof::generate_pprof_using_perf(
//     None,
//     args.target_pid.into(),
//     true,
//     None,
//     std::time::Duration::from_secs(args.timeout_seconds.unwrap_or(5)),
//     Some(rx),
// )?
// .write_to_file(&args.pprof_output)?;
