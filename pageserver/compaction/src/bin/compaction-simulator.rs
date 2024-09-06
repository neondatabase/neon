use clap::{Parser, Subcommand};
use pageserver_compaction::helpers::PAGE_SZ;
use pageserver_compaction::simulator::MockTimeline;
use rand::Rng;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use utils::project_git_version;

project_git_version!(GIT_VERSION);

#[derive(Parser)]
#[command(
    version = GIT_VERSION,
    about = "Neon Pageserver compaction simulator",
    long_about = "A developer tool to visualize and test compaction"
)]
#[command(propagate_version = true)]
struct CliOpts {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    RunSuite,
    Simulate(SimulateCmd),
}

#[derive(Clone, clap::ValueEnum)]
enum Distribution {
    Uniform,
    HotCold,
}

/// Read and update pageserver metadata file
#[derive(Parser)]
struct SimulateCmd {
    distribution: Distribution,

    /// Number of records to digest
    num_records: u64,
    /// Record length
    record_len: u64,

    // Logical database size in MB
    logical_size: u64,
}

async fn simulate(cmd: &SimulateCmd, results_path: &Path) -> anyhow::Result<()> {
    let mut executor = MockTimeline::new();

    // Convert the logical size in MB into a key range.
    let key_range = 0..((cmd.logical_size * 1024 * 1024) / PAGE_SZ);
    //let key_range = u64::MIN..u64::MAX;
    println!(
        "starting simulation with key range {:016X}-{:016X}",
        key_range.start, key_range.end
    );

    // helper function to print progress indicator
    let print_progress = |i| -> anyhow::Result<()> {
        if i == 0 || (i + 1) % 10000 == 0 || i == cmd.num_records - 1 {
            print!(
                "\ringested {} / {} records, {} MiB / {} MiB...",
                i + 1,
                cmd.num_records,
                (i + 1) * cmd.record_len / (1_000_000),
                cmd.num_records * cmd.record_len / (1_000_000),
            );
            std::io::stdout().flush()?;
        }
        Ok(())
    };

    match cmd.distribution {
        Distribution::Uniform => {
            for i in 0..cmd.num_records {
                executor.ingest_uniform(1, cmd.record_len, &key_range)?;
                executor.compact_if_needed().await?;

                print_progress(i)?;
            }
        }
        Distribution::HotCold => {
            let splitpoint = key_range.start + (key_range.end - key_range.start) / 10;
            let hot_key_range = 0..splitpoint;
            let cold_key_range = splitpoint..key_range.end;

            for i in 0..cmd.num_records {
                let chosen_range = if rand::thread_rng().gen_bool(0.9) {
                    &hot_key_range
                } else {
                    &cold_key_range
                };
                executor.ingest_uniform(1, cmd.record_len, chosen_range)?;
                executor.compact_if_needed().await?;

                print_progress(i)?;
            }
        }
    }
    println!("done!");
    executor.flush_l0();
    executor.compact_if_needed().await?;
    let stats = executor.stats()?;

    // Print the stats to stdout, and also to a file
    print!("{stats}");
    std::fs::write(results_path.join("stats.txt"), stats)?;

    let animation_path = results_path.join("compaction-animation.html");
    executor.draw_history(std::fs::File::create(&animation_path)?)?;
    println!(
        "animation: file://{}",
        animation_path.canonicalize()?.display()
    );

    Ok(())
}

async fn run_suite_cmd(results_path: &Path, workload: &SimulateCmd) -> anyhow::Result<()> {
    std::fs::create_dir(results_path)?;

    set_log_file(File::create(results_path.join("log"))?);
    let result = simulate(workload, results_path).await;
    set_log_stdout();
    result
}

async fn run_suite() -> anyhow::Result<()> {
    let top_results_path = PathBuf::from(format!(
        "compaction-suite-results.{}",
        std::time::SystemTime::UNIX_EPOCH.elapsed()?.as_secs()
    ));
    std::fs::create_dir(&top_results_path)?;

    let workload = SimulateCmd {
        distribution: Distribution::Uniform,
        // Generate 20 GB of WAL
        record_len: 1_000,
        num_records: 20_000_000,
        // Logical size 5 GB
        logical_size: 5_000,
    };

    run_suite_cmd(&top_results_path.join("uniform-20GB-5GB"), &workload).await?;

    println!(
        "All tests finished. Results in {}",
        top_results_path.display()
    );
    Ok(())
}

use std::fs::File;
use std::io::Stdout;
use std::sync::Mutex;
use tracing_subscriber::fmt::writer::EitherWriter;
use tracing_subscriber::fmt::MakeWriter;

static LOG_FILE: OnceLock<Mutex<EitherWriter<File, Stdout>>> = OnceLock::new();
fn get_log_output() -> &'static Mutex<EitherWriter<File, Stdout>> {
    LOG_FILE.get_or_init(|| std::sync::Mutex::new(EitherWriter::B(std::io::stdout())))
}

fn set_log_file(f: File) {
    *get_log_output().lock().unwrap() = EitherWriter::A(f);
}

fn set_log_stdout() {
    *get_log_output().lock().unwrap() = EitherWriter::B(std::io::stdout());
}

fn init_logging() -> anyhow::Result<()> {
    // We fall back to printing all spans at info-level or above if
    // the RUST_LOG environment variable is not set.
    let rust_log_env_filter = || {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
    };

    // NB: the order of the with() calls does not matter.
    // See https://docs.rs/tracing-subscriber/0.3.16/tracing_subscriber/layer/index.html#per-layer-filtering
    use tracing_subscriber::prelude::*;
    tracing_subscriber::registry()
        .with({
            let log_layer = tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_ansi(false)
                .with_writer(|| get_log_output().make_writer());
            log_layer.with_filter(rust_log_env_filter())
        })
        .init();

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = CliOpts::parse();

    init_logging()?;

    match cli.command {
        Commands::Simulate(cmd) => {
            simulate(&cmd, &PathBuf::from("/tmp/compactions.html")).await?;
        }
        Commands::RunSuite => {
            run_suite().await?;
        }
    };
    Ok(())
}
