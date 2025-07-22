use std::fs::File;

use clap::Parser;
use tracing::info;
use utils::logging;

/// Re-usable pieces of code that aren't CLI-specific.
mod util {
    pub(crate) mod request_stats;
    #[macro_use]
    pub(crate) mod tokio_thread_local_stats;
    /// Re-usable pieces of CLI-specific code.
    pub(crate) mod cli {
        pub(crate) mod targets;
    }
}

/// The pagebench CLI sub-commands, dispatched in [`main`] below.
mod cmd {
    pub(super) mod aux_files;
    pub(super) mod basebackup;
    pub(super) mod getpage_latest_lsn;
    pub(super) mod idle_streams;
    pub(super) mod ondemand_download_churn;
    pub(super) mod trigger_initial_size_calculation;
}

/// Component-level performance test for pageserver.
#[derive(clap::Parser)]
struct Args {
    /// Takes a client CPU profile into profile.svg. The benchmark must exit cleanly before it's
    /// written, e.g. via --runtime.
    #[arg(long)]
    profile: bool,

    #[command(subcommand)]
    subcommand: Subcommand,
}

#[derive(clap::Subcommand)]
enum Subcommand {
    Basebackup(cmd::basebackup::Args),
    GetPageLatestLsn(cmd::getpage_latest_lsn::Args),
    TriggerInitialSizeCalculation(cmd::trigger_initial_size_calculation::Args),
    OndemandDownloadChurn(cmd::ondemand_download_churn::Args),
    AuxFiles(cmd::aux_files::Args),
    IdleStreams(cmd::idle_streams::Args),
}

fn main() -> anyhow::Result<()> {
    logging::init(
        logging::LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stderr,
    )?;
    logging::replace_panic_hook_with_tracing_panic_hook().forget();

    let args = Args::parse();

    // Start a CPU profile if requested.
    let mut profiler = None;
    if args.profile {
        profiler = Some(
            pprof::ProfilerGuardBuilder::default()
                .frequency(1000)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()?,
        );
    }

    match args.subcommand {
        Subcommand::Basebackup(args) => cmd::basebackup::main(args),
        Subcommand::GetPageLatestLsn(args) => cmd::getpage_latest_lsn::main(args),
        Subcommand::TriggerInitialSizeCalculation(args) => {
            cmd::trigger_initial_size_calculation::main(args)
        }
        Subcommand::OndemandDownloadChurn(args) => cmd::ondemand_download_churn::main(args),
        Subcommand::AuxFiles(args) => cmd::aux_files::main(args),
        Subcommand::IdleStreams(args) => cmd::idle_streams::main(args),
    }?;

    // Generate a CPU flamegraph if requested.
    if let Some(profiler) = profiler {
        let report = profiler.report().build()?;
        drop(profiler); // stop profiling
        let file = File::create("profile.svg")?;
        report.flamegraph(file)?;
        info!("wrote CPU profile flamegraph to profile.svg")
    }

    Ok(())
}
