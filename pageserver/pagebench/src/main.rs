use clap::Parser;
use utils::logging;

/// Re-usable pieces of code that aren't CLI-specific.
mod util {
    pub(crate) mod connstring;
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
    pub(super) mod basebackup;
    pub(super) mod getpage_latest_lsn;
    pub(super) mod trigger_initial_size_calculation;
}

/// Component-level performance test for pageserver.
#[derive(clap::Parser)]
enum Args {
    Basebackup(cmd::basebackup::Args),
    GetPageLatestLsn(cmd::getpage_latest_lsn::Args),
    TriggerInitialSizeCalculation(cmd::trigger_initial_size_calculation::Args),
}

fn main() {
    logging::init(
        logging::LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stderr,
    )
    .unwrap();
    logging::replace_panic_hook_with_tracing_panic_hook().forget();

    let args = Args::parse();
    match args {
        Args::Basebackup(args) => cmd::basebackup::main(args),
        Args::GetPageLatestLsn(args) => cmd::getpage_latest_lsn::main(args),
        Args::TriggerInitialSizeCalculation(args) => {
            cmd::trigger_initial_size_calculation::main(args)
        }
    }
    .unwrap()
}
