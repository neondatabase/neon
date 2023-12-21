use clap::Parser;
use utils::logging;

// libraries
mod cli {
    pub(crate) mod targets;
}
mod util {
    pub(crate) mod connstring;
    pub(crate) mod request_stats;
    #[macro_use]
    pub(crate) mod tokio_thread_local_stats;
}

// sub commands
mod basebackup;
mod getpage_latest_lsn;
mod trigger_initial_size_calculation;

/// Component-level performance test for pageserver.
#[derive(clap::Parser)]
enum Args {
    Basebackup(basebackup::Args),
    GetPageLatestLsn(getpage_latest_lsn::Args),
    TriggerInitialSizeCalculation(trigger_initial_size_calculation::Args),
}

fn main() {
    logging::init(
        logging::LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stderr,
    )
    .unwrap();

    let args = Args::parse();
    match args {
        Args::Basebackup(args) => basebackup::main(args),
        Args::GetPageLatestLsn(args) => getpage_latest_lsn::main(args),
        Args::TriggerInitialSizeCalculation(args) => trigger_initial_size_calculation::main(args),
    }
    .unwrap()
}
